import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDestinationOverrides, alertDestinationSlaOverrides, alertDeliveries } from "@shared/schema";
import { DEFAULT_NOISE_BUDGETS, listAlertNoiseBudgets } from "./alertNoiseBudgetService";
import { getSlaThresholds } from "./alertSlaService";

type DestinationType = "WEBHOOK" | "EMAIL";
type NoiseWindow = "24h" | "7d";
type SlaWindow = "24h" | "7d";

export type DestinationOverrideRow = {
  tenant_id: string;
  destination_key: string;
  destination_type: DestinationType;
  noise_budget_enabled: boolean;
  noise_budget_window_minutes: number | null;
  noise_budget_max_deliveries: number | null;
  sla_enabled: boolean;
  updated_at: Date;
  updated_by_user_id: string | null;
  updated_by_key_id: string | null;
};

export type DestinationSlaOverride = {
  window: SlaWindow;
  p95_ms: number | null;
  success_rate_min_pct: number | null;
  updated_at: Date;
};

export type ResolvedNoiseBudget = {
  enabled: boolean;
  allowed: boolean;
  source: "DESTINATION" | "TENANT_DEFAULT";
  window: string | null;
  window_minutes: number | null;
  max_deliveries: number | null;
  used_in_window: number;
  window_start: Date | null;
};

export type ResolvedSlaThreshold = {
  enabled: boolean;
  source: "DESTINATION" | "TENANT_DEFAULT";
  p95_ms: number;
  success_rate_min_pct: number;
};

const NOISE_WINDOW_MS: Record<NoiseWindow, number> = {
  "24h": 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
};

const roundToMinute = (value: Date) =>
  new Date(Math.floor(value.getTime() / 60000) * 60000);

const windowLabel = (minutes: number) => {
  if (minutes === 24 * 60) return "24h";
  if (minutes === 7 * 24 * 60) return "7d";
  return `${minutes}m`;
};

const clampInt = (value: unknown, min: number, max: number, label: string) => {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  if (!Number.isFinite(num) || !Number.isInteger(num)) {
    throw new Error(`${label} must be an integer`);
  }
  if (num < min || num > max) {
    throw new Error(`${label} must be between ${min} and ${max}`);
  }
  return num;
};

const toNumber = (value: unknown, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const countDeliveriesInWindow = async (args: {
  tenantId: string;
  destinationType: DestinationType;
  destination?: string | null;
  destinationScoped: boolean;
  windowStart: Date;
}) => {
  const conditions = [
    eq(alertDeliveries.tenantId, args.tenantId),
    eq(alertDeliveries.destinationType, args.destinationType),
    sql`${alertDeliveries.createdAt} >= ${args.windowStart}`,
    sql`${alertDeliveries.status} not like 'SKIPPED%'`,
  ];
  if (args.destinationScoped && args.destination) {
    conditions.push(eq(alertDeliveries.endpoint, args.destination));
  }
  const [row] = await db
    .select({ count: sql<number>`count(*)` })
    .from(alertDeliveries)
    .where(and(...conditions));
  return Number(row?.count ?? 0);
};

export async function getDestinationOverrides(tenantId: string, destinationKey: string) {
  const [row] = await db
    .select()
    .from(alertDestinationOverrides)
    .where(and(
      eq(alertDestinationOverrides.tenantId, tenantId),
      eq(alertDestinationOverrides.destinationKey, destinationKey),
    ))
    .limit(1);

  if (!row) return null;

  const slaRows = await db
    .select()
    .from(alertDestinationSlaOverrides)
    .where(and(
      eq(alertDestinationSlaOverrides.tenantId, tenantId),
      eq(alertDestinationSlaOverrides.destinationKey, destinationKey),
    ));

  return {
    row,
    sla_overrides: slaRows,
  };
}

export async function resolveNoiseBudget(args: {
  tenantId: string;
  destinationType: DestinationType;
  destinationKey: string;
  destination?: string | null;
  now?: Date;
}): Promise<ResolvedNoiseBudget> {
  const now = args.now ?? new Date();
  const nowRounded = roundToMinute(now);

  const [overrideRow] = await db
    .select()
    .from(alertDestinationOverrides)
    .where(and(
      eq(alertDestinationOverrides.tenantId, args.tenantId),
      eq(alertDestinationOverrides.destinationKey, args.destinationKey),
    ))
    .limit(1);

  const hasNoiseOverride = Boolean(
    overrideRow && (
      overrideRow.noiseBudgetEnabled === false ||
      overrideRow.noiseBudgetMaxDeliveries != null ||
      overrideRow.noiseBudgetWindowMinutes != null
    ),
  );

  if (overrideRow && overrideRow.noiseBudgetEnabled === false) {
    return {
      enabled: false,
      allowed: true,
      source: "DESTINATION",
      window: null,
      window_minutes: null,
      max_deliveries: null,
      used_in_window: 0,
      window_start: null,
    };
  }

  const destinationScoped = hasNoiseOverride;
  if (overrideRow && (overrideRow.noiseBudgetMaxDeliveries != null || overrideRow.noiseBudgetWindowMinutes != null)) {
    const windowMinutes = overrideRow.noiseBudgetWindowMinutes ?? 24 * 60;
    const maxDeliveries = overrideRow.noiseBudgetMaxDeliveries ?? DEFAULT_NOISE_BUDGETS["24h"][args.destinationType].max_deliveries;
    const windowStart = new Date(nowRounded.getTime() - windowMinutes * 60 * 1000);
    const count = await countDeliveriesInWindow({
      tenantId: args.tenantId,
      destinationType: args.destinationType,
      destination: args.destination,
      destinationScoped,
      windowStart,
    });
    return {
      enabled: true,
      allowed: count < maxDeliveries,
      source: "DESTINATION",
      window: windowLabel(windowMinutes),
      window_minutes: windowMinutes,
      max_deliveries: maxDeliveries,
      used_in_window: count,
      window_start: windowStart,
    };
  }

  let baselineWindowStart: Date | null = null;
  let baselineMaxDeliveries: number | null = null;
  let baselineCount = 0;

  const windows: NoiseWindow[] = ["24h", "7d"];
  for (const window of windows) {
    const windowStart = new Date(nowRounded.getTime() - NOISE_WINDOW_MS[window]);
    const budgets = await listAlertNoiseBudgets(args.tenantId, window);
    const budget = budgets.items.find((item) => item.destination_type === args.destinationType);
    const maxDeliveries = budget?.max_deliveries ?? DEFAULT_NOISE_BUDGETS[window][args.destinationType].max_deliveries;
    const count = await countDeliveriesInWindow({
      tenantId: args.tenantId,
      destinationType: args.destinationType,
      destination: args.destination,
      destinationScoped,
      windowStart,
    });
    if (window === "24h") {
      baselineWindowStart = windowStart;
      baselineMaxDeliveries = maxDeliveries;
      baselineCount = count;
    }
    if (count >= maxDeliveries) {
      return {
        enabled: true,
        allowed: false,
        source: "TENANT_DEFAULT",
        window,
        window_minutes: window === "24h" ? 24 * 60 : 7 * 24 * 60,
        max_deliveries: maxDeliveries,
        used_in_window: count,
        window_start: windowStart,
      };
    }
  }

  return {
    enabled: true,
    allowed: true,
    source: "TENANT_DEFAULT",
    window: "24h",
    window_minutes: 24 * 60,
    max_deliveries: baselineMaxDeliveries ?? DEFAULT_NOISE_BUDGETS["24h"][args.destinationType].max_deliveries,
    used_in_window: baselineCount,
    window_start: baselineWindowStart,
  };
}

export async function resolveSlaThresholds(args: {
  tenantId: string;
  window: SlaWindow;
  destinationType: DestinationType;
  destinationKey: string;
}): Promise<ResolvedSlaThreshold> {
  const defaults = await getSlaThresholds(args.tenantId, args.window);
  const base = defaults[args.destinationType] ?? defaults.WEBHOOK;
  const [overrideRow] = await db
    .select()
    .from(alertDestinationOverrides)
    .where(and(
      eq(alertDestinationOverrides.tenantId, args.tenantId),
      eq(alertDestinationOverrides.destinationKey, args.destinationKey),
    ))
    .limit(1);

  if (overrideRow && overrideRow.slaEnabled === false) {
    return {
      enabled: false,
      source: "TENANT_DEFAULT",
      p95_ms: base.p95_ms_threshold,
      success_rate_min_pct: Number((base.success_rate_threshold * 100).toFixed(2)),
    };
  }

  const [slaRow] = await db
    .select()
    .from(alertDestinationSlaOverrides)
    .where(and(
      eq(alertDestinationSlaOverrides.tenantId, args.tenantId),
      eq(alertDestinationSlaOverrides.destinationKey, args.destinationKey),
      eq(alertDestinationSlaOverrides.window, args.window),
    ))
    .limit(1);

  const p95Override = slaRow?.p95Ms ?? null;
  const successOverride = slaRow?.successRateMinPct ?? null;

  return {
    enabled: true,
    source: p95Override !== null || successOverride !== null ? "DESTINATION" : "TENANT_DEFAULT",
    p95_ms: p95Override ?? base.p95_ms_threshold,
    success_rate_min_pct: successOverride ?? Number((base.success_rate_threshold * 100).toFixed(2)),
  };
}

export async function upsertDestinationOverrides(args: {
  tenantId: string;
  destinationKey: string;
  destinationType: DestinationType;
  noiseBudget?: { enabled?: boolean | null; window_minutes?: number | null; max_deliveries?: number | null } | null;
  sla?: {
    enabled?: boolean | null;
    "24h"?: { p95_ms?: number | null; success_rate_min_pct?: number | null } | null;
    "7d"?: { p95_ms?: number | null; success_rate_min_pct?: number | null } | null;
  } | null;
  updatedByUserId?: string | null;
  updatedByKeyId?: string | null;
  now?: Date;
}) {
  const now = args.now ?? new Date();
  const noiseBudget = args.noiseBudget ?? {};
  const sla = args.sla ?? {};

  const noiseBudgetEnabled = noiseBudget.enabled ?? true;
  const noiseWindowMinutes = clampInt(noiseBudget.window_minutes, 1, 10080, "noise_budget.window_minutes");
  const noiseMaxDeliveries = clampInt(noiseBudget.max_deliveries, 1, 1_000_000, "noise_budget.max_deliveries");

  const slaEnabled = sla.enabled ?? true;
  const sla24hP95 = clampInt(sla["24h"]?.p95_ms, 1, 3_600_000, "sla.24h.p95_ms");
  const sla24hSuccess = clampInt(sla["24h"]?.success_rate_min_pct, 0, 100, "sla.24h.success_rate_min_pct");
  const sla7dP95 = clampInt(sla["7d"]?.p95_ms, 1, 3_600_000, "sla.7d.p95_ms");
  const sla7dSuccess = clampInt(sla["7d"]?.success_rate_min_pct, 0, 100, "sla.7d.success_rate_min_pct");

  await db
    .insert(alertDestinationOverrides)
    .values({
      tenantId: args.tenantId,
      destinationKey: args.destinationKey,
      destinationType: args.destinationType,
      noiseBudgetEnabled,
      noiseBudgetWindowMinutes: noiseWindowMinutes,
      noiseBudgetMaxDeliveries: noiseMaxDeliveries,
      slaEnabled,
      updatedAt: now,
      updatedByUserId: args.updatedByUserId ?? null,
      updatedByKeyId: args.updatedByKeyId ?? null,
    })
    .onConflictDoUpdate({
      target: [alertDestinationOverrides.tenantId, alertDestinationOverrides.destinationKey],
      set: {
        destinationType: args.destinationType,
        noiseBudgetEnabled,
        noiseBudgetWindowMinutes: noiseWindowMinutes,
        noiseBudgetMaxDeliveries: noiseMaxDeliveries,
        slaEnabled,
        updatedAt: now,
        updatedByUserId: args.updatedByUserId ?? null,
        updatedByKeyId: args.updatedByKeyId ?? null,
      },
    });

  const upsertWindow = async (window: SlaWindow, p95Ms: number | null, successRateMinPct: number | null) => {
    if (p95Ms === null && successRateMinPct === null) {
      await db.delete(alertDestinationSlaOverrides).where(and(
        eq(alertDestinationSlaOverrides.tenantId, args.tenantId),
        eq(alertDestinationSlaOverrides.destinationKey, args.destinationKey),
        eq(alertDestinationSlaOverrides.window, window),
      ));
      return;
    }
    await db
      .insert(alertDestinationSlaOverrides)
      .values({
        tenantId: args.tenantId,
        destinationKey: args.destinationKey,
        window,
        p95Ms,
        successRateMinPct,
        updatedAt: now,
        updatedByUserId: args.updatedByUserId ?? null,
        updatedByKeyId: args.updatedByKeyId ?? null,
      })
      .onConflictDoUpdate({
        target: [alertDestinationSlaOverrides.tenantId, alertDestinationSlaOverrides.destinationKey, alertDestinationSlaOverrides.window],
        set: {
          p95Ms,
          successRateMinPct,
          updatedAt: now,
          updatedByUserId: args.updatedByUserId ?? null,
          updatedByKeyId: args.updatedByKeyId ?? null,
        },
      });
  };

  await upsertWindow("24h", sla24hP95, sla24hSuccess);
  await upsertWindow("7d", sla7dP95, sla7dSuccess);

  return getDestinationOverrides(args.tenantId, args.destinationKey);
}
