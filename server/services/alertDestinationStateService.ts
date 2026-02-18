import { and, desc, eq, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries, alertDeliverySlaWindows, alertDestinationStates, alertEndpointHealth, alertSubscriptions } from "@shared/schema";
import { makeDestinationKey } from "./destinationKey";
import { writeAuditEvent } from "./auditLog";
import type { AuditContext } from "../middleware/requestContext";
import { dispatchDestinationStateSystemAlert } from "./alertDispatcher";
import { resolveSlaThresholds } from "./alertDestinationOverridesService";

export type DestinationState = "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
export type EndpointWindow = "1h" | "24h";
export type DestinationStateChangeRole = "OWNER" | "OPERATOR" | "VIEWER" | string;

export const canTransitionDestinationState = (role: DestinationStateChangeRole, state: DestinationState) => {
  const normalizedRole = String(role ?? "").toUpperCase();
  const normalizedState = String(state ?? "").toUpperCase() as DestinationState;
  if (!["OWNER", "OPERATOR"].includes(normalizedRole)) return false;
  if (normalizedState === "DISABLED" && normalizedRole !== "OWNER") return false;
  return true;
};

const AUTO_PAUSE_CONSECUTIVE = () => {
  const value = Number(process.env.ENDPOINT_DOWN_AUTO_PAUSE_CONSECUTIVE ?? 3);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : 3;
};
const RESUME_READY_CONSECUTIVE = () => {
  const value = Number(process.env.ENDPOINT_OK_RESUME_READY_CONSECUTIVE ?? 2);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : 2;
};
const AUTO_RESUME_MIN_DOWN_MINUTES = () => {
  const value = Number(process.env.ENDPOINT_AUTO_RESUME_MIN_DOWN_MINUTES ?? 10);
  return Number.isFinite(value) && value >= 0 ? Math.floor(value) : 10;
};
const AUTO_RESUME_COOLDOWN_MINUTES = () => {
  const value = Number(process.env.ENDPOINT_AUTO_RESUME_COOLDOWN_MINUTES ?? 10);
  return Number.isFinite(value) && value >= 0 ? Math.floor(value) : 10;
};
const AUTO_RESUME_P95_MS = (destinationType: string) => {
  const type = String(destinationType ?? "").toUpperCase();
  const envName = type === "EMAIL"
    ? "ENDPOINT_AUTO_RESUME_P95_MS_EMAIL"
    : "ENDPOINT_AUTO_RESUME_P95_MS_WEBHOOK";
  const fallback = type === "EMAIL" ? 60000 : 5000;
  const value = Number(process.env[envName] ?? fallback);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
};

export const getDestinationState = async (args: {
  tenantId: string;
  destinationType: string;
  destinationKey: string;
}) => {
  const [row] = await db
    .select()
    .from(alertDestinationStates)
    .where(and(
      eq(alertDestinationStates.tenantId, args.tenantId),
      eq(alertDestinationStates.destinationType, args.destinationType),
      eq(alertDestinationStates.destinationKey, args.destinationKey),
    ))
    .limit(1);
  if (!row) {
    return { state: "ACTIVE" as DestinationState };
  }
  return row;
};

export const upsertDestinationState = async (args: {
  tenantId: string;
  userId: string | null | undefined;
  destinationType: string;
  destinationKey: string;
  state: DestinationState;
  reason?: string | null;
  now?: Date;
}) => {
  const now = args.now ?? new Date();
  const normalizedState = String(args.state ?? "").toUpperCase() as DestinationState;
  const reason = args.reason ?? null;
  const base = {
    tenantId: args.tenantId,
    destinationType: args.destinationType,
    destinationKey: args.destinationKey,
    state: normalizedState,
    reason,
    pausedByUserId: null as string | null,
    pausedAt: null as Date | null,
    autoPausedAt: null as Date | null,
    resumeReadyAt: null as Date | null,
    updatedAt: now,
  };

  if (normalizedState === "PAUSED") {
    base.pausedByUserId = args.userId ?? null;
    base.pausedAt = now;
  } else if (normalizedState === "DISABLED") {
    base.pausedByUserId = args.userId ?? null;
    base.pausedAt = now;
  } else if (normalizedState === "AUTO_PAUSED") {
    base.autoPausedAt = now;
  }

  const [row] = await db
    .insert(alertDestinationStates)
    .values({
      ...base,
      createdAt: now,
    })
    .onConflictDoUpdate({
      target: [
        alertDestinationStates.tenantId,
        alertDestinationStates.destinationType,
        alertDestinationStates.destinationKey,
      ],
      set: base,
    })
    .returning();

  return row ?? null;
};

export const bulkUpdateDestinationStates = async (args: {
  tenantId: string;
  userId: string | null | undefined;
  destinationKeys: string[];
  state: DestinationState;
  reason?: string | null;
  now?: Date;
}) => {
  const now = args.now ?? new Date();
  const keys = args.destinationKeys;
  const stateRows = keys.length
    ? await db
        .select({
          destinationKey: alertDestinationStates.destinationKey,
          destinationType: alertDestinationStates.destinationType,
        })
        .from(alertDestinationStates)
        .where(and(
          eq(alertDestinationStates.tenantId, args.tenantId),
          inArray(alertDestinationStates.destinationKey, keys),
        ))
    : [];

  const subscriptionRows = await db.execute(sql`
    SELECT
      channel AS destination_type,
      endpoint AS destination
    FROM alert_subscriptions
    WHERE tenant_id = ${args.tenantId}
    GROUP BY channel, endpoint
  `);

  const subscriptionList = (subscriptionRows as any).rows ?? subscriptionRows;
  const subscriptionMap = new Map<string, string>();
  for (const row of subscriptionList ?? []) {
    const destinationKey = makeDestinationKey(row.destination_type, row.destination);
    if (!subscriptionMap.has(destinationKey)) {
      subscriptionMap.set(destinationKey, row.destination_type);
    }
  }

  const stateMap = new Map(stateRows.map((row) => [row.destinationKey, row.destinationType]));
  const results: Array<{ destination_key: string; status: string; reason?: string }> = [];

  for (const key of keys) {
    const destinationType = stateMap.get(key) ?? subscriptionMap.get(key);
    if (!destinationType) {
      results.push({ destination_key: key, status: "not_found" });
      continue;
    }
    await upsertDestinationState({
      tenantId: args.tenantId,
      userId: args.userId,
      destinationType,
      destinationKey: key,
      state: args.state,
      reason: args.reason ?? null,
      now,
    });
    results.push({ destination_key: key, status: "ok" });
  }

  return { version: "1", results };
};

export const listDestinations = async (args: {
  tenantId: string;
  window?: EndpointWindow;
  state?: string;
  destinationType?: string;
  q?: string;
  limit?: number;
  cursor?: string | null;
  now?: Date;
  includeOverrides?: boolean;
}) => {
  const now = args.now ?? new Date();
  const window = (args.window ?? "24h") as EndpointWindow;
  const limit = Math.min(Math.max(args.limit ?? 50, 1), 200);
  const stateFilter = args.state ? String(args.state).toUpperCase() : undefined;
  const typeFilter = args.destinationType ? String(args.destinationType).toUpperCase() : undefined;
  const query = args.q ? String(args.q).toLowerCase().trim() : "";

  const decodeCursor = (cursor?: string | null) => {
    if (!cursor) return null;
    try {
      const decoded = Buffer.from(cursor, "base64").toString("utf8");
      const [timestamp, key] = decoded.split("|");
      if (!timestamp || !key) return null;
      const time = new Date(timestamp);
      if (Number.isNaN(time.getTime())) return null;
      return { time, key };
    } catch {
      return null;
    }
  };

  const encodeCursor = (time: Date, key: string) =>
    Buffer.from(`${time.toISOString()}|${key}`).toString("base64");

  const window24Start = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const window1Start = new Date(now.getTime() - 60 * 60 * 1000);

  const [stateRows, subscriptionRows, healthRows] = await Promise.all([
    db
      .select()
      .from(alertDestinationStates)
      .where(eq(alertDestinationStates.tenantId, args.tenantId)),
    db.execute(sql`
      SELECT
        channel AS destination_type,
        endpoint AS destination,
        MAX(updated_at) AS updated_at
      FROM alert_subscriptions
      WHERE tenant_id = ${args.tenantId}
      GROUP BY channel, endpoint
    `),
    db.execute(sql`
      SELECT *
      FROM alert_endpoint_health
      WHERE tenant_id = ${args.tenantId}
        AND "window" in ('1h', '24h')
    `),
  ]);

  const stats24Rows = await db.execute(sql`
    SELECT
      destination_type,
      endpoint,
      COUNT(*) FILTER (WHERE status = 'SENT') AS sent,
      COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
      COUNT(*) FILTER (WHERE status like 'SKIPPED%') AS skipped,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
        FILTER (WHERE status = 'SENT' AND latency_ms IS NOT NULL) AS p95
    FROM alert_deliveries
    WHERE tenant_id = ${args.tenantId}
      AND created_at >= ${window24Start}
    GROUP BY destination_type, endpoint
  `);

  const stats1Rows = await db.execute(sql`
    SELECT
      destination_type,
      endpoint,
      COUNT(*) FILTER (WHERE status = 'SENT') AS sent,
      COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
      COUNT(*) FILTER (WHERE status like 'SKIPPED%') AS skipped,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
        FILTER (WHERE status = 'SENT' AND latency_ms IS NOT NULL) AS p95
    FROM alert_deliveries
    WHERE tenant_id = ${args.tenantId}
      AND created_at >= ${window1Start}
    GROUP BY destination_type, endpoint
  `);

  const lastDeliveryRows = await db.execute(sql`
    SELECT DISTINCT ON (destination_type, endpoint)
      destination_type,
      endpoint,
      status,
      created_at
    FROM alert_deliveries
    WHERE tenant_id = ${args.tenantId}
    ORDER BY destination_type, endpoint, created_at DESC
  `);

  const map = new Map<string, any>();

  const ensureEntry = (destinationType: string, destinationKey: string) => {
    const key = `${destinationType}|||${destinationKey}`;
    const existing = map.get(key);
    if (existing) return existing;
    const entry = {
      destination_type: destinationType,
      destination_key: destinationKey,
      destination: null as string | null,
      state: "ACTIVE" as DestinationState,
      reason: null as string | null,
      ready_to_resume: false,
      resume_ready_at: null as Date | null,
      updated_at: null as Date | null,
      stats_24h: null as any,
      stats_1h: null as any,
      endpoint_health_24h: null as any,
      endpoint_health_1h: null as any,
      health: null as any,
      last_delivery: null as any,
    };
    map.set(key, entry);
    return entry;
  };

  const toDate = (value?: Date | string | null) => {
    if (!value) return null;
    if (value instanceof Date) return value;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  };

  const applyUpdatedAt = (entry: any, candidate?: Date | string | null) => {
    const normalized = toDate(candidate);
    if (!normalized) return;
    const current = toDate(entry.updated_at);
    if (!current || normalized.getTime() > current.getTime()) {
      entry.updated_at = normalized;
    }
  };

  for (const row of stateRows) {
    const entry = ensureEntry(row.destinationType, row.destinationKey);
    entry.state = row.state as DestinationState;
    entry.reason = row.reason ?? null;
    entry.resume_ready_at = row.resumeReadyAt ?? null;
    entry.ready_to_resume = Boolean(row.resumeReadyAt && row.resumeReadyAt.getTime() <= now.getTime());
    applyUpdatedAt(entry, row.updatedAt ?? row.createdAt ?? null);
  }

  const subscriptionList = (subscriptionRows as any).rows ?? subscriptionRows;
  for (const row of subscriptionList ?? []) {
    const destinationKey = makeDestinationKey(row.destination_type, row.destination);
    const entry = ensureEntry(row.destination_type, destinationKey);
    entry.destination = row.destination;
    applyUpdatedAt(entry, row.updated_at ? new Date(row.updated_at) : null);
  }

  const healthList = (healthRows as any).rows ?? healthRows;
  for (const row of healthList ?? []) {
    const destinationKey = makeDestinationKey(row.destination_type ?? row.destinationType, row.destination);
    const destinationType = row.destination_type ?? row.destinationType;
    const entry = ensureEntry(destinationType, destinationKey);
    entry.destination = entry.destination ?? row.destination;
    const healthPayload = {
      window: row.window,
      status: row.status,
      success_rate: row.successRate ?? row.success_rate,
      p95_ms: row.p95Ms ?? row.p95_ms,
      last_success_at: row.lastSuccessAt ?? row.last_success_at,
      last_failure_at: row.lastFailureAt ?? row.last_failure_at,
      updated_at: row.updatedAt ?? row.updated_at,
    };
    if (row.window === "24h") {
      entry.endpoint_health_24h = healthPayload;
    } else if (row.window === "1h") {
      entry.endpoint_health_1h = healthPayload;
    }
    applyUpdatedAt(entry, row.updatedAt ?? row.updated_at ?? null);
  }

  const stats24List = (stats24Rows as any).rows ?? stats24Rows;
  for (const row of stats24List ?? []) {
    const destinationKey = makeDestinationKey(row.destination_type, row.endpoint);
    const entry = ensureEntry(row.destination_type, destinationKey);
    entry.destination = entry.destination ?? row.endpoint;
    const sent = Number(row.sent ?? 0);
    const failed = Number(row.failed ?? 0);
    const skipped = Number(row.skipped ?? 0);
    const total = sent + failed;
    entry.stats_24h = {
      sent,
      failed,
      skipped,
      p95_ms: row.p95 === null || row.p95 === undefined ? null : Math.round(Number(row.p95)),
      success_rate: total > 0 ? sent / total : 1,
    };
  }

  const stats1List = (stats1Rows as any).rows ?? stats1Rows;
  for (const row of stats1List ?? []) {
    const destinationKey = makeDestinationKey(row.destination_type, row.endpoint);
    const entry = ensureEntry(row.destination_type, destinationKey);
    entry.destination = entry.destination ?? row.endpoint;
    const sent = Number(row.sent ?? 0);
    const failed = Number(row.failed ?? 0);
    const skipped = Number(row.skipped ?? 0);
    const total = sent + failed;
    entry.stats_1h = {
      sent,
      failed,
      skipped,
      p95_ms: row.p95 === null || row.p95 === undefined ? null : Math.round(Number(row.p95)),
      success_rate: total > 0 ? sent / total : 1,
    };
  }

  const lastList = (lastDeliveryRows as any).rows ?? lastDeliveryRows;
  for (const row of lastList ?? []) {
    const destinationKey = makeDestinationKey(row.destination_type, row.endpoint);
    const entry = ensureEntry(row.destination_type, destinationKey);
    entry.destination = entry.destination ?? row.endpoint;
    entry.last_delivery = {
      status: row.status,
      created_at: row.created_at,
    };
    applyUpdatedAt(entry, row.created_at ? new Date(row.created_at) : null);
  }

  let items = Array.from(map.values()).map((entry) => ({
    ...entry,
    health: window === "1h" ? entry.endpoint_health_1h : entry.endpoint_health_24h,
    updated_at: entry.updated_at ?? entry.resume_ready_at ?? now,
  }));

  if (stateFilter) {
    items = items.filter((item) => String(item.state).toUpperCase() === stateFilter);
  }
  if (typeFilter) {
    items = items.filter((item) => String(item.destination_type).toUpperCase() === typeFilter);
  }
  if (query) {
    items = items.filter((item) =>
      String(item.destination ?? "").toLowerCase().includes(query) ||
      String(item.destination_key ?? "").toLowerCase().includes(query),
    );
  }

  if (args.includeOverrides && items.length > 0) {
    const { alertDestinationOverrides, alertDestinationSlaOverrides } = await import("@shared/schema");
    const { resolveSlaThresholds } = await import("./alertDestinationOverridesService");
    const destinationKeys = items.map((item) => item.destination_key);
    const overrideRows = await db
      .select()
      .from(alertDestinationOverrides)
      .where(and(
        eq(alertDestinationOverrides.tenantId, args.tenantId),
        inArray(alertDestinationOverrides.destinationKey, destinationKeys),
      ));
    const overrideMap = new Map(overrideRows.map((row) => [row.destinationKey, row]));

    const slaOverrideRows = await db
      .select()
      .from(alertDestinationSlaOverrides)
      .where(and(
        eq(alertDestinationSlaOverrides.tenantId, args.tenantId),
        inArray(alertDestinationSlaOverrides.destinationKey, destinationKeys),
      ));
    const slaOverrideMap = new Map<string, any[]>();
    for (const row of slaOverrideRows) {
      const list = slaOverrideMap.get(row.destinationKey) ?? [];
      list.push(row);
      slaOverrideMap.set(row.destinationKey, list);
    }

    items = await Promise.all(items.map(async (item) => {
      const override = overrideMap.get(item.destination_key);
      const slaOverrides = slaOverrideMap.get(item.destination_key) ?? [];
      const windowKey = window === "1h" ? "24h" : window;

      const resolvedSla = await resolveSlaThresholds({
        tenantId: args.tenantId,
        window: windowKey as "24h" | "7d",
        destinationType: String(item.destination_type).toUpperCase() as any,
        destinationKey: item.destination_key,
      });

      const slaOverrideRow = slaOverrides.find((row) => row.window === windowKey);
      return {
        ...item,
        noise_budget_override: override
          ? {
              enabled: override.noiseBudgetEnabled,
              window_minutes: override.noiseBudgetWindowMinutes ?? null,
              max_deliveries: override.noiseBudgetMaxDeliveries ?? null,
              source: "DESTINATION",
            }
          : {
              enabled: true,
              window_minutes: null,
              max_deliveries: null,
              source: "TENANT_DEFAULT",
            },
        sla_override: {
          enabled: override?.slaEnabled ?? true,
          window: windowKey,
          p95_ms: slaOverrideRow?.p95Ms ?? resolvedSla.p95_ms,
          success_rate_min_pct: slaOverrideRow?.successRateMinPct ?? resolvedSla.success_rate_min_pct,
          source: resolvedSla.source,
        },
      };
    }));
  }

  const summary = {
    states: { ACTIVE: 0, PAUSED: 0, AUTO_PAUSED: 0, DISABLED: 0 },
    types: { WEBHOOK: 0, EMAIL: 0 },
  };
  for (const item of items) {
    const state = String(item.state ?? "ACTIVE").toUpperCase();
    const type = String(item.destination_type ?? "").toUpperCase();
    if (summary.states[state as keyof typeof summary.states] !== undefined) {
      summary.states[state as keyof typeof summary.states] += 1;
    }
    if (summary.types[type as keyof typeof summary.types] !== undefined) {
      summary.types[type as keyof typeof summary.types] += 1;
    }
  }

  items.sort((a, b) => {
    const aTime = a.updated_at instanceof Date ? a.updated_at.getTime() : new Date(a.updated_at).getTime();
    const bTime = b.updated_at instanceof Date ? b.updated_at.getTime() : new Date(b.updated_at).getTime();
    if (aTime !== bTime) return bTime - aTime;
    return String(a.destination_key).localeCompare(String(b.destination_key));
  });

  const cursor = decodeCursor(args.cursor ?? null);
  if (cursor) {
    items = items.filter((item) => {
      const itemTime = item.updated_at instanceof Date ? item.updated_at : new Date(item.updated_at);
      if (itemTime.getTime() < cursor.time.getTime()) return true;
      if (itemTime.getTime() === cursor.time.getTime()) {
        return String(item.destination_key) > cursor.key;
      }
      return false;
    });
  }

  const paged = items.slice(0, limit);
  const last = paged[paged.length - 1];
  const next_cursor = paged.length === limit && last
    ? encodeCursor(last.updated_at instanceof Date ? last.updated_at : new Date(last.updated_at), last.destination_key)
    : null;

  return {
    version: "1",
    window,
    items: paged,
    next_cursor,
    summary,
  };
};

export const getDestinationDetail = async (args: {
  tenantId: string;
  destinationKey: string;
  windows?: { endpointHealth?: EndpointWindow; sla?: "24h" | "7d" };
  now?: Date;
}) => {
  const now = args.now ?? new Date();
  const endpointWindow = args.windows?.endpointHealth ?? "1h";
  const slaWindow = args.windows?.sla ?? "24h";
  const list = await listDestinations({
    tenantId: args.tenantId,
    window: endpointWindow,
    limit: 10000,
    now,
  });
  const item = list.items.find((entry: any) => entry.destination_key === args.destinationKey) ?? null;
  if (!item || !item.destination || !item.destination_type) {
    return null;
  }

  const destinationType = String(item.destination_type).toUpperCase();
  const endpoint = item.destination;

  const [healthRow] = await db
    .select({
      window: alertEndpointHealth.window,
      status: alertEndpointHealth.status,
      successRate: alertEndpointHealth.successRate,
      p50Ms: alertEndpointHealth.p50Ms,
      p95Ms: alertEndpointHealth.p95Ms,
      lastSuccessAt: alertEndpointHealth.lastSuccessAt,
      lastFailureAt: alertEndpointHealth.lastFailureAt,
      updatedAt: alertEndpointHealth.updatedAt,
    })
    .from(alertEndpointHealth)
    .where(and(
      eq(alertEndpointHealth.tenantId, args.tenantId),
      eq(alertEndpointHealth.window, endpointWindow),
      eq(alertEndpointHealth.destinationType, destinationType),
      eq(alertEndpointHealth.destination, endpoint),
    ))
    .orderBy(desc(alertEndpointHealth.updatedAt))
    .limit(1);

  const [slaRow] = await db
    .select({
      window: alertDeliverySlaWindows.window,
      windowStart: alertDeliverySlaWindows.windowStart,
      status: alertDeliverySlaWindows.status,
      latencyP50Ms: alertDeliverySlaWindows.latencyP50Ms,
      latencyP95Ms: alertDeliverySlaWindows.latencyP95Ms,
      successRate: alertDeliverySlaWindows.successRate,
      computedAt: alertDeliverySlaWindows.computedAt,
    })
    .from(alertDeliverySlaWindows)
    .where(and(
      eq(alertDeliverySlaWindows.tenantId, args.tenantId),
      eq(alertDeliverySlaWindows.window, slaWindow),
      eq(alertDeliverySlaWindows.destinationType, destinationType),
      eq(alertDeliverySlaWindows.destinationKey, args.destinationKey),
    ))
    .orderBy(desc(alertDeliverySlaWindows.computedAt), desc(alertDeliverySlaWindows.windowStart))
    .limit(1);

  const thresholds = await resolveSlaThresholds({
    tenantId: args.tenantId,
    window: slaWindow as "24h" | "7d",
    destinationType: destinationType as any,
    destinationKey: args.destinationKey,
  });

  const recentDeliveries = await db
    .select({
      id: alertDeliveries.id,
      status: alertDeliveries.status,
      skipReason: alertDeliveries.skipReason,
      clusterId: alertDeliveries.clusterId,
      createdAt: alertDeliveries.createdAt,
      sentAt: alertDeliveries.sentAt,
    })
    .from(alertDeliveries)
    .where(and(
      eq(alertDeliveries.tenantId, args.tenantId),
      eq(alertDeliveries.destinationKey, args.destinationKey),
    ))
    .orderBy(desc(alertDeliveries.createdAt))
    .limit(20);

  return {
    destination_key: args.destinationKey,
    destination_type: destinationType,
    endpoint,
    state: {
      state: item.state,
      reason: item.reason ?? null,
      ready_to_resume: Boolean(item.ready_to_resume),
      resume_ready_at: item.resume_ready_at ?? null,
      updated_at: item.updated_at ?? null,
    },
    endpoint_health: healthRow ? {
      window: healthRow.window,
      status: healthRow.status,
      p50_ms: healthRow.p50Ms ?? null,
      p95_ms: healthRow.p95Ms ?? null,
      success_rate: healthRow.successRate ?? null,
      last_success_at: healthRow.lastSuccessAt ?? null,
      last_failure_at: healthRow.lastFailureAt ?? null,
      computed_at: healthRow.updatedAt ?? null,
    } : null,
    sla: slaRow ? {
      window: slaRow.window,
      status: slaRow.status,
      p50_ms: slaRow.latencyP50Ms ?? null,
      p95_ms: slaRow.latencyP95Ms ?? null,
      success_rate: Number(slaRow.successRate ?? 0) / 100,
      thresholds: {
        p95_ms: thresholds.p95_ms,
        success_rate_min: thresholds.success_rate_min_pct / 100,
      },
      computed_at: slaRow.computedAt ?? null,
      window_start: slaRow.windowStart ?? null,
    } : null,
    recent_deliveries: recentDeliveries,
  };
};


export const applyAutoPauseFromEndpointHealth = async (args: {
  tenantId: string;
  window: EndpointWindow;
  now?: Date;
  auditContext?: AuditContext;
}) => {
  const now = args.now ?? new Date();
  const rows = await db
    .select()
    .from(alertEndpointHealth)
    .where(and(
      eq(alertEndpointHealth.tenantId, args.tenantId),
      eq(alertEndpointHealth.window, args.window),
    ));

  if (rows.length === 0) {
    return { window: args.window, skipped: false, updated_count: 0 };
  }

  let updatedCount = 0;
  const downThreshold = AUTO_PAUSE_CONSECUTIVE();
  const readyThreshold = RESUME_READY_CONSECUTIVE();
  const minDownMs = AUTO_RESUME_MIN_DOWN_MINUTES() * 60 * 1000;
  const cooldownMs = AUTO_RESUME_COOLDOWN_MINUTES() * 60 * 1000;

  for (const row of rows) {
    const destinationKey = makeDestinationKey(row.destinationType, row.destination);
    const [stateRow] = await db
      .select()
      .from(alertDestinationStates)
      .where(and(
        eq(alertDestinationStates.tenantId, args.tenantId),
        eq(alertDestinationStates.destinationType, row.destinationType),
        eq(alertDestinationStates.destinationKey, destinationKey),
      ))
      .limit(1);

    const currentState = (stateRow?.state ?? "ACTIVE") as DestinationState;
    const lastStateChangeAt = stateRow?.updatedAt ?? stateRow?.createdAt ?? null;
    const inCooldown = lastStateChangeAt
      ? now.getTime() - lastStateChangeAt.getTime() < cooldownMs
      : false;

    if (currentState === "PAUSED" || currentState === "DISABLED") {
      continue;
    }

    if (row.status === "DOWN" && row.consecutiveFailures >= downThreshold) {
      if (currentState === "ACTIVE" && !inCooldown) {
        await upsertDestinationState({
          tenantId: args.tenantId,
          userId: null,
          destinationType: row.destinationType,
          destinationKey,
          state: "AUTO_PAUSED",
          reason: `auto-paused: endpoint down ${row.consecutiveFailures}/${downThreshold}`,
          now,
        });
        updatedCount += 1;
        await writeAuditEvent(args.auditContext, {
          action: "ALERT.ENDPOINT_AUTO_PAUSED",
          resourceType: "ALERT_DESTINATION",
          resourceId: destinationKey,
          status: "SUCCESS",
          severity: "SECURITY",
          message: "Destination auto-paused due to endpoint health",
          metadata: {
            window: args.window,
            destination_type: row.destinationType,
            destination_key: destinationKey,
            destination: row.destination,
            consecutive_failures: row.consecutiveFailures,
            threshold: downThreshold,
          },
        });
        try {
          await dispatchDestinationStateSystemAlert({
            tenantId: args.tenantId,
            window: args.window,
            destinationType: row.destinationType,
            destination: row.destination,
            destinationKey,
            state: "AUTO_PAUSED",
            reason: `auto-paused: endpoint down ${row.consecutiveFailures}/${downThreshold}`,
            computedAt: now,
          });
        } catch {
          // ignore system alert failures
        }
      }
      continue;
    }

    if (currentState === "AUTO_PAUSED") {
      const autoPausedAt = stateRow?.autoPausedAt ?? stateRow?.updatedAt ?? null;
      const downLongEnough = !autoPausedAt || now.getTime() - autoPausedAt.getTime() >= minDownMs;
      const p95Threshold = AUTO_RESUME_P95_MS(row.destinationType);
      const p95Ok = row.p95Ms == null || row.p95Ms <= p95Threshold;
      const successOk = (row.attemptsSuccess ?? 0) >= readyThreshold;
      const ready = row.status === "OK" && downLongEnough && p95Ok && successOk;

      if (ready && !stateRow?.resumeReadyAt) {
        await db
          .update(alertDestinationStates)
          .set({
            resumeReadyAt: now,
            reason: "ready to resume",
            updatedAt: now,
          })
          .where(and(
            eq(alertDestinationStates.tenantId, args.tenantId),
            eq(alertDestinationStates.destinationType, row.destinationType),
            eq(alertDestinationStates.destinationKey, destinationKey),
          ));
        updatedCount += 1;
      }

      if (ready && stateRow?.resumeReadyAt) {
        const readyAgeMs = now.getTime() - stateRow.resumeReadyAt.getTime();
        if (readyAgeMs >= cooldownMs) {
          await upsertDestinationState({
            tenantId: args.tenantId,
            userId: null,
            destinationType: row.destinationType,
            destinationKey,
            state: "ACTIVE",
            reason: "auto-resumed",
            now,
          });
          updatedCount += 1;
          await writeAuditEvent(args.auditContext, {
            action: "ALERT.ENDPOINT_AUTO_RESUMED",
            resourceType: "ALERT_DESTINATION",
            resourceId: destinationKey,
            status: "SUCCESS",
            severity: "SECURITY",
            message: "Destination auto-resumed after endpoint recovery",
            metadata: {
              window: args.window,
              destination_type: row.destinationType,
              destination_key: destinationKey,
              destination: row.destination,
              success_rate: row.successRate,
              p95_ms: row.p95Ms,
              min_successes: readyThreshold,
              min_down_minutes: AUTO_RESUME_MIN_DOWN_MINUTES(),
              cooldown_minutes: AUTO_RESUME_COOLDOWN_MINUTES(),
            },
          });
        }
      }

      if (!ready && stateRow?.resumeReadyAt) {
        await db
          .update(alertDestinationStates)
          .set({
            resumeReadyAt: null,
            updatedAt: now,
          })
          .where(and(
            eq(alertDestinationStates.tenantId, args.tenantId),
            eq(alertDestinationStates.destinationType, row.destinationType),
            eq(alertDestinationStates.destinationKey, destinationKey),
          ));
        updatedCount += 1;
      }
    }
  }

  return { window: args.window, skipped: false, updated_count: updatedCount };
};
