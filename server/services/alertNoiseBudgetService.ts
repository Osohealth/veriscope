import { and, eq, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries, alertNoiseBudgetBreaches, alertNoiseBudgets } from "@shared/schema";

export type NoiseWindow = "24h" | "7d";
export type NoiseDestination = "WEBHOOK" | "EMAIL";

export type NoiseBudgetItem = {
  destination_type: NoiseDestination;
  window: NoiseWindow;
  max_deliveries: number;
  source: "DEFAULT" | "CUSTOM";
};

export const DEFAULT_NOISE_BUDGETS: Record<NoiseWindow, Record<NoiseDestination, { max_deliveries: number }>> = {
  "24h": {
    WEBHOOK: { max_deliveries: 50 },
    EMAIL: { max_deliveries: 20 },
  },
  "7d": {
    WEBHOOK: { max_deliveries: 200 },
    EMAIL: { max_deliveries: 100 },
  },
};

const WINDOW_MS: Record<NoiseWindow, number> = {
  "24h": 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
};

const DEST_TYPES: NoiseDestination[] = ["WEBHOOK", "EMAIL"];

export async function listAlertNoiseBudgets(tenantId: string, window: NoiseWindow): Promise<{ window: NoiseWindow; items: NoiseBudgetItem[] }> {
  const rows = await db
    .select()
    .from(alertNoiseBudgets)
    .where(and(eq(alertNoiseBudgets.tenantId, tenantId), eq(alertNoiseBudgets.window, window)));

  const rowMap = new Map(rows.map((row) => [row.destinationType, row]));

  const items = DEST_TYPES.map((destinationType) => {
    const row = rowMap.get(destinationType);
    const defaults = DEFAULT_NOISE_BUDGETS[window][destinationType];
    return {
      destination_type: destinationType,
      window,
      max_deliveries: row?.maxDeliveries ?? defaults.max_deliveries,
      source: row ? "CUSTOM" : "DEFAULT",
    };
  });

  return { window, items };
}

export async function upsertAlertNoiseBudget(input: {
  tenantId: string;
  window: NoiseWindow;
  destinationType: NoiseDestination;
  maxDeliveries: number;
}) {
  const now = new Date();
  const [existing] = await db
    .select()
    .from(alertNoiseBudgets)
    .where(and(
      eq(alertNoiseBudgets.tenantId, input.tenantId),
      eq(alertNoiseBudgets.window, input.window),
      eq(alertNoiseBudgets.destinationType, input.destinationType),
    ))
    .limit(1);

  const [row] = await db
    .insert(alertNoiseBudgets)
    .values({
      tenantId: input.tenantId,
      window: input.window,
      destinationType: input.destinationType,
      maxDeliveries: input.maxDeliveries,
      createdAt: existing ? existing.createdAt : now,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: [alertNoiseBudgets.tenantId, alertNoiseBudgets.destinationType, alertNoiseBudgets.window],
      set: {
        maxDeliveries: input.maxDeliveries,
        updatedAt: now,
      },
    })
    .returning();

  return {
    previous: existing ? { max_deliveries: existing.maxDeliveries } : null,
    current: { max_deliveries: row.maxDeliveries },
  };
}

async function countDeliveriesInWindow(params: {
  tenantId: string;
  destinationType: NoiseDestination;
  windowStart: Date;
}) {
  const [row] = await db
    .select({ count: sql<number>`count(*)` })
    .from(alertDeliveries)
    .where(and(
      eq(alertDeliveries.tenantId, params.tenantId),
      eq(alertDeliveries.destinationType, params.destinationType),
      sql`${alertDeliveries.createdAt} >= ${params.windowStart}`,
      sql`${alertDeliveries.status} not like 'SKIPPED%'`,
    ));
  return Number(row?.count ?? 0);
}

export async function checkNoiseBudget(params: {
  tenantId: string;
  destinationType: NoiseDestination;
  now?: Date;
}): Promise<
  | { allowed: true }
  | {
      allowed: false;
      window: NoiseWindow;
      maxDeliveries: number;
      count: number;
      windowStart: Date;
      emitAudit: boolean;
    }
> {
  const now = params.now ?? new Date();
  const nowRounded = new Date(Math.floor(now.getTime() / 60000) * 60000);

  for (const window of ["24h", "7d"] as NoiseWindow[]) {
    const windowStart = new Date(nowRounded.getTime() - WINDOW_MS[window]);
    const budgets = await listAlertNoiseBudgets(params.tenantId, window);
    const budget = budgets.items.find((item) => item.destination_type === params.destinationType);
    const maxDeliveries = budget?.max_deliveries ?? DEFAULT_NOISE_BUDGETS[window][params.destinationType].max_deliveries;
    const count = await countDeliveriesInWindow({
      tenantId: params.tenantId,
      destinationType: params.destinationType,
      windowStart,
    });
    if (count >= maxDeliveries) {
      return {
        allowed: false,
        window,
        maxDeliveries,
        count,
        windowStart,
        emitAudit: count === maxDeliveries,
      };
    }
  }

  return { allowed: true };
}

export async function recordNoiseBudgetBreachOnce(params: {
  tenantId: string;
  destinationType: string;
  window: string;
  bucketMinute: Date;
}) {
  const [row] = await db
    .insert(alertNoiseBudgetBreaches)
    .values({
      tenantId: params.tenantId,
      destinationType: params.destinationType,
      window: params.window,
      bucketMinute: params.bucketMinute,
    })
    .onConflictDoNothing()
    .returning({ id: alertNoiseBudgetBreaches.id });

  return Boolean(row?.id);
}
