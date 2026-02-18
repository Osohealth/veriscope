import { and, desc, eq, inArray, sql, type SQL } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries, alertDestinationStates, signals } from "@shared/schema";
import { SEVERITY_RANK } from "@shared/signalTypes";

export type AlertDeliveryFilters = {
  tenantId: string;
  userId: string;
  days?: number;
  day?: Date | null;
  entityId?: string;
  subscriptionId?: string;
  subscriptionIds?: string[];
  runId?: string;
  status?: string;
  destinationType?: string;
  endpoint?: string;
  destinationKey?: string;
  severityMin?: string;
  isTest?: boolean;
  cursorCreatedAt?: string | null;
  cursorId?: string | null;
  limit?: number;
};

const severityRankSql = (value: SQL) => sql`(case ${value}
  when 'CRITICAL' then 4
  when 'HIGH' then 3
  when 'MEDIUM' then 2
  when 'LOW' then 1
  else 0
end)`;

const buildDlqExists = (tenantId: string) => sql`exists (
  select 1
  from alert_dlq dlq
  where dlq.delivery_id = ${alertDeliveries.id}
    and dlq.tenant_id = ${tenantId}
    and dlq.attempt_count < dlq.max_attempts
)`;

const buildSeverityExists = (requiredRank: number) => sql`exists (
  select 1
  from signals s
  where s.cluster_id = ${alertDeliveries.clusterId}
    and s.entity_id = ${alertDeliveries.entityId}
    and s.day = ${alertDeliveries.day}
    and ${severityRankSql(sql`upper(s.cluster_severity)`)} >= ${requiredRank}
)`;

const buildSystemSeverityMatch = (requiredRank: number) => sql`(
  ${alertDeliveries.clusterId} like 'sla:%' AND (
    (${alertDeliveries.clusterId} like '%:AT_RISK' AND ${requiredRank} <= 3) OR
    (${alertDeliveries.clusterId} like '%:OK' AND ${requiredRank} <= 1)
  )
)`;

export const buildAlertDeliveriesWhere = (
  filters: AlertDeliveryFilters,
  options?: { includeCursor?: boolean },
) => {
  const conditions: SQL[] = [];

  conditions.push(eq(alertDeliveries.tenantId, filters.tenantId));
  conditions.push(eq(alertDeliveries.userId, filters.userId));

  if (filters.days) {
    const cutoff = new Date(Date.now() - filters.days * 24 * 60 * 60 * 1000);
    conditions.push(sql`${alertDeliveries.createdAt} >= ${cutoff}`);
  }
  if (filters.day) conditions.push(eq(alertDeliveries.day, filters.day));
  if (filters.entityId) conditions.push(eq(alertDeliveries.entityId, filters.entityId));
  if (filters.subscriptionId) conditions.push(eq(alertDeliveries.subscriptionId, filters.subscriptionId));
  if (filters.subscriptionIds && filters.subscriptionIds.length > 0) {
    conditions.push(inArray(alertDeliveries.subscriptionId, filters.subscriptionIds));
  }
  if (filters.runId) conditions.push(eq(alertDeliveries.runId, filters.runId));
  if (filters.destinationType) conditions.push(eq(alertDeliveries.destinationType, filters.destinationType));
  if (filters.endpoint) conditions.push(eq(alertDeliveries.endpoint, filters.endpoint));
  if (filters.destinationKey) conditions.push(eq(alertDeliveries.destinationKey, filters.destinationKey));
  if (filters.isTest !== undefined) conditions.push(eq(alertDeliveries.isTest, filters.isTest));

  if (filters.status) {
    const status = String(filters.status).toUpperCase();
    if (status === "DLQ") {
      conditions.push(buildDlqExists(filters.tenantId));
    } else if (status === "SKIPPED") {
      conditions.push(sql`${alertDeliveries.status} like 'SKIPPED%'`);
    } else {
      conditions.push(eq(alertDeliveries.status, status));
    }
  }

  if (filters.severityMin) {
    const requiredRank = SEVERITY_RANK[String(filters.severityMin).toUpperCase() as keyof typeof SEVERITY_RANK] ?? 0;
    conditions.push(sql`(${buildSeverityExists(requiredRank)} OR ${buildSystemSeverityMatch(requiredRank)})`);
  }

  if (options?.includeCursor && filters.cursorCreatedAt && filters.cursorId) {
    conditions.push(
      sql`(${alertDeliveries.createdAt} < ${filters.cursorCreatedAt}::timestamptz OR (${alertDeliveries.createdAt} = ${filters.cursorCreatedAt}::timestamptz AND ${alertDeliveries.id} < ${filters.cursorId}))`,
    );
  }

  return conditions.length > 0 ? and(...conditions) : undefined;
};

export async function listAlertDeliveries(filters: AlertDeliveryFilters) {
  const limit = Math.min(Math.max(filters.limit ?? 50, 1), 200);
  const whereClause = buildAlertDeliveriesWhere(filters, { includeCursor: true });

  const items = await db
    .select()
    .from(alertDeliveries)
    .where(whereClause)
    .orderBy(desc(alertDeliveries.createdAt), desc(alertDeliveries.id))
    .limit(limit);

  return { items };
}

export async function getAlertDeliveriesSummary(filters: AlertDeliveryFilters) {
  const whereClause = buildAlertDeliveriesWhere(filters);

  const [row] = await db
    .select({
      total: sql<number>`count(*)`,
      sent: sql<number>`count(*) filter (where ${alertDeliveries.status} = 'SENT')`,
      failed: sql<number>`count(*) filter (where ${alertDeliveries.status} = 'FAILED')`,
      skipped: sql<number>`count(*) filter (where ${alertDeliveries.status} like 'SKIPPED%')`,
      skippedNoiseBudget: sql<number>`count(*) filter (where ${alertDeliveries.skipReason} = 'NOISE_BUDGET_EXCEEDED')`,
      dlqPending: sql<number>`count(*) filter (where ${buildDlqExists(filters.tenantId)})`,
      p50Latency: sql<number | null>`percentile_cont(0.5) within group (order by ${alertDeliveries.latencyMs}) filter (where ${alertDeliveries.status} = 'SENT' and ${alertDeliveries.latencyMs} is not null)`,
    })
    .from(alertDeliveries)
    .where(whereClause);

  const destinationStates: Record<string, number> = {
    ACTIVE: 0,
    PAUSED: 0,
    AUTO_PAUSED: 0,
    DISABLED: 0,
  };

  const destinationRows = await db
    .select({
      destinationType: alertDeliveries.destinationType,
      destinationKey: alertDeliveries.destinationKey,
    })
    .from(alertDeliveries)
    .where(whereClause)
    .groupBy(alertDeliveries.destinationType, alertDeliveries.destinationKey);

  if (destinationRows.length > 0) {
    const keys = destinationRows.map((row) => row.destinationKey);
    const stateRows = await db
      .select({
        destinationKey: alertDestinationStates.destinationKey,
        state: alertDestinationStates.state,
      })
      .from(alertDestinationStates)
      .where(and(
        eq(alertDestinationStates.tenantId, filters.tenantId),
        inArray(alertDestinationStates.destinationKey, keys),
      ));
    const stateMap = new Map(stateRows.map((state) => [state.destinationKey, String(state.state).toUpperCase()]));
    for (const row of destinationRows) {
      const state = stateMap.get(row.destinationKey) ?? "ACTIVE";
      destinationStates[state] = (destinationStates[state] ?? 0) + 1;
    }
  }

  return {
    total: Number(row?.total ?? 0),
    sent: Number(row?.sent ?? 0),
    failed: Number(row?.failed ?? 0),
    skipped: Number(row?.skipped ?? 0),
    skipped_noise_budget: Number(row?.skippedNoiseBudget ?? 0),
    dlq_pending: Number(row?.dlqPending ?? 0),
    p50_latency_ms: row?.p50Latency === null || row?.p50Latency === undefined ? null : Number(row.p50Latency),
    destination_states: destinationStates,
  };
}
