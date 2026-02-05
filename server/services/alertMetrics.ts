import { sql } from "drizzle-orm";
import { db } from "../db";

const clampDays = (days?: number) => {
  if (!days) return 30;
  if (days < 1) return 1;
  if (days > 365) return 365;
  return days;
};

type MetricContext = { tenantId?: string; userId?: string };

export async function getDeliveryHealthByDay(days?: number, ctx: MetricContext = {}) {
  const window = clampDays(days);
  const tenantFilter = ctx.tenantId ? sql` AND tenant_id = ${ctx.tenantId}` : sql``;
  const userFilter = ctx.userId ? sql` AND user_id = ${ctx.userId}` : sql``;
  const result = await db.execute(sql`
    SELECT
      day,
      COUNT(*) FILTER (WHERE status='SENT')::float / NULLIF(COUNT(*),0) AS success_rate,
      COUNT(*) FILTER (WHERE status='FAILED') AS failed,
      COUNT(*) FILTER (WHERE status='SKIPPED_DEDUPE') AS deduped,
      COUNT(*) FILTER (WHERE status='SKIPPED_RATE_LIMIT') AS rate_limited
    FROM alert_deliveries
    WHERE day >= (current_date - ${window}::int)
      ${tenantFilter}
      ${userFilter}
    GROUP BY day
    ORDER BY day DESC
  `);
  return result.rows ?? result;
}

export async function getDeliveryLatency(days?: number, ctx: MetricContext = {}) {
  const window = clampDays(days);
  const tenantFilter = ctx.tenantId ? sql` AND tenant_id = ${ctx.tenantId}` : sql``;
  const userFilter = ctx.userId ? sql` AND user_id = ${ctx.userId}` : sql``;
  const result = await db.execute(sql`
    SELECT
      percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY latency_ms) AS p90,
      percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99
    FROM alert_deliveries
    WHERE status='SENT'
      AND day >= (current_date - ${window}::int)
      ${tenantFilter}
      ${userFilter}
  `);
  return result.rows?.[0] ?? (Array.isArray(result) ? result[0] : result);
}

export async function getEndpointHealth(days?: number, ctx: MetricContext = {}) {
  const window = clampDays(days);
  const tenantFilter = ctx.tenantId ? sql` AND tenant_id = ${ctx.tenantId}` : sql``;
  const userFilter = ctx.userId ? sql` AND user_id = ${ctx.userId}` : sql``;
  const result = await db.execute(sql`
    SELECT endpoint,
           COUNT(*) FILTER (WHERE status='FAILED') AS failed,
           COUNT(*) FILTER (WHERE status='SENT') AS sent
    FROM alert_deliveries
    WHERE day >= (current_date - ${window}::int)
      ${tenantFilter}
      ${userFilter}
    GROUP BY endpoint
    ORDER BY failed DESC
  `);
  return result.rows ?? result;
}

export async function getDlqHealth(ctx: MetricContext = {}) {
  const tenantFilter = ctx.tenantId ? sql` AND a.tenant_id = ${ctx.tenantId}` : sql``;
  const userFilter = ctx.userId ? sql` AND a.user_id = ${ctx.userId}` : sql``;
  const result = await db.execute(sql`
    SELECT
      COUNT(*) FILTER (WHERE d.attempt_count < d.max_attempts) AS dlq_depth,
      MIN(next_attempt_at) AS next_due,
      MAX(d.attempt_count) AS max_attempts_seen
    FROM alert_dlq d
    INNER JOIN alert_deliveries a ON a.id = d.delivery_id
    WHERE 1=1
      ${tenantFilter}
      ${userFilter}
  `);
  return result.rows?.[0] ?? (Array.isArray(result) ? result[0] : result);
}

export async function getDlqOverdue(limit: number = 20, ctx: MetricContext = {}) {
  const tenantFilter = ctx.tenantId ? sql` AND a.tenant_id = ${ctx.tenantId}` : sql``;
  const userFilter = ctx.userId ? sql` AND a.user_id = ${ctx.userId}` : sql``;
  const result = await db.execute(sql`
    SELECT
      delivery_id,
      attempt_count,
      now() - next_attempt_at AS overdue_by
    FROM alert_dlq d
    INNER JOIN alert_deliveries a ON a.id = d.delivery_id
    WHERE next_attempt_at < now()
      ${tenantFilter}
      ${userFilter}
    ORDER BY overdue_by DESC
    LIMIT ${limit}
  `);
  return result.rows ?? result;
}
