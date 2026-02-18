import { and, desc, eq, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveryAttempts, alertDeliveries, alertEndpointHealth } from "@shared/schema";
import { writeAuditEvent } from "./auditLog";
import { dispatchEndpointHealthSystemAlert } from "./alertDispatcher";
import { makeDestinationKey } from "./destinationKey";
import { openOrAttachIncident, resolveIncidentIfExists } from "./incidentService";
import type { AuditContext } from "../middleware/requestContext";

export type EndpointWindow = "1h" | "24h";
export type EndpointStatus = "OK" | "DEGRADED" | "DOWN";

const WINDOW_MS: Record<EndpointWindow, number> = {
  "1h": 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
};

const SUCCESS_STATUSES = ["SENT", "SUCCESS"];
const SUCCESS_STATUS_SQL = sql.join(SUCCESS_STATUSES.map((status) => sql`${status}`), sql`, `);

const roundToMinute = (value: Date) => new Date(Math.floor(value.getTime() / 60000) * 60000);

const computeStatus = (args: {
  attemptsTotal: number;
  successRate: number;
  consecutiveFailures: number;
  lastSuccessAt?: Date | null;
}): EndpointStatus => {
  const { attemptsTotal, successRate, consecutiveFailures, lastSuccessAt } = args;
  if (attemptsTotal === 0) return "OK";
  if (!lastSuccessAt && attemptsTotal >= 3) return "DOWN";
  if (successRate < 0.8 || consecutiveFailures >= 5) return "DOWN";
  if (successRate < 0.95 || consecutiveFailures >= 3) return "DEGRADED";
  return "OK";
};

const summarize = (items: Array<{ status: EndpointStatus }>) => {
  const summary = { OK: 0, DEGRADED: 0, DOWN: 0, total: 0 };
  items.forEach((item) => {
    summary.total += 1;
    if (item.status === "OK") summary.OK += 1;
    if (item.status === "DEGRADED") summary.DEGRADED += 1;
    if (item.status === "DOWN") summary.DOWN += 1;
  });
  return summary;
};

export async function computeEndpointHealth({
  tenantId,
  now = new Date(),
  window,
  auditContext,
}: {
  tenantId: string;
  now?: Date;
  window: EndpointWindow;
  auditContext?: AuditContext;
}) {
  const capturedNow = new Date(now);
  const windowStart = roundToMinute(new Date(capturedNow.getTime() - WINDOW_MS[window]));

  const baseResult = await db.execute(sql`
    SELECT
      d.destination_type as destination_type,
      d.endpoint as destination,
      COUNT(*)::int as attempts_total,
      COUNT(*) FILTER (WHERE a.status IN (${SUCCESS_STATUS_SQL}))::int as attempts_success,
      MAX(CASE WHEN a.status IN (${SUCCESS_STATUS_SQL}) THEN a.created_at ELSE NULL END) as last_success_at,
      MAX(CASE WHEN a.status NOT IN (${SUCCESS_STATUS_SQL}) THEN a.created_at ELSE NULL END) as last_failure_at
    FROM alert_delivery_attempts a
    JOIN alert_deliveries d ON d.id = a.delivery_id
    WHERE a.tenant_id = ${tenantId}
      AND a.created_at >= ${windowStart}
    GROUP BY d.destination_type, d.endpoint
  `);
  const baseRows = (baseResult.rows ?? baseResult) as Array<any>;

  if (baseRows.length === 0) {
    return { window, upserted: 0, skipped: false } as const;
  }

  const latencyResult = await db.execute(sql`
    SELECT
      d.destination_type as destination_type,
      d.endpoint as destination,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY a.latency_ms) AS p50_ms,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY a.latency_ms) AS p95_ms
    FROM alert_delivery_attempts a
    JOIN alert_deliveries d ON d.id = a.delivery_id
    WHERE a.tenant_id = ${tenantId}
      AND a.created_at >= ${windowStart}
      AND a.status IN (${SUCCESS_STATUS_SQL})
      AND a.latency_ms IS NOT NULL
    GROUP BY d.destination_type, d.endpoint
  `);
  const latencyRows = (latencyResult.rows ?? latencyResult) as Array<any>;
  const latencyMap = new Map(
    latencyRows.map((row) => [`${row.destination_type}|||${row.destination}`, row]),
  );

  const recentResult = await db.execute(sql`
    SELECT destination_type, destination, status FROM (
      SELECT
        d.destination_type as destination_type,
        d.endpoint as destination,
        a.status as status,
        row_number() OVER (PARTITION BY d.destination_type, d.endpoint ORDER BY a.created_at DESC) as rn
      FROM alert_delivery_attempts a
      JOIN alert_deliveries d ON d.id = a.delivery_id
      WHERE a.tenant_id = ${tenantId}
        AND a.created_at >= ${windowStart}
    ) recent
    WHERE rn <= 50
    ORDER BY destination_type, destination, rn
  `);
  const recentRows = (recentResult.rows ?? recentResult) as Array<any>;
  const recentMap = new Map<string, string[]>();
  recentRows.forEach((row) => {
    const key = `${row.destination_type}|||${row.destination}`;
    const list = recentMap.get(key) ?? [];
    list.push(String(row.status ?? ""));
    recentMap.set(key, list);
  });

  let upserted = 0;

  for (const row of baseRows) {
    const key = `${row.destination_type}|||${row.destination}`;
    const attemptsTotal = Number(row.attempts_total ?? 0);
    const attemptsSuccess = Number(row.attempts_success ?? 0);
    const successRate = attemptsTotal === 0 ? 1 : attemptsSuccess / attemptsTotal;

    const consecutiveFailures = (() => {
      const statuses = recentMap.get(key) ?? [];
      let count = 0;
      for (const status of statuses) {
        if (SUCCESS_STATUSES.includes(status)) break;
        count += 1;
      }
      return count;
    })();

    const lastSuccessAt = row.last_success_at ? new Date(row.last_success_at) : null;
    const lastFailureAt = row.last_failure_at ? new Date(row.last_failure_at) : null;

    const status = computeStatus({
      attemptsTotal,
      successRate,
      consecutiveFailures,
      lastSuccessAt,
    });

    const latency = latencyMap.get(key);
    const p50 = latency?.p50_ms != null ? Math.round(Number(latency.p50_ms)) : null;
    const p95 = latency?.p95_ms != null ? Math.round(Number(latency.p95_ms)) : null;

    const existing = await db
      .select({ status: alertEndpointHealth.status })
      .from(alertEndpointHealth)
      .where(and(
        eq(alertEndpointHealth.tenantId, tenantId),
        eq(alertEndpointHealth.window, window),
        eq(alertEndpointHealth.destinationType, row.destination_type),
        eq(alertEndpointHealth.destination, row.destination),
      ))
      .limit(1);

    await db
      .insert(alertEndpointHealth)
      .values({
        tenantId,
        window,
        destinationType: row.destination_type,
        destination: row.destination,
        status,
        attemptsTotal,
        attemptsSuccess,
        successRate,
        p50Ms: p50,
        p95Ms: p95,
        consecutiveFailures,
        lastSuccessAt,
        lastFailureAt,
        updatedAt: capturedNow,
      })
      .onConflictDoUpdate({
        target: [
          alertEndpointHealth.tenantId,
          alertEndpointHealth.window,
          alertEndpointHealth.destinationType,
          alertEndpointHealth.destination,
        ],
        set: {
          status,
          attemptsTotal,
          attemptsSuccess,
          successRate,
          p50Ms: p50,
          p95Ms: p95,
          consecutiveFailures,
          lastSuccessAt,
          lastFailureAt,
          updatedAt: capturedNow,
        },
      });

    upserted += 1;

    const prevStatus = existing?.[0]?.status as EndpointStatus | undefined;
    if (prevStatus && prevStatus !== status) {
      const destinationKey = makeDestinationKey(row.destination_type, row.destination);
      const action = status === "OK"
        ? "ALERT.ENDPOINT_HEALTH_RECOVERED"
        : status === "DOWN"
          ? "ALERT.ENDPOINT_HEALTH_DOWN"
          : "ALERT.ENDPOINT_HEALTH_DEGRADED";
      await writeAuditEvent(auditContext, {
        action,
        resourceType: "ALERT_DESTINATION",
        resourceId: destinationKey,
        status: "SUCCESS",
        severity: "SECURITY",
        message: `Endpoint ${status.toLowerCase()} (${row.destination_type})`,
        metadata: {
          window,
          destination_type: row.destination_type,
          destination_key: destinationKey,
          destination: row.destination,
          old_status: prevStatus,
          new_status: status,
          success_rate: Number(successRate.toFixed(3)),
          consecutive_failures: consecutiveFailures,
          p95_ms: p95,
        },
      });

      try {
        if (status === "DOWN") {
          await openOrAttachIncident({
            tenantId,
            type: "ENDPOINT_DOWN",
            destinationKey,
            severity: "HIGH",
            title: `Endpoint down (${window} ${row.destination_type})`,
            summary: `Endpoint ${destinationKey} is DOWN`,
            now: capturedNow,
          });
        } else if (status === "OK") {
          await resolveIncidentIfExists({
            tenantId,
            type: "ENDPOINT_DOWN",
            destinationKey,
            now: capturedNow,
          });
        }
      } catch {
        // do not fail endpoint health compute due to incident writes
      }

      try {
        await dispatchEndpointHealthSystemAlert({
          tenantId,
          window,
          destinationType: row.destination_type,
          destination: row.destination,
          status,
          metrics: {
            success_rate: Number(successRate.toFixed(3)),
            p95_ms: p95,
            last_success_at: lastSuccessAt,
            last_failure_at: lastFailureAt,
          },
          computedAt: capturedNow,
        });
      } catch {
        // do not fail endpoint health compute due to alert dispatch errors
      }
    }
  }

  return { window, upserted, skipped: false } as const;
}

export async function getEndpointHealthSnapshot({
  tenantId,
  window,
  destinationType,
  destination,
}: {
  tenantId: string;
  window: EndpointWindow;
  destinationType: string;
  destination: string;
}) {
  const [row] = await db
    .select()
    .from(alertEndpointHealth)
    .where(and(
      eq(alertEndpointHealth.tenantId, tenantId),
      eq(alertEndpointHealth.window, window),
      eq(alertEndpointHealth.destinationType, destinationType),
      eq(alertEndpointHealth.destination, destination),
    ))
    .limit(1);
  return row ?? null;
}

export async function listEndpointHealth({
  tenantId,
  window,
  status,
}: {
  tenantId: string;
  window: EndpointWindow;
  status?: EndpointStatus;
}) {
  const rows = await db
    .select()
    .from(alertEndpointHealth)
    .where(and(
      eq(alertEndpointHealth.tenantId, tenantId),
      eq(alertEndpointHealth.window, window),
      status ? eq(alertEndpointHealth.status, status) : sql`TRUE`,
    ))
    .orderBy(desc(alertEndpointHealth.updatedAt));

  const items = rows.map((row) => ({
    id: row.id,
    tenant_id: row.tenantId,
    destination_type: row.destinationType,
    destination: row.destination,
    window: row.window,
    status: row.status,
    attempts_total: row.attemptsTotal,
    attempts_success: row.attemptsSuccess,
    success_rate: row.successRate,
    p50_ms: row.p50Ms,
    p95_ms: row.p95Ms,
    consecutive_failures: row.consecutiveFailures,
    last_success_at: row.lastSuccessAt,
    last_failure_at: row.lastFailureAt,
    updated_at: row.updatedAt,
  }));

  return {
    version: "1",
    window,
    items,
    summary: summarize(items as Array<{ status: EndpointStatus }>),
  };
}
