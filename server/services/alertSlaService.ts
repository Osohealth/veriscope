import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliverySlaWindows, alertDestinationStates, alertSlaThresholds } from "@shared/schema";
import { writeAuditEvent } from "./auditLog";
import { openOrAttachIncident, resolveIncidentIfExists } from "./incidentService";
import { dispatchSlaSystemAlert } from "./alertDispatcher";
import type { AuditContext } from "../middleware/requestContext";
import { makeDestinationKey } from "./destinationKey";
import { resolveSlaThresholds } from "./alertDestinationOverridesService";

const DEFAULT_SLA_THRESHOLDS = {
  "24h": {
    WEBHOOK: { p95_ms_threshold: 2000, success_rate_threshold: 0.98 },
    EMAIL: { p95_ms_threshold: 60000, success_rate_threshold: 0.95 },
  },
  "7d": {
    WEBHOOK: { p95_ms_threshold: 3000, success_rate_threshold: 0.985 },
    EMAIL: { p95_ms_threshold: 90000, success_rate_threshold: 0.97 },
  },
} as const;

type SlaWindow = "24h" | "7d";
type DestinationType = "WEBHOOK" | "EMAIL";
type ThresholdConfig = {
  p95_ms_threshold: number;
  success_rate_threshold: number;
  source: "DEFAULT" | "CUSTOM";
};

const WINDOWS = [
  { key: "24h", durationMs: 24 * 60 * 60 * 1000 },
  { key: "7d", durationMs: 7 * 24 * 60 * 60 * 1000 },
];

const roundToMinute = (value: Date) =>
  new Date(Math.floor(value.getTime() / 60000) * 60000);

const toNumber = (value: unknown, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

type ComputeSlaInput = {
  tenantId: string;
  now?: Date;
  auditContext?: AuditContext;
  windows?: Array<SlaWindow>;
  skipLock?: boolean;
};

export type AlertSlaRow = {
  destination_type: string;
  destination_key: string;
  window: string;
  window_start: string;
  attempts_total: number;
  attempts_success: number;
  attempts_failed: number;
  latency_p50_ms: number;
  latency_p95_ms: number;
  success_rate_pct: number;
  status: string;
  updated_at: string;
};

export const DEFAULT_SLA_DESTINATIONS: DestinationType[] = ["WEBHOOK", "EMAIL"];

export async function getSlaThresholds(tenantId: string, window: SlaWindow) {
  const defaults = DEFAULT_SLA_THRESHOLDS[window];
  const base: Record<DestinationType, ThresholdConfig> = {
    WEBHOOK: { ...defaults.WEBHOOK, source: "DEFAULT" },
    EMAIL: { ...defaults.EMAIL, source: "DEFAULT" },
  };

  const rows = await db
    .select({
      destinationType: alertSlaThresholds.destinationType,
      p95MsThreshold: alertSlaThresholds.p95MsThreshold,
      successRateThreshold: alertSlaThresholds.successRateThreshold,
    })
    .from(alertSlaThresholds)
    .where(and(eq(alertSlaThresholds.tenantId, tenantId), eq(alertSlaThresholds.window, window)));

  rows.forEach((row) => {
    const destination = String(row.destinationType).toUpperCase() as DestinationType;
    if (destination !== "WEBHOOK" && destination !== "EMAIL") return;
    base[destination] = {
      p95_ms_threshold: toNumber(row.p95MsThreshold, base[destination].p95_ms_threshold),
      success_rate_threshold: toNumber(row.successRateThreshold, base[destination].success_rate_threshold),
      source: "CUSTOM",
    };
  });

  return base;
}

export async function listAlertSlaThresholds(tenantId: string, window: SlaWindow) {
  const thresholds = await getSlaThresholds(tenantId, window);
  return {
    window,
    items: DEFAULT_SLA_DESTINATIONS.map((destination) => ({
      destination_type: destination,
      p95_ms_threshold: thresholds[destination].p95_ms_threshold,
      success_rate_threshold: thresholds[destination].success_rate_threshold,
      source: thresholds[destination].source,
    })),
  };
}

export async function upsertAlertSlaThreshold({
  tenantId,
  window,
  destinationType,
  p95MsThreshold,
  successRateThreshold,
}: {
  tenantId: string;
  window: SlaWindow;
  destinationType: DestinationType;
  p95MsThreshold: number;
  successRateThreshold: number;
}) {
  const [existing] = await db
    .select({
      p95MsThreshold: alertSlaThresholds.p95MsThreshold,
      successRateThreshold: alertSlaThresholds.successRateThreshold,
    })
    .from(alertSlaThresholds)
    .where(and(
      eq(alertSlaThresholds.tenantId, tenantId),
      eq(alertSlaThresholds.window, window),
      eq(alertSlaThresholds.destinationType, destinationType),
    ))
    .limit(1);

  const now = new Date();

  await db
    .insert(alertSlaThresholds)
    .values({
      tenantId,
      window,
      destinationType,
      p95MsThreshold,
      successRateThreshold,
      createdAt: now,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: [alertSlaThresholds.tenantId, alertSlaThresholds.window, alertSlaThresholds.destinationType],
      set: {
        p95MsThreshold,
        successRateThreshold,
        updatedAt: now,
      },
    });

  return {
    previous: existing
      ? {
          p95_ms_threshold: toNumber(existing.p95MsThreshold),
          success_rate_threshold: toNumber(existing.successRateThreshold),
        }
      : null,
    current: {
      p95_ms_threshold: p95MsThreshold,
      success_rate_threshold: successRateThreshold,
    },
  };
}

const normalizeSlaRow = (row: any): AlertSlaRow => ({
  destination_type: row.destination_type,
  destination_key: row.destination_key ?? row.destinationKey,
  window: row.window,
  window_start: row.window_start?.toISOString?.() ?? row.window_start,
  attempts_total: toNumber(row.attempts_total ?? 0),
  attempts_success: toNumber(row.attempts_success ?? 0),
  attempts_failed: toNumber(row.attempts_failed ?? 0),
  latency_p50_ms: toNumber(row.latency_p50_ms ?? 0),
  latency_p95_ms: toNumber(row.latency_p95_ms ?? 0),
  success_rate_pct: Number(Number(row.success_rate ?? 0).toFixed(2)),
  status: row.status,
  updated_at: row.computed_at?.toISOString?.() ?? row.computed_at,
});

const fetchLatestSlaRows = async (tenantId: string, window?: string) => {
  const windowFilter = window ? sql`AND "window" = ${window}` : sql``;
  const result = await db.execute(sql`
    SELECT DISTINCT ON (destination_type, destination_key, "window")
      destination_type,
      destination_key,
      "window",
      window_start,
      attempts_total,
      attempts_success,
      attempts_failed,
      latency_p50_ms,
      latency_p95_ms,
      success_rate,
      status,
      computed_at
    FROM alert_delivery_sla_windows
    WHERE tenant_id = ${tenantId}
      ${windowFilter}
    ORDER BY destination_type, destination_key, "window", window_start DESC, computed_at DESC
  `);
  const rows = (result as any).rows ?? result;
  return (rows ?? []).map(normalizeSlaRow);
};

const tryAdvisoryLock = async (tenantId: string, windowKey: string) => {
  const result = await db.execute(sql`
    SELECT pg_try_advisory_lock(hashtext(${tenantId}), hashtext(${windowKey})) AS locked
  `);
  const row = (result as any).rows?.[0] ?? (Array.isArray(result) ? result[0] : undefined);
  return Boolean(row?.locked);
};

const releaseAdvisoryLock = async (tenantId: string, windowKey: string) => {
  await db.execute(sql`
    SELECT pg_advisory_unlock(hashtext(${tenantId}), hashtext(${windowKey}))
  `);
};

export async function listAlertSlaWindows(tenantId: string, window?: string) {
  const items = await fetchLatestSlaRows(tenantId, window);
  const destinationKeys = Array.from(new Set(items.map((row) => row.destination_key))).filter(Boolean);
  const destinationStates = destinationKeys.length > 0
    ? await db
        .select({
          destinationKey: alertDestinationStates.destinationKey,
          state: alertDestinationStates.state,
          reason: alertDestinationStates.reason,
          resumeReadyAt: alertDestinationStates.resumeReadyAt,
        })
        .from(alertDestinationStates)
        .where(and(
          eq(alertDestinationStates.tenantId, tenantId),
          inArray(alertDestinationStates.destinationKey, destinationKeys),
        ))
    : [];
  const destinationStateMap = new Map(destinationStates.map((row) => [row.destinationKey, row]));

  const resolved = await Promise.all(items.map(async (row) => {
    const windowKey = row.window === "7d" ? "7d" : "24h";
    const thresholds = await resolveSlaThresholds({
      tenantId,
      window: windowKey as SlaWindow,
      destinationType: String(row.destination_type).toUpperCase() as DestinationType,
      destinationKey: row.destination_key,
    });
    const stateRow = destinationStateMap.get(row.destination_key);
    const state = stateRow ? String(stateRow.state ?? "ACTIVE").toUpperCase() : "ACTIVE";
    const resumeReadyAt = stateRow?.resumeReadyAt ? new Date(stateRow.resumeReadyAt) : null;
    const readyToResume = Boolean(state === "AUTO_PAUSED" && resumeReadyAt && resumeReadyAt.getTime() <= Date.now());
    return { row, thresholds };
  }));

  return {
    generated_at: new Date().toISOString(),
    items: resolved.map(({ row, thresholds }) => ({
      destination_key: row.destination_key,
      destination_type: row.destination_type,
      window: row.window,
      window_start: row.window_start,
      attempts_total: row.attempts_total,
      attempts_success: row.attempts_success,
      attempts_failed: row.attempts_failed,
      success_rate_pct: row.success_rate_pct,
      latency_p50_ms: row.latency_p50_ms,
      latency_p95_ms: row.latency_p95_ms,
      status: row.status,
      destination: (() => {
        const stateRow = destinationStateMap.get(row.destination_key);
        const state = stateRow ? String(stateRow.state ?? "ACTIVE").toUpperCase() : "ACTIVE";
        const resumeReadyAt = stateRow?.resumeReadyAt ? new Date(stateRow.resumeReadyAt) : null;
        return {
          destination_key: row.destination_key,
          state,
          reason: stateRow?.reason ?? null,
          ready_to_resume: Boolean(state === "AUTO_PAUSED" && resumeReadyAt && resumeReadyAt.getTime() <= Date.now()),
          resume_ready_at: resumeReadyAt ? resumeReadyAt.toISOString() : null,
        };
      })(),
      sla: {
        p95_ms: thresholds.p95_ms,
        success_rate_pct: thresholds.success_rate_min_pct,
        source: thresholds.source,
      },
      updated_at: row.updated_at,
    })),
  };
}

export async function getAlertSlaSummary(tenantId: string) {
  const rows = await fetchLatestSlaRows(tenantId);
  const grouped = rows.reduce<Record<string, AlertSlaRow[]>>((acc, row) => {
    acc[row.window] = acc[row.window] ?? [];
    acc[row.window].push(row);
    return acc;
  }, {});

  const summarizeRows = (windowRows: AlertSlaRow[]) => {
    const atRisk = windowRows.filter((row) => row.status === "AT_RISK");
    const ok = windowRows.filter((row) => row.status === "OK");
    const atRiskKeys = new Set(atRisk.map((row) => row.destination_key));
    const okKeys = new Set(ok.map((row) => row.destination_key));
    const worstCandidate = [...windowRows].sort((a, b) => {
      const statusRank = (value: string) => (value === "AT_RISK" ? 1 : 0);
      if (statusRank(b.status) !== statusRank(a.status)) {
        return statusRank(b.status) - statusRank(a.status);
      }
      if (b.latency_p95_ms !== a.latency_p95_ms) {
        return b.latency_p95_ms - a.latency_p95_ms;
      }
      return a.success_rate_pct - b.success_rate_pct;
    })[0];

    return {
      overall_status: atRiskKeys.size > 0 ? "AT_RISK" : "OK",
      at_risk_count: atRiskKeys.size,
      ok_count: okKeys.size,
      total: new Set(windowRows.map((row) => row.destination_key)).size,
      worst: worstCandidate
        ? {
            destination_type: worstCandidate.destination_type,
            destination_key: worstCandidate.destination_key,
            latency_p95_ms: worstCandidate.latency_p95_ms,
            success_rate_pct: worstCandidate.success_rate_pct,
          }
        : null,
    };
  };

  const items = Object.entries(grouped).map(([window, windowRows]) => {
    const overall = summarizeRows(windowRows);
    const byDestinationType = windowRows.reduce<Record<string, AlertSlaRow[]>>((acc, row) => {
      const key = row.destination_type;
      acc[key] = acc[key] ?? [];
      acc[key].push(row);
      return acc;
    }, {});

    const destinationSummaries = Object.entries(byDestinationType).reduce<Record<string, any>>((acc, [type, rowsForType]) => {
      acc[type] = summarizeRows(rowsForType);
      return acc;
    }, {});

    return {
      window,
      overall_status: overall.overall_status,
      at_risk_count: overall.at_risk_count,
      ok_count: overall.ok_count,
      worst: overall.worst,
      by_destination_type: destinationSummaries,
    };
  });

  return {
    generated_at: new Date().toISOString(),
    items,
  };
}

export async function computeAlertDeliverySlaWindows({
  tenantId,
  now = new Date(),
  auditContext,
  windows,
  skipLock = false,
}: ComputeSlaInput) {
  const computedAt = new Date(now);
  let windowsComputed = 0;
  let rowsUpserted = 0;
  const windowsSkipped: string[] = [];

  const selectedWindows = windows?.length ? WINDOWS.filter((w) => windows.includes(w.key as SlaWindow)) : WINDOWS;

  for (const window of selectedWindows) {
    const lockKey = `alert_sla_${window.key}`;
    const locked = skipLock ? true : await tryAdvisoryLock(tenantId, lockKey);
    if (!locked) {
      windowsSkipped.push(window.key);
      continue;
    }
    const windowStart = roundToMinute(
      new Date(now.getTime() - window.durationMs),
    );
    try {
      const metricsResult = await db.execute(sql`
        SELECT
          destination_type,
          endpoint,
          COUNT(*) FILTER (WHERE status NOT LIKE 'SKIPPED%') AS attempts_total,
          COUNT(*) FILTER (WHERE status = 'SENT') AS attempts_success,
          COUNT(*) FILTER (WHERE status NOT LIKE 'SKIPPED%' AND status != 'SENT') AS attempts_failed,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)
            FILTER (WHERE status = 'SENT' AND latency_ms IS NOT NULL) AS p50,
          percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
            FILTER (WHERE status = 'SENT' AND latency_ms IS NOT NULL) AS p95
        FROM alert_deliveries
        WHERE tenant_id = ${tenantId}
          AND created_at >= ${windowStart}
        GROUP BY destination_type, endpoint
      `);
      const metricRows = (metricsResult as any).rows ?? metricsResult;

      for (const row of metricRows ?? []) {
        const destinationType = String(row.destination_type ?? "").toUpperCase();
        const endpoint = String(row.endpoint ?? "");
        if (!destinationType || !endpoint) continue;

        const destinationKey = makeDestinationKey(destinationType, endpoint);
        const thresholds = await resolveSlaThresholds({
          tenantId,
          window: window.key as SlaWindow,
          destinationType: destinationType as DestinationType,
          destinationKey,
        });

        const attemptsTotal = toNumber(row.attempts_total ?? 0);
        const attemptsSuccess = toNumber(row.attempts_success ?? 0);
        const attemptsFailed = toNumber(row.attempts_failed ?? Math.max(attemptsTotal - attemptsSuccess, 0));
        const latencyP50Ms = Math.round(toNumber(row.p50 ?? 0));
        const latencyP95Ms = Math.round(toNumber(row.p95 ?? 0));
        const successRate = attemptsTotal === 0
          ? 100
          : Number(((attemptsSuccess / attemptsTotal) * 100).toFixed(2));

        const status =
          latencyP95Ms > thresholds.p95_ms || successRate < thresholds.success_rate_min_pct
            ? "AT_RISK"
            : "OK";

        const previousResult = await db.execute(sql`
          SELECT status
          FROM alert_delivery_sla_windows
          WHERE tenant_id = ${tenantId}
            AND destination_type = ${destinationType}
            AND destination_key = ${destinationKey}
            AND "window" = ${window.key}
            AND window_start = ${windowStart}
          LIMIT 1
        `);
        const previousRow = (previousResult as any).rows?.[0] ?? (Array.isArray(previousResult) ? previousResult[0] : undefined);
        const previousStatus = previousRow?.status ?? null;

        await db.execute(sql`
          INSERT INTO alert_delivery_sla_windows (
            tenant_id,
            destination_type,
            destination_key,
            "window",
            window_start,
            attempts_total,
            attempts_success,
            attempts_failed,
            latency_p50_ms,
            latency_p95_ms,
            success_rate,
            status,
            computed_at
          ) VALUES (
            ${tenantId},
            ${destinationType},
            ${destinationKey},
            ${window.key},
            ${windowStart},
            ${attemptsTotal},
            ${attemptsSuccess},
            ${attemptsFailed},
            ${latencyP50Ms},
            ${latencyP95Ms},
            ${successRate},
            ${status},
            ${computedAt}
          )
          ON CONFLICT (tenant_id, "window", destination_type, destination_key, window_start)
          DO UPDATE SET
            attempts_total = EXCLUDED.attempts_total,
            attempts_success = EXCLUDED.attempts_success,
            attempts_failed = EXCLUDED.attempts_failed,
            latency_p50_ms = EXCLUDED.latency_p50_ms,
            latency_p95_ms = EXCLUDED.latency_p95_ms,
            success_rate = EXCLUDED.success_rate,
            status = EXCLUDED.status,
            computed_at = EXCLUDED.computed_at
        `);

        rowsUpserted += 1;
        windowsComputed += 1;

        if (previousStatus && previousStatus !== status) {
          const action = status === "AT_RISK" ? "ALERT.SLA_AT_RISK" : "ALERT.SLA_RECOVERED";
          await writeAuditEvent(auditContext, {
            action,
            resourceType: "ALERT_DELIVERY_SLA",
            severity: status === "AT_RISK" ? "WARN" : "INFO",
            status: "SUCCESS",
            message: status === "AT_RISK" ? "Delivery SLA at risk" : "Delivery SLA recovered",
            metadata: {
              window: window.key,
              destination_type: destinationType,
              destination_key: destinationKey,
              latency_p95_ms: latencyP95Ms,
              success_rate: successRate,
            },
            tenantId,
          });

          try {
            if (status === "AT_RISK") {
              await openOrAttachIncident({
                tenantId,
                type: "SLA_AT_RISK",
                destinationKey,
                severity: "HIGH",
                title: `SLA at risk (${window.key} ${destinationType})`,
                summary: `SLA degraded for ${destinationType}`,
                now: computedAt,
              });
            } else if (status === "OK") {
              await resolveIncidentIfExists({
                tenantId,
                type: "SLA_AT_RISK",
                destinationKey,
                now: computedAt,
              });
            }
          } catch {
            // do not fail SLA compute due to incident writes
          }

          try {
            await dispatchSlaSystemAlert({
              tenantId,
              window: window.key as SlaWindow,
              destinationType: destinationType,
              destinationKey,
              status: status === "AT_RISK" ? "AT_RISK" : "OK",
              metrics: {
                p95_ms: latencyP95Ms,
                success_rate_pct: successRate,
              },
              thresholds: {
                p95_ms: thresholds.p95_ms,
                success_rate_pct: thresholds.success_rate_min_pct,
              },
              thresholdSource: thresholds.source,
              computedAt: computedAt,
            });
          } catch {
            // do not fail SLA compute due to alert dispatch errors
          }
        }
      }
    } finally {
      if (!skipLock) {
        await releaseAdvisoryLock(tenantId, lockKey);
      }
    }
  }

  return { windowsComputed, rowsUpserted, windowsSkipped };
}

export async function backfillAlertDeliverySlaWindows({
  tenantId,
  days,
  auditContext,
  window,
}: {
  tenantId: string;
  days: number;
  auditContext?: AuditContext;
  window: "24h" | "7d";
}) {
  const windowConfig = WINDOWS.find((item) => item.key === window);
  if (!windowConfig) {
    return { windowsComputed: 0, rowsUpserted: 0, skipped: true, daysUsed: 0 };
  }

  const maxDays = Math.min(Math.max(Math.floor(days), 1), 365);
  let windowsComputed = 0;
  let rowsUpserted = 0;
  const base = new Date();

  const lockKey = `alert_sla_${windowConfig.key}`;
  const locked = await tryAdvisoryLock(tenantId, lockKey);
  if (!locked) {
    return { windowsComputed, rowsUpserted, skipped: true, daysUsed: maxDays };
  }

  try {
    for (let offset = maxDays - 1; offset >= 0; offset -= 1) {
      const now = new Date(base.getTime() - offset * windowConfig.durationMs);
      const result = await computeAlertDeliverySlaWindows({
        tenantId,
        now,
        auditContext,
        windows: [windowConfig.key as "24h" | "7d"],
        skipLock: true,
      });
      windowsComputed += result.windowsComputed;
      rowsUpserted += result.rowsUpserted;
    }
  } finally {
    await releaseAdvisoryLock(tenantId, lockKey);
  }

  return { windowsComputed, rowsUpserted, skipped: false, daysUsed: maxDays };
}

let slaSchedulerStarted = false;

export function startAlertSlaScheduler(tenantId: string, auditContext?: AuditContext) {
  if (slaSchedulerStarted) return;
  if (process.env.DEV_SLA_SCHEDULER !== "true") return;
  slaSchedulerStarted = true;

  setInterval(() => {
    void computeAlertDeliverySlaWindows({ tenantId, windows: ["24h"], auditContext }).catch(() => undefined);
  }, 10 * 60 * 1000);

  setInterval(() => {
    void computeAlertDeliverySlaWindows({ tenantId, windows: ["7d"], auditContext }).catch(() => undefined);
  }, 2 * 60 * 60 * 1000);
}
