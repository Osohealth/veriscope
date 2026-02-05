import { and, desc, eq, gte, inArray, lt, lte, sql } from "drizzle-orm";
import { db } from "../db";
import { logger } from "../middleware/observability";
import { portDailyBaselines, signals } from "@shared/schema";
import { ConfidenceBand, SEVERITY_RANK, SignalSeverity } from "@shared/signalTypes";

const PORT_ENTITY_TYPE = "port";
const MAX_LIST_LIMIT = 500;
const MIN_HISTORY_DAYS = 10;
const HISTORY_WINDOW_DAYS = 30;
const COMPLETENESS_WARN_PCT = 90;

const FOLLOWUP_CHECKS = [
  "weather",
  "strikes",
  "terminal outages",
  "river/lock constraints",
];
const IMPACT_LINES = [
  "Potential impact: delayed discharge/loading -> schedule slippage -> freight spreads / prompt supply timing risk.",
];


export interface SignalListFilters {
  portId?: string;
  signalType?: string;
  severity?: string;
  severityMin?: string;
  dayFrom?: Date | null;
  dayTo?: Date | null;
  limit?: number;
  offset?: number;
  clustered?: boolean;
}

const toNumber = (value: unknown): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const num = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(num)) {
    return null;
  }
  return num;
};

const formatDeltaPct = (deltaPct: number | null): string => {
  if (deltaPct === null) {
    return "n/a";
  }
  const sign = deltaPct >= 0 ? "+" : "";
  return `${sign}${deltaPct.toFixed(1)}%`;
};

export const formatSignalDay = (date: Date): string => {
  return date.toISOString().slice(0, 10);
};

export const parseSignalDay = (input?: string | null): Date | null => {
  if (!input) {
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input)) {
    return null;
  }
  const day = new Date(`${input}T00:00:00Z`);
  if (Number.isNaN(day.getTime())) {
    return null;
  }
  return day;
};

export const getUtcDayStart = (date: Date): Date => {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
};

export const getYesterdayUtcDay = (): Date => {
  const now = new Date();
  const todayUtc = getUtcDayStart(now);
  const yesterdayUtc = new Date(todayUtc);
  yesterdayUtc.setUTCDate(todayUtc.getUTCDate() - 1);
  return yesterdayUtc;
};

const computeZscore = (value: number, baseline: number | null, stddev: number | null): number | null => {
  if (baseline === null || stddev === null || stddev === 0) {
    return null;
  }
  return (value - baseline) / stddev;
};

const computeDeltaPct = (value: number, baseline: number | null): number | null => {
  if (baseline === null || baseline === 0) {
    return null;
  }
  return ((value - baseline) / baseline) * 100;
};

const toZscoreSeverity = (absZ: number): SignalSeverity => {
  if (absZ >= 5) return "CRITICAL";
  if (absZ >= 3) return "HIGH";
  return "MEDIUM";
};

const toMultiplierSeverity = (multiplier: number): SignalSeverity => {
  if (multiplier >= 4) return "CRITICAL";
  if (multiplier >= 2) return "HIGH";
  return "MEDIUM";
};

const clampNumber = (value: number, min: number, max: number) => {
  return Math.min(max, Math.max(min, value));
};

const confidenceFromZscore = (absZ: number) => {
  const score = Math.min(1, absZ / 6);
  const band: ConfidenceBand = score >= 0.8 ? "HIGH" : score >= 0.5 ? "MEDIUM" : "LOW";
  return { score, band, method: "zscore_30d" as const };
};

const confidenceFromMultiplier = (multiplier: number) => {
  const score = Math.min(1, Math.max(0, (multiplier - 1) / 3));
  const band: ConfidenceBand = score >= 0.8 ? "HIGH" : score >= 0.5 ? "MEDIUM" : "LOW";
  return { score, band, method: "multiplier_30d" as const };
};

const adjustConfidenceForCompleteness = (
  confidence: { score: number; band: ConfidenceBand; method: string },
  completenessPct: number,
) => {
  if (completenessPct >= COMPLETENESS_WARN_PCT) {
    return confidence;
  }
  const score = clampNumber(confidence.score * 0.75, 0, 1);
  let band: ConfidenceBand = confidence.band;
  if (completenessPct < 85) {
    band = "LOW";
  } else if (band === "HIGH") {
    band = "MEDIUM";
  }
  return { score, band, method: confidence.method };
};

const computeCompleteness = (historyDays: number) => {
  const pct = clampNumber(Math.round((historyDays / HISTORY_WINDOW_DAYS) * 100), 0, 100);
  const missing = Math.max(0, HISTORY_WINDOW_DAYS - historyDays);
  return { pct, missing };
};

const buildExplanation = (detailLine: string, whyLine: string) => {
  return [
    `${detailLine} ${whyLine}`,
    IMPACT_LINES[0],
    `Next checks: ${FOLLOWUP_CHECKS.join(", ")}.`,
  ].join("\n");
};

export async function evaluatePortSignalsForDay(
  day: Date,
  opts: { portIds?: string[] } = {},
) {
  const targetDay = getUtcDayStart(day);
  const conditions = [eq(portDailyBaselines.day, targetDay)];

  if (opts.portIds && opts.portIds.length > 0) {
    conditions.push(inArray(portDailyBaselines.portId, opts.portIds));
  }

  const whereClause = conditions.length === 1 ? conditions[0] : and(...conditions);
  const baselineRows = await db.select().from(portDailyBaselines).where(whereClause);

  const historyCounts = new Map<string, number>();
  if (baselineRows.length > 0) {
    const portIds = baselineRows.map((row) => row.portId);
    const historyRows = await db
      .select({
        portId: portDailyBaselines.portId,
        count: sql<number>`count(*)`,
      })
      .from(portDailyBaselines)
      .where(
        and(
          inArray(portDailyBaselines.portId, portIds),
          lt(portDailyBaselines.day, targetDay),
          gte(portDailyBaselines.day, sql`${targetDay}::date - interval '30 days'`),
        ),
      )
      .groupBy(portDailyBaselines.portId);

    for (const row of historyRows) {
      historyCounts.set(row.portId, Number(row.count ?? 0));
    }
  }

  const signalRows: (typeof signals.$inferInsert)[] = [];

  for (const row of baselineRows) {
    const portId = row.portId;
    const dayKey = formatSignalDay(targetDay);

    const arrivals = toNumber(row.arrivals) ?? 0;
    const arrivalsAvg = toNumber(row.arrivals30dAvg);
    const arrivalsStd = toNumber(row.arrivals30dStd);

    const dwellHours = toNumber(row.avgDwellHours);
    const dwellAvg = toNumber(row.dwell30dAvg);
    const dwellStd = toNumber(row.dwell30dStd);

    const openCalls = toNumber(row.openCalls) ?? 0;
    const openCallsAvg = toNumber(row.openCalls30dAvg);

    const historyDays = historyCounts.get(portId) ?? 0;
    const hasEnoughHistory = historyDays >= MIN_HISTORY_DAYS;
    const { pct: completenessPct, missing: missingPoints } = computeCompleteness(historyDays);

    const portSignals: (typeof signals.$inferInsert)[] = [];
    const clusterSummaryParts: string[] = [];
    const clusterId = `PORT_DISRUPTION:${portId}:${dayKey}`;
    const clusterKey = `PORT_DISRUPTION|${portId}|${dayKey}`;

    if (hasEnoughHistory && arrivalsAvg !== null && arrivalsStd !== null && arrivalsStd > 0) {
      const z = computeZscore(arrivals, arrivalsAvg, arrivalsStd);
      if (z !== null) {
        const absZ = Math.abs(z);
        if (absZ >= 2) {
          const deltaPct = computeDeltaPct(arrivals, arrivalsAvg);
          const severity = toZscoreSeverity(absZ);
          const confidence = adjustConfidenceForCompleteness(confidenceFromZscore(absZ), completenessPct);
          const detailLine = `Arrivals ${arrivals} vs baseline ${arrivalsAvg.toFixed(1)} (${formatDeltaPct(deltaPct)}, z=${z.toFixed(2)}).`;
          const explanation = buildExplanation(
            detailLine,
            "Unusual arrival volume can signal demand shifts or operational disruptions.",
          );
          clusterSummaryParts.push(`Arrivals ${formatDeltaPct(deltaPct)}`);

          portSignals.push({
            signalType: "PORT_ARRIVALS_ANOMALY",
            entityType: PORT_ENTITY_TYPE,
            entityId: portId,
            day: targetDay,
            severity,
            value: arrivals,
            baseline: arrivalsAvg,
            stddev: arrivalsStd,
            zscore: z,
            deltaPct,
            confidenceScore: confidence.score,
            confidenceBand: confidence.band,
            method: confidence.method,
            clusterId,
            clusterKey,
            clusterType: "PORT_DISRUPTION",
            explanation,
            metadata: {
              metric: "arrivals",
              baseline_window: "30d",
              day: dayKey,
              history_days: historyDays,
              min_history_days: MIN_HISTORY_DAYS,
              drivers: [
                {
                  metric: "arrivals",
                  value: arrivals,
                  baseline: arrivalsAvg,
                  stddev: arrivalsStd,
                  delta_pct: deltaPct,
                  zscore: z,
                },
              ],
              impact: IMPACT_LINES,
              recommended_followups: FOLLOWUP_CHECKS,
              data_quality: {
                history_days_used: historyDays,
                completeness_pct: completenessPct,
                missing_points: missingPoints,
              },
              driver_metrics: [
                {
                  metric: "arrivals",
                  value: arrivals,
                  baseline: arrivalsAvg,
                  stddev: arrivalsStd,
                  delta_pct: deltaPct,
                  zscore: z,
                },
              ],
            },
          });
        }
      }
    }

    if (hasEnoughHistory && dwellHours !== null && dwellAvg !== null && dwellStd !== null && dwellStd > 0) {
      const z = computeZscore(dwellHours, dwellAvg, dwellStd);
      if (z !== null && z >= 2) {
        const absZ = Math.abs(z);
        const deltaPct = computeDeltaPct(dwellHours, dwellAvg);
        const severity = toZscoreSeverity(absZ);
        const confidence = adjustConfidenceForCompleteness(confidenceFromZscore(absZ), completenessPct);
        const detailLine = `Avg dwell ${dwellHours.toFixed(1)}h vs baseline ${dwellAvg.toFixed(1)}h (${formatDeltaPct(deltaPct)}, z=${z.toFixed(2)}).`;
        const explanation = buildExplanation(
          detailLine,
          "Longer dwell times can indicate congestion and delay risk.",
        );
        clusterSummaryParts.push(`Dwell ${formatDeltaPct(deltaPct)}`);

        portSignals.push({
          signalType: "PORT_DWELL_SPIKE",
          entityType: PORT_ENTITY_TYPE,
          entityId: portId,
          day: targetDay,
          severity,
          value: dwellHours,
          baseline: dwellAvg,
          stddev: dwellStd,
          zscore: z,
          deltaPct,
          confidenceScore: confidence.score,
          confidenceBand: confidence.band,
          method: confidence.method,
          clusterId,
          clusterKey,
          clusterType: "PORT_DISRUPTION",
          explanation,
          metadata: {
            metric: "avg_dwell_hours",
            baseline_window: "30d",
            day: dayKey,
            history_days: historyDays,
            min_history_days: MIN_HISTORY_DAYS,
            drivers: [
              {
                metric: "avg_dwell_hours",
                value: dwellHours,
                baseline: dwellAvg,
                stddev: dwellStd,
                delta_pct: deltaPct,
                zscore: z,
              },
            ],
            impact: IMPACT_LINES,
            recommended_followups: FOLLOWUP_CHECKS,
            data_quality: {
              history_days_used: historyDays,
              completeness_pct: completenessPct,
              missing_points: missingPoints,
            },
            driver_metrics: [
              {
                metric: "avg_dwell_hours",
                value: dwellHours,
                baseline: dwellAvg,
                stddev: dwellStd,
                delta_pct: deltaPct,
                zscore: z,
              },
            ],
          },
        });
      }
    }

    if (hasEnoughHistory && openCallsAvg !== null && openCallsAvg >= 5) {
      const multiplier = openCallsAvg > 0 ? openCalls / openCallsAvg : 0;
      if (multiplier >= 1.5) {
        const deltaPct = computeDeltaPct(openCalls, openCallsAvg);
        const severity = toMultiplierSeverity(multiplier);
        const confidence = adjustConfidenceForCompleteness(confidenceFromMultiplier(multiplier), completenessPct);
        const detailLine = `Open calls ${openCalls} vs baseline ${openCallsAvg.toFixed(1)} (${multiplier.toFixed(2)}x, ${formatDeltaPct(deltaPct)}).`;
        const explanation = buildExplanation(
          detailLine,
          "Elevated open calls suggest backlog is building at the port.",
        );
        clusterSummaryParts.push(`Open calls ${multiplier.toFixed(2)}x`);

        portSignals.push({
          signalType: "PORT_CONGESTION_BUILDUP",
          entityType: PORT_ENTITY_TYPE,
          entityId: portId,
          day: targetDay,
          severity,
          value: openCalls,
          baseline: openCallsAvg,
          stddev: null,
          zscore: null,
          deltaPct,
          confidenceScore: confidence.score,
          confidenceBand: confidence.band,
          method: confidence.method,
          clusterId,
          clusterKey,
          clusterType: "PORT_DISRUPTION",
          explanation,
          metadata: {
            metric: "open_calls",
            baseline_window: "30d",
            multiplier,
            day: dayKey,
            history_days: historyDays,
            min_history_days: MIN_HISTORY_DAYS,
            drivers: [
              {
                metric: "open_calls",
                value: openCalls,
                baseline: openCallsAvg,
                stddev: null,
                delta_pct: deltaPct,
                multiplier,
              },
            ],
            impact: IMPACT_LINES,
            recommended_followups: FOLLOWUP_CHECKS,
            data_quality: {
              history_days_used: historyDays,
              completeness_pct: completenessPct,
              missing_points: missingPoints,
            },
            driver_metrics: [
              {
                metric: "open_calls",
                value: openCalls,
                baseline: openCallsAvg,
                stddev: null,
                delta_pct: deltaPct,
                multiplier,
              },
            ],
          },
        });
      }
    }

    if (portSignals.length > 0) {
      const clusterSummary = clusterSummaryParts.join(", ");
      const clusterSeverity = portSignals.reduce<SignalSeverity>((max, signal) => {
        const next = signal.severity as SignalSeverity;
        return SEVERITY_RANK[next] > SEVERITY_RANK[max] ? next : max;
      }, "LOW");
      for (const signal of portSignals) {
        signal.clusterSummary = clusterSummary;
        signal.clusterSeverity = clusterSeverity;
      }
      signalRows.push(...portSignals);
    }
  }

  if (signalRows.length === 0) {
    return { upserted: 0, signals: [] as { id: string }[] };
  }

  const upserted = await db
    .insert(signals)
    .values(signalRows)
    .onConflictDoUpdate({
      target: [signals.signalType, signals.entityType, signals.entityId, signals.day],
      set: {
        severity: sql`excluded.severity`,
        value: sql`excluded.value`,
        baseline: sql`excluded.baseline`,
        stddev: sql`excluded.stddev`,
        zscore: sql`excluded.zscore`,
        deltaPct: sql`excluded.delta_pct`,
        confidenceScore: sql`excluded.confidence_score`,
        confidenceBand: sql`excluded.confidence_band`,
        method: sql`excluded.method`,
        clusterId: sql`excluded.cluster_id`,
        clusterKey: sql`excluded.cluster_key`,
        clusterType: sql`excluded.cluster_type`,
        clusterSeverity: sql`excluded.cluster_severity`,
        clusterSummary: sql`excluded.cluster_summary`,
        explanation: sql`excluded.explanation`,
        metadata: sql`excluded.metadata`,
      },
    })
    .returning({ id: signals.id });

  return { upserted: upserted.length, signals: upserted };
}

export async function listSignals(filters: SignalListFilters) {
  const limit = Math.min(Math.max(filters.limit ?? 50, 1), MAX_LIST_LIMIT);
  const offset = Math.max(filters.offset ?? 0, 0);
  const clustered = filters.clustered ?? false;

  const normalizeSignalRow = (row: any) => ({
    ...row,
    signalType: row.signalType ?? row.signal_type,
    entityType: row.entityType ?? row.entity_type,
    entityId: row.entityId ?? row.entity_id,
    deltaPct: row.deltaPct ?? row.delta_pct,
    confidenceScore: row.confidenceScore ?? row.confidence_score,
    confidenceBand: row.confidenceBand ?? row.confidence_band,
    clusterId: row.clusterId ?? row.cluster_id,
    clusterKey: row.clusterKey ?? row.cluster_key,
    clusterType: row.clusterType ?? row.cluster_type,
    clusterSeverity: row.clusterSeverity ?? row.cluster_severity,
    clusterSummary: row.clusterSummary ?? row.cluster_summary,
    createdAt: row.createdAt ?? row.created_at,
  });

  const conditions = [] as any[];

  if (filters.portId) {
    conditions.push(eq(signals.entityType, PORT_ENTITY_TYPE));
    conditions.push(eq(signals.entityId, filters.portId));
  }

  if (filters.signalType) {
    conditions.push(eq(signals.signalType, filters.signalType));
  }

  if (filters.severity) {
    conditions.push(eq(signals.severity, filters.severity));
  }

  if (filters.severityMin) {
    const rank = SEVERITY_RANK[filters.severityMin as SignalSeverity];
    if (rank !== undefined) {
      conditions.push(
        sql`CASE ${signals.severity}
            WHEN 'LOW' THEN 1
            WHEN 'MEDIUM' THEN 2
            WHEN 'HIGH' THEN 3
            WHEN 'CRITICAL' THEN 4
            ELSE 0
          END >= ${rank}`,
      );
    }
  }

  if (filters.dayFrom) {
    conditions.push(gte(signals.day, filters.dayFrom));
  }

  if (filters.dayTo) {
    conditions.push(lte(signals.day, filters.dayTo));
  }

  const whereSql = conditions.length > 0 ? sql`${sql.join(conditions, sql` AND `)}` : sql`1=1`;

  const severityRankSql = sql`CASE ${signals.severity}
      WHEN 'LOW' THEN 1
      WHEN 'MEDIUM' THEN 2
      WHEN 'HIGH' THEN 3
      WHEN 'CRITICAL' THEN 4
      ELSE 0
    END`;
  const clusterSeverityRankSql = sql`CASE ${signals.clusterSeverity}
      WHEN 'LOW' THEN 1
      WHEN 'MEDIUM' THEN 2
      WHEN 'HIGH' THEN 3
      WHEN 'CRITICAL' THEN 4
      ELSE 0
    END`;

  if (clustered) {
    const itemsResult = await db.execute(sql`
      SELECT *
      FROM (
        SELECT DISTINCT ON (cluster_id) *
        FROM ${signals}
        WHERE ${whereSql}
        ORDER BY cluster_id, ${clusterSeverityRankSql} DESC, confidence_score DESC, created_at DESC
      ) clustered_signals
      LIMIT ${limit} OFFSET ${offset}
    `);

    const totalResult = await db.execute(sql`
      SELECT count(*)::int AS count
      FROM (
        SELECT DISTINCT cluster_id
        FROM ${signals}
        WHERE ${whereSql}
      ) clusters
    `);

    const total = Number((totalResult as any).rows?.[0]?.count ?? 0);
    const items = ((itemsResult as any).rows ?? [])
      .map(normalizeSignalRow)
      .sort((a, b) => {
        const rankA = SEVERITY_RANK[(a.clusterSeverity ?? "LOW") as SignalSeverity] ?? 0;
        const rankB = SEVERITY_RANK[(b.clusterSeverity ?? "LOW") as SignalSeverity] ?? 0;
        if (rankA !== rankB) return rankB - rankA;
        const confA = a.confidenceScore ?? 0;
        const confB = b.confidenceScore ?? 0;
        if (confA !== confB) return confB - confA;
        const timeA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
        const timeB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
        return timeB - timeA;
      });
    return { items, total };
  }

  const items = await db
    .select()
    .from(signals)
    .where(sql`${whereSql}`)
    .orderBy(
      sql`${severityRankSql} DESC`,
      desc(signals.confidenceScore),
      sql`ABS(${signals.zscore}) DESC NULLS LAST`,
      desc(signals.createdAt),
    )
    .limit(limit)
    .offset(offset);

  const totalResult = await db
    .select({ count: sql<number>`count(*)` })
    .from(signals)
    .where(sql`${whereSql}`);

  const total = Number(totalResult[0]?.count ?? 0);

  return { items, total };
}

export async function getSignalById(id: string) {
  const result = await db.select().from(signals).where(eq(signals.id, id)).limit(1);
  return result[0] ?? null;
}

export async function runDailySignalEngine() {
  const day = getYesterdayUtcDay();
  const result = await evaluatePortSignalsForDay(day);
  logger.info("Signal engine run complete", {
    day: formatSignalDay(day),
    upserted: result.upserted,
  });
  return result;
}
