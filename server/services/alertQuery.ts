import { desc, eq, sql, and } from "drizzle-orm";
import { db } from "../db";
import { signals } from "@shared/schema";
import { SEVERITY_RANK, type SignalSeverity } from "@shared/signalTypes";
import { buildSignalResponse } from "./signalResponse";

type AlertCandidateOptions = {
  day?: string;
  entityType?: "port";
  entityId?: string;
  severityMin?: SignalSeverity;
};

const parseDay = (input?: string) => {
  if (!input) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input)) return null;
  const day = new Date(`${input}T00:00:00Z`);
  return Number.isNaN(day.getTime()) ? null : day;
};

const severityRankSql = sql`CASE ${signals.clusterSeverity}
  WHEN 'LOW' THEN 1
  WHEN 'MEDIUM' THEN 2
  WHEN 'HIGH' THEN 3
  WHEN 'CRITICAL' THEN 4
  ELSE 0
END`;

export async function getAlertCandidates(options: AlertCandidateOptions = {}) {
  const conditions = [] as any[];

  if (options.entityType) {
    conditions.push(eq(signals.entityType, options.entityType));
  }
  if (options.entityId) {
    conditions.push(eq(signals.entityId, options.entityId));
  }

  let day = parseDay(options.day ?? undefined);
  if (!day) {
    const whereSql = conditions.length > 0 ? sql`${sql.join(conditions, sql` AND `)}` : sql`1=1`;
    const latestResult = await db.execute(sql`
      SELECT max(day) AS max_day
      FROM ${signals}
      WHERE ${whereSql}
    `);
    const latestDay = (latestResult as any).rows?.[0]?.max_day;
    if (latestDay) {
      day = latestDay instanceof Date ? latestDay : new Date(latestDay);
    }
  }
  if (day) {
    conditions.push(eq(signals.day, day));
  }

  if (options.severityMin) {
    const rank = SEVERITY_RANK[options.severityMin] ?? 0;
    conditions.push(sql`${severityRankSql} >= ${rank}`);
  }

  const whereSql = conditions.length > 0 ? sql`${sql.join(conditions, sql` AND `)}` : sql`1=1`;

  const rowsResult = await db.execute(sql`
    SELECT *
    FROM (
      SELECT DISTINCT ON (cluster_id) *
      FROM ${signals}
      WHERE ${whereSql}
      ORDER BY cluster_id, ${severityRankSql} DESC, confidence_score DESC, created_at DESC
    ) clustered_signals
  `);

  const rows = (rowsResult as any).rows ?? [];
  const sorted = rows.sort((a: any, b: any) => {
    const rankA = SEVERITY_RANK[(a.cluster_severity ?? "LOW") as SignalSeverity] ?? 0;
    const rankB = SEVERITY_RANK[(b.cluster_severity ?? "LOW") as SignalSeverity] ?? 0;
    if (rankA !== rankB) return rankB - rankA;
    const confA = Number(a.confidence_score ?? 0);
    const confB = Number(b.confidence_score ?? 0);
    if (confA !== confB) return confB - confA;
    const timeA = a.created_at ? new Date(a.created_at).getTime() : 0;
    const timeB = b.created_at ? new Date(b.created_at).getTime() : 0;
    return timeB - timeA;
  });

  return sorted.map((row: any) =>
    buildSignalResponse(row, { compat: false, includeEntity: false }),
  );
}
