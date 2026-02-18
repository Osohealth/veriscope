import { sql } from "drizzle-orm";
import { db } from "../db";

const clampDays = (value?: number) => {
  const parsed = Number.isFinite(Number(value)) ? Number(value) : 30;
  return Math.max(1, Math.min(365, Math.floor(parsed)));
};

export type DistStats = { p50: number | null; p95: number | null; mean: number | null; n: number };
export type IncidentsMetricsV1 = {
  version: "1";
  window_days: number;
  open_count: number;
  mtta_ms: DistStats;
  mttr_ms: DistStats;
  by_type: Record<string, { open_count: number; mtta_ms: DistStats; mttr_ms: DistStats }>;
};

const statsRowToDist = (row: any): DistStats => ({
  p50: row?.p50 ?? null,
  p95: row?.p95 ?? null,
  mean: row?.mean ?? null,
  n: Number(row?.n ?? 0),
});

export async function getIncidentMetricsV1(args: { tenantId: string; days?: number }): Promise<IncidentsMetricsV1> {
  const days = clampDays(args.days);
  const windowStart = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

  const openCountRows = await db.execute(sql`
    SELECT count(*)::int AS n
    FROM incidents
    WHERE tenant_id = ${args.tenantId}
      AND status = 'OPEN'
      AND opened_at >= ${windowStart}
  `);
  const open_count = Number(openCountRows.rows?.[0]?.n ?? 0);

  const mttaRows = await db.execute(sql`
    SELECT
      percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (acked_at - opened_at))*1000) AS p50,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (acked_at - opened_at))*1000) AS p95,
      avg(EXTRACT(EPOCH FROM (acked_at - opened_at))*1000) AS mean,
      count(*)::int AS n
    FROM incidents
    WHERE tenant_id = ${args.tenantId}
      AND opened_at >= ${windowStart}
      AND acked_at IS NOT NULL
      AND acked_at >= opened_at
  `);
  const mtta_ms = statsRowToDist(mttaRows.rows?.[0]);

  const mttrRows = await db.execute(sql`
    SELECT
      percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (resolved_at - opened_at))*1000) AS p50,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (resolved_at - opened_at))*1000) AS p95,
      avg(EXTRACT(EPOCH FROM (resolved_at - opened_at))*1000) AS mean,
      count(*)::int AS n
    FROM incidents
    WHERE tenant_id = ${args.tenantId}
      AND opened_at >= ${windowStart}
      AND resolved_at IS NOT NULL
      AND resolved_at >= opened_at
  `);
  const mttr_ms = statsRowToDist(mttrRows.rows?.[0]);

  const types = ["SLA_AT_RISK", "ENDPOINT_DOWN"] as const;
  const by_type: IncidentsMetricsV1["by_type"] = {};

  for (const t of types) {
    const typeOpen = await db.execute(sql`
      SELECT count(*)::int AS n
      FROM incidents
      WHERE tenant_id = ${args.tenantId}
        AND opened_at >= ${windowStart}
        AND status = 'OPEN'
        AND type = ${t}
    `);

    const typeMtta = await db.execute(sql`
      SELECT
        percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (acked_at - opened_at))*1000) AS p50,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (acked_at - opened_at))*1000) AS p95,
        avg(EXTRACT(EPOCH FROM (acked_at - opened_at))*1000) AS mean,
        count(*)::int AS n
      FROM incidents
      WHERE tenant_id = ${args.tenantId}
        AND opened_at >= ${windowStart}
        AND type = ${t}
        AND acked_at IS NOT NULL
        AND acked_at >= opened_at
    `);

    const typeMttr = await db.execute(sql`
      SELECT
        percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (resolved_at - opened_at))*1000) AS p50,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (resolved_at - opened_at))*1000) AS p95,
        avg(EXTRACT(EPOCH FROM (resolved_at - opened_at))*1000) AS mean,
        count(*)::int AS n
      FROM incidents
      WHERE tenant_id = ${args.tenantId}
        AND opened_at >= ${windowStart}
        AND type = ${t}
        AND resolved_at IS NOT NULL
        AND resolved_at >= opened_at
    `);

    by_type[t] = {
      open_count: Number(typeOpen.rows?.[0]?.n ?? 0),
      mtta_ms: statsRowToDist(typeMtta.rows?.[0]),
      mttr_ms: statsRowToDist(typeMttr.rows?.[0]),
    };
  }

  return {
    version: "1",
    window_days: days,
    open_count,
    mtta_ms,
    mttr_ms,
    by_type,
  };
}
