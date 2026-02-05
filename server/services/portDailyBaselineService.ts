import { db } from "../db";
import { desc, eq, sql } from "drizzle-orm";
import { portDailyBaselines } from "@shared/schema";
import { logger } from "../middleware/observability";
import { runDailySignalEngine } from "./signalEngine";

export interface BackfillOptions {
  days?: number;
}

export async function backfillPortDailyBaselines(options: BackfillOptions = {}) {
  const days = Math.max(1, Math.floor(options.days ?? 60));

  const result = await db.execute(sql`
    WITH params AS (
      SELECT
        ${days}::int AS days,
        date_trunc('day', NOW() AT TIME ZONE 'UTC') AS today_utc
    ),
    days AS (
      SELECT generate_series(
        (SELECT today_utc FROM params) - ((SELECT days FROM params) - 1) * INTERVAL '1 day',
        (SELECT today_utc FROM params),
        INTERVAL '1 day'
      ) AS day_utc
    ),
    ports AS (
      SELECT id AS port_id FROM ports
    ),
    base AS (
      SELECT
        p.port_id,
        d.day_utc::date AS day,
        COUNT(pc.id) FILTER (
          WHERE pc.arrival_time >= d.day_utc
            AND pc.arrival_time < d.day_utc + INTERVAL '1 day'
        ) AS arrivals,
        COUNT(pc.id) FILTER (
          WHERE pc.departure_time IS NOT NULL
            AND pc.departure_time >= d.day_utc
            AND pc.departure_time < d.day_utc + INTERVAL '1 day'
        ) AS departures,
        COUNT(DISTINCT pc.vessel_id) FILTER (
          WHERE pc.arrival_time >= d.day_utc
            AND pc.arrival_time < d.day_utc + INTERVAL '1 day'
        ) AS unique_vessels,
        AVG(EXTRACT(EPOCH FROM (pc.departure_time - pc.arrival_time)) / 3600.0) FILTER (
          WHERE pc.departure_time IS NOT NULL
            AND pc.departure_time >= d.day_utc
            AND pc.departure_time < d.day_utc + INTERVAL '1 day'
        ) AS avg_dwell_hours,
        COUNT(pc.id) FILTER (
          WHERE pc.arrival_time < d.day_utc + INTERVAL '1 day'
            AND (pc.departure_time IS NULL OR pc.departure_time >= d.day_utc + INTERVAL '1 day')
        ) AS open_calls
      FROM ports p
      CROSS JOIN days d
      LEFT JOIN port_calls pc ON pc.port_id = p.port_id
      GROUP BY p.port_id, d.day_utc
    ),
    rollup AS (
      SELECT
        base.*,
        AVG(arrivals::double precision) OVER (
          PARTITION BY port_id
          ORDER BY day
          ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS arrivals_30d_avg,
        STDDEV_SAMP(arrivals::double precision) OVER (
          PARTITION BY port_id
          ORDER BY day
          ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS arrivals_30d_std,
        AVG(avg_dwell_hours) OVER (
          PARTITION BY port_id
          ORDER BY day
          ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS dwell_30d_avg,
        STDDEV_SAMP(avg_dwell_hours) OVER (
          PARTITION BY port_id
          ORDER BY day
          ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS dwell_30d_std,
        AVG(open_calls::double precision) OVER (
          PARTITION BY port_id
          ORDER BY day
          ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS open_calls_30d_avg
      FROM base
    )
    INSERT INTO port_daily_baselines (
      port_id,
      day,
      arrivals,
      departures,
      unique_vessels,
      avg_dwell_hours,
      open_calls,
      arrivals_30d_avg,
      arrivals_30d_std,
      dwell_30d_avg,
      dwell_30d_std,
      open_calls_30d_avg,
      updated_at
    )
    SELECT
      port_id,
      day,
      COALESCE(arrivals, 0)::int,
      COALESCE(departures, 0)::int,
      COALESCE(unique_vessels, 0)::int,
      avg_dwell_hours,
      COALESCE(open_calls, 0)::int,
      arrivals_30d_avg,
      arrivals_30d_std,
      dwell_30d_avg,
      dwell_30d_std,
      open_calls_30d_avg,
      NOW()
    FROM rollup
    ON CONFLICT (port_id, day) DO UPDATE SET
      arrivals = EXCLUDED.arrivals,
      departures = EXCLUDED.departures,
      unique_vessels = EXCLUDED.unique_vessels,
      avg_dwell_hours = EXCLUDED.avg_dwell_hours,
      open_calls = EXCLUDED.open_calls,
      arrivals_30d_avg = EXCLUDED.arrivals_30d_avg,
      arrivals_30d_std = EXCLUDED.arrivals_30d_std,
      dwell_30d_avg = EXCLUDED.dwell_30d_avg,
      dwell_30d_std = EXCLUDED.dwell_30d_std,
      open_calls_30d_avg = EXCLUDED.open_calls_30d_avg,
      updated_at = EXCLUDED.updated_at;
  `);

  return { days, rowsAffected: result.rowCount ?? 0 };
}

export async function getPortDailyBaselines(portId: string, days: number = 30) {
  const limit = Math.max(1, Math.min(Math.floor(days), 365));
  return db
    .select()
    .from(portDailyBaselines)
    .where(eq(portDailyBaselines.portId, portId))
    .orderBy(desc(portDailyBaselines.day))
    .limit(limit);
}

let baselineIntervalId: NodeJS.Timeout | null = null;

export function startPortDailyBaselineScheduler() {
  if (baselineIntervalId) {
    return;
  }

  const run = async () => {
    try {
      const result = await backfillPortDailyBaselines({ days: 31 });
      logger.info("Port daily baselines updated", result);
      try {
        await runDailySignalEngine();
      } catch (error) {
        logger.error("Signal engine run failed", {
          error: (error as Error).message,
        });
      }
    } catch (error) {
      logger.error("Port daily baseline update failed", {
        error: (error as Error).message,
      });
    }
  };

  run();
  baselineIntervalId = setInterval(run, 24 * 60 * 60 * 1000);
  logger.info("Port daily baseline scheduler started", {
    intervalHours: 24,
  });
}

export function stopPortDailyBaselineScheduler() {
  if (!baselineIntervalId) {
    return;
  }
  clearInterval(baselineIntervalId);
  baselineIntervalId = null;
  logger.info("Port daily baseline scheduler stopped");
}
