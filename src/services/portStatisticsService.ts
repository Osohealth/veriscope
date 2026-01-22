import { sql } from "drizzle-orm";
import { db } from "../db";

export type PortMetrics7d = {
  arrivals_7d: number;
  departures_7d: number;
  unique_vessels_7d: number;
  avg_dwell_hours_7d: number;
  open_calls: number;
};

const WINDOW_DAYS = 7;

// departures_7d is computed by departure_time_utc within the 7-day window.
export async function getPortMetrics7d(portId: string): Promise<PortMetrics7d> {
  const result = await db.execute<{
    arrivals_7d: number;
    departures_7d: number;
    unique_vessels_7d: number;
    avg_dwell_hours_7d: number | null;
    open_calls: number;
  }>(sql`
    WITH windowed AS (
      SELECT
        port_id,
        vessel_id,
        arrival_time_utc,
        departure_time_utc
      FROM port_calls
      WHERE port_id = ${portId}
        AND arrival_time_utc >= NOW() - INTERVAL '${WINDOW_DAYS} days'
    )
    SELECT
      (SELECT COUNT(*) FROM windowed) AS arrivals_7d,
      (SELECT COUNT(*) FROM port_calls
        WHERE port_id = ${portId}
          AND departure_time_utc IS NOT NULL
          AND departure_time_utc >= NOW() - INTERVAL '${WINDOW_DAYS} days'
      ) AS departures_7d,
      (SELECT COUNT(DISTINCT vessel_id) FROM windowed) AS unique_vessels_7d,
      (SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (COALESCE(departure_time_utc, NOW()) - arrival_time_utc)) / 3600), 0)
        FROM windowed
      ) AS avg_dwell_hours_7d,
      (SELECT COUNT(*) FROM port_calls
        WHERE port_id = ${portId}
          AND departure_time_utc IS NULL
      ) AS open_calls
  `);

  const row = result.rows[0];

  return {
    arrivals_7d: Number(row?.arrivals_7d ?? 0),
    departures_7d: Number(row?.departures_7d ?? 0),
    unique_vessels_7d: Number(row?.unique_vessels_7d ?? 0),
    avg_dwell_hours_7d: Number(row?.avg_dwell_hours_7d ?? 0),
    open_calls: Number(row?.open_calls ?? 0),
  };
}
