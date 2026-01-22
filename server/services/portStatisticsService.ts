import { db } from '../db';
import { sql } from 'drizzle-orm';

export interface PortMetrics7d {
  arrivals: number;
  departures: number;
  unique_vessels: number;
  avg_dwell_hours: number;
  median_dwell_hours: number;
  open_calls: number;
}

export interface DailyArrivalsDepartures {
  day: string;
  arrivals: number;
  departures: number;
}

export async function getPortMetrics7d(portId: string): Promise<PortMetrics7d> {
  const result = await db.execute(sql`
    WITH params AS (
      SELECT
        ${portId}::varchar AS port_id,
        NOW() AT TIME ZONE 'UTC' AS now_utc,
        (NOW() AT TIME ZONE 'UTC') - INTERVAL '7 days' AS start_utc
    ),
    calls_7d AS (
      SELECT pc.*
      FROM port_calls pc
      JOIN params p ON p.port_id = pc.port_id
      WHERE pc.arrival_time >= (SELECT start_utc FROM params)
    ),
    dwell AS (
      SELECT
        pc.id,
        pc.vessel_id,
        pc.arrival_time,
        pc.departure_time,
        EXTRACT(EPOCH FROM (COALESCE(pc.departure_time, (SELECT now_utc FROM params)) - pc.arrival_time)) / 3600.0
          AS dwell_hours
      FROM calls_7d pc
    )
    SELECT
      (SELECT COUNT(*) FROM calls_7d)::int AS arrivals,
      (SELECT COUNT(*) FROM calls_7d WHERE departure_time IS NOT NULL)::int AS departures,
      (SELECT COUNT(DISTINCT vessel_id) FROM calls_7d)::int AS unique_vessels,
      COALESCE((SELECT ROUND(AVG(dwell_hours)::numeric, 2) FROM dwell), 0) AS avg_dwell_hours,
      COALESCE((SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dwell_hours)::numeric, 2) FROM dwell), 0) AS median_dwell_hours,
      (SELECT COUNT(*) FROM calls_7d WHERE departure_time IS NULL)::int AS open_calls
  `);
  
  const row = result.rows[0] as any;
  
  return {
    arrivals: parseInt(row?.arrivals) || 0,
    departures: parseInt(row?.departures) || 0,
    unique_vessels: parseInt(row?.unique_vessels) || 0,
    avg_dwell_hours: parseFloat(row?.avg_dwell_hours) || 0,
    median_dwell_hours: parseFloat(row?.median_dwell_hours) || 0,
    open_calls: parseInt(row?.open_calls) || 0,
  };
}

export async function getDailyArrivalsTimeSeries(portId: string): Promise<DailyArrivalsDepartures[]> {
  const result = await db.execute(sql`
    WITH days AS (
      SELECT generate_series(
        date_trunc('day', (NOW() AT TIME ZONE 'UTC') - INTERVAL '6 days'),
        date_trunc('day', (NOW() AT TIME ZONE 'UTC')),
        INTERVAL '1 day'
      ) AS day_utc
    )
    SELECT
      d.day_utc::date AS day,
      (SELECT COUNT(*)::int FROM port_calls pc 
       WHERE pc.port_id = ${portId}
         AND pc.arrival_time >= d.day_utc
         AND pc.arrival_time < d.day_utc + INTERVAL '1 day') AS arrivals,
      (SELECT COUNT(*)::int FROM port_calls pc 
       WHERE pc.port_id = ${portId}
         AND pc.departure_time IS NOT NULL
         AND pc.departure_time >= d.day_utc
         AND pc.departure_time < d.day_utc + INTERVAL '1 day') AS departures
    FROM days d
    ORDER BY d.day_utc
  `);
  
  return result.rows.map((row: any) => ({
    day: row.day,
    arrivals: parseInt(row.arrivals) || 0,
    departures: parseInt(row.departures) || 0,
  }));
}

export async function getTopBusyPorts(limit: number = 20): Promise<any[]> {
  const result = await db.execute(sql`
    WITH params AS (
      SELECT (NOW() AT TIME ZONE 'UTC') - INTERVAL '7 days' AS start_utc
    )
    SELECT
      p.id AS port_id,
      p.name,
      p.country_code,
      COUNT(pc.id)::int AS arrivals_7d,
      COUNT(DISTINCT pc.vessel_id)::int AS unique_vessels_7d,
      COALESCE(ROUND(AVG(EXTRACT(EPOCH FROM (COALESCE(pc.departure_time, NOW() AT TIME ZONE 'UTC') - pc.arrival_time))/3600.0)::numeric, 2), 0)
        AS avg_dwell_hours_7d
    FROM port_calls pc
    JOIN ports p ON p.id = pc.port_id
    WHERE pc.arrival_time >= (SELECT start_utc FROM params)
    GROUP BY p.id, p.name, p.country_code
    ORDER BY arrivals_7d DESC
    LIMIT ${limit}
  `);
  
  return result.rows.map((row: any) => ({
    port_id: row.port_id,
    name: row.name,
    country_code: row.country_code,
    arrivals_7d: parseInt(row.arrivals_7d) || 0,
    unique_vessels_7d: parseInt(row.unique_vessels_7d) || 0,
    avg_dwell_hours_7d: parseFloat(row.avg_dwell_hours_7d) || 0,
  }));
}

export async function getVesselsCurrentlyInPort(portId: string): Promise<number> {
  const result = await db.execute(sql`
    SELECT COUNT(*)::int AS vessels_currently_in_port
    FROM port_calls
    WHERE port_id = ${portId}
      AND departure_time IS NULL
  `);
  
  return parseInt((result.rows[0] as any)?.vessels_currently_in_port) || 0;
}
