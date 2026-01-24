import { db } from "../db";
import { signals } from "../../drizzle/schema";
import { sql } from "drizzle-orm";

type SignalSeverity = "LOW" | "MEDIUM" | "HIGH";

type BaselineRow = {
  portId: string;
  date: string;
  arrivals30dAvg: number | null;
  arrivals30dStd: number | null;
  dwell30dAvg: number | null;
  dwell30dStd: number | null;
  portName: string;
  arrivalsToday: number;
  avgDwellHoursToday: number | null;
};

type SignalCandidate = {
  signalType: string;
  entityType: "port";
  entityId: string;
  severity: SignalSeverity;
  value: number;
  baseline: number;
  deltaPct: number;
  explanation: string;
  signalDate: string;
};

function formatDeltaPct(value: number) {
  const rounded = Math.round(value * 10) / 10;
  return `${rounded}%`;
}

function severityFromArrivalsZScore(zScore: number): SignalSeverity | null {
  const abs = Math.abs(zScore);
  if (abs >= 3) return "HIGH";
  if (abs >= 2) return "MEDIUM";
  return null;
}

function severityFromDwell(zScore: number): SignalSeverity | null {
  if (zScore >= 3) return "HIGH";
  if (zScore >= 2) return "MEDIUM";
  return null;
}

function buildArrivalsSignal(options: {
  portName: string;
  signalDate: string;
  entityId: string;
  value: number;
  baselineAvg: number;
  baselineStd: number | null;
}) {
  const { portName, entityId, signalDate, value, baselineAvg, baselineStd } = options;
  if (!Number.isFinite(baselineAvg) || baselineAvg === 0) {
    return null;
  }

  const deltaPct = ((value - baselineAvg) / baselineAvg) * 100;
  const zScore = baselineStd && baselineStd > 0 ? (value - baselineAvg) / baselineStd : null;
  const severity = zScore !== null ? severityFromArrivalsZScore(zScore) : null;

  if (!severity) {
    return null;
  }

  const direction = deltaPct >= 0 ? "increased" : "dropped";
  const sigma = zScore !== null ? Math.abs(zScore).toFixed(1) : "n/a";
  const explanation = `Port of ${portName} arrivals ${direction} ${Math.abs(
    Math.round(deltaPct),
  )}% vs 30-day average (σ=${sigma}). This may indicate congestion, weather disruption, or supply re-routing.`;

  return {
    signalType: "PORT_ARRIVALS_ANOMALY",
    entityType: "port",
    entityId,
    severity,
    value,
    baseline: baselineAvg,
    deltaPct,
    explanation,
    signalDate,
  } satisfies SignalCandidate;
}

function buildDwellSignal(options: {
  portName: string;
  signalDate: string;
  entityId: string;
  value: number;
  baselineAvg: number;
  baselineStd: number | null;
}) {
  const { portName, signalDate, entityId, value, baselineAvg, baselineStd } = options;
  if (!Number.isFinite(baselineAvg) || baselineAvg === 0 || !baselineStd || baselineStd <= 0) {
    return null;
  }
  const zScore = (value - baselineAvg) / baselineStd;
  const severity = severityFromDwell(zScore);

  if (!severity) {
    return null;
  }

  const deltaPct = ((value - baselineAvg) / baselineAvg) * 100;
  const explanation = `Port of ${portName} average dwell time is ${value.toFixed(
    1,
  )}h vs 30-day average ${baselineAvg.toFixed(
    1,
  )}h (σ=${zScore.toFixed(1)}). This may indicate congestion, labor issues, or berth constraints.`;

  return {
    signalType: "PORT_DWELL_SPIKE",
    entityType: "port",
    entityId,
    severity,
    value,
    baseline: baselineAvg,
    deltaPct,
    explanation,
    signalDate,
  } satisfies SignalCandidate;
}

function buildSignalsFromBaseline(row: BaselineRow): SignalCandidate[] {
  const signals: SignalCandidate[] = [];

  if (row.arrivals30dAvg !== null) {
    const arrivalsSignal = buildArrivalsSignal({
      portName: row.portName,
      entityId: row.portId,
      signalDate: row.date,
      value: row.arrivalsToday,
      baselineAvg: row.arrivals30dAvg,
      baselineStd: row.arrivals30dStd,
    });
    if (arrivalsSignal) signals.push(arrivalsSignal);
  }

  if (row.avgDwellHoursToday !== null && row.dwell30dAvg !== null) {
    const dwellSignal = buildDwellSignal({
      portName: row.portName,
      entityId: row.portId,
      signalDate: row.date,
      value: row.avgDwellHoursToday,
      baselineAvg: row.dwell30dAvg,
      baselineStd: row.dwell30dStd,
    });
    if (dwellSignal) signals.push(dwellSignal);
  }

  return signals;
}

export async function runSignalEngine(forDate?: string) {
  const dateExpr = forDate ? sql`${forDate}::date` : sql`current_date`;
  const rows = await db.execute<BaselineRow>(sql`
    with today_metrics as (
      select
        p.id as port_id,
        p.name as port_name,
        coalesce(
          count(*) filter (where pc.arrival_time_utc::date = ${dateExpr}),
          0
        ) as arrivals_today,
        avg(
          extract(epoch from (coalesce(pc.departure_time_utc, now()) - pc.arrival_time_utc)) / 3600.0
        ) filter (where pc.arrival_time_utc::date = ${dateExpr}) as avg_dwell_hours_today
      from ports p
      left join port_calls pc on pc.port_id = p.id
      group by p.id, p.name
    )
    select
      b.port_id as "portId",
      b.date::text as "date",
      b.arrivals_30d_avg as "arrivals30dAvg",
      b.arrivals_30d_std as "arrivals30dStd",
      b.dwell_30d_avg as "dwell30dAvg",
      b.dwell_30d_std as "dwell30dStd",
      t.port_name as "portName",
      t.arrivals_today as "arrivalsToday",
      t.avg_dwell_hours_today as "avgDwellHoursToday"
    from port_daily_baselines b
    join today_metrics t on t.port_id = b.port_id
    where b.date = ${dateExpr}
  `);

  const candidates = rows.rows.flatMap(buildSignalsFromBaseline);

  if (candidates.length === 0) {
    return { inserted: 0 };
  }

  await db
    .insert(signals)
    .values(candidates)
    .onConflictDoNothing({
      target: [
        signals.signalType,
        signals.entityType,
        signals.entityId,
        signals.signalDate,
      ],
    });

  return { inserted: candidates.length };
}

export function scheduleSignalEngine(options?: {
  intervalHours?: number;
}) {
  const intervalHours = Math.max(options?.intervalHours ?? 24, 1);
  const intervalMs = intervalHours * 60 * 60 * 1000;

  const run = async () => {
    try {
      await runSignalEngine();
    } catch (error) {
      console.error("signal engine run failed", error);
    }
  };

  void run();
  return setInterval(run, intervalMs);
}
