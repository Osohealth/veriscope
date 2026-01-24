import { db } from "../db";
import { signals } from "../../drizzle/schema";
import { sql } from "drizzle-orm";

type SignalSeverity = "LOW" | "MEDIUM" | "HIGH";

type BaselineRow = {
  portId: string;
  date: string;
  arrivals: number;
  arrivals30dAvg: number | null;
  arrivals30dStd: number | null;
  avgDwellHours: number | null;
  dwell30dAvg: number | null;
  dwell30dStd: number | null;
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

function severityFromZScore(zScore: number): SignalSeverity | null {
  const abs = Math.abs(zScore);
  if (abs >= 2) return "HIGH";
  if (abs >= 1.5) return "MEDIUM";
  if (abs >= 1) return "LOW";
  return null;
}

function buildSignalForMetric(options: {
  signalType: string;
  entityId: string;
  signalDate: string;
  value: number;
  baselineAvg: number;
  baselineStd: number | null;
  metricLabel: string;
}) {
  const { signalType, entityId, signalDate, value, baselineAvg, baselineStd, metricLabel } =
    options;
  if (!Number.isFinite(baselineAvg) || baselineAvg === 0) {
    return null;
  }

  const deltaPct = ((value - baselineAvg) / baselineAvg) * 100;
  const zScore = baselineStd && baselineStd > 0 ? (value - baselineAvg) / baselineStd : null;
  const severity = zScore !== null ? severityFromZScore(zScore) : null;

  if (!severity) {
    return null;
  }

  const zPart = zScore !== null ? ` (z=${zScore.toFixed(2)})` : "";
  const explanation = `${metricLabel} is ${value} vs 30d avg ${baselineAvg.toFixed(
    2,
  )}${zPart}, delta ${formatDeltaPct(deltaPct)}.`;

  return {
    signalType,
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
    const arrivalsSignal = buildSignalForMetric({
      signalType: "PORT_ARRIVALS_ANOMALY",
      entityId: row.portId,
      signalDate: row.date,
      value: row.arrivals,
      baselineAvg: row.arrivals30dAvg,
      baselineStd: row.arrivals30dStd,
      metricLabel: "Arrivals",
    });
    if (arrivalsSignal) signals.push(arrivalsSignal);
  }

  if (row.avgDwellHours !== null && row.dwell30dAvg !== null) {
    const dwellSignal = buildSignalForMetric({
      signalType: "PORT_DWELL_ANOMALY",
      entityId: row.portId,
      signalDate: row.date,
      value: row.avgDwellHours,
      baselineAvg: row.dwell30dAvg,
      baselineStd: row.dwell30dStd,
      metricLabel: "Avg dwell hours",
    });
    if (dwellSignal) signals.push(dwellSignal);
  }

  return signals;
}

export async function runSignalEngine(forDate?: string) {
  const dateExpr = forDate ? sql`${forDate}::date` : sql`current_date`;
  const rows = await db.execute<BaselineRow>(sql`
    select
      port_id as "portId",
      date::text as "date",
      arrivals,
      arrivals_30d_avg as "arrivals30dAvg",
      arrivals_30d_std as "arrivals30dStd",
      avg_dwell_hours as "avgDwellHours",
      dwell_30d_avg as "dwell30dAvg",
      dwell_30d_std as "dwell30dStd"
    from port_daily_baselines
    where date = ${dateExpr}
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
