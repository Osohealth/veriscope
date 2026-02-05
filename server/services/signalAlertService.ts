import type { SignalSeverity, ConfidenceBand } from "@shared/signalTypes";
import { formatSignalDay } from "./signalEngine";

type DriverMetric = {
  metric: string;
  value: number;
  baseline?: number | null;
  stddev?: number | null;
  zscore?: number | null;
  delta_pct?: number | null;
  multiplier?: number | null;
};

type SignalRowLike = {
  day: Date | string;
  entityType: string;
  entityId: string;
  clusterId?: string | null;
  clusterSeverity?: SignalSeverity | null;
  confidenceScore?: number | null;
  confidenceBand?: ConfidenceBand | null;
  clusterSummary?: string | null;
  metadata?: any;
};

export type SignalClusterAlertPayload = {
  event_type: "VERISCOPE_SIGNAL_CLUSTER";
  day: string;
  entity_type: string;
  entity_id: string;
  cluster_id: string | null;
  cluster_severity: SignalSeverity | null;
  confidence_score: number | null;
  confidence_band: ConfidenceBand | null;
  cluster_summary: string | null;
  top_drivers: DriverMetric[];
  impact: string[];
  followups: string[];
  data_quality: {
    history_days_used: number;
    completeness_pct: number;
    missing_points: number;
  } | null;
};

export function buildSignalClusterAlertPayload(signal: SignalRowLike): SignalClusterAlertPayload {
  const day = signal.day instanceof Date ? formatSignalDay(signal.day) : String(signal.day);
  const metadata = signal.metadata ?? {};
  const drivers = (metadata.drivers ?? metadata.driver_metrics ?? []) as DriverMetric[];
  const impact = (metadata.impact ?? []) as string[];
  const followups = (metadata.recommended_followups ?? []) as string[];
  const dataQuality = metadata.data_quality ?? null;

  return {
    event_type: "VERISCOPE_SIGNAL_CLUSTER",
    day,
    entity_type: signal.entityType,
    entity_id: signal.entityId,
    cluster_id: signal.clusterId ?? null,
    cluster_severity: signal.clusterSeverity ?? null,
    confidence_score: signal.confidenceScore ?? null,
    confidence_band: signal.confidenceBand ?? null,
    cluster_summary: signal.clusterSummary ?? null,
    top_drivers: drivers.slice(0, 1),
    impact,
    followups,
    data_quality: dataQuality,
  };
}
