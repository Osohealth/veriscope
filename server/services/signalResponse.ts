export type SignalEntity = {
  id: string;
  type: "port";
  name: string;
  code: string;
  unlocode: string;
};

type SignalRowLike = {
  id: string;
  signalType?: string;
  entityType?: string;
  entityId?: string;
  day: Date | string;
  severity: string;
  value: number;
  baseline?: number | null;
  stddev?: number | null;
  zscore?: number | null;
  deltaPct?: number | null;
  confidenceScore?: number | null;
  confidenceBand?: string | null;
  method?: string | null;
  clusterId?: string | null;
  clusterKey?: string | null;
  clusterType?: string | null;
  clusterSeverity?: string | null;
  clusterSummary?: string | null;
  explanation: string;
  metadata?: any;
  createdAt?: Date | string | null;
};

type SignalResponseOptions = {
  compat: boolean;
  includeEntity: boolean;
  entityMap?: Map<string, SignalEntity>;
};

const normalizeSignal = (signal: any): SignalRowLike => ({
  ...signal,
  signalType: signal.signalType ?? signal.signal_type,
  entityType: signal.entityType ?? signal.entity_type,
  entityId: signal.entityId ?? signal.entity_id,
  deltaPct: signal.deltaPct ?? signal.delta_pct,
  confidenceScore: signal.confidenceScore ?? signal.confidence_score,
  confidenceBand: signal.confidenceBand ?? signal.confidence_band,
  clusterId: signal.clusterId ?? signal.cluster_id,
  clusterKey: signal.clusterKey ?? signal.cluster_key,
  clusterType: signal.clusterType ?? signal.cluster_type,
  clusterSeverity: signal.clusterSeverity ?? signal.cluster_severity,
  clusterSummary: signal.clusterSummary ?? signal.cluster_summary,
  createdAt: signal.createdAt ?? signal.created_at,
});

const formatSignalDay = (day: Date | string): string => {
  if (day instanceof Date) {
    return day.toISOString().slice(0, 10);
  }
  return String(day);
};

export const buildSignalResponse = (
  rawSignal: any,
  options: SignalResponseOptions,
) => {
  const signal = normalizeSignal(rawSignal);
  const drivers = signal.metadata?.drivers ?? null;
  const impact = signal.metadata?.impact ?? null;
  const followups = signal.metadata?.recommended_followups ?? null;
  const explainability = drivers || impact || followups
    ? {
        drivers: drivers ?? [],
        impact: impact ?? [],
        followups: followups ?? [],
      }
    : null;
  const dataQuality = signal.metadata?.data_quality ?? null;
  const minimalMetadata = {
    day: signal.metadata?.day,
    metric: signal.metadata?.metric,
    baseline_window: signal.metadata?.baseline_window,
    min_history_days: signal.metadata?.min_history_days,
    data_quality: dataQuality,
  };
  const createdAt = signal.createdAt ? new Date(signal.createdAt).toISOString() : null;
  const entityRow = options.includeEntity && signal.entityType === "port"
    ? options.entityMap?.get(String(signal.entityId))
    : undefined;

  return {
    id: signal.id,
    signal_type: signal.signalType,
    entity_type: signal.entityType,
    entity_id: signal.entityId,
    day: formatSignalDay(signal.day),
    ...(entityRow ? { entity: entityRow } : {}),
    severity: signal.severity,
    value: signal.value,
    baseline: signal.baseline,
    stddev: signal.stddev,
    zscore: signal.zscore,
    delta_pct: signal.deltaPct,
    confidence_score: signal.confidenceScore,
    confidence_band: signal.confidenceBand,
    method: signal.method,
    cluster_id: signal.clusterId,
    cluster_key: signal.clusterKey,
    cluster_type: signal.clusterType,
    cluster_severity: signal.clusterSeverity,
    cluster_summary: signal.clusterSummary,
    explanation: signal.explanation,
    explainability,
    drivers: options.compat ? drivers : undefined,
    impact: options.compat ? impact : undefined,
    followups: options.compat ? followups : undefined,
    data_quality: dataQuality,
    metadata: options.compat ? signal.metadata : minimalMetadata,
    created_at: createdAt,
  };
};
