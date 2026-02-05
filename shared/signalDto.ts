import type { ConfidenceBand, SignalSeverity } from "./signalTypes";

export type ExplainDriver = {
  metric: string;
  value: number;
  baseline?: number | null;
  stddev?: number | null;
  zscore?: number | null;
  delta_pct?: number | null;
  unit?: string | null;
};

export type Explainability = {
  drivers: ExplainDriver[];
  impact: string[];
  followups: string[];
};

export type DataQuality = {
  history_days_used: number;
  completeness_pct: number;
  missing_points: number;
};

export type SignalDTO = {
  id: string;
  signal_type: string;
  entity_type: "port" | string;
  entity_id: string;
  day: string;
  entity?: {
    id: string;
    type: "port";
    name: string;
    code: string;
    unlocode: string;
  };

  severity: SignalSeverity;

  confidence_score: number;
  confidence_band: ConfidenceBand;
  method: string;

  cluster_id?: string | null;
  cluster_key?: string | null;
  cluster_type?: string | null;
  cluster_summary?: string | null;
  cluster_severity?: SignalSeverity | null;

  value: number;
  baseline?: number | null;
  stddev?: number | null;
  zscore?: number | null;
  delta_pct?: number | null;

  explanation: string;
  explainability?: Explainability | null;
  metadata: {
    metric?: string;
    baseline_window?: string;
    data_quality: DataQuality;
    [k: string]: any;
  };

  created_at: string;
};
