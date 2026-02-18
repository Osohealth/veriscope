export type QualityBand = "LOW" | "MEDIUM" | "HIGH";
export type QualityReason = { code: string; weight: number; note?: string };

export type QualityInput = {
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  confidence_band?: "LOW" | "MEDIUM" | "HIGH";
  confidence_score?: number;
  data_quality?: { completeness_pct?: number; missing_points?: number; history_days_used?: number };
  cluster_type?: string | null;
  method?: string | null;
  explainability?: { drivers?: any[] } | null;
};

export type QualityResult = {
  score: number;
  band: QualityBand;
  reasons: QualityReason[];
  version: "1";
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

export function computeAlertQuality(input: QualityInput): QualityResult {
  let score = 50;
  const reasons: QualityReason[] = [];

  const severityWeight: Record<QualityInput["severity"], number> = {
    CRITICAL: 25,
    HIGH: 15,
    MEDIUM: 5,
    LOW: 0,
  };
  const severityDelta = severityWeight[input.severity];
  score += severityDelta;
  reasons.push({ code: "SEVERITY", weight: severityDelta, note: input.severity });

  if (input.confidence_band) {
    const confidenceWeight: Record<NonNullable<QualityInput["confidence_band"]>, number> = {
      HIGH: 15,
      MEDIUM: 5,
      LOW: -15,
    };
    const bandDelta = confidenceWeight[input.confidence_band];
    score += bandDelta;
    reasons.push({ code: "CONFIDENCE_BAND", weight: bandDelta, note: input.confidence_band });
  }

  if (typeof input.confidence_score === "number" && Number.isFinite(input.confidence_score)) {
    const scoreDelta = Math.round(clamp(input.confidence_score, 0, 1) * 10);
    score += scoreDelta;
    reasons.push({ code: "CONFIDENCE_SCORE", weight: scoreDelta, note: String(input.confidence_score) });
  }

  const completeness = input.data_quality?.completeness_pct;
  if (typeof completeness === "number" && Number.isFinite(completeness)) {
    let dqDelta = 0;
    if (completeness >= 95) dqDelta = 10;
    else if (completeness >= 90) dqDelta = 5;
    else if (completeness >= 80) dqDelta = -5;
    else dqDelta = -15;
    score += dqDelta;
    reasons.push({ code: "DATA_QUALITY", weight: dqDelta, note: `${completeness}%` });
  }

  const hasDrivers = Array.isArray(input.explainability?.drivers) && input.explainability!.drivers!.length > 0;
  if (hasDrivers) {
    score += 5;
    reasons.push({ code: "EXPLAINABILITY", weight: 5 });
  }

  if (input.cluster_type?.startsWith("SLA_")) {
    if (input.cluster_type === "SLA_AT_RISK") {
      const before = score;
      score = Math.max(score, 70);
      if (score !== before) reasons.push({ code: "SLA_AT_RISK_MIN", weight: score - before });
    } else if (input.cluster_type === "SLA_RECOVERED") {
      const before = score;
      score = Math.min(score, 40);
      if (score !== before) reasons.push({ code: "SLA_RECOVERED_MAX", weight: score - before });
    }
  }

  score = Math.round(clamp(score, 0, 100));

  let band: QualityBand = "LOW";
  if (score >= 75) band = "HIGH";
  else if (score >= 50) band = "MEDIUM";

  return { score, band, reasons, version: "1" };
}
