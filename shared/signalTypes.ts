export type SignalSeverity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
export type ConfidenceBand = "LOW" | "MEDIUM" | "HIGH";

export const SEVERITY_RANK: Record<SignalSeverity, number> = {
  LOW: 1,
  MEDIUM: 2,
  HIGH: 3,
  CRITICAL: 4,
};
