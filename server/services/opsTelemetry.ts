import { logger } from "../middleware/observability";

export const OPS_SCHEMA_VERSION = "2";

export type OpsCounters = {
  deliveries_created_total: number;
  deliveries_blocked_cooldown_total: number;
  deliveries_blocked_dlq_total: number;
  deliveries_success_total: number;
  deliveries_failure_total: number;
  deliveries_error_network_total: number;
  deliveries_error_http_total: number;
  deliveries_error_validation_total: number;
  deliveries_error_unknown_total: number;
  escalation_runs_total: number;
  escalation_runs_skipped_lock_total: number;
  escalation_runs_escalated_total: number;
};

type LatencySummary = {
  p50: number | null;
  p95: number | null;
  count: number;
};

type OpsLatencySnapshot = {
  EMAIL: LatencySummary;
  WEBHOOK: LatencySummary;
};

type OpsRunDurationSnapshot = {
  p50: number | null;
  p95: number | null;
  count: number;
};

const MAX_SAMPLES = 200;

const counters: OpsCounters = {
  deliveries_created_total: 0,
  deliveries_blocked_cooldown_total: 0,
  deliveries_blocked_dlq_total: 0,
  deliveries_success_total: 0,
  deliveries_failure_total: 0,
  deliveries_error_network_total: 0,
  deliveries_error_http_total: 0,
  deliveries_error_validation_total: 0,
  deliveries_error_unknown_total: 0,
  escalation_runs_total: 0,
  escalation_runs_skipped_lock_total: 0,
  escalation_runs_escalated_total: 0,
};

const deliveryLatencySamples: Record<"EMAIL" | "WEBHOOK", number[]> = {
  EMAIL: [],
  WEBHOOK: [],
};

const escalationRunDurations: number[] = [];

export function incrementOpsCounter(key: keyof OpsCounters, amount = 1) {
  counters[key] = (counters[key] ?? 0) + amount;
}

export function getOpsCounters() {
  return { ...counters };
}

const recordSample = (samples: number[], value: number) => {
  if (!Number.isFinite(value)) return;
  samples.push(value);
  if (samples.length > MAX_SAMPLES) {
    samples.shift();
  }
};

const percentile = (samples: number[], p: number): number | null => {
  if (samples.length === 0) return null;
  const sorted = [...samples].sort((a, b) => a - b);
  const idx = Math.max(0, Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1));
  return sorted[idx] ?? null;
};

export function recordDeliveryLatency(destinationType: "EMAIL" | "WEBHOOK", latencyMs: number | null | undefined) {
  if (!Number.isFinite(latencyMs as number)) return;
  recordSample(deliveryLatencySamples[destinationType], Number(latencyMs));
}

export function recordEscalationRunDuration(durationMs: number | null | undefined) {
  if (!Number.isFinite(durationMs as number)) return;
  recordSample(escalationRunDurations, Number(durationMs));
}

export function recordDeliveryErrorCategory(category: "network" | "http" | "validation" | "unknown") {
  const keyMap: Record<string, keyof OpsCounters> = {
    network: "deliveries_error_network_total",
    http: "deliveries_error_http_total",
    validation: "deliveries_error_validation_total",
    unknown: "deliveries_error_unknown_total",
  };
  const key = keyMap[category] ?? "deliveries_error_unknown_total";
  incrementOpsCounter(key);
}

export function resetOpsTelemetry() {
  Object.keys(counters).forEach((key) => {
    (counters as any)[key] = 0;
  });
  deliveryLatencySamples.EMAIL = [];
  deliveryLatencySamples.WEBHOOK = [];
  escalationRunDurations.length = 0;
}

export function getOpsSnapshot() {
  const latencySnapshot: OpsLatencySnapshot = {
    EMAIL: {
      p50: percentile(deliveryLatencySamples.EMAIL, 0.5),
      p95: percentile(deliveryLatencySamples.EMAIL, 0.95),
      count: deliveryLatencySamples.EMAIL.length,
    },
    WEBHOOK: {
      p50: percentile(deliveryLatencySamples.WEBHOOK, 0.5),
      p95: percentile(deliveryLatencySamples.WEBHOOK, 0.95),
      count: deliveryLatencySamples.WEBHOOK.length,
    },
  };

  const runSnapshot: OpsRunDurationSnapshot = {
    p50: percentile(escalationRunDurations, 0.5),
    p95: percentile(escalationRunDurations, 0.95),
    count: escalationRunDurations.length,
  };

  return {
    ops_schema_version: OPS_SCHEMA_VERSION,
    counters: getOpsCounters(),
    delivery_latency_ms: latencySnapshot,
    escalation_run_duration_ms: runSnapshot,
  };
}

export function logOpsEvent(event: string, metadata: Record<string, any> = {}) {
  logger.info(event, metadata);
}
