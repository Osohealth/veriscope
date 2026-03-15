export type AlertDecision = {
  version: "1";
  evaluated_at: string;
  playbook_version?: string;
  subscription: {
    id: string;
    scope: "GLOBAL" | "PORT";
    entity_id: string;
    severity_min: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
    destination_type: "WEBHOOK" | "EMAIL";
    destination_redacted: string;
    enabled: boolean;
  };
  selection: {
    day: string;
    window: "DAY";
    clustered: boolean;
    bundle: {
      enabled: boolean;
      top_n: number;
      size: number;
      overflow: number;
      included: Array<{
        cluster_id: string;
        cluster_type: string;
        cluster_severity: string;
        confidence_score: number;
        confidence_band: string;
        summary: string;
        reason_rank: number;
        entity?: { id: string; type: string; name: string; code: string; unlocode: string } | null;
      }>;
      excluded_preview?: Array<{
        cluster_id: string;
        cluster_severity: string;
        summary: string;
        excluded_reason: "BUNDLE_OVERFLOW" | "SEVERITY_MIN" | "QUALITY" | "NOISE_BUDGET" | "DEDUPE";
      }>;
    };
  };
  gates: {
    severity_min_pass: boolean;
    dedupe: {
      applied: boolean;
      key?: string;
      ttl_hours?: number;
      blocked?: boolean;
      blocked_reason?: string;
    };
    noise_budget: {
      applied: boolean;
      allowed: boolean;
      enabled?: boolean;
      source?: "DESTINATION" | "TENANT_DEFAULT";
      window?: string;
      window_minutes?: number | null;
      max?: number;
      max_deliveries?: number | null;
      used_before?: number;
      used_in_window?: number;
    };
    sla_thresholds?: {
      window?: "24h" | "7d" | null;
      source?: "DESTINATION" | "TENANT_DEFAULT";
      p95_ms?: number;
      success_rate_min_pct?: number;
    };
    quality: {
      applied: boolean;
      score: number;
      band: "HIGH" | "MEDIUM" | "LOW";
      suppressed: boolean;
      reason?: string;
    };
    rate_limit: {
      applied: boolean;
      per_endpoint: number;
      allowed: boolean;
    };
    endpoint_health: {
      applied: boolean;
      window: string;
      status: "OK" | "DEGRADED" | "DOWN" | "UNKNOWN";
      allowed: boolean;
    };
    destination_state: {
      applied: boolean;
      state: "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED" | "UNKNOWN";
      allowed: boolean;
      reason?: string;
      ready_to_resume?: boolean;
      force_retry?: boolean;
    };
    escalation?: {
      level: number;
      target_type: string;
      target_ref: string;
      policy_id?: string;
      dedupe_key?: string;
      allowed?: boolean;
    };
    routing?: {
      allowed: boolean;
      reason?: "NO_USER_CONTACT_METHOD";
      chosen_method_type?: "EMAIL" | "WEBHOOK";
      chosen_method_id?: string;
      target_user_id?: string;
    };
  };
  suppressed_counts: {
    dedupe: number;
    noise_budget: number;
    quality: number;
    overflow: number;
  };
};
