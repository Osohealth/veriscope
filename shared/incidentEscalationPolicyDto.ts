export type IncidentEscalationType = "ALL" | "SLA_AT_RISK" | "ENDPOINT_DOWN";

export type IncidentEscalationSeverity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";

export type IncidentEscalationTargetType = "ROLE" | "USER" | "EMAIL" | "WEBHOOK";

export type IncidentEscalationPolicyDTO = {
  id: string;
  tenant_id: string;
  enabled: boolean;
  level: number;
  after_minutes: number;
  incident_type: IncidentEscalationType;
  severity_min: IncidentEscalationSeverity;
  target_type: IncidentEscalationTargetType;
  target_ref: string;
  target_name?: string | null;
  last_validated_at?: string | null;
  last_routing_health?: Record<string, any> | null;
  routing_health?: {
    routes_total: number;
    routes_allowed: number;
    routes_blocked: number;
    blocked_reasons?: string[];
    warnings_count: number;
  } | null;
  created_at: string;
  updated_at: string;
};
