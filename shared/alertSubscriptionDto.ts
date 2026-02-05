export type AlertChannel = "WEBHOOK" | "EMAIL";
export type AlertSeverity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
export type AlertConfidence = "LOW" | "MEDIUM" | "HIGH";

export type AlertSubscriptionDTO = {
  id: string;
  user_id: string;
  scope: "PORT" | "GLOBAL";
  entity_type: string;
  entity_id: string;
  severity_min: AlertSeverity;
  confidence_min?: AlertConfidence | null;
  channel: AlertChannel;
  endpoint: string;
  secret?: string | null;
  signature_version?: string;
  is_enabled: boolean;
  last_test_at?: string | null;
  last_test_status?: "SENT" | "FAILED" | null;
  last_test_error?: string | null;
  created_at: string;
  updated_at: string;
};

export type CreateAlertSubscriptionDTO = {
  user_id: string;
  scope?: "PORT" | "GLOBAL";
  entity_type?: string;
  entity_id: string;
  severity_min?: AlertSeverity;
  confidence_min?: AlertConfidence | null;
  channel?: AlertChannel;
  endpoint: string;
  secret?: string | null;
  is_enabled?: boolean;
};
