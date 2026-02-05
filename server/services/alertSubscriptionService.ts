import type { AlertChannel, AlertConfidence, AlertSeverity } from "@shared/alertSubscriptionDto";

type CreateInput = {
  user_id?: string;
  entity_type?: string;
  entity_id?: string;
  severity_min?: string;
  confidence_min?: string | null;
  channel?: string;
  endpoint?: string;
  secret?: string | null;
  is_enabled?: boolean;
};

export const validateAlertSubscriptionInput = (input: CreateInput, allowHttp: boolean) => {
  const errors: string[] = [];
  const userId = input.user_id ? String(input.user_id) : "";
  const entityId = input.entity_id ? String(input.entity_id) : "";
  const endpoint = input.endpoint ? String(input.endpoint) : "";

  if (!userId || !entityId || !endpoint) {
    errors.push("user_id, entity_id, endpoint are required");
  }

  const severityValue = String(input.severity_min ?? "HIGH").toUpperCase();
  const validSeverity = ["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityValue);
  if (!validSeverity) {
    errors.push("invalid severity_min");
  }

  const channelValue = String(input.channel ?? "WEBHOOK").toUpperCase();
  const validChannel = ["WEBHOOK"].includes(channelValue);
  if (!validChannel) {
    errors.push("invalid channel");
  }

  if (input.confidence_min) {
    const confidenceValue = String(input.confidence_min).toUpperCase();
    if (!["LOW", "MEDIUM", "HIGH"].includes(confidenceValue)) {
      errors.push("invalid confidence_min");
    }
  }

  const isValidEndpoint = (value: string) => {
    try {
      const parsed = new URL(value);
      if (parsed.protocol === "https:") return true;
      if (allowHttp && parsed.protocol === "http:") return true;
      return false;
    } catch {
      return false;
    }
  };
  if (endpoint && !isValidEndpoint(endpoint)) {
    errors.push("invalid endpoint url");
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  return {
    ok: true,
    value: {
      userId,
      entityType: String(input.entity_type ?? "port"),
      entityId,
      severityMin: severityValue as AlertSeverity,
      confidenceMin: input.confidence_min ? String(input.confidence_min).toUpperCase() as AlertConfidence : null,
      channel: channelValue as AlertChannel,
      endpoint,
      secret: input.secret ? String(input.secret) : null,
      isEnabled: Boolean(input.is_enabled ?? true),
    },
  };
};
