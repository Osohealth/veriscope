export type PlaybookContext = {
  clusterType?: string | null;
  status?: string | null;
  destinationType?: string | null;
  skipReason?: string | null;
};

export type AlertPlaybook = {
  version: "1";
  title: string;
  checks: string[];
  actions: string[];
  notes?: string[];
};

export const PLAYBOOK_VERSION: AlertPlaybook["version"] = "1";

const normalize = (value?: string | null) => (value ?? "").toUpperCase();

const pushUnique = (list: string[], value: string) => {
  if (!list.includes(value)) list.push(value);
};

export function getAlertPlaybook(ctx: PlaybookContext): AlertPlaybook {
  const clusterType = normalize(ctx.clusterType);
  const status = normalize(ctx.status);
  const destinationType = normalize(ctx.destinationType);
  const skipReason = normalize(ctx.skipReason);

  const isSla = clusterType.startsWith("SLA_");
  const isPortDisruption =
    clusterType.includes("PORT") ||
    clusterType.includes("DISRUPTION") ||
    clusterType.includes("CONGESTION");

  let title = "Next actions";
  const checks: string[] = [];
  const actions: string[] = [];
  const notes: string[] = [];

  if (isSla) {
    title = clusterType.includes("RECOVERED") || clusterType.includes("OK")
      ? "SLA recovered"
      : "SLA at risk";

    pushUnique(checks, "Check endpoint health and 2xx response rates.");
    pushUnique(checks, "Inspect DLQ depth and last error messages.");
    pushUnique(checks, "Verify retry policy and rate limits.");
    pushUnique(checks, "Review SLA windows (24h / 7d).");

    if (clusterType.includes("RECOVERED") || clusterType.includes("OK")) {
      pushUnique(actions, "Confirm stability over the next hour.");
      pushUnique(actions, "Close incident notes if applicable.");
    } else {
      pushUnique(actions, "Retry failed deliveries or trigger manual retry.");
      pushUnique(actions, "Notify endpoint owner / on-call.");
      pushUnique(actions, "Consider temporary fallback channel.");
    }
  } else if (isPortDisruption) {
    title = "Port disruption playbook";
    pushUnique(checks, "Check weather, strikes, and terminal outages.");
    pushUnique(checks, "Review berth availability and queue trends.");
    pushUnique(checks, "Check river/lock constraints and port advisories.");
    pushUnique(checks, "Validate congestion metrics and recent arrivals.");

    pushUnique(actions, "Notify ops/trading and adjust schedules.");
    pushUnique(actions, "Update ETAs or reroute if feasible.");
    pushUnique(actions, "Increase monitoring cadence for the port.");
  } else {
    title = "Alert response";
    pushUnique(checks, "Review alert details and recent history.");
    pushUnique(actions, "Acknowledge or escalate based on impact.");
  }

  if (clusterType.startsWith("ENDPOINT_")) {
    title = clusterType.includes("RECOVERED") ? "Endpoint recovered" : "Endpoint health degraded";
    pushUnique(checks, "Review endpoint health and recent failure patterns.");
    pushUnique(checks, "Check DNS, TLS, and network reachability.");
    pushUnique(checks, "Inspect retry and timeout settings.");
    pushUnique(actions, "Notify the endpoint owner or on-call.");
    if (clusterType.includes("RECOVERED")) {
      pushUnique(actions, "Verify stability before closing the incident.");
    } else {
      pushUnique(actions, "Consider a fallback channel while the endpoint recovers.");
    }
  }

  if (destinationType === "WEBHOOK") {
    pushUnique(checks, "Verify webhook endpoint TLS/cert health.");
    pushUnique(actions, "Validate webhook receiver and retry logic.");
  }

  if (destinationType === "EMAIL") {
    pushUnique(checks, "Check sender domain, SPF/DKIM, and bounce rate.");
    pushUnique(actions, "Confirm inbox delivery and routing rules.");
  }

  if (status === "FAILED" || status === "DLQ") {
    pushUnique(actions, "Retry delivery from Alert Activity.");
  }

  if (status === "SENT") {
    pushUnique(actions, "Confirm downstream system processed the alert.");
  }

  if (status === "SKIPPED") {
    pushUnique(notes, "This delivery was skipped by gating rules.");
  }

  if (skipReason === "NOISE_BUDGET_EXCEEDED") {
    pushUnique(actions, "Increase noise budget cap or tighten subscription filters.");
  }

  if (skipReason === "QUALITY_BELOW_THRESHOLD") {
    pushUnique(actions, "Lower the minimum quality threshold or review signal sensitivity.");
  }

  if (skipReason === "ENDPOINT_DOWN") {
    pushUnique(checks, "Confirm endpoint is reachable and returning 2xx.");
    pushUnique(checks, "Review endpoint health panel for recent failures.");
    pushUnique(actions, "Pause the subscription or fix the endpoint.");
    pushUnique(actions, "Retry delivery once the endpoint is healthy.");
    pushUnique(notes, "Suppressed to avoid DLQ inflation while the endpoint is down.");
  }

  if (skipReason === "DESTINATION_PAUSED") {
    pushUnique(checks, "Confirm maintenance window or pause reason.");
    pushUnique(actions, "Resume destination when maintenance is complete.");
  }

  if (skipReason === "DESTINATION_AUTO_PAUSED") {
    pushUnique(checks, "Review endpoint health and recent failures.");
    pushUnique(actions, "Resume destination when health stabilizes.");
    pushUnique(notes, "Auto-paused to prevent repeated delivery failures.");
  }

  if (skipReason === "DESTINATION_DISABLED") {
    pushUnique(checks, "Confirm if destination should remain disabled.");
    pushUnique(actions, "Re-enable destination if alerts should resume.");
  }

  if (checks.length === 0) {
    pushUnique(checks, "Review alert details and context.");
  }

  if (actions.length === 0) {
    pushUnique(actions, "Monitor the situation and update stakeholders.");
  }

  return {
    version: PLAYBOOK_VERSION,
    title,
    checks,
    actions,
    notes: notes.length > 0 ? notes : undefined,
  };
}
