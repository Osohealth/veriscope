import type { AlertDecision } from "@shared/alertDecisionDto";
import { PLAYBOOK_VERSION } from "@shared/alertPlaybook";

type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
type DestinationType = "WEBHOOK" | "EMAIL";

type SubscriptionLike = {
  id: string;
  scope: "GLOBAL" | "PORT";
  entityId: string;
  severityMin: Severity;
  destinationType: DestinationType;
  destination: string;
  enabled: boolean;
};

type SelectedClusterLike = {
  cluster_id: string;
  cluster_type: string;
  cluster_severity: Severity;
  confidence_score: number;
  confidence_band: "HIGH" | "MEDIUM" | "LOW";
  cluster_summary: string;
  entity?: { id: string; type: string; name: string; code: string; unlocode: string } | null;
};

export function redactDestination(dest: string): string {
  try {
    if (dest.includes("@")) {
      const [user, domain] = dest.split("@");
      return `${user.slice(0, 2)}***@${domain}`;
    }
    const url = new URL(dest);
    return `${url.protocol}//${url.host}${url.pathname}`;
  } catch {
    return dest.slice(0, 8) + "***";
  }
}

export function buildAlertDecision(args: {
  evaluatedAtIso: string;
  day: string;
  clustered: boolean;
  subscription: SubscriptionLike;
  included: SelectedClusterLike[];
  bundle: { enabled: boolean; topN: number; overflow: number; excludedPreview?: AlertDecision["selection"]["bundle"]["excluded_preview"] };
  gates: AlertDecision["gates"];
  suppressedCounts: AlertDecision["suppressed_counts"];
}): AlertDecision {
  const { evaluatedAtIso, day, clustered, subscription, included, bundle, suppressedCounts } = args;
  const gates = { ...args.gates } as AlertDecision["gates"];
  if (!gates.destination_state) {
    (gates as any).destination_state = {
      applied: false,
      state: "UNKNOWN",
      allowed: true,
    };
  }

  return {
    version: "1",
    evaluated_at: evaluatedAtIso,
    playbook_version: PLAYBOOK_VERSION,
    subscription: {
      id: subscription.id,
      scope: subscription.scope,
      entity_id: subscription.entityId,
      severity_min: subscription.severityMin,
      destination_type: subscription.destinationType,
      destination_redacted: redactDestination(subscription.destination),
      enabled: subscription.enabled,
    },
    selection: {
      day,
      window: "DAY",
      clustered,
      bundle: {
        enabled: bundle.enabled,
        top_n: bundle.topN,
        size: included.length,
        overflow: bundle.overflow,
        included: included.map((c, idx) => ({
          cluster_id: c.cluster_id,
          cluster_type: c.cluster_type,
          cluster_severity: c.cluster_severity,
          confidence_score: c.confidence_score,
          confidence_band: c.confidence_band,
          summary: c.cluster_summary,
          reason_rank: idx + 1,
          entity: c.entity ?? null,
        })),
        excluded_preview: bundle.excludedPreview,
      },
    },
    gates,
    suppressed_counts: suppressedCounts,
  };
}
