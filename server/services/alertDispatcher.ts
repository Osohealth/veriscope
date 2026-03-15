import { eq, and, sql, inArray } from "drizzle-orm";
import { randomUUID } from "node:crypto";
import { db } from "../db";
import { alertDeliveries, alertDeliveryAttempts, alertDlq, alertEndpointHealth, alertRuns, alertSubscriptions, ports, tenantUsers } from "@shared/schema";
import { GLOBAL_SCOPE_ENTITY_ID, normalizeScope } from "./alertScope";
import { getAlertCandidates } from "./alertQuery";
import { shouldSendAlert, markAlertSent } from "./alertDedupe";
import { buildWebhookPayload, buildWebhookRequest, computeBundleIdempotencyKey, sendWebhook } from "./webhookSender";
import { renderAlertBundleEmail, renderAlertEmail, sendEmail } from "./emailSender";
import { SEVERITY_RANK } from "@shared/signalTypes";
import { computeNextAttempt } from "./alertDlq";
import { ALERT_BUNDLE_MAX_BYTES, ALERT_BUNDLE_TOP_N, ALERT_DEDUPE_TTL_HOURS, ALERT_RATE_LIMIT_PER_ENDPOINT, DLQ_MAX_ATTEMPTS } from "../config/alerting";
import { TENANT_DEMO_ID } from "../config/tenancy";
import { recordNoiseBudgetBreachOnce } from "./alertNoiseBudgetService";
import { resolveNoiseBudget, type ResolvedNoiseBudget } from "./alertDestinationOverridesService";
import { writeAuditEvent } from "./auditLog";
import { computeAlertQuality } from "./alertQualityService";
import { recordQualityGateSuppressOnce } from "./alertQualityGateService";
import { buildAlertDecision } from "./alertDecisionBuilder";
import { hashKey, makeDestinationKey } from "./destinationKey";
import { getDestinationGate } from "./alertDestinationGate";
import { incrementOpsCounter, logOpsEvent, recordDeliveryErrorCategory, recordDeliveryLatency } from "./opsTelemetry";
import type { DeliveryInsertShape, GateResult } from "../types/alerting";

type DestinationState = "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED" | "UNKNOWN";
type SlaSystemAlertArgs = {
  tenantId: string;
  window: "24h" | "7d";
  destinationType: string;
  destinationKey: string;
  status: "AT_RISK" | "OK";
  metrics: { p95_ms: number; success_rate_pct: number };
  thresholds: { p95_ms: number; success_rate_pct: number };
  thresholdSource?: "DESTINATION" | "TENANT_DEFAULT";
  computedAt: Date;
  now?: Date;
};

type EndpointHealthSystemAlertArgs = {
  tenantId: string;
  window: EndpointHealthWindow;
  destinationType: string;
  destination: string;
  status: EndpointHealthStatus;
  metrics: EndpointHealthMetrics;
  computedAt: Date;
  now?: Date;
};

type DestinationStateSystemAlertArgs = {
  tenantId: string;
  window: EndpointHealthWindow;
  destinationType: string;
  destination: string;
  destinationKey: string;
  state: "AUTO_PAUSED" | "PAUSED" | "DISABLED";
  reason?: string | null;
  computedAt: Date;
  now?: Date;
};

type IncidentEscalationSystemAlertArgs = {
  tenantId: string;
  now?: Date;
  incident: {
    id: string;
    type: string;
    severity: string;
    destinationKey?: string | null;
    title: string;
    summary: string;
    openedAt: Date;
  };
  level: number;
  policy: {
    id?: string;
    targetType: string;
    targetRef: string;
    afterMinutes: number;
    severityMin: string;
  };
  destinations?: Array<{
    destinationType: "EMAIL" | "WEBHOOK";
    destination: string;
    destinationKey?: string;
    targetUserId?: string | null;
  }>;
  routingGate?: {
    allowed: boolean;
    reason?: "NO_USER_CONTACT_METHOD";
    chosen_method_type?: "EMAIL" | "WEBHOOK";
    chosen_method_id?: string;
    target_user_id?: string;
  };
};

type RunAlertsOptions = {
  day?: string;
  tenantId?: string;
  userId?: string;
  now?: Date;
};

type EndpointHealthStatus = "OK" | "DEGRADED" | "DOWN";
type EndpointHealthWindow = "1h" | "24h";
type EndpointHealthMetrics = {
  success_rate: number;
  p95_ms: number | null;
  last_success_at?: Date | null;
  last_failure_at?: Date | null;
};

const buildCandidateQuality = (candidate: any) =>
  computeAlertQuality({
    severity: String(candidate.cluster_severity ?? candidate.severity ?? "LOW").toUpperCase(),
    confidence_band: String(candidate.confidence_band ?? "LOW").toUpperCase(),
    confidence_score: typeof candidate.confidence_score === "number" ? candidate.confidence_score : undefined,
    data_quality: candidate.data_quality ?? candidate.metadata?.data_quality,
    cluster_type: candidate.cluster_type ?? null,
    method: candidate.method ?? null,
    explainability: candidate.explainability ?? null,
  } as any);

const buildSystemQuality = (status: "AT_RISK" | "OK") =>
  computeAlertQuality({
    severity: status === "AT_RISK" ? "HIGH" : "LOW",
    confidence_band: status === "AT_RISK" ? "HIGH" : "LOW",
    confidence_score: status === "AT_RISK" ? 1 : 0.4,
    cluster_type: status === "AT_RISK" ? "SLA_AT_RISK" : "SLA_RECOVERED",
  } as any);

const buildEndpointHealthQuality = (status: EndpointHealthStatus) =>
  computeAlertQuality({
    severity: status === "DOWN" ? "HIGH" : status === "DEGRADED" ? "MEDIUM" : "LOW",
    confidence_band: status === "DOWN" ? "HIGH" : status === "DEGRADED" ? "MEDIUM" : "LOW",
    confidence_score: status === "DOWN" ? 1 : status === "DEGRADED" ? 0.7 : 0.4,
    cluster_type: status === "DOWN" ? "ENDPOINT_DOWN" : status === "DEGRADED" ? "ENDPOINT_DEGRADED" : "ENDPOINT_RECOVERED",
  } as any);

const buildIncidentEscalationQuality = (severity: string) =>
  computeAlertQuality({
    severity: String(severity ?? "HIGH").toUpperCase(),
    confidence_band: "HIGH",
    confidence_score: 1,
    cluster_type: "INCIDENT_ESCALATION",
  } as any);

const defaultEndpointGate = {
  applied: false,
  window: "1h",
  status: "UNKNOWN",
  allowed: true,
} as const;

const getEndpointHealthGate = async (args: {
  tenantId: string;
  destinationType: string;
  destination: string;
  window?: EndpointHealthWindow;
}) => {
  const window = args.window ?? "1h";
  const [row] = await db
    .select()
    .from(alertEndpointHealth)
    .where(and(
      eq(alertEndpointHealth.tenantId, args.tenantId),
      eq(alertEndpointHealth.window, window),
      eq(alertEndpointHealth.destinationType, args.destinationType),
      eq(alertEndpointHealth.destination, args.destination),
    ))
    .limit(1);
  if (!row) {
    return { ...defaultEndpointGate, window };
  }
  const status = (row.status ?? "UNKNOWN") as EndpointHealthStatus | "UNKNOWN";
  return {
    applied: true,
    window,
    status,
    allowed: status !== "DOWN",
    snapshot: row,
  };
};

const getDestinationStateGate = async (args: {
  tenantId: string;
  destinationType: string;
  destination: string;
  now: Date;
}) => {
  const destinationKey = makeDestinationKey(args.destinationType, args.destination);
  const gate = await getDestinationGate({
    tenantId: args.tenantId,
    destinationKey,
    now: args.now,
  });
  return {
    applied: gate.applied,
    destinationKey,
    state: gate.state,
    allowed: gate.state === "ACTIVE",
    reason: gate.reason,
    readyToResume: gate.readyToResume,
  };
};

const toDecisionDestinationGate = (gate: {
  applied: boolean;
  state: DestinationState;
  allowed: boolean;
  reason?: string;
  readyToResume?: boolean;
}) => ({
  applied: gate.applied,
  state: gate.state,
  allowed: gate.allowed,
  ready_to_resume: gate.readyToResume,
  ...(gate.reason ? { reason: gate.reason } : {}),
});

const buildNoiseBudgetGate = (budget: ResolvedNoiseBudget, allowedOverride?: boolean) => ({
  applied: true,
  enabled: budget.enabled,
  source: budget.source,
  window: budget.window ?? "custom",
  window_minutes: budget.window_minutes ?? null,
  max: budget.max_deliveries ?? 0,
  max_deliveries: budget.max_deliveries ?? 0,
  used_before: budget.used_in_window ?? 0,
  used_in_window: budget.used_in_window ?? 0,
  allowed: allowedOverride ?? budget.allowed,
});

const QUALITY_BAND_RANK: Record<string, number> = {
  LOW: 1,
  MEDIUM: 2,
  HIGH: 3,
};

const toSeverityKey = (value: unknown) =>
  String(value ?? "").toUpperCase() as keyof typeof SEVERITY_RANK;
const getSeverityRank = (value: unknown) => SEVERITY_RANK[toSeverityKey(value)] ?? 0;

const insertDelivery = (values: any): any => {
  incrementOpsCounter("deliveries_created_total");
  return db.insert(alertDeliveries).values({
    ...values,
    destinationKey: values.destinationKey ?? makeDestinationKey(values.destinationType, values.endpoint),
  });
};

const logDeliveryCreated = (baseLog: Record<string, any>, details: Record<string, any>) => {
  logOpsEvent("DELIVERY_CREATED", { ...baseLog, ...details });
};

const logDeliveryResult = (baseLog: Record<string, any>, details: Record<string, any>) => {
  if (details.status === "SENT") {
    incrementOpsCounter("deliveries_success_total");
  } else if (details.status === "FAILED") {
    incrementOpsCounter("deliveries_failure_total");
  }
  if (details.destinationType && details.latencyMs !== undefined && details.latencyMs !== null) {
    const destType = String(details.destinationType).toUpperCase() as "EMAIL" | "WEBHOOK";
    if (destType === "EMAIL" || destType === "WEBHOOK") {
      recordDeliveryLatency(destType, Number(details.latencyMs));
    }
  }
  if (details.errorCategory) {
    const category = String(details.errorCategory).toLowerCase();
    if (["network", "http", "validation", "unknown"].includes(category)) {
      recordDeliveryErrorCategory(category as any);
    } else {
      recordDeliveryErrorCategory("unknown");
    }
  }
  logOpsEvent("DELIVERY_RESULT", { ...baseLog, ...details });
};

const isBelowQualityGate = (sub: any, quality: { score: number; band: string }) => {
  if (sub?.minQualityScore !== null && sub?.minQualityScore !== undefined) {
    const threshold = Number(sub.minQualityScore);
    if (Number.isFinite(threshold)) {
      return quality.score < threshold;
    }
  }

  if (sub?.minQualityBand) {
    const required = QUALITY_BAND_RANK[String(sub.minQualityBand).toUpperCase()] ?? 0;
    const actual = QUALITY_BAND_RANK[String(quality.band).toUpperCase()] ?? 0;
    return actual < required;
  }

  return false;
};

type BundleCandidate = {
  candidate: any;
  quality: { score: number; band: string; reasons: any[]; version: string };
  severityRank: number;
  confidenceScore: number;
  createdAtMs: number;
  clusterId: string;
};

const normalizeDayValue = (day: any) => (
  day instanceof Date ? day.toISOString().slice(0, 10) : String(day)
);

const buildBundleItems = (
  items: BundleCandidate[],
  entityMap: Map<string, { id: string; name: string; code: string; unlocode: string | null }> | null,
) => items.map((entry) => {
  const row = entry.candidate;
  const entity = entityMap?.get(String(row.entity_id ?? row.entityId));
  return {
    cluster_id: String(row.cluster_id ?? row.clusterId ?? ""),
    cluster_type: row.cluster_type ?? row.clusterType ?? null,
    cluster_summary: row.cluster_summary ?? row.clusterSummary ?? null,
    day: normalizeDayValue(row.day),
    severity: String(row.cluster_severity ?? row.clusterSeverity ?? row.severity ?? "LOW").toUpperCase(),
    confidence: {
      score: typeof row.confidence_score === "number" ? row.confidence_score : row.confidenceScore ?? null,
      band: row.confidence_band ?? row.confidenceBand ?? null,
      method: row.method ?? null,
    },
    quality: {
      score: entry.quality.score,
      band: entry.quality.band,
      reasons: entry.quality.reasons ?? [],
      version: entry.quality.version,
    },
    entity: entity
      ? { id: entity.id, type: "port", name: entity.name, code: entity.code, unlocode: entity.unlocode }
      : null,
  };
});

const buildDecisionIncluded = (items: any[]) => items.map((item, idx) => ({
  cluster_id: String(item.cluster_id ?? ""),
  cluster_type: item.cluster_type ?? "UNKNOWN",
  cluster_severity: String(item.severity ?? "LOW").toUpperCase(),
  confidence_score: typeof item?.confidence?.score === "number" ? item.confidence.score : 0,
  confidence_band: String(item?.confidence?.band ?? "LOW").toUpperCase(),
  cluster_summary: item.cluster_summary ?? "",
  entity: item.entity ?? null,
  reason_rank: idx + 1,
}));

const sortBundleCandidates = (a: BundleCandidate, b: BundleCandidate) => {
  if (a.severityRank !== b.severityRank) return b.severityRank - a.severityRank;
  if (a.quality.score !== b.quality.score) return b.quality.score - a.quality.score;
  if (a.confidenceScore !== b.confidenceScore) return b.confidenceScore - a.confidenceScore;
  if (a.createdAtMs !== b.createdAtMs) return b.createdAtMs - a.createdAtMs;
  return a.clusterId.localeCompare(b.clusterId);
};

const applyBundlePayloadLimit = (
  payload: any,
  overflow: number,
): { payload: any; items: any[]; overflow: number; truncated: number } => {
  if (!payload || !Array.isArray(payload.items) || ALERT_BUNDLE_MAX_BYTES <= 0) {
    return { payload, items: payload?.items ?? [], overflow, truncated: 0 };
  }

  let items = payload.items.slice();
  let truncated = 0;
  let serialized = JSON.stringify({ ...payload, items });

  while (items.length > 1 && serialized.length > ALERT_BUNDLE_MAX_BYTES) {
    items.pop();
    truncated += 1;
    serialized = JSON.stringify({ ...payload, items });
  }

  const nextOverflow = overflow + truncated;
  const summary = payload.summary
    ? {
        ...payload.summary,
        overflow: nextOverflow,
        sent_items: typeof payload.summary.sent_items === "number" && payload.summary.sent_items > 0
          ? items.length
          : payload.summary.sent_items,
      }
    : undefined;

  const nextPayload = {
    ...payload,
    items,
    ...(truncated > 0 ? { bundle_truncated: true, bundle_truncated_items: truncated } : {}),
    ...(summary ? { summary } : {}),
  };

  return { payload: nextPayload, items, overflow: nextOverflow, truncated };
};

export async function runAlerts(options: RunAlertsOptions = {}) {
  const now = options.now ?? new Date();
  const evaluatedAtIso = now.toISOString();
  const nowBucket = new Date(Math.floor(now.getTime() / 60000) * 60000);
  const tenantId = options.tenantId ?? TENANT_DEMO_ID;
  const [runRow] = await db
    .insert(alertRuns)
    .values({
      tenantId,
      day: options.day ?? null,
      status: "SUCCESS",
      startedAt: now,
    })
    .returning();

  const summary = {
    day: options.day ?? null,
    candidates_total: 0,
    subscriptions: 0,
    matched_total: 0,
    suppressed_quality_total: 0,
    sent_total: 0,
    skipped_dedupe_total: 0,
    failed_total: 0,
    skipped_rate_limit_total: 0,
    skipped_noise_budget_total: 0,
  };
  const errorDetails: { endpoint: string; cluster_id: string; error_type: string; message: string }[] = [];
  const perEndpointCount = new Map<string, number>();

  try {
    const subscriptions = await db
      .select()
      .from(alertSubscriptions)
      .where(and(
        eq(alertSubscriptions.tenantId, tenantId),
        alertSubscriptions.isEnabled,
      ));

    summary.subscriptions = subscriptions.length;

    for (const sub of subscriptions) {
      if (options.userId && sub.userId !== options.userId) {
        continue;
      }

      const scope = normalizeScope((sub as any).scope);
      const destinationGate = await getDestinationStateGate({
        tenantId,
        destinationType: sub.channel,
        destination: sub.endpoint,
        now,
      });
      const candidates = await getAlertCandidates({
        day: options.day,
        entityType: sub.entityType as "port",
        entityId: scope === "GLOBAL" ? undefined : sub.entityId,
        severityMin: sub.severityMin as any,
      });

      const wrappedCandidates: BundleCandidate[] = [];
      for (const candidate of candidates) {
        if (sub.confidenceMin) {
          const required = getSeverityRank(sub.confidenceMin);
          const actual = getSeverityRank(candidate.confidence_band ?? "LOW");
          if (actual < required) {
            continue;
          }
        }

        const quality = buildCandidateQuality(candidate);
        const severity = String(candidate.cluster_severity ?? candidate.severity ?? "LOW").toUpperCase();
        const severityRank = getSeverityRank(severity);
        const confidenceScore = Number(candidate.confidence_score ?? 0);
        const createdAtMs = candidate.created_at ? new Date(candidate.created_at).getTime() : 0;
        wrappedCandidates.push({
          candidate,
          quality,
          severityRank,
          confidenceScore,
          createdAtMs,
          clusterId: String(candidate.cluster_id),
        });
      }

      summary.candidates_total += wrappedCandidates.length;
      summary.matched_total += wrappedCandidates.length;

      const suppressed: BundleCandidate[] = [];
      const dedupeEligible: BundleCandidate[] = [];
      for (const entry of wrappedCandidates) {
        if (isBelowQualityGate(sub, entry.quality)) {
          suppressed.push(entry);
          continue;
        }
        dedupeEligible.push(entry);
      }

      if (suppressed.length > 0) {
        summary.suppressed_quality_total += suppressed.length;
      }

      let skippedDedupe = 0;
      const sendable: BundleCandidate[] = [];
      for (const entry of dedupeEligible) {
        const dedupeKey = {
          tenantId,
          clusterId: entry.clusterId,
          channel: sub.channel,
          endpoint: sub.endpoint,
        };
        const allowed = await shouldSendAlert({ ...dedupeKey, now });
        if (!allowed) {
          skippedDedupe += 1;
          continue;
        }
        sendable.push(entry);
      }

      summary.skipped_dedupe_total += skippedDedupe;

      const sortedSuppressed = suppressed.slice().sort(sortBundleCandidates);
      if (sortedSuppressed.length > 0) {
        const primarySuppressed = sortedSuppressed[0];
        const dayValue = normalizeDayValue(primarySuppressed.candidate.day);
        const inserted = await recordQualityGateSuppressOnce({
          tenantId,
          subscriptionId: sub.id,
          day: dayValue,
        });
        if (inserted) {
          await writeAuditEvent(undefined, {
            tenantId,
            actorType: "SYSTEM",
            action: "ALERT.QUALITY_GATE_SUPPRESSING",
            resourceType: "ALERT_SUBSCRIPTION",
            resourceId: sub.id,
            severity: "WARN",
            status: "DENIED",
            message: "Alert suppressed due to quality gate.",
            metadata: {
              min_quality_score: sub.minQualityScore ?? null,
              min_quality_band: sub.minQualityBand ?? null,
              quality_score: primarySuppressed.quality.score,
              quality_band: primarySuppressed.quality.band,
              cluster_id: String(primarySuppressed.candidate.cluster_id),
              day: normalizeDayValue(primarySuppressed.candidate.day),
            },
          });
        }
      }

      if (sendable.length === 0) {
        if (sortedSuppressed.length > 0) {
          const primary = sortedSuppressed[0];
          const topSuppressed = sortedSuppressed.slice(0, ALERT_BUNDLE_TOP_N);
          const overflow = Math.max(sortedSuppressed.length - topSuppressed.length, 0);
          const dayValue = normalizeDayValue(primary.candidate.day);
          const entityIds = Array.from(new Set(topSuppressed.map((entry) => String(entry.candidate.entity_id ?? entry.candidate.entityId))))
            .filter(Boolean);
          const entityRows = entityIds.length > 0
            ? await db.select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
              .from(ports)
              .where(inArray(ports.id, entityIds))
            : [];
          const entityMap = new Map(entityRows.map((row) => [row.id, row]));

          const scopeValue = normalizeScope((sub as any).scope);
          const subscriptionEntity = scopeValue === "GLOBAL"
            ? null
            : entityMap.get(String(sub.entityId)) ?? null;
          const items = buildBundleItems(topSuppressed, entityMap);
          const bundlePayloadRaw = {
            payload_version: "1.2",
            type: "ALERT_BUNDLE",
            sent_at: now.toISOString(),
            tenant_id: tenantId,
            run_id: runRow.id,
            subscription: {
              id: sub.id,
              scope: scopeValue,
              entity: subscriptionEntity
                ? { id: subscriptionEntity.id, type: "port", name: subscriptionEntity.name, code: subscriptionEntity.code, unlocode: subscriptionEntity.unlocode }
                : null,
              severity_min: sub.severityMin,
              destination_type: sub.channel,
              destination: sub.endpoint,
            },
            summary: {
              matched_total: wrappedCandidates.length,
              sent_items: 0,
              overflow,
              skipped_dedupe: skippedDedupe,
              skipped_noise_budget: 0,
              suppressed_quality: suppressed.length,
            },
            items,
          };
          const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, overflow);
          const decision = buildAlertDecision({
            evaluatedAtIso,
            day: normalizeDayValue(primary.candidate.day),
            clustered: true,
            subscription: {
              id: sub.id,
              scope: scopeValue,
              entityId: String(sub.entityId),
              severityMin: sub.severityMin,
              destinationType: sub.channel,
              destination: sub.endpoint,
              enabled: Boolean(sub.isEnabled),
            },
            included: buildDecisionIncluded(finalItems),
            bundle: {
              enabled: true,
              topN: ALERT_BUNDLE_TOP_N,
              overflow: finalOverflow,
            },
            gates: {
              severity_min_pass: true,
              dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
              noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
              quality: {
                applied: true,
                score: primary.quality.score,
                band: primary.quality.band,
                suppressed: true,
                reason: "QUALITY_BELOW_THRESHOLD",
              },
              rate_limit: { applied: false, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
              endpoint_health: defaultEndpointGate,
              destination_state: toDecisionDestinationGate(destinationGate),
            },
            suppressedCounts: {
              dedupe: skippedDedupe,
              noise_budget: 0,
              quality: suppressed.length,
              overflow: finalOverflow,
            },
          });

          await insertDelivery({
            runId: runRow.id,
            tenantId,
            userId: sub.userId,
            subscriptionId: sub.id,
            clusterId: String(primary.candidate.cluster_id),
            entityType: primary.candidate.entity_type ?? "port",
            entityId: String(primary.candidate.entity_id),
            day: dayValue,
            destinationType: sub.channel,
            endpoint: sub.endpoint,
            status: "SKIPPED_QUALITY",
            skipReason: "QUALITY_BELOW_THRESHOLD",
            isBundle: true,
            bundleSize: finalItems.length,
            bundleOverflow: finalOverflow,
            bundlePayload: bundlePayload,
            decision,
            qualityScore: primary.quality.score,
            qualityBand: primary.quality.band,
            qualityReasons: primary.quality.reasons,
            qualityVersion: primary.quality.version,
            attempts: 0,
            createdAt: now,
          });
        }
        continue;
      }

      const sortedSendable = sendable.slice().sort(sortBundleCandidates);
      const topItems = sortedSendable.slice(0, ALERT_BUNDLE_TOP_N);
      const overflow = Math.max(sortedSendable.length - topItems.length, 0);
      const primary = topItems[0];
      const dayValue = normalizeDayValue(primary.candidate.day);
      const entityIds = Array.from(new Set(topItems.map((entry) => String(entry.candidate.entity_id ?? entry.candidate.entityId))))
        .filter(Boolean);
      const entityRows = entityIds.length > 0
        ? await db.select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
          .from(ports)
          .where(inArray(ports.id, entityIds))
        : [];
      const entityMap = new Map(entityRows.map((row) => [row.id, row]));
      const scopeValue = normalizeScope((sub as any).scope);
      const subscriptionEntity = scopeValue === "GLOBAL"
        ? null
        : entityMap.get(String(sub.entityId)) ?? null;
      const items = buildBundleItems(topItems, entityMap);

      const bundleSummaryBase = {
        matched_total: wrappedCandidates.length,
        skipped_dedupe: skippedDedupe,
        suppressed_quality: suppressed.length,
      };

      if (!destinationGate.allowed) {
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity
              ? { id: subscriptionEntity.id, type: "port", name: subscriptionEntity.name, code: subscriptionEntity.code, unlocode: subscriptionEntity.unlocode }
              : null,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow,
            skipped_noise_budget: 0,
          },
          items,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, overflow);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day: normalizeDayValue(primary.candidate.day),
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: { applied: true, score: primary.quality.score, band: primary.quality.band, suppressed: false },
            rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: {
              applied: defaultEndpointGate.applied,
              window: defaultEndpointGate.window,
              status: defaultEndpointGate.status as any,
              allowed: true,
            },
            destination_state: toDecisionDestinationGate(destinationGate),
          },
          suppressedCounts: {
            dedupe: skippedDedupe,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        const skipReason = destinationGate.state === "PAUSED"
          ? "DESTINATION_PAUSED"
          : destinationGate.state === "AUTO_PAUSED"
            ? "DESTINATION_AUTO_PAUSED"
            : "DESTINATION_DISABLED";

        const status = destinationGate.state === "PAUSED"
          ? "SKIPPED_DESTINATION_PAUSED"
          : destinationGate.state === "AUTO_PAUSED"
            ? "SKIPPED_DESTINATION_AUTO_PAUSED"
            : "SKIPPED_DESTINATION_DISABLED";

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId: String(primary.candidate.cluster_id),
          entityType: primary.candidate.entity_type ?? "port",
          entityId: String(primary.candidate.entity_id),
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status,
          skipReason,
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: primary.quality.score,
          qualityBand: primary.quality.band,
          qualityReasons: primary.quality.reasons,
          qualityVersion: primary.quality.version,
          attempts: 0,
          createdAt: now,
        });
        continue;
      }

      const endpointGate = await getEndpointHealthGate({
        tenantId,
        destinationType: sub.channel,
        destination: sub.endpoint,
      });

      if (!endpointGate.allowed) {
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity
              ? { id: subscriptionEntity.id, type: "port", name: subscriptionEntity.name, code: subscriptionEntity.code, unlocode: subscriptionEntity.unlocode }
              : null,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow,
            skipped_noise_budget: 0,
          },
          items,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, overflow);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day: normalizeDayValue(primary.candidate.day),
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: { applied: true, score: primary.quality.score, band: primary.quality.band, suppressed: false },
            rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: {
              applied: endpointGate.applied,
              window: endpointGate.window,
              status: endpointGate.status as any,
              allowed: false,
            },
            destination_state: toDecisionDestinationGate(destinationGate),
          },
          suppressedCounts: {
            dedupe: skippedDedupe,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId: String(primary.candidate.cluster_id),
          entityType: primary.candidate.entity_type ?? "port",
          entityId: String(primary.candidate.entity_id),
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SKIPPED_ENDPOINT_DOWN",
          skipReason: "ENDPOINT_DOWN",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: primary.quality.score,
          qualityBand: primary.quality.band,
          qualityReasons: primary.quality.reasons,
          qualityVersion: primary.quality.version,
          attempts: 0,
          createdAt: now,
        });
        continue;
      }

      const noiseBudget = await resolveNoiseBudget({
        tenantId,
        destinationType: sub.channel as any,
        destinationKey: destinationGate.destinationKey,
        destination: sub.endpoint,
        now,
      });
      if (!noiseBudget.allowed) {
        summary.skipped_noise_budget_total += sendable.length;
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity
              ? { id: subscriptionEntity.id, type: "port", name: subscriptionEntity.name, code: subscriptionEntity.code, unlocode: subscriptionEntity.unlocode }
              : null,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow,
            skipped_noise_budget: sendable.length,
          },
          items,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, overflow);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day: normalizeDayValue(primary.candidate.day),
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: buildNoiseBudgetGate(noiseBudget),
            quality: { applied: true, score: primary.quality.score, band: primary.quality.band, suppressed: false },
            rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: {
              applied: endpointGate.applied,
              window: endpointGate.window,
              status: endpointGate.status as any,
              allowed: true,
            },
            destination_state: toDecisionDestinationGate(destinationGate),
          },
          suppressedCounts: {
            dedupe: skippedDedupe,
            noise_budget: sendable.length,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId: String(primary.candidate.cluster_id),
          entityType: primary.candidate.entity_type ?? "port",
          entityId: String(primary.candidate.entity_id),
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SKIPPED_NOISE_BUDGET",
          skipReason: "NOISE_BUDGET_EXCEEDED",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: primary.quality.score,
          qualityBand: primary.quality.band,
          qualityReasons: primary.quality.reasons,
          qualityVersion: primary.quality.version,
          attempts: 0,
          createdAt: now,
        });

        if (noiseBudget.enabled) {
          const inserted = await recordNoiseBudgetBreachOnce({
            tenantId,
            destinationType: sub.channel as any,
            window: noiseBudget.window ?? "custom",
            bucketMinute: nowBucket,
          });
          if (inserted) {
            await writeAuditEvent(undefined, {
              tenantId,
              actorType: "SYSTEM",
              action: "ALERT.NOISE_BUDGET_EXCEEDED",
              resourceType: "ALERT_DELIVERY",
              severity: "WARN",
              status: "DENIED",
              message: "Noise budget exceeded for destination.",
              metadata: {
                destination_type: sub.channel,
                destination_key: destinationGate.destinationKey,
                source: noiseBudget.source,
                window: noiseBudget.window,
                window_minutes: noiseBudget.window_minutes,
                max_deliveries: noiseBudget.max_deliveries,
                count: noiseBudget.used_in_window,
                window_start: noiseBudget.window_start?.toISOString?.() ?? null,
              },
            });
          }
        }
        continue;
      }

      const rateKey = sub.id;
      const currentCount = perEndpointCount.get(rateKey) ?? 0;
      if (currentCount >= ALERT_RATE_LIMIT_PER_ENDPOINT) {
        summary.skipped_rate_limit_total += sendable.length;
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity
              ? { id: subscriptionEntity.id, type: "port", name: subscriptionEntity.name, code: subscriptionEntity.code, unlocode: subscriptionEntity.unlocode }
              : null,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow,
            skipped_noise_budget: 0,
          },
          items,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, overflow);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day: normalizeDayValue(primary.candidate.day),
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: { applied: true, score: primary.quality.score, band: primary.quality.band, suppressed: false },
            rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: false },
            endpoint_health: {
              applied: endpointGate.applied,
              window: endpointGate.window,
              status: endpointGate.status as any,
              allowed: true,
            },
            destination_state: toDecisionDestinationGate(destinationGate),
          },
          suppressedCounts: {
            dedupe: skippedDedupe,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId: String(primary.candidate.cluster_id),
          entityType: primary.candidate.entity_type ?? "port",
          entityId: String(primary.candidate.entity_id),
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SKIPPED_RATE_LIMIT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: primary.quality.score,
          qualityBand: primary.quality.band,
          qualityReasons: primary.quality.reasons,
          qualityVersion: primary.quality.version,
          attempts: 0,
          createdAt: now,
        });
        continue;
      }

      const bundlePayloadRaw = {
        payload_version: "1.2",
        type: "ALERT_BUNDLE",
        sent_at: now.toISOString(),
        tenant_id: tenantId,
        run_id: runRow.id,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entity: subscriptionEntity
            ? { id: subscriptionEntity.id, type: "port", name: subscriptionEntity.name, code: subscriptionEntity.code, unlocode: subscriptionEntity.unlocode }
            : null,
          severity_min: sub.severityMin,
          destination_type: sub.channel,
          destination: sub.endpoint,
        },
        summary: {
          ...bundleSummaryBase,
          sent_items: items.length,
          overflow,
          skipped_noise_budget: 0,
        },
        items,
      };
      const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, overflow);
      const finalCandidates = topItems.slice(0, finalItems.length);
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day: normalizeDayValue(primary.candidate.day),
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: buildNoiseBudgetGate(noiseBudget),
          quality: { applied: true, score: primary.quality.score, band: primary.quality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: skippedDedupe,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });

      try {
        if (sub.channel === "WEBHOOK") {
          const bundleDay = normalizeDayValue(primary.candidate.day);
          const { body, headers } = buildWebhookRequest({
            payload: bundlePayload,
            secret: sub.secret ?? null,
            subscriptionId: sub.id,
            clusterId: String(primary.candidate.cluster_id),
            day: bundleDay,
            now,
            idempotencyKey: computeBundleIdempotencyKey(sub.id, runRow.id, bundleDay),
          });

          const result = await sendWebhook({ endpoint: sub.endpoint, body, headers });
          const attemptLogs = (result as any)?.attemptLogs ?? [];
          const attempts = attemptLogs.length || 1;
          const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

          for (const log of attemptLogs) {
            console.log({
              run_id: runRow.id,
              subscription_id: sub.id,
              cluster_id: primary.candidate.cluster_id,
              endpoint: sub.endpoint,
              attempt: log.attempt,
              status: log.status,
              latency_ms: log.latency_ms,
              http_status: log.http_status,
            });
          }

          const [delivery] = await insertDelivery({
            runId: runRow.id,
            tenantId,
            userId: sub.userId,
            subscriptionId: sub.id,
            clusterId: String(primary.candidate.cluster_id),
            entityType: primary.candidate.entity_type ?? "port",
            entityId: String(primary.candidate.entity_id),
            day: dayValue,
            destinationType: sub.channel,
            endpoint: sub.endpoint,
            status: "SENT",
            isBundle: true,
            bundleSize: finalItems.length,
            bundleOverflow: finalOverflow,
            bundlePayload: bundlePayload,
            decision,
            attempts,
            lastHttpStatus: last?.http_status ?? result?.status ?? null,
            latencyMs: last?.latency_ms ?? null,
            sentAt: now,
            qualityScore: primary.quality.score,
            qualityBand: primary.quality.band,
            qualityReasons: primary.quality.reasons,
            qualityVersion: primary.quality.version,
            createdAt: now,
          }).returning();

          const baseAttempt = Math.max((delivery?.attempts ?? 0) - attempts, 0);
          const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
            .map((log: any) => ({
              tenantId,
              deliveryId: delivery.id,
              attemptNo: baseAttempt + log.attempt,
              status: log.status === "SUCCESS" ? "SENT" : "FAILED",
              latencyMs: log.latency_ms ?? null,
              httpStatus: log.http_status ?? null,
              error: log.status === "SUCCESS" ? null : "Webhook delivery failed",
              sentAt: log.status === "SUCCESS" ? now : null,
              createdAt: now,
            }));
          if (attemptRows.length) {
            await db.insert(alertDeliveryAttempts).values(attemptRows);
          }
        } else if (sub.channel === "EMAIL") {
          const email = renderAlertBundleEmail({ payload: bundlePayload });
          await sendEmail({ to: sub.endpoint, subject: email.subject, text: email.text });
          const [delivery] = await insertDelivery({
            runId: runRow.id,
            tenantId,
            userId: sub.userId,
            subscriptionId: sub.id,
            clusterId: String(primary.candidate.cluster_id),
            entityType: primary.candidate.entity_type ?? "port",
            entityId: String(primary.candidate.entity_id),
            day: dayValue,
            destinationType: sub.channel,
            endpoint: sub.endpoint,
            status: "SENT",
            isBundle: true,
            bundleSize: finalItems.length,
            bundleOverflow: finalOverflow,
            bundlePayload: bundlePayload,
            decision,
            attempts: 1,
            sentAt: now,
            qualityScore: primary.quality.score,
            qualityBand: primary.quality.band,
            qualityReasons: primary.quality.reasons,
            qualityVersion: primary.quality.version,
            createdAt: now,
          }).returning();

          await db.insert(alertDeliveryAttempts).values({
            tenantId,
            deliveryId: delivery.id,
            attemptNo: 1,
            status: "SENT",
            latencyMs: null,
            httpStatus: null,
            error: null,
            sentAt: now,
            createdAt: now,
          });
        }

        for (const entry of finalCandidates) {
          await markAlertSent({
            tenantId,
            clusterId: entry.clusterId,
            channel: sub.channel,
            endpoint: sub.endpoint,
            now,
            ttlHours: ALERT_DEDUPE_TTL_HOURS,
          });
        }
        summary.sent_total += 1;
        perEndpointCount.set(rateKey, currentCount + 1);
      } catch (error: any) {
        summary.failed_total += 1;
        errorDetails.push({
          endpoint: sub.endpoint,
          cluster_id: String(primary.candidate.cluster_id),
          error_type: error?.name ?? "ERROR",
          message: error?.message ?? "Alert send failed",
        });

        const attemptLogs = error?.attemptLogs ?? [];
        for (const log of attemptLogs) {
          console.log({
            run_id: runRow.id,
            subscription_id: sub.id,
            cluster_id: primary.candidate.cluster_id,
            endpoint: sub.endpoint,
            attempt: log.attempt,
            status: log.status,
            latency_ms: log.latency_ms,
            http_status: log.http_status,
          });
        }

        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId: String(primary.candidate.cluster_id),
          entityType: primary.candidate.entity_type ?? "port",
          entityId: String(primary.candidate.entity_id),
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "FAILED",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts: attemptLogs.length || 1,
          lastHttpStatus: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.http_status : null,
          latencyMs: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.latency_ms : null,
          error: error?.message ?? "Alert send failed",
          qualityScore: primary.quality.score,
          qualityBand: primary.quality.band,
          qualityReasons: primary.quality.reasons,
          qualityVersion: primary.quality.version,
          createdAt: now,
        }).returning();

        const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "FAILED", latency_ms: null, http_status: null }])
          .map((log: any) => ({
            tenantId,
            deliveryId: delivery.id,
            attemptNo: log.attempt,
            status: "FAILED",
            latencyMs: log.latency_ms ?? null,
            httpStatus: log.http_status ?? null,
            error: error?.message ?? "Alert send failed",
            sentAt: null,
            createdAt: now,
          }));
        await db.insert(alertDeliveryAttempts).values(attemptRows);

        const nextAttemptAt = computeNextAttempt(now, 1);
        await db.insert(alertDlq).values({
          deliveryId: delivery.id,
          tenantId,
          nextAttemptAt,
          attemptCount: 1,
          maxAttempts: DLQ_MAX_ATTEMPTS,
          lastError: error?.message ?? "Alert send failed",
        }).onConflictDoUpdate({
          target: [alertDlq.deliveryId],
          set: {
            nextAttemptAt,
            attemptCount: sql`${alertDlq.attemptCount} + 1`,
            lastError: error?.message ?? "Alert send failed",
          },
        });
      }

    }

    await db.update(alertRuns)
      .set({
        status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
        finishedAt: new Date(),
        summary,
        error: errorDetails.length ? errorDetails : null,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

    return { runId: runRow.id, status: summary.failed_total > 0 ? "FAILED" : "SUCCESS", summary };
  } catch (error: any) {
    await db.update(alertRuns)
      .set({
        status: "FAILED",
        finishedAt: new Date(),
        error: errorDetails.length ? errorDetails : [{ message: error.message }],
        summary,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

    return { runId: runRow.id, status: "FAILED", summary };
  }
}

export async function dispatchSlaSystemAlert(args: SlaSystemAlertArgs) {
  const now = args.now ?? args.computedAt ?? new Date();
  const evaluatedAtIso = now.toISOString();
  const nowBucket = new Date(Math.floor(now.getTime() / 60000) * 60000);
  const tenantId = args.tenantId;
  const [runRow] = await db
    .insert(alertRuns)
    .values({
      tenantId,
      status: "SUCCESS",
      startedAt: now,
    })
    .returning();

  const summary = {
    candidates_total: 0,
    subscriptions: 0,
    matched_total: 0,
    suppressed_quality_total: 0,
    sent_total: 0,
    skipped_dedupe_total: 0,
    failed_total: 0,
    skipped_rate_limit_total: 0,
    skipped_noise_budget_total: 0,
  };
  const perEndpointCount = new Map<string, number>();

  const owners = await db
    .select({ userId: tenantUsers.userId })
    .from(tenantUsers)
    .where(and(
      eq(tenantUsers.tenantId, tenantId),
      eq(tenantUsers.role, "OWNER"),
      eq(tenantUsers.status, "ACTIVE"),
    ));

  const ownerIds = owners.map((row) => row.userId).filter(Boolean);
  if (ownerIds.length === 0) {
    await db.update(alertRuns)
      .set({
        status: "SUCCESS",
        finishedAt: new Date(),
        summary,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));
    return { runId: runRow.id, status: "SUCCESS", summary };
  }

  const subscriptions = await db
    .select()
    .from(alertSubscriptions)
    .where(and(
      eq(alertSubscriptions.tenantId, tenantId),
      alertSubscriptions.isEnabled,
      inArray(alertSubscriptions.userId, ownerIds),
    ));

  summary.subscriptions = subscriptions.length;

  const clusterId = `sla:${tenantId}:${args.window}:${args.destinationType}:${args.destinationKey}:${args.status}`;
  const baseLog = {
    tenantId,
    clusterId,
    window: args.window,
    status: args.status,
  };
    const day = args.computedAt.toISOString().slice(0, 10);
    const payload =
      args.status === "AT_RISK"
        ? {
            type: "SLA_AT_RISK",
            window: args.window,
            destination_type: args.destinationType,
            destination_key: args.destinationKey,
            metrics: args.metrics,
            thresholds: args.thresholds,
            status: "AT_RISK",
            computed_at: args.computedAt.toISOString(),
          }
        : {
            type: "SLA_RECOVERED",
            window: args.window,
            destination_type: args.destinationType,
            destination_key: args.destinationKey,
            status: "OK",
            computed_at: args.computedAt.toISOString(),
          };
  const systemQuality = buildSystemQuality(args.status);
  const systemClusterType = args.status === "AT_RISK" ? "SLA_AT_RISK" : "SLA_RECOVERED";
    const systemClusterSummary =
      args.status === "AT_RISK"
        ? `SLA at risk (${args.window} ${args.destinationType})`
        : `SLA recovered (${args.window} ${args.destinationType})`;
    const systemCandidate = {
      cluster_id: clusterId,
      cluster_type: systemClusterType,
      cluster_summary: systemClusterSummary,
      day,
      cluster_severity: args.status === "AT_RISK" ? "HIGH" : "LOW",
      confidence_score: args.status === "AT_RISK" ? 1 : 0.4,
      confidence_band: args.status === "AT_RISK" ? "HIGH" : "LOW",
      method: "sla_window",
      entity_id: args.destinationKey,
      entity_type: "system",
    };
    const systemEntry: BundleCandidate = {
      candidate: systemCandidate,
      quality: systemQuality,
      severityRank: getSeverityRank(systemCandidate.cluster_severity),
      confidenceScore: Number(systemCandidate.confidence_score ?? 0),
      createdAtMs: now.getTime(),
      clusterId,
    };
  const slaGate = {
    window: args.window,
    source: args.thresholdSource,
    p95_ms: args.thresholds.p95_ms,
    success_rate_min_pct: args.thresholds.success_rate_pct,
  };

  for (const sub of subscriptions) {
    summary.candidates_total += 1;
    summary.matched_total += 1;

    const dayValue = day;
    const suppressed: BundleCandidate[] = [];
    const sendable: BundleCandidate[] = [];
    let skippedDedupe = 0;

    if (isBelowQualityGate(sub, systemQuality)) {
      suppressed.push(systemEntry);
      summary.suppressed_quality_total += 1;
    } else {
      const dedupeKey = {
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
      };
      const allowed = await shouldSendAlert({ ...dedupeKey, now });
      if (!allowed) {
        skippedDedupe += 1;
        logOpsEvent("DELIVERY_SKIPPED_IDEMPOTENT", {
          ...baseLog,
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey: makeDestinationKey(sub.channel, sub.endpoint),
        });
      } else {
        sendable.push(systemEntry);
      }
    }

    summary.skipped_dedupe_total += skippedDedupe;

    const scopeValue = "GLOBAL";
    const subscriptionEntity = null;
    const bundleSummaryBase = {
      matched_total: 1,
      skipped_dedupe: skippedDedupe,
      suppressed_quality: suppressed.length,
    };

    const destinationGate = await getDestinationStateGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
      now,
    });
    const destinationKey = destinationGate.destinationKey;

    if (suppressed.length > 0) {
      const inserted = await recordQualityGateSuppressOnce({
        tenantId,
        subscriptionId: sub.id,
        day: dayValue,
      });
      if (inserted) {
        await writeAuditEvent(undefined, {
          tenantId,
          actorType: "SYSTEM",
          action: "ALERT.QUALITY_GATE_SUPPRESSING",
          resourceType: "ALERT_SUBSCRIPTION",
          resourceId: sub.id,
          severity: "WARN",
          status: "DENIED",
          message: "Alert suppressed due to quality gate.",
          metadata: {
            min_quality_score: sub.minQualityScore ?? null,
            min_quality_band: sub.minQualityBand ?? null,
            quality_score: systemQuality.score,
            quality_band: systemQuality.band,
            cluster_id: clusterId,
            day,
          },
        });
      }
    }

    if (sendable.length === 0) {
      if (suppressed.length > 0) {
        const items = buildBundleItems(suppressed, null);
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow: 0,
            skipped_noise_budget: 0,
          },
          items,
          system: payload,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day,
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
            sla_thresholds: slaGate,
            quality: {
              applied: true,
              score: systemQuality.score,
              band: systemQuality.band,
              suppressed: true,
              reason: "QUALITY_BELOW_THRESHOLD",
            },
            rate_limit: { applied: false, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: defaultEndpointGate,
            destination_state: toDecisionDestinationGate(destinationGate),
          },
          suppressedCounts: {
            dedupe: 0,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SKIPPED_QUALITY",
          skipReason: "QUALITY_BELOW_THRESHOLD",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          attempts: 0,
          createdAt: now,
        });
      }
      continue;
    }

    const items = buildBundleItems(sendable, null);
    const bundlePayloadRaw = {
      payload_version: "1.2",
      type: "ALERT_BUNDLE",
      sent_at: now.toISOString(),
      tenant_id: tenantId,
      run_id: runRow.id,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entity: subscriptionEntity,
        severity_min: sub.severityMin,
        destination_type: sub.channel,
        destination: sub.endpoint,
      },
      summary: {
        ...bundleSummaryBase,
        sent_items: items.length,
        overflow: 0,
        skipped_noise_budget: 0,
      },
      items,
      system: payload,
    };
    const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);
    if (!destinationGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          sla_thresholds: slaGate,
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: defaultEndpointGate.applied,
            window: defaultEndpointGate.window,
            status: defaultEndpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      const skipReason = destinationGate.state === "PAUSED"
        ? "DESTINATION_PAUSED"
        : destinationGate.state === "AUTO_PAUSED"
          ? "DESTINATION_AUTO_PAUSED"
          : "DESTINATION_DISABLED";
      const status = destinationGate.state === "PAUSED"
        ? "SKIPPED_DESTINATION_PAUSED"
        : destinationGate.state === "AUTO_PAUSED"
          ? "SKIPPED_DESTINATION_AUTO_PAUSED"
          : "SKIPPED_DESTINATION_DISABLED";
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status,
        skipReason,
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }
    const endpointGate = await getEndpointHealthGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
    });

    if (!endpointGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          sla_thresholds: slaGate,
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: false,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_ENDPOINT_DOWN",
        skipReason: "ENDPOINT_DOWN",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }

    const noiseBudget = await resolveNoiseBudget({
      tenantId,
      destinationType: sub.channel as any,
      destinationKey: destinationGate.destinationKey,
      destination: sub.endpoint,
      now,
    });
    if (!noiseBudget.allowed) {
      summary.skipped_noise_budget_total += 1;
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: buildNoiseBudgetGate(noiseBudget),
          sla_thresholds: slaGate,
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 1,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_NOISE_BUDGET",
        skipReason: "NOISE_BUDGET_EXCEEDED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      logDeliveryCreated(baseLog, {
        status: "SKIPPED_NOISE_BUDGET",
        skipReason: "NOISE_BUDGET_EXCEEDED",
        destinationType: sub.channel,
        destination: sub.endpoint,
        destinationKey,
      });
      if (noiseBudget.enabled) {
        const inserted = await recordNoiseBudgetBreachOnce({
          tenantId,
          destinationType: sub.channel as any,
          window: noiseBudget.window ?? "custom",
          bucketMinute: nowBucket,
        });
        if (inserted) {
          await writeAuditEvent(undefined, {
            tenantId,
            actorType: "SYSTEM",
            action: "ALERT.NOISE_BUDGET_EXCEEDED",
            resourceType: "ALERT_DELIVERY",
            severity: "WARN",
            status: "DENIED",
            message: "Noise budget exceeded for destination.",
            metadata: {
              destination_type: sub.channel,
              destination_key: destinationGate.destinationKey,
              source: noiseBudget.source,
              window: noiseBudget.window,
              window_minutes: noiseBudget.window_minutes,
              max_deliveries: noiseBudget.max_deliveries,
              count: noiseBudget.used_in_window,
              window_start: noiseBudget.window_start?.toISOString?.() ?? null,
            },
          });
        }
      }
      continue;
    }

    const rateKey = sub.id;
    const currentCount = perEndpointCount.get(rateKey) ?? 0;
    if (currentCount >= ALERT_RATE_LIMIT_PER_ENDPOINT) {
      summary.skipped_rate_limit_total += 1;
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          sla_thresholds: slaGate,
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: false },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_RATE_LIMIT",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }

    const decision = buildAlertDecision({
      evaluatedAtIso,
      day,
      clustered: true,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entityId: String(sub.entityId),
        severityMin: sub.severityMin,
        destinationType: sub.channel,
        destination: sub.endpoint,
        enabled: Boolean(sub.isEnabled),
      },
      included: buildDecisionIncluded(finalItems),
      bundle: {
        enabled: true,
        topN: ALERT_BUNDLE_TOP_N,
        overflow: finalOverflow,
      },
      gates: {
        severity_min_pass: true,
        dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
        noise_budget: buildNoiseBudgetGate(noiseBudget),
        sla_thresholds: slaGate,
        quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
        rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
        endpoint_health: {
          applied: endpointGate.applied,
          window: endpointGate.window,
          status: endpointGate.status as any,
          allowed: true,
        },
      },
      suppressedCounts: {
        dedupe: 0,
        noise_budget: 0,
        quality: suppressed.length,
        overflow: finalOverflow,
      },
    });

    try {
      if (sub.channel === "WEBHOOK") {
        const { body, headers } = buildWebhookRequest({
          payload: bundlePayload,
          secret: sub.secret ?? null,
          subscriptionId: sub.id,
          clusterId,
          day,
          now,
          idempotencyKey: computeBundleIdempotencyKey(sub.id, runRow.id, day),
        });

        const result = await sendWebhook({ endpoint: sub.endpoint, body, headers });
        const attemptLogs = (result as any)?.attemptLogs ?? [];
        const attempts = attemptLogs.length || 1;
        const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts,
          lastHttpStatus: last?.http_status ?? result?.status ?? null,
          latencyMs: last?.latency_ms ?? null,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();
        logDeliveryCreated(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
        });
        logDeliveryResult(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: null,
        });
        logDeliveryCreated(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
        });
        logDeliveryResult(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: last?.latency_ms ?? null,
        });

        const baseAttempt = Math.max((delivery?.attempts ?? 0) - attempts, 0);
        const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
          .map((log: any) => ({
            tenantId,
            deliveryId: delivery.id,
            attemptNo: baseAttempt + log.attempt,
            status: log.status === "SUCCESS" ? "SENT" : "FAILED",
            latencyMs: log.latency_ms ?? null,
            httpStatus: log.http_status ?? null,
            error: log.status === "SUCCESS" ? null : "Webhook delivery failed",
            sentAt: log.status === "SUCCESS" ? now : null,
            createdAt: now,
          }));
        if (attemptRows.length) {
          await db.insert(alertDeliveryAttempts).values(attemptRows);
        }
        incrementOpsCounter("deliveries_success_total");
        if (last?.latency_ms !== null && last?.latency_ms !== undefined) {
          recordDeliveryLatency("WEBHOOK", Number(last.latency_ms));
        }
        logOpsEvent("DELIVERY_CREATED", {
          ...baseLog,
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
        });
        logOpsEvent("DELIVERY_RESULT", {
          ...baseLog,
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: last?.latency_ms ?? null,
        });
      } else if (sub.channel === "EMAIL") {
        const email = renderAlertBundleEmail({ payload: bundlePayload });
        await sendEmail({ to: sub.endpoint, subject: email.subject, text: email.text });
        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts: 1,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();

        await db.insert(alertDeliveryAttempts).values({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: 1,
          status: "SENT",
          latencyMs: null,
          httpStatus: null,
          error: null,
          sentAt: now,
          createdAt: now,
        });
        incrementOpsCounter("deliveries_success_total");
        logOpsEvent("DELIVERY_CREATED", {
          ...baseLog,
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
        });
        logOpsEvent("DELIVERY_RESULT", {
          ...baseLog,
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: null,
        });
      }

      await markAlertSent({
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
        now,
        ttlHours: ALERT_DEDUPE_TTL_HOURS,
      });
      summary.sent_total += 1;
      perEndpointCount.set(rateKey, currentCount + 1);
    } catch (error: any) {
      summary.failed_total += 1;

      const attemptLogs = error?.attemptLogs ?? [];
      const [delivery] = await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "FAILED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        attempts: attemptLogs.length || 1,
        lastHttpStatus: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.http_status : null,
        latencyMs: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.latency_ms : null,
        error: error?.message ?? "Alert send failed",
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        createdAt: now,
      }).returning();

      const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "FAILED", latency_ms: null, http_status: null }])
        .map((log: any) => ({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: log.attempt,
          status: "FAILED",
          latencyMs: log.latency_ms ?? null,
          httpStatus: log.http_status ?? null,
          error: error?.message ?? "Alert send failed",
          sentAt: null,
          createdAt: now,
        }));
      await db.insert(alertDeliveryAttempts).values(attemptRows);

      const nextAttemptAt = computeNextAttempt(now, 1);
      await db.insert(alertDlq).values({
        deliveryId: delivery.id,
        tenantId,
        nextAttemptAt,
        attemptCount: 1,
        maxAttempts: DLQ_MAX_ATTEMPTS,
        lastError: error?.message ?? "Alert send failed",
      }).onConflictDoUpdate({
        target: [alertDlq.deliveryId],
        set: {
          nextAttemptAt,
          attemptCount: sql`${alertDlq.attemptCount} + 1`,
          lastError: error?.message ?? "Alert send failed",
        },
      });
    }
  }

  await db.update(alertRuns)
    .set({
      status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
      finishedAt: new Date(),
      summary,
    })
    .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

  return { runId: runRow.id, status: summary.failed_total > 0 ? "FAILED" : "SUCCESS", summary };
}

export async function dispatchEndpointHealthSystemAlert(args: EndpointHealthSystemAlertArgs) {
  const now = args.now ?? args.computedAt ?? new Date();
  const evaluatedAtIso = now.toISOString();
  const nowBucket = new Date(Math.floor(now.getTime() / 60000) * 60000);
  const tenantId = args.tenantId;
  const [runRow] = await db
    .insert(alertRuns)
    .values({
      tenantId,
      status: "SUCCESS",
      startedAt: now,
    })
    .returning();

  const summary = {
    candidates_total: 0,
    subscriptions: 0,
    matched_total: 0,
    suppressed_quality_total: 0,
    sent_total: 0,
    skipped_dedupe_total: 0,
    failed_total: 0,
    skipped_rate_limit_total: 0,
    skipped_noise_budget_total: 0,
  };
  const perEndpointCount = new Map<string, number>();

  const owners = await db
    .select({ userId: tenantUsers.userId })
    .from(tenantUsers)
    .where(and(
      eq(tenantUsers.tenantId, tenantId),
      eq(tenantUsers.role, "OWNER"),
      eq(tenantUsers.status, "ACTIVE"),
    ));

  const ownerIds = owners.map((row) => row.userId).filter(Boolean);
  if (ownerIds.length === 0) {
    await db.update(alertRuns)
      .set({
        status: "SUCCESS",
        finishedAt: new Date(),
        summary,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));
    return { runId: runRow.id, status: "SUCCESS", summary };
  }

  const subscriptions = await db
    .select()
    .from(alertSubscriptions)
    .where(and(
      eq(alertSubscriptions.tenantId, tenantId),
      alertSubscriptions.isEnabled,
      inArray(alertSubscriptions.userId, ownerIds),
    ));

  summary.subscriptions = subscriptions.length;

  const destHash = makeDestinationKey(args.destinationType, args.destination);
  const clusterId = `endpoint:${tenantId}:${args.window}:${args.destinationType}:${destHash}:${args.status}`;
  const baseLog = {
    tenantId,
    clusterId,
    window: args.window,
    status: args.status,
  };
  const day = args.computedAt.toISOString().slice(0, 10);
  const payload = {
    type: "ENDPOINT_HEALTH",
    version: "1",
    window: args.window,
    destination_type: args.destinationType,
    destination_hash: destHash,
    status: args.status,
    metrics: {
      success_rate: args.metrics.success_rate,
      p95_ms: args.metrics.p95_ms,
      last_success_at: args.metrics.last_success_at ?? null,
      last_failure_at: args.metrics.last_failure_at ?? null,
    },
  };

  const systemQuality = buildEndpointHealthQuality(args.status);
  const systemClusterType =
    args.status === "DOWN"
      ? "ENDPOINT_DOWN"
      : args.status === "DEGRADED"
        ? "ENDPOINT_DEGRADED"
        : "ENDPOINT_RECOVERED";
  const systemClusterSummary =
    args.status === "DOWN"
      ? `Endpoint DOWN (${args.window} ${args.destinationType}) p95=${args.metrics.p95_ms ?? "--"}ms success=${Math.round(args.metrics.success_rate * 100)}%`
      : args.status === "DEGRADED"
        ? `Endpoint DEGRADED (${args.window} ${args.destinationType}) p95=${args.metrics.p95_ms ?? "--"}ms success=${Math.round(args.metrics.success_rate * 100)}%`
        : `Endpoint RECOVERED (${args.window} ${args.destinationType})`;
  const systemCandidate = {
    cluster_id: clusterId,
    cluster_type: systemClusterType,
    cluster_summary: systemClusterSummary,
    day,
    cluster_severity: args.status === "DOWN" ? "HIGH" : args.status === "DEGRADED" ? "MEDIUM" : "LOW",
    confidence_score: args.status === "DOWN" ? 1 : args.status === "DEGRADED" ? 0.7 : 0.4,
    confidence_band: args.status === "DOWN" ? "HIGH" : args.status === "DEGRADED" ? "MEDIUM" : "LOW",
    method: "endpoint_health",
    entity_id: args.destinationType,
    entity_type: "system",
  };
  const systemEntry: BundleCandidate = {
    candidate: systemCandidate,
    quality: systemQuality,
    severityRank: getSeverityRank(systemCandidate.cluster_severity),
    confidenceScore: Number(systemCandidate.confidence_score ?? 0),
    createdAtMs: now.getTime(),
    clusterId,
  };
  for (const sub of subscriptions) {
    summary.candidates_total += 1;
    summary.matched_total += 1;

    const dayValue = day;
    const suppressed: BundleCandidate[] = [];
    const sendable: BundleCandidate[] = [];
    let skippedDedupe = 0;

    if (isBelowQualityGate(sub, systemQuality)) {
      suppressed.push(systemEntry);
      summary.suppressed_quality_total += 1;
    } else {
      const dedupeKey = {
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
      };
      const allowed = await shouldSendAlert({ ...dedupeKey, now });
      if (!allowed) {
        skippedDedupe += 1;
      } else {
        sendable.push(systemEntry);
      }
    }

    summary.skipped_dedupe_total += skippedDedupe;

    const scopeValue = "GLOBAL";
    const subscriptionEntity = null;
    const bundleSummaryBase = {
      matched_total: 1,
      skipped_dedupe: skippedDedupe,
      suppressed_quality: suppressed.length,
    };

    const destinationGate = await getDestinationStateGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
      now,
    });
    const destinationKey = destinationGate.destinationKey;

    if (suppressed.length > 0) {
      const inserted = await recordQualityGateSuppressOnce({
        tenantId,
        subscriptionId: sub.id,
        day: dayValue,
      });
      if (inserted) {
        await writeAuditEvent(undefined, {
          tenantId,
          actorType: "SYSTEM",
          action: "ALERT.QUALITY_GATE_SUPPRESSING",
          resourceType: "ALERT_SUBSCRIPTION",
          resourceId: sub.id,
          severity: "WARN",
          status: "DENIED",
          message: "Alert suppressed due to quality gate.",
          metadata: {
            min_quality_score: sub.minQualityScore ?? null,
            min_quality_band: sub.minQualityBand ?? null,
            quality_score: systemQuality.score,
            quality_band: systemQuality.band,
            cluster_id: clusterId,
            day,
          },
        });
      }
    }

    if (sendable.length === 0) {
      if (suppressed.length > 0) {
        const items = buildBundleItems(suppressed, null);
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow: 0,
            skipped_noise_budget: 0,
          },
          items,
          system: payload,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day,
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: {
              applied: true,
              score: systemQuality.score,
              band: systemQuality.band,
              suppressed: true,
              reason: "QUALITY_BELOW_THRESHOLD",
            },
            rate_limit: { applied: false, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: defaultEndpointGate,
            destination_state: toDecisionDestinationGate(destinationGate),
          },
          suppressedCounts: {
            dedupe: 0,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SKIPPED_QUALITY",
          skipReason: "QUALITY_BELOW_THRESHOLD",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          attempts: 0,
          createdAt: now,
        });
      }
      continue;
    }

    const items = buildBundleItems(sendable, null);
    const bundlePayloadRaw = {
      payload_version: "1.2",
      type: "ALERT_BUNDLE",
      sent_at: now.toISOString(),
      tenant_id: tenantId,
      run_id: runRow.id,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entity: subscriptionEntity,
        severity_min: sub.severityMin,
        destination_type: sub.channel,
        destination: sub.endpoint,
      },
      summary: {
        ...bundleSummaryBase,
        sent_items: items.length,
        overflow: 0,
        skipped_noise_budget: 0,
      },
      items,
      system: payload,
    };
    const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);

    if (!destinationGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: defaultEndpointGate.applied,
            window: defaultEndpointGate.window,
            status: defaultEndpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      const skipReason = destinationGate.state === "PAUSED"
        ? "DESTINATION_PAUSED"
        : destinationGate.state === "AUTO_PAUSED"
          ? "DESTINATION_AUTO_PAUSED"
          : "DESTINATION_DISABLED";
      const status = destinationGate.state === "PAUSED"
        ? "SKIPPED_DESTINATION_PAUSED"
        : destinationGate.state === "AUTO_PAUSED"
          ? "SKIPPED_DESTINATION_AUTO_PAUSED"
          : "SKIPPED_DESTINATION_DISABLED";
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status,
        skipReason,
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }

    const endpointGate = await getEndpointHealthGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
    });
    if (!endpointGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: false,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });

      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_ENDPOINT_DOWN",
        skipReason: "ENDPOINT_DOWN",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }

    const noiseBudget = await resolveNoiseBudget({
      tenantId,
      destinationType: sub.channel as any,
      destinationKey: destinationGate.destinationKey,
      destination: sub.endpoint,
      now,
    });
    if (!noiseBudget.allowed) {
      summary.skipped_noise_budget_total += 1;
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: buildNoiseBudgetGate(noiseBudget),
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 1,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_NOISE_BUDGET",
        skipReason: "NOISE_BUDGET_EXCEEDED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      if (noiseBudget.enabled) {
        const inserted = await recordNoiseBudgetBreachOnce({
          tenantId,
          destinationType: sub.channel as any,
          window: noiseBudget.window ?? "custom",
          bucketMinute: nowBucket,
        });
        if (inserted) {
          await writeAuditEvent(undefined, {
            tenantId,
            actorType: "SYSTEM",
            action: "ALERT.NOISE_BUDGET_EXCEEDED",
            resourceType: "ALERT_DELIVERY",
            severity: "WARN",
            status: "DENIED",
            message: "Noise budget exceeded for destination.",
            metadata: {
              destination_type: sub.channel,
              destination_key: destinationGate.destinationKey,
              source: noiseBudget.source,
              window: noiseBudget.window,
              window_minutes: noiseBudget.window_minutes,
              max_deliveries: noiseBudget.max_deliveries,
              count: noiseBudget.used_in_window,
              window_start: noiseBudget.window_start?.toISOString?.() ?? null,
            },
          });
        }
      }
      continue;
    }

    const rateKey = sub.id;
    const currentCount = perEndpointCount.get(rateKey) ?? 0;
    if (currentCount >= ALERT_RATE_LIMIT_PER_ENDPOINT) {
      summary.skipped_rate_limit_total += 1;
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: false },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_RATE_LIMIT",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }

    const decision = buildAlertDecision({
      evaluatedAtIso,
      day,
      clustered: true,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entityId: String(sub.entityId),
        severityMin: sub.severityMin,
        destinationType: sub.channel,
        destination: sub.endpoint,
        enabled: Boolean(sub.isEnabled),
      },
      included: buildDecisionIncluded(finalItems),
      bundle: {
        enabled: true,
        topN: ALERT_BUNDLE_TOP_N,
        overflow: finalOverflow,
      },
      gates: {
        severity_min_pass: true,
        dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
        noise_budget: buildNoiseBudgetGate(noiseBudget),
        quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
        rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
        endpoint_health: {
          applied: endpointGate.applied,
          window: endpointGate.window,
          status: endpointGate.status as any,
          allowed: true,
        },
      },
      suppressedCounts: {
        dedupe: 0,
        noise_budget: 0,
        quality: suppressed.length,
        overflow: finalOverflow,
      },
    });

    try {
      if (sub.channel === "WEBHOOK") {
        const { body, headers } = buildWebhookRequest({
          payload: bundlePayload,
          secret: sub.secret ?? null,
          subscriptionId: sub.id,
          clusterId,
          day,
          now,
          idempotencyKey: computeBundleIdempotencyKey(sub.id, runRow.id, day),
        });

        const result = await sendWebhook({ endpoint: sub.endpoint, body, headers });
        const attemptLogs = (result as any)?.attemptLogs ?? [];
        const attempts = attemptLogs.length || 1;
        const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts,
          lastHttpStatus: last?.http_status ?? result?.status ?? null,
          latencyMs: last?.latency_ms ?? null,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();

        logDeliveryCreated(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: last?.latency_ms ?? null,
        });
        logDeliveryResult(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: last?.latency_ms ?? null,
        });

        const baseAttempt = Math.max((delivery?.attempts ?? 0) - attempts, 0);
        const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
          .map((log: any) => ({
            tenantId,
            deliveryId: delivery.id,
            attemptNo: baseAttempt + log.attempt,
            status: log.status === "SUCCESS" ? "SENT" : "FAILED",
            latencyMs: log.latency_ms ?? null,
            httpStatus: log.http_status ?? null,
            error: log.status === "SUCCESS" ? null : "Webhook delivery failed",
            sentAt: log.status === "SUCCESS" ? now : null,
            createdAt: now,
          }));
        if (attemptRows.length) {
          await db.insert(alertDeliveryAttempts).values(attemptRows);
        }
      } else if (sub.channel === "EMAIL") {
        const email = renderAlertBundleEmail({ payload: bundlePayload });
        await sendEmail({ to: sub.endpoint, subject: email.subject, text: email.text });
        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts: 1,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();
        logDeliveryCreated(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: null,
        });
        logDeliveryResult(baseLog, {
          status: "SENT",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
          latencyMs: null,
        });

        await db.insert(alertDeliveryAttempts).values({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: 1,
          status: "SENT",
          latencyMs: null,
          httpStatus: null,
          error: null,
          sentAt: now,
          createdAt: now,
        });
      }

      await markAlertSent({
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
        now,
        ttlHours: ALERT_DEDUPE_TTL_HOURS,
      });
      summary.sent_total += 1;
      perEndpointCount.set(rateKey, currentCount + 1);
    } catch (error: any) {
      summary.failed_total += 1;

      const attemptLogs = error?.attemptLogs ?? [];
      const [delivery] = await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "FAILED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        attempts: attemptLogs.length || 1,
        lastHttpStatus: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.http_status : null,
        latencyMs: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.latency_ms : null,
        error: error?.message ?? "Alert send failed",
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        createdAt: now,
      }).returning();

      const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "FAILED", latency_ms: null, http_status: null }])
        .map((log: any) => ({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: log.attempt,
          status: "FAILED",
          latencyMs: log.latency_ms ?? null,
          httpStatus: log.http_status ?? null,
          error: error?.message ?? "Alert send failed",
          sentAt: null,
          createdAt: now,
        }));
      await db.insert(alertDeliveryAttempts).values(attemptRows);

      const nextAttemptAt = computeNextAttempt(now, 1);
      await db.insert(alertDlq).values({
        deliveryId: delivery.id,
        tenantId,
        nextAttemptAt,
        attemptCount: 1,
        maxAttempts: DLQ_MAX_ATTEMPTS,
        lastError: error?.message ?? "Alert send failed",
      }).onConflictDoUpdate({
        target: [alertDlq.deliveryId],
        set: {
          nextAttemptAt,
          attemptCount: sql`${alertDlq.attemptCount} + 1`,
          lastError: error?.message ?? "Alert send failed",
        },
      });
    }
  }

  await db.update(alertRuns)
    .set({
      status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
      finishedAt: new Date(),
      summary,
    })
    .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

  return { runId: runRow.id, status: summary.failed_total > 0 ? "FAILED" : "SUCCESS", summary };
}

export async function dispatchDestinationStateSystemAlert(args: DestinationStateSystemAlertArgs) {
  const now = args.now ?? args.computedAt ?? new Date();
  const evaluatedAtIso = now.toISOString();
  const nowBucket = new Date(Math.floor(now.getTime() / 60000) * 60000);
  const tenantId = args.tenantId;
  const [runRow] = await db
    .insert(alertRuns)
    .values({
      tenantId,
      status: "SUCCESS",
      startedAt: now,
    })
    .returning();

  const summary = {
    candidates_total: 0,
    subscriptions: 0,
    matched_total: 0,
    suppressed_quality_total: 0,
    sent_total: 0,
    skipped_dedupe_total: 0,
    failed_total: 0,
    skipped_rate_limit_total: 0,
    skipped_noise_budget_total: 0,
  };

  const owners = await db
    .select({ userId: tenantUsers.userId })
    .from(tenantUsers)
    .where(and(
      eq(tenantUsers.tenantId, tenantId),
      eq(tenantUsers.role, "OWNER"),
      eq(tenantUsers.status, "ACTIVE"),
    ));

  const ownerIds = owners.map((row) => row.userId).filter(Boolean);
  if (ownerIds.length === 0) {
    await db.update(alertRuns)
      .set({
        status: "SUCCESS",
        finishedAt: new Date(),
        summary,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));
    return { runId: runRow.id, status: "SUCCESS", summary };
  }

  const subscriptions = await db
    .select()
    .from(alertSubscriptions)
    .where(and(
      eq(alertSubscriptions.tenantId, tenantId),
      alertSubscriptions.isEnabled,
      inArray(alertSubscriptions.userId, ownerIds),
    ));

  summary.subscriptions = subscriptions.length;

  const clusterType =
    args.state === "AUTO_PAUSED"
      ? "DESTINATION_AUTO_PAUSED"
      : args.state === "PAUSED"
        ? "DESTINATION_PAUSED"
        : "DESTINATION_DISABLED";
  const clusterId = `destination-state:${tenantId}:${args.window}:${args.destinationType}:${args.destinationKey}:${args.state}`;
  const baseLog = {
    tenantId,
    clusterId,
    window: args.window,
    state: args.state,
  };
  const day = args.computedAt.toISOString().slice(0, 10);
  const payload = {
    type: "DESTINATION_STATE",
    version: "1",
    window: args.window,
    destination_type: args.destinationType,
    destination_key: args.destinationKey,
    state: args.state,
    reason: args.reason ?? null,
    computed_at: args.computedAt.toISOString(),
  };
  const severity = args.state === "AUTO_PAUSED" ? "MEDIUM" : "LOW";
  const systemQuality = computeAlertQuality({
    severity,
    confidence_band: severity === "MEDIUM" ? "MEDIUM" : "LOW",
    confidence_score: severity === "MEDIUM" ? 0.7 : 0.4,
    cluster_type: clusterType,
  } as any);
  const systemCandidate = {
    cluster_id: clusterId,
    cluster_type: clusterType,
    cluster_summary: `Destination ${args.state.replace("_", " ").toLowerCase()} (${args.window} ${args.destinationType})`,
    day,
    cluster_severity: severity,
    confidence_score: severity === "MEDIUM" ? 0.7 : 0.4,
    confidence_band: severity === "MEDIUM" ? "MEDIUM" : "LOW",
    method: "destination_state",
    entity_id: args.destinationType,
    entity_type: "system",
  };
  const systemEntry: BundleCandidate = {
    candidate: systemCandidate,
    quality: systemQuality,
    severityRank: getSeverityRank(systemCandidate.cluster_severity),
    confidenceScore: Number(systemCandidate.confidence_score ?? 0),
    createdAtMs: now.getTime(),
    clusterId,
  };

  for (const sub of subscriptions) {
    summary.candidates_total += 1;
    summary.matched_total += 1;

    const dayValue = day;
    const suppressed: BundleCandidate[] = [];
    const sendable: BundleCandidate[] = [];
    let skippedDedupe = 0;

    if (isBelowQualityGate(sub, systemQuality)) {
      suppressed.push(systemEntry);
      summary.suppressed_quality_total += 1;
    } else {
      const dedupeKey = {
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
      };
      const allowed = await shouldSendAlert({ ...dedupeKey, now });
      if (!allowed) {
        skippedDedupe += 1;
      } else {
        sendable.push(systemEntry);
      }
    }

    summary.skipped_dedupe_total += skippedDedupe;

    const scopeValue = "GLOBAL";
    const subscriptionEntity = null;
    const bundleSummaryBase = {
      matched_total: 1,
      skipped_dedupe: skippedDedupe,
      suppressed_quality: suppressed.length,
    };

    const destinationGateOverride = {
      applied: true,
      state: args.state as DestinationState,
      allowed: true,
      reason: args.reason ?? undefined,
    };
    const destinationKey = makeDestinationKey(sub.channel, sub.endpoint);

    if (sendable.length === 0) {
      if (suppressed.length > 0) {
        const items = buildBundleItems(suppressed, null);
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow: 0,
            skipped_noise_budget: 0,
          },
          items,
          system: payload,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day,
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: {
              applied: true,
              score: systemQuality.score,
              band: systemQuality.band,
              suppressed: true,
              reason: "QUALITY_BELOW_THRESHOLD",
            },
            rate_limit: { applied: false, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: defaultEndpointGate,
            destination_state: destinationGateOverride,
          },
          suppressedCounts: {
            dedupe: 0,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SKIPPED_QUALITY",
          skipReason: "QUALITY_BELOW_THRESHOLD",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          attempts: 0,
          createdAt: now,
        });
      }
      continue;
    }

    const items = buildBundleItems(sendable, null);
    const bundlePayloadRaw = {
      payload_version: "1.2",
      type: "ALERT_BUNDLE",
      sent_at: now.toISOString(),
      tenant_id: tenantId,
      run_id: runRow.id,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entity: subscriptionEntity,
        severity_min: sub.severityMin,
        destination_type: sub.channel,
        destination: sub.endpoint,
      },
      summary: {
        ...bundleSummaryBase,
        sent_items: items.length,
        overflow: 0,
        skipped_noise_budget: 0,
      },
      items,
      system: payload,
    };
    const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);

    const endpointGate = await getEndpointHealthGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
    });
    if (!endpointGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: false,
          },
          destination_state: destinationGateOverride,
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });

      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_ENDPOINT_DOWN",
        skipReason: "ENDPOINT_DOWN",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      continue;
    }

    const noiseBudget = await resolveNoiseBudget({
      tenantId,
      destinationType: sub.channel as any,
      destinationKey,
      destination: sub.endpoint,
      now,
    });
    if (!noiseBudget.allowed) {
      summary.skipped_noise_budget_total += 1;
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: buildNoiseBudgetGate(noiseBudget),
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
          destination_state: destinationGateOverride,
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 1,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.destinationType,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        status: "SKIPPED_NOISE_BUDGET",
        skipReason: "NOISE_BUDGET_EXCEEDED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      if (noiseBudget.enabled) {
        const inserted = await recordNoiseBudgetBreachOnce({
          tenantId,
          destinationType: sub.channel as any,
          window: noiseBudget.window ?? "custom",
          bucketMinute: nowBucket,
        });
        if (inserted) {
          await writeAuditEvent(undefined, {
            tenantId,
            actorType: "SYSTEM",
            action: "ALERT.NOISE_BUDGET_EXCEEDED",
            resourceType: "ALERT_DELIVERY",
            severity: "WARN",
            status: "DENIED",
            message: "Noise budget exceeded for destination.",
            metadata: {
              destination_type: sub.channel,
              destination_key: destinationKey,
              source: noiseBudget.source,
              window: noiseBudget.window,
              window_minutes: noiseBudget.window_minutes,
              max_deliveries: noiseBudget.max_deliveries,
              count: noiseBudget.used_in_window,
              window_start: noiseBudget.window_start?.toISOString?.() ?? null,
            },
          });
        }
      }
      continue;
    }

    const decision = buildAlertDecision({
      evaluatedAtIso,
      day,
      clustered: true,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entityId: String(sub.entityId),
        severityMin: sub.severityMin,
        destinationType: sub.channel,
        destination: sub.endpoint,
        enabled: Boolean(sub.isEnabled),
      },
      included: buildDecisionIncluded(finalItems),
      bundle: {
        enabled: true,
        topN: ALERT_BUNDLE_TOP_N,
        overflow: finalOverflow,
      },
      gates: {
        severity_min_pass: true,
        dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
        noise_budget: buildNoiseBudgetGate(noiseBudget),
        quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
        rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
        endpoint_health: {
          applied: endpointGate.applied,
          window: endpointGate.window,
          status: endpointGate.status as any,
          allowed: true,
        },
        destination_state: destinationGateOverride,
      },
      suppressedCounts: {
        dedupe: 0,
        noise_budget: 0,
        quality: suppressed.length,
        overflow: finalOverflow,
      },
    });

    try {
      if (sub.channel === "WEBHOOK") {
        const { body, headers } = buildWebhookRequest({
          payload: bundlePayload,
          secret: sub.secret ?? null,
          subscriptionId: sub.id,
          clusterId,
          day,
          now,
          idempotencyKey: computeBundleIdempotencyKey(sub.id, runRow.id, day),
        });

        const result = await sendWebhook({ endpoint: sub.endpoint, body, headers });
        const attemptLogs = (result as any)?.attemptLogs ?? [];
        const attempts = attemptLogs.length || 1;
        const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts,
          lastHttpStatus: last?.http_status ?? result?.status ?? null,
          latencyMs: last?.latency_ms ?? null,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();

        const baseAttempt = Math.max((delivery?.attempts ?? 0) - attempts, 0);
        const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
          .map((log: any) => ({
            tenantId,
            deliveryId: delivery.id,
            attemptNo: baseAttempt + log.attempt,
            status: log.status === "SUCCESS" ? "SENT" : "FAILED",
            latencyMs: log.latency_ms ?? null,
            httpStatus: log.http_status ?? null,
            error: log.status === "SUCCESS" ? null : "Webhook delivery failed",
            sentAt: now,
            createdAt: now,
          }));
        if (attemptRows.length > 0) {
          await db.insert(alertDeliveryAttempts).values(attemptRows);
        }

        summary.sent_total += 1;
      } else {
        const emailPayload = renderAlertBundleEmail({ payload: bundlePayload });
        const result = await sendEmail({
          to: sub.endpoint,
          subject: emailPayload.subject,
          text: emailPayload.text,
        });

        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.destinationType,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts: 1,
          lastHttpStatus: result?.ok ? 200 : null,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();

        await db.insert(alertDeliveryAttempts).values({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: 1,
          status: "SENT",
          httpStatus: result?.ok ? 200 : null,
          sentAt: now,
          createdAt: now,
        });

        summary.sent_total += 1;
      }
    } catch (error) {
      summary.failed_total += 1;
    }
  }

  await db.update(alertRuns)
    .set({
      status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
      finishedAt: new Date(),
      summary,
    })
    .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

  return { runId: runRow.id, status: summary.failed_total > 0 ? "FAILED" : "SUCCESS", summary };
}

export async function dispatchIncidentEscalationSystemAlert(args: IncidentEscalationSystemAlertArgs) {
  const now = args.now ?? new Date();
  const evaluatedAtIso = now.toISOString();
  const nowBucket = new Date(Math.floor(now.getTime() / 60000) * 60000);
  const tenantId = args.tenantId;
  const [runRow] = await db
    .insert(alertRuns)
    .values({
      tenantId,
      status: "SUCCESS",
      startedAt: now,
    })
    .returning();

  const summary = {
    candidates_total: 0,
    subscriptions: 0,
    matched_total: 0,
    suppressed_quality_total: 0,
    sent_total: 0,
    skipped_dedupe_total: 0,
    failed_total: 0,
    skipped_rate_limit_total: 0,
    skipped_noise_budget_total: 0,
  };
  const perEndpointCount = new Map<string, number>();
  let didAttempt = false;
  let dedupeBlocked = false;

  const targetType = String(args.policy.targetType ?? "").toUpperCase();
  const targetRef = String(args.policy.targetRef ?? "").trim();
  const targetHash = hashKey(`${targetType}:${targetRef}`);
  const policyTag = args.policy.id ? `:policy:${args.policy.id}` : "";

  let subscriptions: any[] = [];
  if (args.destinations && args.destinations.length > 0) {
    subscriptions = args.destinations.map((dest) => ({
      id: randomUUID(),
      tenantId,
      userId: dest.targetUserId ?? null,
      scope: "GLOBAL",
      entityType: "system",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      confidenceMin: null,
      minQualityBand: null,
      minQualityScore: null,
      channel: dest.destinationType,
      endpoint: dest.destination,
      secret: null,
      signatureVersion: "v1",
      isEnabled: true,
    }));
  } else if (targetType === "SUBSCRIPTION") {
    const [sub] = await db
      .select()
      .from(alertSubscriptions)
      .where(and(
        eq(alertSubscriptions.tenantId, tenantId),
        eq(alertSubscriptions.id, targetRef),
        alertSubscriptions.isEnabled,
      ))
      .limit(1);
    if (sub) {
      subscriptions = [sub];
    }
  } else if (targetType === "ROLE") {
    const role = targetRef.toUpperCase();
    const users = await db
      .select({ userId: tenantUsers.userId })
      .from(tenantUsers)
      .where(and(
        eq(tenantUsers.tenantId, tenantId),
        eq(tenantUsers.role, role),
        eq(tenantUsers.status, "ACTIVE"),
      ));
    const userIds = users.map((row) => row.userId).filter(Boolean);
    if (userIds.length > 0) {
      subscriptions = await db
        .select()
        .from(alertSubscriptions)
        .where(and(
          eq(alertSubscriptions.tenantId, tenantId),
          alertSubscriptions.isEnabled,
          inArray(alertSubscriptions.userId, userIds),
        ));
    }
  } else if (targetType === "USER") {
    const users = await db
      .select({ userId: tenantUsers.userId })
      .from(tenantUsers)
      .where(and(
        eq(tenantUsers.tenantId, tenantId),
        eq(tenantUsers.userId, targetRef),
        eq(tenantUsers.status, "ACTIVE"),
      ));
    if (users.length > 0) {
      subscriptions = await db
        .select()
        .from(alertSubscriptions)
        .where(and(
          eq(alertSubscriptions.tenantId, tenantId),
          alertSubscriptions.isEnabled,
          eq(alertSubscriptions.userId, targetRef),
        ));
    }
  } else if (targetType === "EMAIL" || targetType === "WEBHOOK") {
    const syntheticId = randomUUID();
    subscriptions = [{
      id: syntheticId,
      tenantId,
      userId: null,
      scope: "GLOBAL",
      entityType: "system",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      confidenceMin: null,
      minQualityBand: null,
      minQualityScore: null,
      channel: targetType,
      endpoint: targetRef,
      secret: null,
      signatureVersion: "v1",
      isEnabled: true,
    }];
  }

  summary.subscriptions = subscriptions.length;
  if (subscriptions.length === 0) {
    await db.update(alertRuns)
      .set({
        status: "SUCCESS",
        finishedAt: new Date(),
        summary,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));
    return { runId: runRow.id, status: "SUCCESS", summary, didAttempt: false, dedupeBlocked: false };
  }

  const clusterId = `incident-escalation:${tenantId}:${args.incident.id}:level:${args.level}${policyTag}:target:${targetHash}`;
  const dedupeKey = `incident-escalation:${tenantId}:${args.incident.id}:level:${args.level}${policyTag}:target:${targetHash}`;
  const day = now.toISOString().slice(0, 10);
  const baseLog = {
    tenantId,
    incidentId: args.incident.id,
    policyId: args.policy.id ?? null,
    level: args.level,
    targetType,
    targetRef,
    clusterId,
    dedupeKey,
  };
  const escalationGate = {
    level: args.level,
    target_type: targetType,
    target_ref: targetRef,
    policy_id: args.policy.id,
    dedupe_key: dedupeKey,
    allowed: true,
  };

  const routingGate = args.routingGate;

  const payload = {
    type: "INCIDENT_ESCALATION",
    version: "1",
    incident_id: args.incident.id,
    incident_type: args.incident.type,
    severity: args.incident.severity,
    destination_key: args.incident.destinationKey ?? null,
    escalation_level: args.level,
    target_type: targetType,
    target_ref: targetRef,
    target_hash: targetHash,
    after_minutes: args.policy.afterMinutes,
    opened_at: args.incident.openedAt.toISOString(),
    title: args.incident.title,
    summary: args.incident.summary,
  };

  const severity = String(args.incident.severity ?? "HIGH").toUpperCase();
  const systemQuality = buildIncidentEscalationQuality(severity);
  const systemCandidate = {
    cluster_id: clusterId,
    cluster_type: "INCIDENT_ESCALATION",
    cluster_summary: `Incident escalated L${args.level}: ${args.incident.title}`,
    day,
    cluster_severity: severity,
    confidence_score: 1,
    confidence_band: "HIGH",
    method: "incident_escalation",
    entity_id: args.incident.destinationKey ?? args.incident.id,
    entity_type: "system",
  };
  const systemEntry: BundleCandidate = {
    candidate: systemCandidate,
    quality: systemQuality,
    severityRank: getSeverityRank(systemCandidate.cluster_severity),
    confidenceScore: Number(systemCandidate.confidence_score ?? 0),
    createdAtMs: now.getTime(),
    clusterId,
  };

  for (const sub of subscriptions) {
    summary.candidates_total += 1;
    summary.matched_total += 1;

    const dayValue = day;
    const suppressed: BundleCandidate[] = [];
    const sendable: BundleCandidate[] = [];
    let skippedDedupe = 0;

    if (isBelowQualityGate(sub, systemQuality)) {
      suppressed.push(systemEntry);
      summary.suppressed_quality_total += 1;
    } else {
      const dedupeKey = {
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
      };
      const allowed = await shouldSendAlert({ ...dedupeKey, now });
      if (!allowed) {
        skippedDedupe += 1;
      } else {
        sendable.push(systemEntry);
      }
    }

    summary.skipped_dedupe_total += skippedDedupe;

    const scopeValue = "GLOBAL";
    const subscriptionEntity = null;
    const bundleSummaryBase = {
      matched_total: 1,
      skipped_dedupe: skippedDedupe,
      suppressed_quality: suppressed.length,
    };

    const destinationGate = await getDestinationStateGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
      now,
    });
    const destinationKey = destinationGate.destinationKey;

    if (routingGate && routingGate.allowed === false) {
      if (skippedDedupe > 0 && sendable.length === 0) {
        dedupeBlocked = true;
        continue;
      }
      const items = buildBundleItems([systemEntry], null);
      const bundlePayloadRaw = {
        payload_version: "1.2",
        type: "ALERT_BUNDLE",
        sent_at: now.toISOString(),
        tenant_id: tenantId,
        run_id: runRow.id,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entity: subscriptionEntity,
          severity_min: sub.severityMin,
          destination_type: sub.channel,
          destination: sub.endpoint,
        },
        summary: {
          matched_total: 1,
          skipped_dedupe: 0,
          suppressed_quality: 0,
          sent_items: 0,
          overflow: 0,
          skipped_noise_budget: 0,
        },
        items,
        system: payload,
      };
      const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: defaultEndpointGate,
          destination_state: toDecisionDestinationGate(destinationGate),
          escalation: escalationGate,
          routing: routingGate,
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: 0,
          overflow: finalOverflow,
        },
      });

      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.incident.id,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        destinationKey,
        status: "SKIPPED_DESTINATION_ROUTING",
        skipReason: "NO_USER_CONTACT_METHOD",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      logDeliveryCreated(baseLog, {
        status: "SKIPPED_DESTINATION_ROUTING",
        skipReason: "NO_USER_CONTACT_METHOD",
        destinationType: sub.channel,
        destination: sub.endpoint,
        destinationKey,
      });
      didAttempt = true;
      continue;
    }

    if (suppressed.length > 0) {
      const inserted = await recordQualityGateSuppressOnce({
        tenantId,
        subscriptionId: sub.id,
        day: dayValue,
      });
      if (inserted) {
        await writeAuditEvent(undefined, {
          tenantId,
          actorType: "SYSTEM",
          action: "ALERT.QUALITY_GATE_SUPPRESSING",
          resourceType: "ALERT_SUBSCRIPTION",
          resourceId: sub.id,
          severity: "WARN",
          status: "DENIED",
          message: "Alert suppressed due to quality gate.",
          metadata: {
            min_quality_score: sub.minQualityScore ?? null,
            min_quality_band: sub.minQualityBand ?? null,
            quality_score: systemQuality.score,
            quality_band: systemQuality.band,
            cluster_id: clusterId,
            day,
          },
        });
      }
    }

    if (sendable.length === 0) {
      if (suppressed.length > 0) {
        const items = buildBundleItems(suppressed, null);
        const bundlePayloadRaw = {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: now.toISOString(),
          tenant_id: tenantId,
          run_id: runRow.id,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entity: subscriptionEntity,
            severity_min: sub.severityMin,
            destination_type: sub.channel,
            destination: sub.endpoint,
          },
          summary: {
            ...bundleSummaryBase,
            sent_items: 0,
            overflow: 0,
            skipped_noise_budget: 0,
          },
          items,
          system: payload,
        };
        const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);
        const decision = buildAlertDecision({
          evaluatedAtIso,
          day,
          clustered: true,
          subscription: {
            id: sub.id,
            scope: scopeValue,
            entityId: String(sub.entityId),
            severityMin: sub.severityMin,
            destinationType: sub.channel,
            destination: sub.endpoint,
            enabled: Boolean(sub.isEnabled),
          },
          included: buildDecisionIncluded(finalItems),
          bundle: {
            enabled: true,
            topN: ALERT_BUNDLE_TOP_N,
            overflow: finalOverflow,
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
            noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: {
              applied: true,
              score: systemQuality.score,
              band: systemQuality.band,
              suppressed: true,
              reason: "QUALITY_BELOW_THRESHOLD",
            },
            rate_limit: { applied: false, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
            endpoint_health: defaultEndpointGate,
            destination_state: toDecisionDestinationGate(destinationGate),
            escalation: escalationGate,
          routing: routingGate,
          },
          suppressedCounts: {
            dedupe: 0,
            noise_budget: 0,
            quality: suppressed.length,
            overflow: finalOverflow,
          },
        });

        await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.incident.id,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          destinationKey,
          status: "SKIPPED_QUALITY",
          skipReason: "QUALITY_BELOW_THRESHOLD",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          attempts: 0,
          createdAt: now,
        });
        logDeliveryCreated(baseLog, {
          status: "SKIPPED_QUALITY",
          skipReason: "QUALITY_BELOW_THRESHOLD",
          destinationType: sub.channel,
          destination: sub.endpoint,
          destinationKey,
        });
        didAttempt = true;
      } else if (skippedDedupe > 0) {
        dedupeBlocked = true;
      }
      continue;
    }

    const items = buildBundleItems(sendable, null);
    const bundlePayloadRaw = {
      payload_version: "1.2",
      type: "ALERT_BUNDLE",
      sent_at: now.toISOString(),
      tenant_id: tenantId,
      run_id: runRow.id,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entity: subscriptionEntity,
        severity_min: sub.severityMin,
        destination_type: sub.channel,
        destination: sub.endpoint,
      },
      summary: {
        ...bundleSummaryBase,
        sent_items: items.length,
        overflow: 0,
        skipped_noise_budget: 0,
      },
      items,
      system: payload,
    };
    const { payload: bundlePayload, items: finalItems, overflow: finalOverflow } = applyBundlePayloadLimit(bundlePayloadRaw, 0);

    if (!destinationGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: defaultEndpointGate.applied,
            window: defaultEndpointGate.window,
            status: defaultEndpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
          escalation: escalationGate,
          routing: routingGate,
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      const skipReason = destinationGate.state === "PAUSED"
        ? "DESTINATION_PAUSED"
        : destinationGate.state === "AUTO_PAUSED"
          ? "DESTINATION_AUTO_PAUSED"
          : "DESTINATION_DISABLED";
      const status = destinationGate.state === "PAUSED"
        ? "SKIPPED_DESTINATION_PAUSED"
        : destinationGate.state === "AUTO_PAUSED"
          ? "SKIPPED_DESTINATION_AUTO_PAUSED"
          : "SKIPPED_DESTINATION_DISABLED";
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.incident.id,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        destinationKey,
        status,
        skipReason,
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      logDeliveryCreated(baseLog, {
        status,
        skipReason,
        destinationType: sub.channel,
        destination: sub.endpoint,
        destinationKey,
      });
      didAttempt = true;
      continue;
    }

    const endpointGate = await getEndpointHealthGate({
      tenantId,
      destinationType: sub.channel,
      destination: sub.endpoint,
    });
    if (!endpointGate.allowed) {
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 0, used_before: 0, allowed: true },
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: false,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
          escalation: escalationGate,
          routing: routingGate,
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 0,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });

      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.incident.id,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        destinationKey,
        status: "SKIPPED_ENDPOINT_DOWN",
        skipReason: "ENDPOINT_DOWN",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      logDeliveryCreated(baseLog, {
        status: "SKIPPED_ENDPOINT_DOWN",
        skipReason: "ENDPOINT_DOWN",
        destinationType: sub.channel,
        destination: sub.endpoint,
        destinationKey,
      });
      didAttempt = true;
      continue;
    }

    const noiseBudget = await resolveNoiseBudget({
      tenantId,
      destinationType: sub.channel as any,
      destinationKey,
      destination: sub.endpoint,
      now,
    });
    if (!noiseBudget.allowed) {
      summary.skipped_noise_budget_total += 1;
      const decision = buildAlertDecision({
        evaluatedAtIso,
        day,
        clustered: true,
        subscription: {
          id: sub.id,
          scope: scopeValue,
          entityId: String(sub.entityId),
          severityMin: sub.severityMin,
          destinationType: sub.channel,
          destination: sub.endpoint,
          enabled: Boolean(sub.isEnabled),
        },
        included: buildDecisionIncluded(finalItems),
        bundle: {
          enabled: true,
          topN: ALERT_BUNDLE_TOP_N,
          overflow: finalOverflow,
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
          noise_budget: buildNoiseBudgetGate(noiseBudget),
          quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
          rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
          endpoint_health: {
            applied: endpointGate.applied,
            window: endpointGate.window,
            status: endpointGate.status as any,
            allowed: true,
          },
          destination_state: toDecisionDestinationGate(destinationGate),
          escalation: escalationGate,
          routing: routingGate,
        },
        suppressedCounts: {
          dedupe: 0,
          noise_budget: 1,
          quality: suppressed.length,
          overflow: finalOverflow,
        },
      });
      await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.incident.id,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        destinationKey,
        status: "SKIPPED_NOISE_BUDGET",
        skipReason: "NOISE_BUDGET_EXCEEDED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        attempts: 0,
        createdAt: now,
      });
      if (noiseBudget.enabled) {
        const inserted = await recordNoiseBudgetBreachOnce({
          tenantId,
          destinationType: sub.channel as any,
          window: noiseBudget.window ?? "custom",
          bucketMinute: nowBucket,
        });
        if (inserted) {
          await writeAuditEvent(undefined, {
            tenantId,
            actorType: "SYSTEM",
            action: "ALERT.NOISE_BUDGET_EXCEEDED",
            resourceType: "ALERT_DELIVERY",
            severity: "WARN",
            status: "DENIED",
            message: "Noise budget exceeded for destination.",
            metadata: {
              destination_type: sub.channel,
              destination_key: destinationKey,
              source: noiseBudget.source,
              window: noiseBudget.window,
              window_minutes: noiseBudget.window_minutes,
              max_deliveries: noiseBudget.max_deliveries,
              count: noiseBudget.used_in_window,
              window_start: noiseBudget.window_start?.toISOString?.() ?? null,
            },
          });
        }
      }
      didAttempt = true;
      continue;
    }

    const decision = buildAlertDecision({
      evaluatedAtIso,
      day,
      clustered: true,
      subscription: {
        id: sub.id,
        scope: scopeValue,
        entityId: String(sub.entityId),
        severityMin: sub.severityMin,
        destinationType: sub.channel,
        destination: sub.endpoint,
        enabled: Boolean(sub.isEnabled),
      },
      included: buildDecisionIncluded(finalItems),
      bundle: {
        enabled: true,
        topN: ALERT_BUNDLE_TOP_N,
        overflow: finalOverflow,
      },
      gates: {
        severity_min_pass: true,
        dedupe: { applied: true, ttl_hours: ALERT_DEDUPE_TTL_HOURS, blocked: false },
        noise_budget: buildNoiseBudgetGate(noiseBudget),
        quality: { applied: true, score: systemQuality.score, band: systemQuality.band, suppressed: false },
        rate_limit: { applied: true, per_endpoint: ALERT_RATE_LIMIT_PER_ENDPOINT, allowed: true },
        endpoint_health: {
          applied: endpointGate.applied,
          window: endpointGate.window,
          status: endpointGate.status as any,
          allowed: true,
        },
        destination_state: toDecisionDestinationGate(destinationGate),
        escalation: escalationGate,
        routing: routingGate,
      },
      suppressedCounts: {
        dedupe: 0,
        noise_budget: 0,
        quality: suppressed.length,
        overflow: finalOverflow,
      },
    });

    try {
      if (sub.channel === "WEBHOOK") {
        const { body, headers } = buildWebhookRequest({
          payload: bundlePayload,
          secret: sub.secret ?? null,
          subscriptionId: sub.id,
          clusterId,
          day,
          now,
          idempotencyKey: computeBundleIdempotencyKey(sub.id, runRow.id, day),
        });

        const result = await sendWebhook({ endpoint: sub.endpoint, body, headers });
        const attemptLogs = (result as any)?.attemptLogs ?? [];
        const attempts = attemptLogs.length || 1;
        const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.incident.id,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          destinationKey,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts,
          lastHttpStatus: last?.http_status ?? result?.status ?? null,
          latencyMs: last?.latency_ms ?? null,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();

        const baseAttempt = Math.max((delivery?.attempts ?? 0) - attempts, 0);
        const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
          .map((log: any) => ({
            tenantId,
            deliveryId: delivery.id,
            attemptNo: baseAttempt + log.attempt,
            status: log.status === "SUCCESS" ? "SENT" : "FAILED",
            latencyMs: log.latency_ms ?? null,
            httpStatus: log.http_status ?? null,
            error: log.status === "SUCCESS" ? null : "Webhook delivery failed",
            sentAt: log.status === "SUCCESS" ? now : null,
            createdAt: now,
          }));
        if (attemptRows.length) {
          await db.insert(alertDeliveryAttempts).values(attemptRows);
        }
      } else if (sub.channel === "EMAIL") {
        const email = renderAlertBundleEmail({ payload: bundlePayload });
        await sendEmail({ to: sub.endpoint, subject: email.subject, text: email.text });
        const [delivery] = await insertDelivery({
          runId: runRow.id,
          tenantId,
          userId: sub.userId,
          subscriptionId: sub.id,
          clusterId,
          entityType: "system",
          entityId: args.incident.id,
          day: dayValue,
          destinationType: sub.channel,
          endpoint: sub.endpoint,
          destinationKey,
          status: "SENT",
          isBundle: true,
          bundleSize: finalItems.length,
          bundleOverflow: finalOverflow,
          bundlePayload: bundlePayload,
          decision,
          attempts: 1,
          sentAt: now,
          qualityScore: systemQuality.score,
          qualityBand: systemQuality.band,
          qualityReasons: systemQuality.reasons,
          qualityVersion: systemQuality.version,
          createdAt: now,
        }).returning();

        await db.insert(alertDeliveryAttempts).values({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: 1,
          status: "SENT",
          latencyMs: null,
          httpStatus: null,
          error: null,
          sentAt: now,
          createdAt: now,
        });
      }

      await markAlertSent({
        tenantId,
        clusterId,
        channel: sub.channel,
        endpoint: sub.endpoint,
        now,
        ttlHours: ALERT_DEDUPE_TTL_HOURS,
      });
      summary.sent_total += 1;
      didAttempt = true;
      const rateKey = `${sub.channel}:${sub.endpoint}`;
      const currentCount = perEndpointCount.get(rateKey) ?? 0;
      perEndpointCount.set(rateKey, currentCount + 1);
    } catch (error: any) {
      summary.failed_total += 1;
      didAttempt = true;

      const attemptLogs = error?.attemptLogs ?? [];
      const lastAttempt = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;
      const errorMessage = error?.message ?? "Alert send failed";
      const errorCategory =
        error?.name === "AbortError"
          ? "network"
          : String(errorMessage).includes("Webhook response")
            ? "http"
            : "unknown";
      const [delivery] = await insertDelivery({
        runId: runRow.id,
        tenantId,
        userId: sub.userId,
        subscriptionId: sub.id,
        clusterId,
        entityType: "system",
        entityId: args.incident.id,
        day: dayValue,
        destinationType: sub.channel,
        endpoint: sub.endpoint,
        destinationKey,
        status: "FAILED",
        isBundle: true,
        bundleSize: finalItems.length,
        bundleOverflow: finalOverflow,
        bundlePayload: bundlePayload,
        decision,
        attempts: attemptLogs.length || 1,
        lastHttpStatus: lastAttempt?.http_status ?? null,
        latencyMs: lastAttempt?.latency_ms ?? null,
        error: errorMessage,
        qualityScore: systemQuality.score,
        qualityBand: systemQuality.band,
        qualityReasons: systemQuality.reasons,
        qualityVersion: systemQuality.version,
        createdAt: now,
      }).returning();
      logDeliveryCreated(baseLog, {
        status: "FAILED",
        destinationType: sub.channel,
        destination: sub.endpoint,
        destinationKey,
        errorCategory,
      });
      logDeliveryResult(baseLog, {
        status: "FAILED",
        destinationType: sub.channel,
        destination: sub.endpoint,
        destinationKey,
        latencyMs: lastAttempt?.latency_ms ?? null,
        errorCategory,
      });

      const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "FAILED", latency_ms: null, http_status: null }])
        .map((log: any) => ({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: log.attempt,
          status: "FAILED",
          latencyMs: log.latency_ms ?? null,
          httpStatus: log.http_status ?? null,
          error: error?.message ?? "Alert send failed",
          sentAt: null,
          createdAt: now,
        }));
      await db.insert(alertDeliveryAttempts).values(attemptRows);

      const nextAttemptAt = computeNextAttempt(now, 1);
      await db.insert(alertDlq).values({
        deliveryId: delivery.id,
        tenantId,
        nextAttemptAt,
        attemptCount: 1,
        maxAttempts: DLQ_MAX_ATTEMPTS,
        lastError: error?.message ?? "Alert send failed",
      }).onConflictDoUpdate({
        target: [alertDlq.deliveryId],
        set: {
          nextAttemptAt,
          attemptCount: sql`${alertDlq.attemptCount} + 1`,
          lastError: error?.message ?? "Alert send failed",
        },
      });
    }
  }

  await db.update(alertRuns)
    .set({
      status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
      finishedAt: new Date(),
      summary,
    })
    .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

  return {
    runId: runRow.id,
    status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
    summary,
    didAttempt,
    dedupeBlocked,
  };
}
