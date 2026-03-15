import test from "node:test";
import { strict as assert } from "node:assert";
import { randomUUID, createHmac } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../../server/db";
import {
  alertDedupe,
  alertDeliveries,
  alertDeliveryAttempts,
  alertDeliverySlaWindows,
  alertDestinationStates,
  alertDlq,
  alertEndpointHealth,
  alertNoiseBudgetBreaches,
  alertNoiseBudgets,
  alertQualityGateBreaches,
  alertRuns,
  alertSlaThresholds,
  alertSubscriptions,
  apiKeys,
  auditEvents,
  auditExports,
  incidentEscalationPolicies,
  incidentEscalations,
  incidents,
  signals,
  tenantSettings,
  tenantUsers,
  userContactMethods,
} from "@shared/schema";
import { validateAlertSubscriptionInput } from "../../server/services/alertSubscriptionService";
import { markAlertSent, shouldSendAlert } from "../../server/services/alertDedupe";
import { getAlertCandidates } from "../../server/services/alertQuery";
import { renderAlertEmail } from "../../server/services/emailSender";
import { getDeliveryHealthByDay, getDeliveryLatency, getEndpointHealth, getDlqHealth, getDlqOverdue } from "../../server/services/alertMetrics";
import { getAlertDeliveriesSummary, listAlertDeliveries } from "../../server/services/alertDeliveries";
import { getAlertSubscriptionsSummary, listAlertSubscriptionsPage } from "../../server/services/alertSubscriptions";
import { getAuditEventsSummary, listAuditEvents } from "../../server/services/auditEvents";
import { auditEventsToCsv, auditEventsToJsonl, createAuditExport, fetchAuditEventsForExport, getAuditExport, signExportToken, verifyExportToken } from "../../server/services/auditExport";
import { purgeAuditEventsForTenant } from "../../server/services/auditRetention";
import { hashApiKey } from "../../server/services/apiKeyService";
import { retryDeliveryById } from "../../server/services/alertDlqQueue";
import { authenticateApiKey } from "../../server/middleware/apiKeyAuth";
import { requireRole as requireAlertRole } from "../../server/auth/requireRole";
import { writeAuditEvent } from "../../server/services/auditLog";
import { applyRateLimit } from "../../server/middleware/rateLimitMiddleware";
import { getTenantSettings, upsertTenantSettings } from "../../server/services/tenantSettings";
import { backfillAlertDeliverySlaWindows, computeAlertDeliverySlaWindows, getAlertSlaSummary, getSlaThresholds, listAlertSlaWindows } from "../../server/services/alertSlaService";
import { listAlertNoiseBudgets, recordNoiseBudgetBreachOnce } from "../../server/services/alertNoiseBudgetService";
import { resolveNoiseBudget, resolveSlaThresholds, upsertDestinationOverrides } from "../../server/services/alertDestinationOverridesService";
import { computeAlertQuality } from "../../server/services/alertQualityService";
import { computeEndpointHealth, listEndpointHealth } from "../../server/services/alertEndpointHealthService";
import { getIncidentMetricsV1 } from "../../server/services/incidentMetrics";
import { applyAutoPauseFromEndpointHealth, bulkUpdateDestinationStates, canTransitionDestinationState, getDestinationDetail, listDestinations } from "../../server/services/alertDestinationStateService";
import { getAlertPlaybook } from "@shared/alertPlaybook";
import { getIncidentById, listIncidents } from "../../server/services/incidentService";
import { autoAckIncidents, autoResolveIncidents, runIncidentAutomation } from "../../server/services/incidentAutomationService";
import { getIncidentEscalationSnapshot, listIncidentEscalationPolicies, runIncidentEscalations, upsertIncidentEscalationPolicy, validateEscalationPolicyTarget } from "../../server/services/incidentEscalationService";
import { validateRoutingPolicyDraft } from "../../server/services/alertRoutingValidationService";
import { getRoutingHealthForPolicy } from "../../server/services/alertRoutingHealthService";
import { makeDestinationKey } from "../../server/services/destinationKey";
import { createUserContactMethod } from "../../server/services/userContactMethodsService";
import { GLOBAL_SCOPE_ENTITY_ID } from "../../server/services/alertScope";
import { listTeamUsersDirectory } from "../../server/services/teamUsersDirectory";
import { createTestHarness } from "../test/harness";
import {
  insertApiKeys,
  insertTenantUsers,
  releaseAdvisoryLock,
  tryAdvisoryLock,
  withTransientRetry,
} from "../bootstrap";

const stableStringify = (value: any) => JSON.stringify(value, (_key, val) => {
  if (val && typeof val === "object" && !Array.isArray(val)) {
    return Object.keys(val).sort().reduce((acc: any, key) => {
      acc[key] = val[key];
      return acc;
    }, {});
  }
  return val;
});

const percentileCont = (values: number[], p: number) => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * p;
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return sorted[lower];
  const weight = pos - lower;
  return sorted[lower] + (sorted[upper] - sorted[lower]) * weight;
};


test.describe("alerts", () => {
  const harness = createTestHarness({ label: "alerts", cleanup: "tenant", schema: "alerts" });

  test.before(async () => {
    await harness.beforeAll();
  });

  test.beforeEach(async (t) => {
    await harness.beforeEach(t);
  });

  test.afterEach(async () => {
    await harness.afterEach();
  });

  test.after(async () => {
    await harness.afterAll();
  });

  test("alerts pipeline", async () => {
    const debug = process.env.TEST_DEBUG === "1";
    const log = (label: string) => {
      if (debug) {
        console.log(`TEST_STEP: ${label}`);
      }
    };
    log("main start");

    const webhookModule = await import("../../server/services/webhookSender");
    const { buildWebhookPayload, buildWebhookRequest, computeIdempotencyKey, sendWebhook, WebhookSendError } = webhookModule;
    const { runAlerts } = await import("../../server/services/alertDispatcher");
    
    const testDay = new Date(Date.UTC(2026, 0, 15));
    
    const portA = randomUUID();
    const portB = randomUUID();
    const portC = randomUUID();
    const portD = randomUUID();
    const portE = randomUUID();
    const portF = randomUUID();
    const missingPort = randomUUID();
    const subUserId = randomUUID();
    const dispatcherSubUserId = randomUUID();
    const tenantA = harness.newTenantId();
    const tenantB = harness.newTenantId();
    const thresholdTenant = harness.newTenantId();
    const thresholdUser = randomUUID();
    const noiseTenant = harness.newTenantId();
    const noiseUser = randomUUID();
    const qualityTenant = harness.newTenantId();
    const qualityUser = randomUUID();
    const qualityAllowTenant = harness.newTenantId();
    const qualityAllowUser = randomUUID();
    const bundleTenant = harness.newTenantId();
    const bundleUser = randomUUID();
    const bundleDedupeTenant = harness.newTenantId();
    const bundleDedupeUser = randomUUID();
    
    const portIds = [portA, portB, portC, portD, portE, portF];
    harness.registerPortIds(portIds);
    harness.registerMissingPortId(missingPort);
    const computeSla = (args: Parameters<typeof computeAlertDeliverySlaWindows>[0]) =>
      withTransientRetry(() => computeAlertDeliverySlaWindows(args), "computeAlertDeliverySlaWindows");
    
    await db.delete(signals).where(sql`1=1`);
    await db.delete(apiKeys).where(sql`1=1`);
    
      const [subRow] = await db
        .insert(alertSubscriptions)
        .values({
          tenantId: tenantA,
          userId: subUserId,
          entityType: "port",
          entityId: portA,
          severityMin: "HIGH",
          channel: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          isEnabled: true,
          updatedAt: new Date(),
        })
        .returning();
      assert(subRow?.id, "alert subscription created");
    
      const listSubs = await db
        .select()
        .from(alertSubscriptions)
        .where(and(eq(alertSubscriptions.userId, subUserId), eq(alertSubscriptions.tenantId, tenantA)));
      assert.strictEqual(listSubs.length, 1, "alert subscription list returns row");
    
      const invalidSeverity = validateAlertSubscriptionInput({
        user_id: randomUUID(),
        entity_id: portA,
        endpoint: "http://localhost:9999/webhook",
        severity_min: "SEVERE",
      }, true);
      assert(!invalidSeverity.ok, "invalid severity fails validation");
    
      const dedupeKey = {
        tenantId: tenantA,
        clusterId: "CLUSTER-TEST-1",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
      };
      await db.delete(alertDedupe).where(eq(alertDedupe.clusterId, dedupeKey.clusterId));
      const now = new Date("2026-02-01T00:00:00Z");
      const firstCheck = await shouldSendAlert({ ...dedupeKey, now });
      assert(firstCheck, "dedupe allows first send");
      await markAlertSent({ ...dedupeKey, now, ttlHours: 24 });
    
      const secondCheck = await shouldSendAlert({ ...dedupeKey, now: new Date("2026-02-01T10:00:00Z") });
      assert(!secondCheck, "dedupe blocks within ttl");
    
      const thirdCheck = await shouldSendAlert({ ...dedupeKey, now: new Date("2026-02-02T02:00:00Z") });
      assert(thirdCheck, "dedupe allows after ttl");
    
      const alertDay = new Date(Date.UTC(2026, 0, 20));
      await db.insert(signals).values([
        {
          signalType: "PORT_DWELL_SPIKE",
          entityType: "port",
          entityId: portA,
          day: alertDay,
          severity: "HIGH",
          value: 10,
          baseline: 6,
          stddev: 1,
          zscore: 4,
          deltaPct: 60,
          confidenceScore: 0.9,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "CLUSTER-A",
          clusterKey: "CLUSTER-A",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "A",
          explanation: "Cluster A",
          metadata: { day: "2026-01-20" },
          createdAt: new Date("2026-01-20T10:00:00Z"),
        },
        {
          signalType: "PORT_CONGESTION_BUILDUP",
          entityType: "port",
          entityId: portB,
          day: alertDay,
          severity: "CRITICAL",
          value: 20,
          baseline: 10,
          stddev: null,
          zscore: null,
          deltaPct: 100,
          confidenceScore: 0.7,
          confidenceBand: "MEDIUM",
          method: "multiplier_30d",
          clusterId: "CLUSTER-B",
          clusterKey: "CLUSTER-B",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "CRITICAL",
          clusterSummary: "B",
          explanation: "Cluster B",
          metadata: { day: "2026-01-20" },
          createdAt: new Date("2026-01-20T09:00:00Z"),
        },
        {
          signalType: "TEST_BUNDLE_CRIT",
          entityType: "port",
          entityId: portA,
          day: alertDay,
          severity: "HIGH",
          value: 5,
          baseline: 10,
          stddev: 1,
          zscore: -5,
          deltaPct: -50,
          confidenceScore: 0.1,
          confidenceBand: "LOW",
          method: "zscore_30d",
          clusterId: "CLUSTER-LOWQ",
          clusterKey: "CLUSTER-LOWQ",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "Low quality",
          explanation: "Cluster LowQ",
          metadata: { day: "2026-01-20", data_quality: { completeness_pct: 70 } },
          createdAt: new Date("2026-01-20T08:00:00Z"),
        },
      ]);
    
      await db.insert(signals).values({
        signalType: "PORT_DWELL_SPIKE",
        entityType: "port",
        entityId: portA,
        day: new Date(Date.UTC(2026, 0, 21)),
        severity: "HIGH",
        value: 11,
        baseline: 6,
        stddev: 1,
        zscore: 5,
        deltaPct: 70,
        confidenceScore: 0.8,
        confidenceBand: "HIGH",
        method: "zscore_30d",
        clusterId: "CLUSTER-C",
        clusterKey: "CLUSTER-C",
        clusterType: "PORT_DISRUPTION",
        clusterSeverity: "HIGH",
        clusterSummary: "C",
        explanation: "Cluster C",
        metadata: { day: "2026-01-21" },
        createdAt: new Date("2026-01-21T09:00:00Z"),
      });
    
      await db.insert(signals).values({
        signalType: "PORT_ARRIVALS_ANOMALY",
        entityType: "port",
        entityId: portA,
        day: new Date(Date.UTC(2026, 0, 22)),
        severity: "HIGH",
        value: 4,
        baseline: 10,
        stddev: 2,
        zscore: -3,
        deltaPct: -60,
        confidenceScore: 0.1,
        confidenceBand: "LOW",
        method: "zscore_30d",
        clusterId: "CLUSTER-LOWQ2",
        clusterKey: "CLUSTER-LOWQ2",
        clusterType: "PORT_DISRUPTION",
        clusterSeverity: "HIGH",
        clusterSummary: "LowQ2",
        explanation: "Cluster LowQ2",
        metadata: { day: "2026-01-22", data_quality: { completeness_pct: 70 } },
        createdAt: new Date("2026-01-22T09:00:00Z"),
      });
    
      await db.insert(signals).values({
        signalType: "PORT_DWELL_SPIKE",
        entityType: "port",
        entityId: portA,
        day: new Date(Date.UTC(2026, 0, 23)),
        severity: "HIGH",
        value: 12,
        baseline: 6,
        stddev: 1,
        zscore: 6,
        deltaPct: 100,
        confidenceScore: 0.9,
        confidenceBand: "HIGH",
        method: "zscore_30d",
        clusterId: "CLUSTER-HIGHQ",
        clusterKey: "CLUSTER-HIGHQ",
        clusterType: "PORT_DISRUPTION",
        clusterSeverity: "HIGH",
        clusterSummary: "HighQ",
        explanation: "Cluster HighQ",
        metadata: { day: "2026-01-23", data_quality: { completeness_pct: 98 } },
        createdAt: new Date("2026-01-23T09:00:00Z"),
      });
    
      const candidates = await getAlertCandidates({ day: "2026-01-20", severityMin: "HIGH" });
      assert.strictEqual(candidates.length, 3, "returns one per cluster");
      assert.strictEqual(candidates[0].cluster_id, "CLUSTER-B", "higher severity ranks first");
      assert.strictEqual(candidates[1].cluster_id, "CLUSTER-A");
      assert.strictEqual(candidates[2].cluster_id, "CLUSTER-LOWQ");
    
      const highOnly = await getAlertCandidates({ day: "2026-01-20", severityMin: "CRITICAL" });
      assert.strictEqual(highOnly.length, 1, "filters by severity min");
      assert.strictEqual(highOnly[0].cluster_id, "CLUSTER-B");
    
      const payload = buildWebhookPayload({
        day: "2026-01-20",
        entityType: "port",
        entityId: portA,
        clusterId: "CLUSTER-A",
        clusterSeverity: "HIGH",
        confidenceScore: 0.9,
        confidenceBand: "HIGH",
        clusterSummary: "A",
        metadata: { drivers: [{ metric: "arrivals", value: 1 }] },
      }, { sentAt: new Date("2026-01-20T12:00:00Z"), version: "1.1" });
      assert.strictEqual(payload.payload_version, "1.1");
      assert.strictEqual(payload.sent_at, "2026-01-20T12:00:00.000Z");
      assert(payload.event_type, "payload includes event_type");
    
      const idempotencyKey = computeIdempotencyKey("sub-1", "CLUSTER-A", "2026-01-20");
      const idempotencyKey2 = computeIdempotencyKey("sub-2", "CLUSTER-A", "2026-01-20");
      assert(idempotencyKey !== idempotencyKey2, "idempotency key differs per subscription");
      assert.strictEqual(idempotencyKey, computeIdempotencyKey("sub-1", "CLUSTER-A", "2026-01-20"));
    
      const emailRendered = renderAlertEmail({
        signal: {
          clusterSeverity: "HIGH",
          clusterType: "PORT_DISRUPTION",
          day: "2026-01-20",
          clusterSummary: "Arrivals -10%",
          metadata: {
            drivers: [{ metric: "arrivals", delta_pct: -10, zscore: -2.5 }],
            impact: ["Impact line"],
            recommended_followups: ["Check weather"],
          },
        },
        entity: { name: "Rotterdam" },
        link: "/signals/abc",
      });
      assert(emailRendered.subject.includes("HIGH PORT_DISRUPTION"));
      assert(emailRendered.subject.includes("Rotterdam"));
      assert(emailRendered.subject.includes("2026-01-20"));
      assert(emailRendered.text.includes("Arrivals -10%"));
      assert(emailRendered.text.includes("Impact line"));
      assert(emailRendered.text.includes("Check weather"));
    
      log("webhook/email helpers validated");
    
      const originalFetch = globalThis.fetch;
      const stubFetchOk = async () => ({ ok: true, status: 200 }) as any;
      const stubFetchFail = async () => ({ ok: false, status: 500 }) as any;
      let attemptCount = 0;
      const webhookRequest = buildWebhookRequest({
        payload,
        secret: "test_secret",
        subscriptionId: "sub-1",
        clusterId: "CLUSTER-A",
        day: "2026-01-20",
        now: new Date("2026-01-20T12:00:00Z"),
      });
      assert(webhookRequest.headers["X-Veriscope-Timestamp"], "timestamp header set");
      assert(webhookRequest.headers["X-Veriscope-Signature"], "signature header set");
      const timestamp = webhookRequest.headers["X-Veriscope-Timestamp"];
      const expectedSig = createHmac("sha256", "test_secret")
        .update(`v1:${timestamp}:${webhookRequest.body}`)
        .digest("hex");
      assert.strictEqual(webhookRequest.headers["X-Veriscope-Signature"], `v1=${expectedSig}`);
    
      const unsignedRequest = buildWebhookRequest({
        payload,
        secret: null,
        subscriptionId: "sub-1",
        clusterId: "CLUSTER-A",
        day: "2026-01-20",
        now: new Date("2026-01-20T12:00:00Z"),
      });
      assert(!unsignedRequest.headers["X-Veriscope-Timestamp"]);
      assert(!unsignedRequest.headers["X-Veriscope-Signature"]);
    
      globalThis.fetch = async () => {
        attemptCount += 1;
        if (attemptCount === 1) {
          return { ok: false, status: 500 } as any;
        }
        return { ok: true, status: 200 } as any;
      };
    
      await sendWebhook({
        endpoint: "http://localhost:9999/webhook",
        body: webhookRequest.body,
        headers: webhookRequest.headers,
        timeoutMs: 50,
        attempts: 3,
      });
      assert.strictEqual(attemptCount, 2, "webhook retries once then succeeds");
    
      globalThis.fetch = async () => {
        const error = new Error("timeout");
        (error as any).name = "AbortError";
        throw error;
      };
      let threw = false;
      try {
        await sendWebhook({
          endpoint: "http://localhost:9999/webhook",
          body: webhookRequest.body,
          headers: webhookRequest.headers,
          timeoutMs: 10,
          attempts: 3,
        });
      } catch (error) {
        threw = error instanceof WebhookSendError;
      }
      assert(threw, "webhook fails after retries");
    
      globalThis.fetch = stubFetchOk;
    
      const dispatchEndpoint = "http://localhost:9999/webhook";
      const dispatchDestinationKey = makeDestinationKey("WEBHOOK", dispatchEndpoint);
      await db.delete(alertDestinationStates).where(and(
        eq(alertDestinationStates.tenantId, tenantA),
        eq(alertDestinationStates.destinationKey, dispatchDestinationKey),
      ));
      await db.delete(alertEndpointHealth).where(and(
        eq(alertEndpointHealth.tenantId, tenantA),
        eq(alertEndpointHealth.destinationType, "WEBHOOK"),
        eq(alertEndpointHealth.destination, dispatchEndpoint),
      ));
    
      await db.insert(alertSubscriptions).values({
        tenantId: tenantA,
        userId: dispatcherSubUserId,
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: dispatchEndpoint,
        isEnabled: true,
        updatedAt: new Date(),
      });
    
      let dispatchAttempts = 0;
      globalThis.fetch = async () => {
        dispatchAttempts += 1;
        return { ok: true, status: 200 } as any;
      };
    
      await db.delete(alertDedupe).where(eq(alertDedupe.clusterId, "CLUSTER-A"));
      const dispatchRun1 = await runAlerts({ day: "2026-01-20", tenantId: tenantA, userId: dispatcherSubUserId, now: new Date("2026-01-20T12:00:00Z") });
      assert.strictEqual(dispatchRun1.summary.sent_total, 1, "first run sends");
    
      const dispatchRun2 = await runAlerts({ day: "2026-01-20", tenantId: tenantA, userId: dispatcherSubUserId, now: new Date("2026-01-20T12:10:00Z") });
      assert.strictEqual(dispatchRun2.summary.sent_total, 0, "second run skips");
      assert.strictEqual(dispatchRun2.summary.skipped_dedupe_total, 2, "second run dedupes");
    
      globalThis.fetch = stubFetchOk;
    
      log("dispatcher dedupe checks complete");
    
      const defaultNoise = await listAlertNoiseBudgets(noiseTenant, "24h");
      const webhookDefault = defaultNoise.items.find((item) => item.destination_type === "WEBHOOK");
      assert.strictEqual(webhookDefault?.source, "DEFAULT", "noise budgets default when none configured");
    
      const noiseNow = new Date("2026-01-15T12:30:00Z");
      const [noiseSub] = await db.insert(alertSubscriptions).values({
        tenantId: noiseTenant,
        userId: noiseUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "LOW",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: noiseNow,
      }).returning();
    
      const [noiseRun] = await db.insert(alertRuns).values({
        tenantId: noiseTenant,
        status: "SUCCESS",
        startedAt: noiseNow,
        finishedAt: noiseNow,
      }).returning();
    
      await db.insert(alertDeliveries).values({
        runId: noiseRun.id,
        tenantId: noiseTenant,
        userId: noiseUser,
        subscriptionId: noiseSub.id,
        clusterId: "NOISE-BASE",
        entityType: "port",
        entityId: portA,
        day: testDay,
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "SENT",
        attempts: 1,
        createdAt: noiseNow,
      });
    
      await db.insert(alertNoiseBudgets).values({
        tenantId: noiseTenant,
        destinationType: "WEBHOOK",
        window: "24h",
        maxDeliveries: 1,
        createdAt: noiseNow,
        updatedAt: noiseNow,
      });
    
      globalThis.fetch = async () => {
        return { ok: true, status: 200 } as any;
      };
    
      await db.insert(signals).values({
        signalType: "PORT_DWELL_SPIKE",
        entityType: "port",
        entityId: portA,
        day: testDay,
        severity: "HIGH",
        value: 10,
        baseline: 6,
        stddev: 1,
        zscore: 4,
        deltaPct: 60,
        confidenceScore: 0.9,
        confidenceBand: "HIGH",
        method: "zscore_30d",
        clusterId: "NOISE-CLUSTER",
        clusterKey: "NOISE-CLUSTER",
        clusterType: "PORT_DISRUPTION",
        clusterSeverity: "HIGH",
        clusterSummary: "Noise budget cluster",
        explanation: "Noise budget signal seed",
        metadata: { day: "2026-01-15" },
        createdAt: noiseNow,
      });
    
      await runAlerts({ day: "2026-01-15", tenantId: noiseTenant, userId: noiseUser, now: new Date("2026-01-15T12:45:00Z") });
    
      const noiseSkipped = await db
        .select()
        .from(alertDeliveries)
        .where(and(eq(alertDeliveries.tenantId, noiseTenant), eq(alertDeliveries.status, "SKIPPED_NOISE_BUDGET")));
      assert(noiseSkipped.length >= 1, "noise budget skip created");
      assert.strictEqual(noiseSkipped[0].skipReason, "NOISE_BUDGET_EXCEEDED", "skip reason recorded");
      assert(noiseSkipped[0].decision, "decision stored on noise budget skip");
      assert.strictEqual(
        (noiseSkipped[0].decision as any)?.gates?.noise_budget?.allowed,
        false,
        "decision records noise budget gate blocked"
      );
      assert(typeof noiseSkipped[0].qualityScore === "number", "quality score recorded on skip");
      assert(noiseSkipped[0].qualityBand, "quality band recorded on skip");
      assert(noiseSkipped[0].qualityVersion, "quality version recorded on skip");
    
      const noiseDlq = await db
        .select()
        .from(alertDlq)
        .where(eq(alertDlq.tenantId, noiseTenant));
      assert.strictEqual(noiseDlq.length, 0, "noise budget skips do not create DLQ rows");
    
      const [noiseAudit] = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, noiseTenant),
          eq(auditEvents.action, "ALERT.NOISE_BUDGET_EXCEEDED"),
        ))
        .limit(1);
      assert(noiseAudit, "noise budget audit emitted");
    
      const breachBucket = new Date("2026-01-15T12:30:00Z");
      const firstBreach = await recordNoiseBudgetBreachOnce({
        tenantId: noiseTenant,
        destinationType: "WEBHOOK",
        window: "24h",
        bucketMinute: breachBucket,
      });
      const secondBreach = await recordNoiseBudgetBreachOnce({
        tenantId: noiseTenant,
        destinationType: "WEBHOOK",
        window: "24h",
        bucketMinute: breachBucket,
      });
      assert.strictEqual(firstBreach, true, "first breach recorded");
      assert.strictEqual(secondBreach, false, "second breach ignored");
    
      const overrideTenant = harness.newTenantId();
      const overrideUser = randomUUID();
      const overrideNow = new Date("2026-01-16T12:00:00Z");
      const overrideDestA = "http://localhost:9999/webhook-a";
      const overrideDestB = "http://localhost:9999/webhook-b";
      const overrideKeyA = makeDestinationKey("WEBHOOK", overrideDestA);
      const overrideKeyB = makeDestinationKey("WEBHOOK", overrideDestB);
    
      const [overrideRun] = await db.insert(alertRuns).values({
        tenantId: overrideTenant,
        status: "SUCCESS",
        startedAt: overrideNow,
        finishedAt: overrideNow,
      }).returning();
    
      const [overrideSubA] = await db.insert(alertSubscriptions).values({
        tenantId: overrideTenant,
        userId: overrideUser,
        scope: "GLOBAL",
        entityType: "port",
        entityId: GLOBAL_SCOPE_ENTITY_ID,
        severityMin: "LOW",
        channel: "WEBHOOK",
        endpoint: overrideDestA,
        isEnabled: true,
        updatedAt: overrideNow,
      }).returning();
    
      const [overrideSubB] = await db.insert(alertSubscriptions).values({
        tenantId: overrideTenant,
        userId: overrideUser,
        scope: "GLOBAL",
        entityType: "port",
        entityId: GLOBAL_SCOPE_ENTITY_ID,
        severityMin: "LOW",
        channel: "WEBHOOK",
        endpoint: overrideDestB,
        isEnabled: true,
        updatedAt: overrideNow,
      }).returning();
    
      await db.insert(alertDeliveries).values([
        {
          runId: overrideRun.id,
          tenantId: overrideTenant,
          userId: overrideUser,
          subscriptionId: overrideSubA.id,
          clusterId: "OVERRIDE-A",
          entityType: "port",
          entityId: portA,
          day: testDay,
          destinationType: "WEBHOOK",
          endpoint: overrideDestA,
          status: "SENT",
          attempts: 1,
          createdAt: overrideNow,
        },
        {
          runId: overrideRun.id,
          tenantId: overrideTenant,
          userId: overrideUser,
          subscriptionId: overrideSubB.id,
          clusterId: "OVERRIDE-B",
          entityType: "port",
          entityId: portA,
          day: testDay,
          destinationType: "WEBHOOK",
          endpoint: overrideDestB,
          status: "SENT",
          attempts: 1,
          createdAt: overrideNow,
        },
      ]);
    
      await db.insert(alertNoiseBudgets).values({
        tenantId: overrideTenant,
        destinationType: "WEBHOOK",
        window: "24h",
        maxDeliveries: 1,
        createdAt: overrideNow,
        updatedAt: overrideNow,
      });
    
      await upsertDestinationOverrides({
        tenantId: overrideTenant,
        destinationKey: overrideKeyA,
        destinationType: "WEBHOOK",
        noiseBudget: { enabled: true, window_minutes: 60, max_deliveries: 5 },
        updatedByUserId: overrideUser,
        updatedByKeyId: null,
        now: overrideNow,
      });
    
      const budgetA = await resolveNoiseBudget({
        tenantId: overrideTenant,
        destinationType: "WEBHOOK",
        destinationKey: overrideKeyA,
        destination: overrideDestA,
        now: overrideNow,
      });
      const budgetB = await resolveNoiseBudget({
        tenantId: overrideTenant,
        destinationType: "WEBHOOK",
        destinationKey: overrideKeyB,
        destination: overrideDestB,
        now: overrideNow,
      });
    
      assert.strictEqual(budgetA.allowed, true, "destination override allows more deliveries");
      assert.strictEqual(budgetA.source, "DESTINATION", "override source flagged");
      assert.strictEqual(budgetB.allowed, false, "tenant default budget still blocks other destinations");
      assert.strictEqual(budgetB.source, "TENANT_DEFAULT", "tenant default source flagged");
    
      globalThis.fetch = async () => {
        return { ok: true, status: 200 } as any;
      };
    
      const [qualityGateSub] = await db
        .insert(alertSubscriptions)
        .values({
          tenantId: qualityTenant,
          userId: qualityUser,
          entityType: "port",
          entityId: portA,
          severityMin: "HIGH",
          minQualityBand: "HIGH",
          channel: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          isEnabled: true,
          updatedAt: new Date(),
        })
        .returning();
    
      await runAlerts({ day: "2026-01-22", tenantId: qualityTenant, userId: qualityUser, now: new Date("2026-01-22T12:00:00Z") });
    
      const qualitySkipped = await db
        .select()
        .from(alertDeliveries)
        .where(and(
          eq(alertDeliveries.tenantId, qualityTenant),
          eq(alertDeliveries.subscriptionId, qualityGateSub.id),
          eq(alertDeliveries.status, "SKIPPED_QUALITY"),
        ));
      assert(qualitySkipped.length >= 1, "quality gate skips low quality");
      assert.strictEqual(qualitySkipped[0].skipReason, "QUALITY_BELOW_THRESHOLD", "quality skip reason recorded");
    
      const qualityAttempts = await db
        .select()
        .from(alertDeliveryAttempts)
        .where(eq(alertDeliveryAttempts.tenantId, qualityTenant));
      assert.strictEqual(qualityAttempts.length, 0, "quality gate skip does not create attempts");
    
      const qualityAuditInitial = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, qualityTenant),
          eq(auditEvents.action, "ALERT.QUALITY_GATE_SUPPRESSING"),
        ));
      assert.strictEqual(qualityAuditInitial.length, 1, "quality gate audit emitted once");
    
      await runAlerts({ day: "2026-01-22", tenantId: qualityTenant, userId: qualityUser, now: new Date("2026-01-22T12:10:00Z") });
      const qualityAuditAgain = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, qualityTenant),
          eq(auditEvents.action, "ALERT.QUALITY_GATE_SUPPRESSING"),
        ));
      assert.strictEqual(qualityAuditAgain.length, 1, "quality gate audit idempotent per day");
    
      const [qualityAllowSub] = await db
        .insert(alertSubscriptions)
        .values({
          tenantId: qualityAllowTenant,
          userId: qualityAllowUser,
          entityType: "port",
          entityId: portA,
          severityMin: "HIGH",
          minQualityBand: "MEDIUM",
          channel: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          isEnabled: true,
          updatedAt: new Date(),
        })
        .returning();
    
      await runAlerts({ day: "2026-01-23", tenantId: qualityAllowTenant, userId: qualityAllowUser, now: new Date("2026-01-23T12:00:00Z") });
      const allowDeliveries = await db
        .select()
        .from(alertDeliveries)
        .where(and(
          eq(alertDeliveries.tenantId, qualityAllowTenant),
          eq(alertDeliveries.subscriptionId, qualityAllowSub.id),
        ));
      assert(allowDeliveries.length >= 1, "quality gate allows high quality delivery");
      assert(allowDeliveries.every((row) => row.status !== "SKIPPED_QUALITY"), "allowed delivery not skipped by quality gate");
    
      globalThis.fetch = stubFetchOk;
    
      log("noise budget + quality gate checks complete");
    
      globalThis.fetch = async () => {
        return { ok: true, status: 200 } as any;
      };
    
      const bundleDay = new Date(Date.UTC(2026, 1, 2));
      await db.insert(alertSubscriptions).values({
        tenantId: bundleTenant,
        userId: bundleUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "LOW",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: bundleDay,
      });
    
      await db.insert(signals).values([
        {
          signalType: "TEST_DEDUPED_A",
          entityType: "port",
          entityId: portA,
          day: bundleDay,
          severity: "CRITICAL",
          value: 12,
          baseline: 6,
          stddev: 1,
          zscore: 6,
          deltaPct: 100,
          confidenceScore: 0.95,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "BUNDLE-CRIT",
          clusterKey: "BUNDLE-CRIT",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "CRITICAL",
          clusterSummary: "Bundle critical",
          explanation: "Bundle critical",
          metadata: { day: "2026-02-02" },
          createdAt: new Date("2026-02-02T08:00:00Z"),
        },
        {
          signalType: "TEST_BUNDLE_HIGH1",
          entityType: "port",
          entityId: portA,
          day: bundleDay,
          severity: "HIGH",
          value: 10,
          baseline: 6,
          stddev: 1,
          zscore: 4,
          deltaPct: 66,
          confidenceScore: 0.9,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "BUNDLE-HIGH-1",
          clusterKey: "BUNDLE-HIGH-1",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "Bundle high 1",
          explanation: "Bundle high 1",
          metadata: { day: "2026-02-02" },
          createdAt: new Date("2026-02-02T08:05:00Z"),
        },
        {
          signalType: "TEST_BUNDLE_HIGH2",
          entityType: "port",
          entityId: portA,
          day: bundleDay,
          severity: "HIGH",
          value: 9,
          baseline: 6,
          stddev: 1,
          zscore: 3,
          deltaPct: 50,
          confidenceScore: 0.6,
          confidenceBand: "MEDIUM",
          method: "zscore_30d",
          clusterId: "BUNDLE-HIGH-2",
          clusterKey: "BUNDLE-HIGH-2",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "Bundle high 2",
          explanation: "Bundle high 2",
          metadata: { day: "2026-02-02" },
          createdAt: new Date("2026-02-02T08:10:00Z"),
        },
        {
          signalType: "TEST_BUNDLE_MED",
          entityType: "port",
          entityId: portA,
          day: bundleDay,
          severity: "MEDIUM",
          value: 7,
          baseline: 6,
          stddev: 1,
          zscore: 1,
          deltaPct: 10,
          confidenceScore: 0.8,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "BUNDLE-MED",
          clusterKey: "BUNDLE-MED",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "MEDIUM",
          clusterSummary: "Bundle med",
          explanation: "Bundle med",
          metadata: { day: "2026-02-02" },
          createdAt: new Date("2026-02-02T08:15:00Z"),
        },
        {
          signalType: "TEST_BUNDLE_LOW",
          entityType: "port",
          entityId: portA,
          day: bundleDay,
          severity: "LOW",
          value: 6,
          baseline: 6,
          stddev: 1,
          zscore: 0,
          deltaPct: 0,
          confidenceScore: 0.8,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "BUNDLE-LOW",
          clusterKey: "BUNDLE-LOW",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "LOW",
          clusterSummary: "Bundle low",
          explanation: "Bundle low",
          metadata: { day: "2026-02-02" },
          createdAt: new Date("2026-02-02T08:20:00Z"),
        },
      ]);
    
      const bundleRun = await runAlerts({ day: "2026-02-02", tenantId: bundleTenant, userId: bundleUser, now: new Date("2026-02-02T12:00:00Z") });
      assert.strictEqual(bundleRun.summary.sent_total, 1, "bundle run sends one delivery");
      const bundleDeliveries = await db
        .select()
        .from(alertDeliveries)
        .where(eq(alertDeliveries.tenantId, bundleTenant));
      assert.strictEqual(bundleDeliveries.length, 1, "bundle creates single delivery row");
      const bundleDelivery = bundleDeliveries[0] as any;
      assert.strictEqual(bundleDelivery.bundleSize, 3, "bundle size is top N");
      assert.strictEqual(bundleDelivery.bundleOverflow, 2, "bundle overflow count");
      assert(bundleDelivery.decision, "decision stored on bundle delivery");
      const bundlePayload = bundleDelivery.bundlePayload as any;
      assert(bundlePayload?.items?.length === 3, "bundle payload items length");
      assert.strictEqual(bundleDelivery.clusterId, bundlePayload.items[0].cluster_id, "primary delivery cluster matches bundle[0]");
      assert.strictEqual(bundlePayload.items[0].cluster_id, "BUNDLE-CRIT");
      assert.strictEqual(bundlePayload.items[1].cluster_id, "BUNDLE-HIGH-1");
      assert.strictEqual(bundlePayload.items[2].cluster_id, "BUNDLE-HIGH-2");
      const bundleAttempts = await db
        .select()
        .from(alertDeliveryAttempts)
        .where(eq(alertDeliveryAttempts.deliveryId, bundleDelivery.id));
      assert.strictEqual(bundleAttempts.length, 1, "bundle creates one attempt");
    
      const retryTenant = harness.newTenantId();
      const retryUser = randomUUID();
      const retrySubId = randomUUID();
      const retryRunId = randomUUID();
      const retryDay = new Date(Date.UTC(2026, 1, 4));
      const retryDecision = {
        version: "1",
        evaluated_at: new Date("2026-02-04T12:00:00Z").toISOString(),
        subscription: {
          id: retrySubId,
          scope: "PORT",
          entity_id: portA,
          severity_min: "HIGH",
          destination_type: "WEBHOOK",
          destination_redacted: "http://localhost:9999/webhook",
          enabled: true,
        },
        selection: {
          day: "2026-02-04",
          window: "DAY",
          clustered: true,
          bundle: {
            enabled: true,
            top_n: 3,
            size: 1,
            overflow: 0,
            included: [
              {
                cluster_id: "RETRY-CLUSTER",
                cluster_type: "PORT_DISRUPTION",
                cluster_severity: "HIGH",
                confidence_score: 1,
                confidence_band: "HIGH",
                summary: "Retry bundle",
                reason_rank: 1,
              },
            ],
          },
        },
        gates: {
          severity_min_pass: true,
          dedupe: { applied: true, blocked: false },
          noise_budget: { applied: true, window: "24h", max: 10, used_before: 1, allowed: true },
          quality: { applied: true, score: 90, band: "HIGH", suppressed: false },
          rate_limit: { applied: true, per_endpoint: 50, allowed: true },
        },
        suppressed_counts: { dedupe: 0, noise_budget: 0, quality: 0, overflow: 0 },
      };
    
      await db.insert(alertRuns).values({
        id: retryRunId,
        tenantId: retryTenant,
        day: retryDay,
        status: "FAILED",
        startedAt: retryDay,
      });
      await db.insert(alertSubscriptions).values({
        id: retrySubId,
        tenantId: retryTenant,
        userId: retryUser,
        entityType: "port",
        entityId: portA,
        scope: "PORT",
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
      });
      await db.insert(alertDeliveries).values({
        runId: retryRunId,
        tenantId: retryTenant,
        userId: retryUser,
        subscriptionId: retrySubId,
        clusterId: "RETRY-CLUSTER",
        entityType: "port",
        entityId: portA,
        day: retryDay,
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "FAILED",
        isBundle: true,
        bundleSize: 1,
        bundleOverflow: 0,
        bundlePayload: {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: new Date("2026-02-04T12:00:00Z").toISOString(),
          subscription: { id: retrySubId },
          summary: { matched_total: 1, sent_items: 1, overflow: 0 },
          items: [{ cluster_id: "RETRY-CLUSTER", severity: "HIGH", cluster_summary: "Retry bundle" }],
        },
        decision: retryDecision,
        attempts: 1,
        createdAt: retryDay,
      });
    
      const originalFetchRetry = globalThis.fetch;
      globalThis.fetch = async () => ({ ok: true, status: 200 }) as any;
      const retryBefore = stableStringify(retryDecision);
      await retryDeliveryById({ deliveryId: (await db.select().from(alertDeliveries).where(eq(alertDeliveries.subscriptionId, retrySubId)).limit(1))[0].id, tenantId: retryTenant, userId: retryUser, now: new Date("2026-02-04T12:10:00Z") });
      const retryRow = (await db.select().from(alertDeliveries).where(eq(alertDeliveries.subscriptionId, retrySubId)).limit(1))[0] as any;
      const retryAfter = stableStringify(retryRow.decision);
      assert.strictEqual(retryAfter, retryBefore, "decision immutable across retry");
      globalThis.fetch = stubFetchOk;
    
      const gateTenant = harness.newTenantId();
      const gateUser = randomUUID();
      const gateRunId = randomUUID();
      const gateSubId = randomUUID();
      const gateNow = new Date("2026-02-05T10:00:00Z");
      const gateEndpoint = "http://localhost:9999/gated";
      const gateKey = makeDestinationKey("WEBHOOK", gateEndpoint);
    
      await db.insert(alertRuns).values({
        id: gateRunId,
        tenantId: gateTenant,
        day: gateNow,
        status: "FAILED",
        startedAt: gateNow,
      });
      await db.insert(alertSubscriptions).values({
        id: gateSubId,
        tenantId: gateTenant,
        userId: gateUser,
        entityType: "port",
        entityId: portA,
        scope: "PORT",
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: gateEndpoint,
        isEnabled: true,
      });
      const [gateDelivery] = await db.insert(alertDeliveries).values({
        runId: gateRunId,
        tenantId: gateTenant,
        userId: gateUser,
        subscriptionId: gateSubId,
        clusterId: "GATE-CLUSTER",
        entityType: "port",
        entityId: portA,
        day: gateNow,
        destinationType: "WEBHOOK",
        endpoint: gateEndpoint,
        status: "FAILED",
        bundlePayload: {
          payload_version: "1.2",
          type: "ALERT_BUNDLE",
          sent_at: gateNow.toISOString(),
          subscription: { id: gateSubId },
          summary: { matched_total: 1, sent_items: 1, overflow: 0 },
          items: [{ cluster_id: "GATE-CLUSTER", severity: "HIGH", cluster_summary: "Gate bundle" }],
        },
        decision: {
          version: "1",
          evaluated_at: gateNow.toISOString(),
          subscription: {
            id: gateSubId,
            scope: "PORT",
            entity_id: portA,
            severity_min: "HIGH",
            destination_type: "WEBHOOK",
            destination_redacted: gateEndpoint,
            enabled: true,
          },
          selection: {
            day: "2026-02-05",
            window: "DAY",
            clustered: true,
            bundle: { enabled: true, top_n: 1, size: 1, overflow: 0, included: [] },
          },
          gates: {
            severity_min_pass: true,
            dedupe: { applied: true, blocked: false },
            noise_budget: { applied: false, window: "24h", max: 0, used_before: 0, allowed: true },
            quality: { applied: true, score: 80, band: "HIGH", suppressed: false },
            rate_limit: { applied: false, per_endpoint: 999, allowed: true },
            endpoint_health: { applied: false, window: "1h", status: "OK", allowed: true },
            destination_state: { applied: true, state: "ACTIVE", allowed: true },
          },
          suppressed_counts: { dedupe: 0, noise_budget: 0, quality: 0, overflow: 0 },
        },
        attempts: 0,
        createdAt: gateNow,
      }).returning();
    
      const setGateState = async (state: "DISABLED" | "PAUSED" | "AUTO_PAUSED") => {
        await db.delete(alertDestinationStates).where(and(
          eq(alertDestinationStates.tenantId, gateTenant),
          eq(alertDestinationStates.destinationKey, gateKey),
        ));
        await db.insert(alertDestinationStates).values({
          tenantId: gateTenant,
          destinationType: "WEBHOOK",
          destinationKey: gateKey,
          state,
          reason: "test gate",
          pausedByUserId: state === "PAUSED" || state === "DISABLED" ? gateUser : null,
          pausedAt: state === "PAUSED" || state === "DISABLED" ? gateNow : null,
          autoPausedAt: state === "AUTO_PAUSED" ? gateNow : null,
          createdAt: gateNow,
          updatedAt: gateNow,
        });
      };
    
      for (const state of ["DISABLED", "PAUSED", "AUTO_PAUSED"] as const) {
        await setGateState(state);
        const attemptsBefore = await db
          .select()
          .from(alertDeliveryAttempts)
          .where(eq(alertDeliveryAttempts.deliveryId, gateDelivery.id));
        const dlqBefore = await db
          .select()
          .from(alertDlq)
          .where(eq(alertDlq.deliveryId, gateDelivery.id));
        const result = await retryDeliveryById({
          deliveryId: gateDelivery.id,
          tenantId: gateTenant,
          userId: gateUser,
          now: gateNow,
        });
        assert.strictEqual(result.status, "destination_blocked", `retry blocked when ${state}`);
        const attemptsAfter = await db
          .select()
          .from(alertDeliveryAttempts)
          .where(eq(alertDeliveryAttempts.deliveryId, gateDelivery.id));
        const dlqAfter = await db
          .select()
          .from(alertDlq)
          .where(eq(alertDlq.deliveryId, gateDelivery.id));
        assert.strictEqual(attemptsAfter.length, attemptsBefore.length, "no attempts on blocked retry");
        assert.strictEqual(dlqAfter.length, dlqBefore.length, "no dlq rows on blocked retry");
      }
    
      await setGateState("AUTO_PAUSED");
      const attemptsBeforeForce = await db
        .select()
        .from(alertDeliveryAttempts)
        .where(eq(alertDeliveryAttempts.deliveryId, gateDelivery.id));
      const forceResult = await retryDeliveryById({
        deliveryId: gateDelivery.id,
        tenantId: gateTenant,
        userId: gateUser,
        now: gateNow,
        force: true,
      });
      assert.notStrictEqual(forceResult.status, "destination_blocked", "force retry allowed for auto-paused");
      const attemptsAfterForce = await db
        .select()
        .from(alertDeliveryAttempts)
        .where(eq(alertDeliveryAttempts.deliveryId, gateDelivery.id));
      assert.strictEqual(attemptsAfterForce.length, attemptsBeforeForce.length + 1, "force retry writes attempt");
      const [forceDelivery] = await db
        .select()
        .from(alertDeliveries)
        .where(eq(alertDeliveries.id, gateDelivery.id))
        .limit(1);
      assert.strictEqual(
        (forceDelivery as any)?.decision?.gates?.destination_state?.force_retry,
        true,
        "force retry recorded in decision",
      );
    
      const dedupeDay = new Date(Date.UTC(2026, 1, 3));
      await db.insert(alertSubscriptions).values({
        tenantId: bundleDedupeTenant,
        userId: bundleDedupeUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "LOW",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: dedupeDay,
      });
      await db.insert(signals).values([
        {
          signalType: "TEST_DEDUPED_B",
          entityType: "port",
          entityId: portA,
          day: dedupeDay,
          severity: "HIGH",
          value: 10,
          baseline: 6,
          stddev: 1,
          zscore: 4,
          deltaPct: 60,
          confidenceScore: 0.8,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "DEDUPED-A",
          clusterKey: "DEDUPED-A",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "Deduped A",
          explanation: "Deduped A",
          metadata: { day: "2026-02-03" },
          createdAt: new Date("2026-02-03T08:00:00Z"),
        },
        {
          signalType: "TEST_DEDUPED_C",
          entityType: "port",
          entityId: portA,
          day: dedupeDay,
          severity: "HIGH",
          value: 10,
          baseline: 6,
          stddev: 1,
          zscore: 4,
          deltaPct: 60,
          confidenceScore: 0.8,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "DEDUPED-B",
          clusterKey: "DEDUPED-B",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "Deduped B",
          explanation: "Deduped B",
          metadata: { day: "2026-02-03" },
          createdAt: new Date("2026-02-03T08:05:00Z"),
        },
        {
          signalType: "PORT_ARRIVALS_ANOMALY",
          entityType: "port",
          entityId: portA,
          day: dedupeDay,
          severity: "HIGH",
          value: 10,
          baseline: 6,
          stddev: 1,
          zscore: 4,
          deltaPct: 60,
          confidenceScore: 0.8,
          confidenceBand: "HIGH",
          method: "zscore_30d",
          clusterId: "DEDUPED-C",
          clusterKey: "DEDUPED-C",
          clusterType: "PORT_DISRUPTION",
          clusterSeverity: "HIGH",
          clusterSummary: "Deduped C",
          explanation: "Deduped C",
          metadata: { day: "2026-02-03" },
          createdAt: new Date("2026-02-03T08:10:00Z"),
        },
      ]);
    
      await db.insert(alertDedupe).values([
        {
          tenantId: bundleDedupeTenant,
          clusterId: "DEDUPED-A",
          channel: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          lastSentAt: new Date("2026-02-03T09:00:00Z"),
          ttlHours: 24,
        },
        {
          tenantId: bundleDedupeTenant,
          clusterId: "DEDUPED-B",
          channel: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          lastSentAt: new Date("2026-02-03T09:00:00Z"),
          ttlHours: 24,
        },
      ]);
    
      await runAlerts({ day: "2026-02-03", tenantId: bundleDedupeTenant, userId: bundleDedupeUser, now: new Date("2026-02-03T12:00:00Z") });
      const dedupeRows = await db
        .select()
        .from(alertDeliveries)
        .where(eq(alertDeliveries.tenantId, bundleDedupeTenant));
      assert.strictEqual(dedupeRows.length, 1, "dedupe bundle creates one delivery row");
      const dedupePayload = (dedupeRows[0] as any).bundlePayload as any;
      assert.strictEqual(dedupePayload?.summary?.skipped_dedupe, 2, "bundle summary tracks dedupe");
      assert.strictEqual(dedupePayload?.items?.length, 1, "bundle items reflect dedupe");
    
      globalThis.fetch = stubFetchOk;
    
      const qualityHigh = computeAlertQuality({
        severity: "CRITICAL",
        confidence_band: "HIGH",
        confidence_score: 1,
        data_quality: { completeness_pct: 100 },
        explainability: { drivers: [{ metric: "arrivals" }] },
      });
      assert.strictEqual(qualityHigh.score, 100, "quality high score clamps to 100");
      assert.strictEqual(qualityHigh.band, "HIGH", "quality high band");
    
      const qualityLow = computeAlertQuality({
        severity: "MEDIUM",
        confidence_band: "LOW",
        confidence_score: 0,
        data_quality: { completeness_pct: 70 },
      });
      assert.strictEqual(qualityLow.score, 25, "quality low score deterministic");
      assert.strictEqual(qualityLow.band, "LOW", "quality low band");
    
      const qualityAtRisk = computeAlertQuality({
        severity: "LOW",
        confidence_band: "LOW",
        cluster_type: "SLA_AT_RISK",
      });
      assert(qualityAtRisk.score >= 70, "SLA_AT_RISK clamps to minimum 70");
    
      const qualityRecovered = computeAlertQuality({
        severity: "HIGH",
        confidence_band: "HIGH",
        cluster_type: "SLA_RECOVERED",
      });
      assert(qualityRecovered.score <= 40, "SLA_RECOVERED clamps to max 40");
    
      const slaPlaybook = getAlertPlaybook({
        clusterType: "SLA_AT_RISK",
        status: "FAILED",
        destinationType: "WEBHOOK",
      });
      assert.strictEqual(slaPlaybook.version, "1", "playbook version is stable");
      assert(
        slaPlaybook.actions.some((item) => item.includes("Retry failed deliveries")),
        "SLA playbook includes retry guidance",
      );
    
      const noisePlaybook = getAlertPlaybook({
        clusterType: "PORT_DISRUPTION",
        status: "SKIPPED",
        destinationType: "WEBHOOK",
        skipReason: "NOISE_BUDGET_EXCEEDED",
      });
      assert(
        noisePlaybook.actions.includes("Increase noise budget cap or tighten subscription filters."),
        "noise budget playbook guidance present",
      );
    
      log("quality scoring checks complete");
    
      globalThis.fetch = stubFetchFail;
      const failedRun = await runAlerts({ day: "2026-01-21", tenantId: tenantA, userId: dispatcherSubUserId, now: new Date("2026-01-21T12:30:00Z") });
      assert.strictEqual(failedRun.status, "FAILED", "failed run sets status FAILED");
      assert(failedRun.summary.failed_total >= 1, "failed run increments failed_total");
      globalThis.fetch = stubFetchOk;
    
      const healthRows = await getDeliveryHealthByDay(30);
      assert(Array.isArray(healthRows), "delivery health returns array");
      const latency = await getDeliveryLatency(30);
      assert(latency, "latency query returns a row");
      const endpointHealth = await getEndpointHealth(30);
      assert(Array.isArray(endpointHealth), "endpoint health returns array");
      const dlqHealth = await getDlqHealth();
      assert(dlqHealth, "dlq health returns a row");
      const dlqOverdue = await getDlqOverdue(5);
      assert(Array.isArray(dlqOverdue), "dlq overdue returns array");
    
      log("metrics queries complete");
    
      const apiUserA = randomUUID();
      const apiUserB = randomUUID();
      const apiUserDisabled = randomUUID();
      const apiKeyRaw = "test_api_key_1";
      const prevAlertsKey = process.env.ALERTS_API_KEY;
      const prevAlertsUser = process.env.ALERTS_USER_ID;
      process.env.ALERTS_API_KEY = "";
      process.env.ALERTS_USER_ID = "";
      await insertApiKeys({
        tenantId: tenantA,
        userId: apiUserA,
        keyHash: hashApiKey(apiKeyRaw),
        name: "test-key",
      });
      await insertApiKeys({
        tenantId: tenantA,
        userId: apiUserB,
        keyHash: hashApiKey("test_api_key_no_member"),
        name: "no-member",
      });
      await insertApiKeys({
        tenantId: tenantA,
        userId: apiUserDisabled,
        keyHash: hashApiKey("test_api_key_disabled"),
        name: "disabled-member",
      });
      await insertTenantUsers({
        tenantId: tenantA,
        userId: apiUserA,
        email: "user-a@example.com",
        role: "OWNER",
        status: "ACTIVE",
        createdBy: apiUserA,
      });
      await insertTenantUsers({
        tenantId: tenantA,
        userId: apiUserDisabled,
        email: "user-disabled@example.com",
        role: "VIEWER",
        status: "DISABLED",
        createdBy: apiUserA,
      });
    
      let statusCode = 0;
      let jsonBody: any = null;
      let nextCalled = false;
      const resMock = {
        setHeader() {
          return undefined;
        },
        status(code: number) {
          statusCode = code;
          return this;
        },
        json(body: any) {
          jsonBody = body;
          return this;
        },
      } as any;
    
      await authenticateApiKey({ headers: {}, path: "/v1/alert-deliveries", method: "GET" } as any, resMock, () => {
        nextCalled = true;
      });
      assert.strictEqual(statusCode, 401, "missing api key returns 401");
      assert(!nextCalled, "missing api key does not call next");
    
      const [authDenied] = await db
        .select()
        .from(auditEvents)
        .where(eq(auditEvents.action, "AUTH.API_KEY_DENIED"))
        .limit(1);
      assert(authDenied, "auth denied audit event written");
    
      statusCode = 0;
      jsonBody = null;
      nextCalled = false;
      const reqAuth: any = { headers: { authorization: `Bearer ${apiKeyRaw}` } };
      await authenticateApiKey(reqAuth, resMock, () => {
        nextCalled = true;
      });
      assert(nextCalled, "valid api key calls next");
      assert.strictEqual(reqAuth.auth?.userId, apiUserA, "auth user id attached");
      assert.strictEqual(reqAuth.auth?.tenantId, tenantA, "auth tenant id attached");
    
      statusCode = 0;
      jsonBody = null;
      nextCalled = false;
      await authenticateApiKey({ headers: { authorization: "Bearer test_api_key_no_member" } } as any, resMock, () => {
        nextCalled = true;
      });
      assert.strictEqual(statusCode, 403, "api key without membership is forbidden");
      assert(!nextCalled, "no member does not call next");
    
      statusCode = 0;
      jsonBody = null;
      nextCalled = false;
      await authenticateApiKey({ headers: { authorization: "Bearer test_api_key_disabled" } } as any, resMock, () => {
        nextCalled = true;
      });
      assert.strictEqual(statusCode, 403, "disabled member is forbidden");
      assert(!nextCalled, "disabled member does not call next");
      process.env.ALERTS_API_KEY = prevAlertsKey ?? "";
      process.env.ALERTS_USER_ID = prevAlertsUser ?? "";
    
      const viewerKeyRaw = "test_api_key_viewer";
      const operatorKeyRaw = "test_api_key_operator";
      const ownerKeyRaw = "test_api_key_owner";
      await insertApiKeys([
        {
          tenantId: tenantA,
          userId: apiUserA,
          keyHash: hashApiKey(viewerKeyRaw),
          name: "viewer-key",
          role: "VIEWER",
        },
        {
          tenantId: tenantA,
          userId: apiUserA,
          keyHash: hashApiKey(operatorKeyRaw),
          name: "operator-key",
          role: "OPERATOR",
        },
        {
          tenantId: tenantA,
          userId: apiUserA,
          keyHash: hashApiKey(ownerKeyRaw),
          name: "owner-key",
          role: "OWNER",
        },
      ]);
    
      const resolveAuth = async (rawKey: string) => {
        let called = false;
        const req: any = { headers: { authorization: `Bearer ${rawKey}` } };
        await authenticateApiKey(req, resMock, () => {
          called = true;
        });
        assert(called, "auth middleware calls next");
        return req.auth;
      };
    
      const viewerAuth = await resolveAuth(viewerKeyRaw);
      const operatorAuth = await resolveAuth(operatorKeyRaw);
      const ownerAuth = await resolveAuth(ownerKeyRaw);
    
      assert.strictEqual(viewerAuth?.role, "VIEWER", "viewer role attached");
      assert.strictEqual(operatorAuth?.role, "OPERATOR", "operator role attached");
      assert.strictEqual(ownerAuth?.role, "OWNER", "owner role attached");
    
      assert.doesNotThrow(() => requireAlertRole(viewerAuth as any, "VIEWER"), "viewer can read");
      assert.throws(() => requireAlertRole(viewerAuth as any, "OPERATOR"), /Forbidden/, "viewer cannot operate");
      assert.doesNotThrow(() => requireAlertRole(operatorAuth as any, "OPERATOR"), "operator can operate");
      assert.throws(() => requireAlertRole(operatorAuth as any, "OWNER"), /Forbidden/, "operator cannot own");
      assert.doesNotThrow(() => requireAlertRole(ownerAuth as any, "OWNER"), "owner can manage");
    
      const [runRow] = await db.insert(alertRuns).values({
        tenantId: tenantA,
        status: "SUCCESS",
        startedAt: new Date(),
        finishedAt: new Date(),
      }).returning();
    
      const [subA] = await db.insert(alertSubscriptions).values({
        tenantId: tenantA,
        userId: apiUserA,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: new Date(),
      }).returning();
    
      await writeAuditEvent(undefined, {
        tenantId: tenantA,
        action: "ALERT.SUBSCRIPTION_CREATED",
        resourceType: "ALERT_SUBSCRIPTION",
        resourceId: subA.id,
        severity: "INFO",
        status: "SUCCESS",
        message: "Alert subscription created",
        metadata: { channel: subA.channel ?? null },
        actorType: "API_KEY",
        actorUserId: subA.userId,
        actorLabel: "test-key",
      });
    
      const [auditSubCreated] = await db
        .select()
        .from(auditEvents)
        .where(
          and(
            eq(auditEvents.action, "ALERT.SUBSCRIPTION_CREATED"),
            eq(auditEvents.resourceId, subA.id),
          ),
        )
        .limit(1);
      assert(auditSubCreated, "subscription create audit event written");
    
      const [subB] = await db.insert(alertSubscriptions).values({
        tenantId: tenantB,
        userId: apiUserB,
        scope: "PORT",
        entityType: "port",
        entityId: portB,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: new Date(),
      }).returning();
    
      const t3 = new Date("2026-02-01T10:03:00Z");
      const t2 = new Date("2026-02-01T10:02:00Z");
      const t1 = new Date("2026-02-01T10:01:00Z");
    
      await db.insert(alertDeliveries).values([
        {
          runId: runRow.id,
          tenantId: tenantA,
          userId: subA.userId,
          subscriptionId: subA.id,
          clusterId: "PAG-1",
          entityType: "port",
          entityId: portA,
          day: new Date("2026-02-01"),
          destinationType: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          status: "SENT",
          attempts: 1,
          createdAt: t3,
        },
        {
          runId: runRow.id,
          tenantId: tenantA,
          userId: subA.userId,
          subscriptionId: subA.id,
          clusterId: "PAG-2",
          entityType: "port",
          entityId: portA,
          day: new Date("2026-02-01"),
          destinationType: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          status: "SENT",
          attempts: 1,
          createdAt: t2,
        },
        {
          runId: runRow.id,
          tenantId: tenantA,
          userId: subA.userId,
          subscriptionId: subA.id,
          clusterId: "PAG-3",
          entityType: "port",
          entityId: portA,
          day: new Date("2026-02-01"),
          destinationType: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          status: "SENT",
          attempts: 1,
          createdAt: t1,
        },
        {
          runId: runRow.id,
          tenantId: tenantB,
          userId: subB.userId,
          subscriptionId: subB.id,
          clusterId: "PAG-B",
          entityType: "port",
          entityId: portB,
          day: new Date("2026-02-01"),
          destinationType: "WEBHOOK",
          endpoint: "http://localhost:9999/webhook",
          status: "SENT",
          attempts: 1,
          createdAt: t3,
        },
      ]);
    
      const page1 = await listAlertDeliveries({
        tenantId: tenantA,
        userId: subA.userId,
        subscriptionIds: [subA.id],
        limit: 2,
      });
      assert.strictEqual(page1.items.length, 2, "page1 returns 2");
      const cursorCreatedAt = (page1.items[page1.items.length - 1].createdAt as Date).toISOString();
      const cursorId = page1.items[page1.items.length - 1].id;
    
      const page2 = await listAlertDeliveries({
        tenantId: tenantA,
        userId: subA.userId,
        subscriptionIds: [subA.id],
        limit: 2,
        cursorCreatedAt,
        cursorId,
      });
      assert.strictEqual(page2.items.length, 1, "page2 returns remaining 1");
      const page1Ids = new Set(page1.items.map((row) => row.id));
      const page2Ids = new Set(page2.items.map((row) => row.id));
      assert([...page1Ids].every((id) => !page2Ids.has(id)), "no overlap between pages");
    
      const scoped = await listAlertDeliveries({
        tenantId: tenantA,
        userId: apiUserA,
        limit: 10,
      });
      assert(scoped.items.every((row) => row.userId === apiUserA), "scoped list returns only user rows");
    
      const scopedTenantB = await listAlertDeliveries({
        tenantId: tenantB,
        userId: apiUserB,
        limit: 10,
      });
      assert.strictEqual(scopedTenantB.items.length, 1, "tenant B sees only its delivery");
      assert.strictEqual(scopedTenantB.items[0].clusterId, "PAG-B", "tenant B delivery is isolated");
    
      const summaryUser = randomUUID();
      const [summarySubWebhook] = await db.insert(alertSubscriptions).values({
        tenantId: tenantA,
        userId: summaryUser,
        scope: "GLOBAL",
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: new Date(),
      }).returning();
    
      const [summarySubEmail] = await db.insert(alertSubscriptions).values({
        tenantId: tenantA,
        userId: summaryUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "EMAIL",
        endpoint: "alerts@veriscope.dev",
        isEnabled: true,
        updatedAt: new Date(),
      }).returning();
    
      const [summarySubDisabled] = await db.insert(alertSubscriptions).values({
        tenantId: tenantA,
        userId: summaryUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:5001/webhook",
        isEnabled: false,
        updatedAt: new Date(),
      }).returning();
    
      const [summaryRun] = await db.insert(alertRuns).values({
        tenantId: tenantA,
        status: "SUCCESS",
        startedAt: new Date(),
        finishedAt: new Date(),
      }).returning();
    
      const [sentDelivery] = await db.insert(alertDeliveries).values({
        runId: summaryRun.id,
        tenantId: tenantA,
        userId: summaryUser,
        subscriptionId: summarySubWebhook.id,
        clusterId: "SUM-1",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-02"),
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "SENT",
        attempts: 1,
        latencyMs: 15,
        createdAt: new Date("2026-02-02T10:00:00Z"),
      }).returning();
    
      const [emailDelivery] = await db.insert(alertDeliveries).values({
        runId: summaryRun.id,
        tenantId: tenantA,
        userId: summaryUser,
        subscriptionId: summarySubEmail.id,
        clusterId: "SUM-2",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-02"),
        destinationType: "EMAIL",
        endpoint: "alerts@veriscope.dev",
        status: "SENT",
        attempts: 1,
        createdAt: new Date("2026-02-02T10:01:00Z"),
      }).returning();
    
      const [dlqDelivery] = await db.insert(alertDeliveries).values({
        runId: summaryRun.id,
        tenantId: tenantA,
        userId: summaryUser,
        subscriptionId: summarySubWebhook.id,
        clusterId: "SUM-3",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-02"),
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "FAILED",
        attempts: 1,
        createdAt: new Date("2026-02-02T10:02:00Z"),
      }).returning();
    
      const [terminalDelivery] = await db.insert(alertDeliveries).values({
        runId: summaryRun.id,
        tenantId: tenantA,
        userId: summaryUser,
        subscriptionId: summarySubWebhook.id,
        clusterId: "SUM-4",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-02"),
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "FAILED",
        attempts: 1,
        createdAt: new Date("2026-02-02T10:03:00Z"),
      }).returning();
    
      await db.insert(alertDlq).values({
        tenantId: tenantA,
        deliveryId: dlqDelivery.id,
        attemptCount: 1,
        maxAttempts: 3,
        nextAttemptAt: new Date(),
        lastError: "fetch failed",
      });
    
      await db.insert(alertDlq).values({
        tenantId: tenantA,
        deliveryId: terminalDelivery.id,
        attemptCount: 3,
        maxAttempts: 3,
        nextAttemptAt: new Date(),
        lastError: "fetch failed",
      });
    
      await db.insert(alertDestinationStates).values({
        tenantId: tenantA,
        destinationType: "WEBHOOK",
        destinationKey: makeDestinationKey("WEBHOOK", "http://localhost:9999/webhook"),
        state: "PAUSED",
        reason: "test paused",
        pausedByUserId: summaryUser,
        pausedAt: new Date("2026-02-02T10:05:00Z"),
        createdAt: new Date("2026-02-02T10:05:00Z"),
        updatedAt: new Date("2026-02-02T10:05:00Z"),
      });
    
      const summaryAll = await getAlertDeliveriesSummary({
        tenantId: tenantA,
        userId: summaryUser,
        days: 30,
      });
      assert.strictEqual(summaryAll.total, 4, "summary total counts all deliveries");
      assert.strictEqual(summaryAll.sent, 2, "summary counts sent");
      assert.strictEqual(summaryAll.failed, 2, "summary counts failed");
      assert.strictEqual(summaryAll.dlq_pending, 1, "summary counts retryable DLQ only");
      assert.strictEqual(summaryAll.skipped, 0, "summary skipped count");
      assert.strictEqual(summaryAll.p50_latency_ms, 15, "summary p50 latency uses sent rows with latency");
      assert.strictEqual(summaryAll.destination_states?.PAUSED ?? 0, 1, "summary counts paused destinations");
      assert.strictEqual(summaryAll.destination_states?.ACTIVE ?? 0, 1, "summary counts active destinations");
    
      const summaryEmail = await getAlertDeliveriesSummary({
        tenantId: tenantA,
        userId: summaryUser,
        days: 30,
        destinationType: "EMAIL",
      });
      assert.strictEqual(summaryEmail.total, 1, "destination filter applies to summary");
    
      const summaryDlq = await getAlertDeliveriesSummary({
        tenantId: tenantA,
        userId: summaryUser,
        days: 30,
        status: "DLQ",
      });
      assert.strictEqual(summaryDlq.total, 1, "dlq filter counts retryable only");
    
      const subsSummary = await getAlertSubscriptionsSummary({
        tenantId: tenantA,
        userId: summaryUser,
      });
      assert.strictEqual(subsSummary.total, 3, "subscriptions summary total");
      assert.strictEqual(subsSummary.enabled, 2, "subscriptions enabled count");
      assert.strictEqual(subsSummary.disabled, 1, "subscriptions disabled count");
      assert.strictEqual(subsSummary.webhook, 2, "subscriptions webhook count");
      assert.strictEqual(subsSummary.email, 1, "subscriptions email count");
      assert.strictEqual(subsSummary.scope_global, 1, "subscriptions global scope count");
      assert.strictEqual(subsSummary.scope_port, 2, "subscriptions port scope count");
    
      const subsPage1 = await listAlertSubscriptionsPage({
        tenantId: tenantA,
        userId: summaryUser,
        limit: 2,
      });
      const subCursorCreatedAt = subsPage1.items[subsPage1.items.length - 1].createdAtRaw;
      const subCursorId = subsPage1.items[subsPage1.items.length - 1].id;
    
      const subsPage2 = await listAlertSubscriptionsPage({
        tenantId: tenantA,
        userId: summaryUser,
        limit: 2,
        cursorCreatedAt: subCursorCreatedAt,
        cursorId: subCursorId,
      });
      const subsPage1Ids = new Set(subsPage1.items.map((row) => row.id));
      const subsPage2Ids = new Set(subsPage2.items.map((row) => row.id));
      assert([...subsPage1Ids].every((id) => !subsPage2Ids.has(id)), "subscriptions paging no overlap");
    
      const auditTenant = harness.newTenantId();
      const auditTenantOther = harness.newTenantId();
      const auditBase = new Date("2026-02-06T10:00:00Z");
      const auditIds = [randomUUID(), randomUUID(), randomUUID(), randomUUID(), randomUUID()];
      const auditResourceId = randomUUID();
    
      await db.insert(auditEvents).values([
        {
          id: auditIds[0],
          tenantId: auditTenant,
          actorType: "API_KEY",
          actorUserId: apiUserA,
          action: "ALERT.SUBSCRIPTION_CREATED",
          resourceType: "ALERT_SUBSCRIPTION",
          resourceId: auditResourceId,
          severity: "INFO",
          status: "SUCCESS",
          message: "Subscription created",
          createdAt: new Date(auditBase.getTime() + 1000),
        },
        {
          id: auditIds[1],
          tenantId: auditTenant,
          actorType: "API_KEY",
          actorUserId: apiUserA,
          action: "AUTH.API_KEY_DENIED",
          resourceType: "API_KEY",
          severity: "SECURITY",
          status: "DENIED",
          message: "Invalid API key",
          createdAt: new Date(auditBase.getTime() + 2000),
        },
        {
          id: auditIds[2],
          tenantId: auditTenant,
          actorType: "API_KEY",
          actorUserId: apiUserA,
          action: "ALERT.DELIVERY_RETRY_REQUESTED",
          resourceType: "ALERT_DELIVERY",
          resourceId: auditResourceId,
          severity: "WARN",
          status: "FAILED",
          message: "Retry failed",
          createdAt: new Date(auditBase.getTime() + 3000),
        },
        {
          id: auditIds[3],
          tenantId: auditTenant,
          actorType: "API_KEY",
          actorUserId: apiUserA,
          action: "TEAM.INVITE_CREATED",
          resourceType: "TENANT_INVITE",
          severity: "SECURITY",
          status: "SUCCESS",
          message: "Invite created",
          createdAt: new Date(auditBase.getTime() + 4000),
        },
        {
          id: auditIds[4],
          tenantId: auditTenantOther,
          actorType: "API_KEY",
          actorUserId: apiUserB,
          action: "ALERT.SUBSCRIPTION_CREATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "Subscription created",
          createdAt: new Date(auditBase.getTime() + 5000),
        },
      ]);
    
      const auditPage1 = await listAuditEvents({ tenantId: auditTenant, days: 365, limit: 2 });
      assert.strictEqual(auditPage1.items.length, 2, "audit page1 returns 2");
      const auditCursorCreatedAt = (auditPage1.items[auditPage1.items.length - 1].createdAt as Date).toISOString();
      const auditCursorId = auditPage1.items[auditPage1.items.length - 1].id;
      const auditPage2 = await listAuditEvents({
        tenantId: auditTenant,
        days: 365,
        limit: 2,
        cursorCreatedAt: auditCursorCreatedAt,
        cursorId: auditCursorId,
      });
      const auditPage1Ids = new Set(auditPage1.items.map((row) => row.id));
      const auditPage2Ids = new Set(auditPage2.items.map((row) => row.id));
      assert([...auditPage1Ids].every((id) => !auditPage2Ids.has(id)), "audit paging no overlap");
    
      const auditSummary = await getAuditEventsSummary({ tenantId: auditTenant, days: 365 });
      assert.strictEqual(auditSummary.total, 4, "audit summary totals tenant rows");
      assert.strictEqual(auditSummary.security, 2, "audit summary security count");
      assert.strictEqual(auditSummary.denied, 1, "audit summary denied count");
      assert.strictEqual(auditSummary.failed, 1, "audit summary failed count");
    
      const auditSecurity = await listAuditEvents({ tenantId: auditTenant, days: 365, severityMin: "SECURITY" });
      assert(auditSecurity.items.every((row) => row.severity === "SECURITY"), "severity min filters audit events");
    
      const auditSummaryB = await getAuditEventsSummary({ tenantId: auditTenantOther, days: 365 });
      assert.strictEqual(auditSummaryB.total, 1, "tenant isolation for audit summary");
    
      const destinationAuditTenant = harness.newTenantId();
      const destinationAuditOther = randomUUID();
      const destinationKey = `dest_${randomUUID()}`;
      const destinationAuditBase = new Date("2026-02-07T10:00:00Z");
      const destinationAuditIds = [randomUUID(), randomUUID(), randomUUID(), randomUUID()];
    
      await db.insert(auditEvents).values([
        {
          id: destinationAuditIds[0],
          tenantId: destinationAuditTenant,
          actorType: "API_KEY",
          actorUserId: apiUserA,
          action: "ALERT.DESTINATION_STATE_CHANGED",
          resourceType: "ALERT_DESTINATION",
          resourceId: destinationKey,
          severity: "SECURITY",
          status: "SUCCESS",
          message: "Destination paused",
          metadata: { destination_key: destinationKey },
          createdAt: new Date(destinationAuditBase.getTime() + 1000),
        },
        {
          id: destinationAuditIds[1],
          tenantId: destinationAuditTenant,
          actorType: "SYSTEM",
          action: "ALERT.ENDPOINT_AUTO_PAUSED",
          resourceType: "ALERT_DESTINATION",
          resourceId: destinationKey,
          severity: "SECURITY",
          status: "SUCCESS",
          message: "Destination auto-paused",
          metadata: { destination_key: destinationKey },
          createdAt: new Date(destinationAuditBase.getTime() + 2000),
        },
        {
          id: destinationAuditIds[2],
          tenantId: destinationAuditTenant,
          actorType: "SYSTEM",
          action: "ALERT.ENDPOINT_HEALTH_DOWN",
          resourceType: "ALERT_DESTINATION",
          resourceId: destinationKey,
          severity: "SECURITY",
          status: "SUCCESS",
          message: "Endpoint down",
          metadata: { destination_key: destinationKey },
          createdAt: new Date(destinationAuditBase.getTime() + 3000),
        },
        {
          id: destinationAuditIds[3],
          tenantId: destinationAuditOther,
          actorType: "API_KEY",
          actorUserId: apiUserB,
          action: "ALERT.DESTINATION_STATE_CHANGED",
          resourceType: "ALERT_DESTINATION",
          resourceId: destinationKey,
          severity: "SECURITY",
          status: "SUCCESS",
          message: "Other tenant destination",
          metadata: { destination_key: destinationKey },
          createdAt: new Date(destinationAuditBase.getTime() + 4000),
        },
      ]);
    
      const destinationAuditList = await listAuditEvents({
        tenantId: destinationAuditTenant,
        days: 365,
        resourceType: "ALERT_DESTINATION",
        resourceId: destinationKey,
        limit: 10,
      });
      assert.strictEqual(destinationAuditList.items.length, 3, "destination audit filter returns tenant rows");
      assert(destinationAuditList.items.every((row) => row.resourceId === destinationKey), "destination audit resource_id matches");
    
      const destinationPage1 = await listAuditEvents({
        tenantId: destinationAuditTenant,
        days: 365,
        resourceType: "ALERT_DESTINATION",
        resourceId: destinationKey,
        limit: 2,
      });
      const destinationCursorCreatedAt = (destinationPage1.items[destinationPage1.items.length - 1].createdAt as Date).toISOString();
      const destinationCursorId = destinationPage1.items[destinationPage1.items.length - 1].id;
      const destinationPage2 = await listAuditEvents({
        tenantId: destinationAuditTenant,
        days: 365,
        resourceType: "ALERT_DESTINATION",
        resourceId: destinationKey,
        limit: 2,
        cursorCreatedAt: destinationCursorCreatedAt,
        cursorId: destinationCursorId,
      });
      const destinationPage1Ids = new Set(destinationPage1.items.map((row) => row.id));
      const destinationPage2Ids = new Set(destinationPage2.items.map((row) => row.id));
      assert([...destinationPage1Ids].every((id) => !destinationPage2Ids.has(id)), "destination audit paging no overlap");
    
      const destinationAuditOtherList = await listAuditEvents({
        tenantId: destinationAuditOther,
        days: 365,
        resourceType: "ALERT_DESTINATION",
        resourceId: destinationKey,
        limit: 10,
      });
      assert.strictEqual(destinationAuditOtherList.items.length, 1, "destination audit tenant isolation");
    
      const destinationAuditWriteTenant = harness.newTenantId();
      const destinationAuditWriteKey = `dest_${randomUUID()}`;
      await writeAuditEvent(undefined, {
        tenantId: destinationAuditWriteTenant,
        actorType: "API_KEY",
        action: "ALERT.DESTINATION_STATE_CHANGED",
        resourceType: "ALERT_DESTINATION",
        resourceId: destinationAuditWriteKey,
        status: "SUCCESS",
        severity: "SECURITY",
        message: "Destination state changed",
        metadata: { destination_key: destinationAuditWriteKey },
      });
      const [destinationAuditWriteRow] = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, destinationAuditWriteTenant),
          eq(auditEvents.action, "ALERT.DESTINATION_STATE_CHANGED"),
        ))
        .limit(1);
      assert.strictEqual(destinationAuditWriteRow?.resourceId, destinationAuditWriteKey, "destination audit resource_id stored");
    
      const retentionTenant = harness.newTenantId();
      const retentionDefault = await getTenantSettings(retentionTenant);
      assert.strictEqual(retentionDefault.audit_retention_days, 90, "default retention days");
    
      const retentionUpdated = await upsertTenantSettings(retentionTenant, { audit_retention_days: 120 });
      assert.strictEqual(retentionUpdated.audit_retention_days, 120, "retention update persists");
    
      await assert.rejects(
        () => upsertTenantSettings(retentionTenant, { audit_retention_days: 3 }),
        /between 7 and 3650/i,
        "retention clamp rejects invalid value",
      );
    
      const purgeTenant = harness.newTenantId();
      const purgeOtherTenant = harness.newTenantId();
      const oldEventTime = new Date(Date.now() - 12 * 24 * 60 * 60 * 1000);
      const newEventTime = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000);
    
      await db.insert(auditEvents).values([
        {
          tenantId: purgeTenant,
          actorType: "API_KEY",
          action: "ALERT.SUBSCRIPTION_CREATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "Old event 1",
          createdAt: oldEventTime,
        },
        {
          tenantId: purgeTenant,
          actorType: "API_KEY",
          action: "ALERT.SUBSCRIPTION_UPDATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "Old event 2",
          createdAt: oldEventTime,
        },
        {
          tenantId: purgeTenant,
          actorType: "API_KEY",
          action: "ALERT.SUBSCRIPTION_UPDATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "New event",
          createdAt: newEventTime,
        },
        {
          tenantId: purgeOtherTenant,
          actorType: "API_KEY",
          action: "ALERT.SUBSCRIPTION_CREATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "Other tenant old",
          createdAt: oldEventTime,
        },
      ]);
    
      const purgeResult = await purgeAuditEventsForTenant({ tenantId: purgeTenant, retentionDays: 7, batchSize: 100 });
      assert.strictEqual(purgeResult.deleted, 2, "purge deletes only old rows");
    
      const [purgeRemaining] = await db
        .select({ count: sql<number>`count(*)` })
        .from(auditEvents)
        .where(eq(auditEvents.tenantId, purgeTenant));
      assert.strictEqual(Number(purgeRemaining?.count ?? 0), 1, "purge leaves recent rows");
    
      const [purgeOtherRemaining] = await db
        .select({ count: sql<number>`count(*)` })
        .from(auditEvents)
        .where(eq(auditEvents.tenantId, purgeOtherTenant));
      assert.strictEqual(Number(purgeOtherRemaining?.count ?? 0), 1, "purge is tenant-isolated");
    
      const exportTenant = harness.newTenantId();
      const exportUser = randomUUID();
      const exportEventBase = new Date("2026-02-10T10:00:00Z");
    
      await db.insert(auditEvents).values([
        {
          tenantId: exportTenant,
          actorType: "API_KEY",
          actorUserId: exportUser,
          action: "ALERT.SUBSCRIPTION_CREATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "Export event 1",
          createdAt: exportEventBase,
        },
        {
          tenantId: exportTenant,
          actorType: "API_KEY",
          actorUserId: exportUser,
          action: "ALERT.SUBSCRIPTION_UPDATED",
          resourceType: "ALERT_SUBSCRIPTION",
          severity: "INFO",
          status: "SUCCESS",
          message: "Export event 2",
          createdAt: new Date(exportEventBase.getTime() + 1000),
        },
      ]);
    
      const exportFilters = { tenantId: exportTenant, days: 365 };
      const exportRow = await createAuditExport({
        tenantId: exportTenant,
        userId: exportUser,
        format: "csv",
        filters: exportFilters,
        maxRows: 2,
      });
      assert(exportRow.exportId, "export row created");
    
      const exportMeta = await getAuditExport(exportRow.exportId, exportTenant);
      assert(exportMeta, "export metadata fetches");
      assert.strictEqual(exportMeta?.rowCount, 2, "export row count stored");
    
      const tokenCheck = verifyExportToken(exportTenant, exportRow.exportId, exportRow.token);
      assert(tokenCheck.ok, "valid export token passes");
    
      const expiredToken = signExportToken(exportTenant, exportRow.exportId, Math.floor(Date.now() / 1000) - 10);
      const expiredCheck = verifyExportToken(exportTenant, exportRow.exportId, expiredToken);
      assert(!expiredCheck.ok, "expired token fails");
    
      const foreignExport = await getAuditExport(exportRow.exportId, randomUUID());
      assert.strictEqual(foreignExport, null, "tenant isolation for export metadata");
    
      const exportRows = await fetchAuditEventsForExport(exportFilters, 10);
      assert.strictEqual(exportRows.length, 2, "export fetch returns rows");
    
      const csv = auditEventsToCsv(exportRows);
      const csvLines = csv.trim().split("\n");
      assert.strictEqual(csvLines.length, exportRows.length + 1, "csv includes header + rows");
    
      const jsonl = auditEventsToJsonl(exportRows);
      const jsonlLines = jsonl.trim().split("\n");
      assert.strictEqual(jsonlLines.length, exportRows.length, "jsonl row count");
    
      const prevGlobalLimit = process.env.RATE_LIMIT_GLOBAL_PER_MIN;
      const prevExportLimit = process.env.RATE_LIMIT_EXPORT_PER_MIN;
      const prevWriteLimit = process.env.RATE_LIMIT_WRITE_PER_MIN;
      process.env.RATE_LIMIT_GLOBAL_PER_MIN = "2";
      process.env.RATE_LIMIT_EXPORT_PER_MIN = "1";
      process.env.RATE_LIMIT_WRITE_PER_MIN = "2";
    
      const realDateNow = Date.now;
      const fixedRateNow = new Date("2026-02-12T12:00:00Z").getTime();
      Date.now = () => fixedRateNow;
    
      const rateTenantA = harness.newTenantId();
      const rateTenantB = harness.newTenantId();
      const rateKeyA = hashApiKey("rate_limit_key_a");
      const rateKeyB = hashApiKey("rate_limit_key_b");
      const rateUser = randomUUID();
    
      const buildRateReq = (tenantId: string, keyHash: string, method: string, path: string) => ({
        method,
        path,
        auth: {
          tenantId,
          keyHash,
          userId: rateUser,
          role: "OWNER",
          apiKeyId: "rate-key",
          apiKeyName: "rate-key",
        },
        auditContext: {
          tenantId,
          actorType: "API_KEY",
          actorUserId: rateUser,
          actorApiKeyId: randomUUID(),
          actorLabel: "rate-key",
          requestId: randomUUID(),
          ip: null,
          userAgent: null,
        },
      } as any);
    
      const resFactory = () => {
        const res: any = {
          statusCode: 200,
          body: null as any,
          headers: new Map<string, string>(),
          setHeader(key: string, value: string) {
            res.headers.set(key, String(value));
          },
          status(code: number) {
            res.statusCode = code;
            return res;
          },
          json(body: any) {
            res.body = body;
            return res;
          },
        };
        return res;
      };
    
      await db.execute(sql`DELETE FROM rate_limit_buckets`);
      await db.delete(auditEvents).where(eq(auditEvents.action, "SECURITY.RATE_LIMIT_EXCEEDED"));
    
      const req1 = buildRateReq(rateTenantA, rateKeyA, "GET", "/v1/alert-deliveries");
      const res1 = resFactory();
      assert(await applyRateLimit(req1, res1), "rate limit allows first request");
    
      const req2 = buildRateReq(rateTenantA, rateKeyA, "GET", "/v1/alert-deliveries");
      const res2 = resFactory();
      assert(await applyRateLimit(req2, res2), "rate limit allows second request");
    
      const req3 = buildRateReq(rateTenantA, rateKeyA, "GET", "/v1/alert-deliveries");
      const res3 = resFactory();
      const allowed3 = await applyRateLimit(req3, res3);
      assert.strictEqual(allowed3, false, "rate limit blocks third request");
      assert.strictEqual(res3.statusCode, 429, "429 returned");
      assert.strictEqual(res3.body?.scope, "GLOBAL", "scope is GLOBAL");
      assert(res3.headers.get("Retry-After"), "Retry-After header set");
      assert(res3.headers.get("X-RateLimit-Reset"), "X-RateLimit-Reset header set");
    
      const reqTenantB = buildRateReq(rateTenantB, rateKeyB, "GET", "/v1/alert-deliveries");
      const resTenantB = resFactory();
      assert(await applyRateLimit(reqTenantB, resTenantB), "tenant B has independent bucket");
    
      await db.execute(sql`DELETE FROM rate_limit_buckets`);
      const exportKey = hashApiKey("rate_limit_export_key");
      const exportReq1 = buildRateReq(rateTenantA, exportKey, "POST", "/v1/audit-events/export");
      const exportRes1 = resFactory();
      assert(await applyRateLimit(exportReq1, exportRes1), "export allowed first");
    
      const exportReq2 = buildRateReq(rateTenantA, exportKey, "POST", "/v1/audit-events/export");
      const exportRes2 = resFactory();
      const exportAllowed2 = await applyRateLimit(exportReq2, exportRes2);
      assert.strictEqual(exportAllowed2, false, "export limited on second request");
      assert.strictEqual(exportRes2.body?.scope, "EXPORT", "export scope reported");
    
      const rateAuditRows = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.action, "SECURITY.RATE_LIMIT_EXCEEDED"), eq(auditEvents.tenantId, rateTenantA)));
      assert(rateAuditRows.length >= 1, "rate limit audit event emitted");
    
      Date.now = realDateNow;
    
      const slaTenant = harness.newTenantId();
      const slaUser = randomUUID();
      const slaNow = new Date("2026-02-10T12:00:00Z");
      const slaWindowStart = new Date(Math.floor((slaNow.getTime() - 24 * 60 * 60 * 1000) / 60000) * 60000);
      const defaultThresholds = await getSlaThresholds(slaTenant, "24h");
      assert.strictEqual(defaultThresholds.WEBHOOK.source, "DEFAULT", "default thresholds apply when no rows exist");
    
      const [slaRun] = await db.insert(alertRuns).values({
        tenantId: slaTenant,
        status: "SUCCESS",
        startedAt: slaNow,
        finishedAt: slaNow,
      }).returning();
    
      const [slaSub] = await db.insert(alertSubscriptions).values({
        tenantId: slaTenant,
        userId: slaUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: slaNow,
      }).returning();
    
      const slaDestination = "http://localhost:9999/webhook";
      const slaDestinationKey = makeDestinationKey("WEBHOOK", slaDestination);
      await db.insert(alertDeliveries).values(
        Array.from({ length: 10 }).map((_, idx) => ({
          runId: slaRun.id,
          tenantId: slaTenant,
          userId: slaUser,
          subscriptionId: slaSub.id,
          clusterId: `SLA-CLUSTER-${idx}`,
          entityType: "port",
          entityId: portA,
          day: new Date("2026-02-10"),
          destinationType: "WEBHOOK",
          endpoint: slaDestination,
          status: "SENT",
          latencyMs: 150,
          attempts: 1,
          createdAt: new Date(slaNow.getTime() - idx * 1000),
        })),
      );
    
      const slaAuditContext: any = {
        tenantId: slaTenant,
        actorType: "API_KEY",
        actorUserId: slaUser,
        actorApiKeyId: randomUUID(),
        actorLabel: "sla-key",
        requestId: randomUUID(),
        ip: null,
        userAgent: null,
      };
    
      await computeSla({ tenantId: slaTenant, now: slaNow, auditContext: slaAuditContext , skipLock: true});
    
      const [slaOkRow] = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(and(
          eq(alertDeliverySlaWindows.tenantId, slaTenant),
          eq(alertDeliverySlaWindows.destinationType, "WEBHOOK"),
          eq(alertDeliverySlaWindows.destinationKey, slaDestinationKey),
          eq(alertDeliverySlaWindows.window, "24h"),
          eq(alertDeliverySlaWindows.windowStart, slaWindowStart),
        ))
        .limit(1);
      assert(slaOkRow, "sla ok row created");
      assert.strictEqual(slaOkRow.status, "OK", "sla status OK");
      assert(
        slaOkRow.latencyP95Ms <= defaultThresholds.WEBHOOK.p95_ms_threshold,
        "p95 under default SLA threshold",
      );
      assert(
        Number(slaOkRow.successRate) >= defaultThresholds.WEBHOOK.success_rate_threshold * 100,
        "success rate meets default threshold",
      );
    
      const slaDestinationB = "http://localhost:9999/webhook-b";
      const slaDestinationKeyB = makeDestinationKey("WEBHOOK", slaDestinationB);
      await db.insert(alertDeliveries).values(
        Array.from({ length: 6 }).map((_, idx) => ({
          runId: slaRun.id,
          tenantId: slaTenant,
          userId: slaUser,
          subscriptionId: slaSub.id,
          clusterId: `SLA-CLUSTER-B-${idx}`,
          entityType: "port",
          entityId: portA,
          day: new Date("2026-02-10"),
          destinationType: "WEBHOOK",
          endpoint: slaDestinationB,
          status: "SENT",
          latencyMs: 120,
          attempts: 1,
          createdAt: new Date(slaNow.getTime() - idx * 2000),
        })),
      );
    
      await computeSla({ tenantId: slaTenant, now: slaNow, auditContext: slaAuditContext , skipLock: true});
      const slaRows = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(and(
          eq(alertDeliverySlaWindows.tenantId, slaTenant),
          eq(alertDeliverySlaWindows.destinationType, "WEBHOOK"),
          eq(alertDeliverySlaWindows.window, "24h"),
          eq(alertDeliverySlaWindows.windowStart, slaWindowStart),
        ));
      const slaKeys = new Set(slaRows.map((row) => row.destinationKey));
      assert.strictEqual(slaKeys.size, 2, "SLA rows created per destination_key");
      assert(slaKeys.has(slaDestinationKey), "SLA row contains primary destination key");
      assert(slaKeys.has(slaDestinationKeyB), "SLA row contains secondary destination key");
    
      const resumeReadyAt = new Date(slaNow.getTime() + 5 * 60 * 1000);
      await db.insert(alertDestinationStates).values({
        tenantId: slaTenant,
        destinationType: "WEBHOOK",
        destinationKey: slaDestinationKey,
        state: "AUTO_PAUSED",
        reason: "ENDPOINT_DOWN",
        autoPausedAt: slaNow,
        resumeReadyAt,
        createdAt: slaNow,
        updatedAt: slaNow,
      });
    
      const slaList = await listAlertSlaWindows(slaTenant, "24h");
      const slaItem = slaList.items.find((row: any) => row.destination_key === slaDestinationKey);
      assert(slaItem, "SLA list includes destination key");
      assert.strictEqual(slaItem?.destination?.state, "AUTO_PAUSED", "SLA list includes destination state");
    
      await db.update(alertDeliveries)
        .set({ latencyMs: 6000 })
        .where(and(
          eq(alertDeliveries.tenantId, slaTenant),
          eq(alertDeliveries.endpoint, slaDestination),
        ));
      await computeSla({ tenantId: slaTenant, now: slaNow, auditContext: slaAuditContext , skipLock: true});
    
      const [slaRiskRow] = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(and(
          eq(alertDeliverySlaWindows.tenantId, slaTenant),
          eq(alertDeliverySlaWindows.destinationType, "WEBHOOK"),
          eq(alertDeliverySlaWindows.destinationKey, slaDestinationKey),
          eq(alertDeliverySlaWindows.window, "24h"),
          eq(alertDeliverySlaWindows.windowStart, slaWindowStart),
        ))
        .limit(1);
      assert.strictEqual(slaRiskRow?.status, "AT_RISK", "sla status at risk");
    
      const [slaAtRiskAudit] = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.action, "ALERT.SLA_AT_RISK"), eq(auditEvents.tenantId, slaTenant)))
        .limit(1);
      assert(slaAtRiskAudit, "sla at-risk audit emitted");
    
      const [slaIncident] = await db
        .select()
        .from(incidents)
        .where(and(
          eq(incidents.tenantId, slaTenant),
          eq(incidents.type, "SLA_AT_RISK"),
          eq(incidents.destinationKey, slaDestinationKey),
        ))
        .limit(1);
      assert(slaIncident, "SLA at-risk incident opened");
      assert.strictEqual(slaIncident?.status, "OPEN", "SLA incident status open");
    
      await computeSla({ tenantId: slaTenant, now: slaNow, auditContext: slaAuditContext, skipLock: true });
      const slaIncidentRows = await db
        .select()
        .from(incidents)
        .where(and(
          eq(incidents.tenantId, slaTenant),
          eq(incidents.type, "SLA_AT_RISK"),
          eq(incidents.destinationKey, slaDestinationKey),
        ));
      assert.strictEqual(slaIncidentRows.length, 1, "no duplicate SLA incidents");
    
      const slaSummary = await getAlertSlaSummary(slaTenant);
      const slaSummary24 = slaSummary.items.find((item) => item.window === "24h");
      assert.strictEqual(slaSummary24?.by_destination_type?.WEBHOOK?.at_risk_count, 1, "summary counts at-risk destinations");
      assert.strictEqual(slaSummary24?.by_destination_type?.WEBHOOK?.ok_count, 1, "summary counts OK destinations");
    
      await db.update(alertDeliveries)
        .set({ latencyMs: 120 })
        .where(and(
          eq(alertDeliveries.tenantId, slaTenant),
          eq(alertDeliveries.endpoint, slaDestination),
        ));
      await computeSla({ tenantId: slaTenant, now: slaNow, auditContext: slaAuditContext , skipLock: true});
    
      const [slaRecoveredAudit] = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.action, "ALERT.SLA_RECOVERED"), eq(auditEvents.tenantId, slaTenant)))
        .limit(1);
      assert(slaRecoveredAudit, "sla recovered audit emitted");
    
      const [slaResolvedIncident] = await db
        .select()
        .from(incidents)
        .where(and(
          eq(incidents.tenantId, slaTenant),
          eq(incidents.type, "SLA_AT_RISK"),
          eq(incidents.destinationKey, slaDestinationKey),
        ))
        .limit(1);
      assert.strictEqual(slaResolvedIncident?.status, "RESOLVED", "SLA recovery resolves incident");
    
      const tenantIsolated = harness.newTenantId();
      const [isoRun] = await db.insert(alertRuns).values({
        tenantId: tenantIsolated,
        status: "SUCCESS",
        startedAt: slaNow,
        finishedAt: slaNow,
      }).returning();
    
      const [isoSub] = await db.insert(alertSubscriptions).values({
        tenantId: tenantIsolated,
        userId: slaUser,
        scope: "PORT",
        entityType: "port",
        entityId: portB,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: slaNow,
      }).returning();
    
      const [isoDelivery] = await db.insert(alertDeliveries).values({
        runId: isoRun.id,
        tenantId: tenantIsolated,
        userId: slaUser,
        subscriptionId: isoSub.id,
        clusterId: "SLA-ISO",
        entityType: "port",
        entityId: portB,
        day: new Date("2026-02-10"),
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "SENT",
        attempts: 1,
        createdAt: slaNow,
      }).returning();
    
      await db.update(alertDeliveries)
        .set({ latencyMs: 10000 })
        .where(eq(alertDeliveries.id, isoDelivery.id));
    
      await computeSla({ tenantId: slaTenant, now: slaNow, auditContext: slaAuditContext , skipLock: true});
      const [slaStillOk] = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(and(
          eq(alertDeliverySlaWindows.tenantId, slaTenant),
          eq(alertDeliverySlaWindows.destinationType, "WEBHOOK"),
          eq(alertDeliverySlaWindows.destinationKey, slaDestinationKey),
          eq(alertDeliverySlaWindows.window, "24h"),
          eq(alertDeliverySlaWindows.windowStart, slaWindowStart),
        ))
        .limit(1);
      assert.strictEqual(slaStillOk?.status, "OK", "tenant isolation keeps SLA OK");
    
      const isoRows = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(eq(alertDeliverySlaWindows.tenantId, tenantIsolated));
      assert.strictEqual(isoRows.length, 0, "tenant isolation for SLA windows");
    
      const thresholdNow = new Date("2026-02-12T12:00:00Z");
      const thresholdWindowStart = new Date(Math.floor((thresholdNow.getTime() - 24 * 60 * 60 * 1000) / 60000) * 60000);
      const [thresholdRun] = await db.insert(alertRuns).values({
        tenantId: thresholdTenant,
        status: "SUCCESS",
        startedAt: thresholdNow,
        finishedAt: thresholdNow,
      }).returning();
    
      const [thresholdSub] = await db.insert(alertSubscriptions).values({
        tenantId: thresholdTenant,
        userId: thresholdUser,
        scope: "PORT",
        entityType: "port",
        entityId: portA,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: thresholdNow,
      }).returning();
    
      const thresholdDestination = "http://localhost:9999/webhook";
      const thresholdDestinationKey = makeDestinationKey("WEBHOOK", thresholdDestination);
      const [thresholdDelivery] = await db.insert(alertDeliveries).values({
        runId: thresholdRun.id,
        tenantId: thresholdTenant,
        userId: thresholdUser,
        subscriptionId: thresholdSub.id,
        clusterId: "SLA-THRESHOLD",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-12"),
        destinationType: "WEBHOOK",
        endpoint: thresholdDestination,
        status: "SENT",
        latencyMs: 120,
        attempts: 1,
        createdAt: thresholdNow,
      }).returning();
    
      await computeSla({ tenantId: thresholdTenant, now: thresholdNow , skipLock: true});
      const [thresholdOk] = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(and(
          eq(alertDeliverySlaWindows.tenantId, thresholdTenant),
          eq(alertDeliverySlaWindows.destinationType, "WEBHOOK"),
          eq(alertDeliverySlaWindows.destinationKey, thresholdDestinationKey),
          eq(alertDeliverySlaWindows.window, "24h"),
          eq(alertDeliverySlaWindows.windowStart, thresholdWindowStart),
        ))
        .limit(1);
      assert.strictEqual(thresholdOk?.status, "OK", "default thresholds keep SLA OK");
    
      await db.insert(alertSlaThresholds).values({
        tenantId: thresholdTenant,
        window: "24h",
        destinationType: "WEBHOOK",
        p95MsThreshold: 10,
        successRateThreshold: 0.99,
        createdAt: thresholdNow,
        updatedAt: thresholdNow,
      });
    
      await computeSla({ tenantId: thresholdTenant, now: thresholdNow , skipLock: true});
      const [thresholdRisk] = await db
        .select()
        .from(alertDeliverySlaWindows)
        .where(and(
          eq(alertDeliverySlaWindows.tenantId, thresholdTenant),
          eq(alertDeliverySlaWindows.destinationType, "WEBHOOK"),
          eq(alertDeliverySlaWindows.destinationKey, thresholdDestinationKey),
          eq(alertDeliverySlaWindows.window, "24h"),
          eq(alertDeliverySlaWindows.windowStart, thresholdWindowStart),
        ))
        .limit(1);
      assert.strictEqual(thresholdRisk?.status, "AT_RISK", "custom thresholds override defaults");
    
      const thresholdsTenantB = await getSlaThresholds(tenantB, "24h");
      assert.strictEqual(thresholdsTenantB.WEBHOOK.source, "DEFAULT", "tenant isolation for thresholds");
    
      const backfillResult = await backfillAlertDeliverySlaWindows({
        tenantId: slaTenant,
        days: 1000,
        window: "24h",
        auditContext: slaAuditContext,
      });
      assert.strictEqual(backfillResult.daysUsed, 365, "backfill clamps days to 365");
    
      const lockKey = "alert_sla_24h";
      const lockHandle = await tryAdvisoryLock(slaTenant, lockKey);
      try {
        assert(lockHandle.locked, "advisory lock acquired for backfill test");
    
        const skippedResult = await backfillAlertDeliverySlaWindows({
          tenantId: slaTenant,
          days: 1,
          window: "24h",
          auditContext: slaAuditContext,
        });
        assert.strictEqual(skippedResult.skipped, true, "backfill skips when lock held");
      } finally {
        await releaseAdvisoryLock(lockHandle);
      }
    
      const slaSystemTenant = harness.newTenantId();
      const slaSystemUser = randomUUID();
      const [systemMember] = await insertTenantUsers({
        tenantId: slaSystemTenant,
        userId: slaSystemUser,
        email: "owner@veriscope.dev",
        role: "OWNER",
        status: "ACTIVE",
        createdAt: slaNow,
      }, { returning: true });
    
      const [systemSub] = await db.insert(alertSubscriptions).values({
        tenantId: slaSystemTenant,
        userId: slaSystemUser,
        scope: "GLOBAL",
        entityType: "port",
        entityId: GLOBAL_SCOPE_ENTITY_ID,
        severityMin: "HIGH",
        channel: "EMAIL",
        endpoint: "owner@veriscope.dev",
        isEnabled: true,
        updatedAt: slaNow,
      }).returning();
    
      const [systemRun] = await db.insert(alertRuns).values({
        tenantId: slaSystemTenant,
        status: "SUCCESS",
        startedAt: slaNow,
        finishedAt: slaNow,
      }).returning();
    
      const systemDestination = "http://localhost:9999/webhook";
      const [systemDelivery] = await db.insert(alertDeliveries).values({
        runId: systemRun.id,
        tenantId: slaSystemTenant,
        userId: slaSystemUser,
        subscriptionId: systemSub.id,
        clusterId: "SLA-BASE",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-10"),
        destinationType: "WEBHOOK",
        endpoint: systemDestination,
        status: "SENT",
        latencyMs: 150,
        attempts: 1,
        createdAt: slaNow,
      }).returning();
    
      await computeSla({ tenantId: slaSystemTenant, now: slaNow, auditContext: slaAuditContext, windows: ["24h"] , skipLock: true});
    
      const initialSystemAlerts = await db
        .select()
        .from(alertDeliveries)
        .where(and(eq(alertDeliveries.tenantId, slaSystemTenant), sql`${alertDeliveries.clusterId} like 'sla:%'`));
      assert.strictEqual(initialSystemAlerts.length, 0, "no SLA system alerts on initial OK");
    
      await db.update(alertDeliveries)
        .set({ latencyMs: 7000 })
        .where(eq(alertDeliveries.id, systemDelivery.id));
      await computeSla({ tenantId: slaSystemTenant, now: slaNow, auditContext: slaAuditContext, windows: ["24h"] , skipLock: true});
    
      const atRiskAudit = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, slaSystemTenant),
          eq(auditEvents.action, "ALERT.SLA_AT_RISK"),
        ));
      assert(atRiskAudit.length >= 1, "SLA_AT_RISK emitted");
    
      await computeSla({ tenantId: slaSystemTenant, now: slaNow, auditContext: slaAuditContext, windows: ["24h"] , skipLock: true});
      const atRiskAuditAgain = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, slaSystemTenant),
          eq(auditEvents.action, "ALERT.SLA_AT_RISK"),
        ));
      assert.strictEqual(atRiskAuditAgain.length, atRiskAudit.length, "no duplicate SLA_AT_RISK");
    
      await db.update(alertDeliveries)
        .set({ latencyMs: 120 })
        .where(eq(alertDeliveries.id, systemDelivery.id));
      await computeSla({ tenantId: slaSystemTenant, now: slaNow, auditContext: slaAuditContext, windows: ["24h"] , skipLock: true});
    
      const recoveredAudit = await db
        .select()
        .from(auditEvents)
        .where(and(
          eq(auditEvents.tenantId, slaSystemTenant),
          eq(auditEvents.action, "ALERT.SLA_RECOVERED"),
        ));
      assert(recoveredAudit.length >= 1, "SLA_RECOVERED emitted");
    
      const endpointTenant = harness.newTenantId();
      const endpointUser = randomUUID();
      const endpointNow = new Date("2026-02-15T12:00:00Z");
      const [endpointRun] = await db.insert(alertRuns).values({
        tenantId: endpointTenant,
        status: "SUCCESS",
        startedAt: endpointNow,
        finishedAt: endpointNow,
      }).returning();
    
      const [endpointSub] = await db.insert(alertSubscriptions).values({
        tenantId: endpointTenant,
        userId: endpointUser,
        scope: "GLOBAL",
        entityType: "port",
        entityId: GLOBAL_SCOPE_ENTITY_ID,
        severityMin: "HIGH",
        channel: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        isEnabled: true,
        updatedAt: endpointNow,
      }).returning();
    
      const [endpointDelivery] = await db.insert(alertDeliveries).values({
        runId: endpointRun.id,
        tenantId: endpointTenant,
        userId: endpointUser,
        subscriptionId: endpointSub.id,
        clusterId: "ENDPOINT-TEST",
        entityType: "port",
        entityId: portA,
        day: new Date("2026-02-15"),
        destinationType: "WEBHOOK",
        endpoint: "http://localhost:9999/webhook",
        status: "SENT",
        attempts: 1,
        createdAt: endpointNow,
      }).returning();
    
      await db.insert(alertDeliveryAttempts).values([
        { tenantId: endpointTenant, deliveryId: endpointDelivery.id, attemptNo: 1, status: "SENT", latencyMs: 100, createdAt: new Date(endpointNow.getTime() - 10 * 60 * 1000) },
        { tenantId: endpointTenant, deliveryId: endpointDelivery.id, attemptNo: 2, status: "SENT", latencyMs: 200, createdAt: new Date(endpointNow.getTime() - 9 * 60 * 1000) },
        { tenantId: endpointTenant, deliveryId: endpointDelivery.id, attemptNo: 3, status: "SENT", latencyMs: 300, createdAt: new Date(endpointNow.getTime() - 8 * 60 * 1000) },
        { tenantId: endpointTenant, deliveryId: endpointDelivery.id, attemptNo: 4, status: "SENT", latencyMs: 400, createdAt: new Date(endpointNow.getTime() - 7 * 60 * 1000) },
        { tenantId: endpointTenant, deliveryId: endpointDelivery.id, attemptNo: 5, status: "SENT", latencyMs: 500, createdAt: new Date(endpointNow.getTime() - 6 * 60 * 1000) },
        { tenantId: endpointTenant, deliveryId: endpointDelivery.id, attemptNo: 6, status: "FAILED", latencyMs: 0, createdAt: new Date(endpointNow.getTime() - 5 * 60 * 1000) },
      ]);
    
      await computeEndpointHealth({ tenantId: endpointTenant, now: endpointNow, window: "24h" });
      const endpointList = await listEndpointHealth({ tenantId: endpointTenant, window: "24h" });
      assert.strictEqual(endpointList.items.length, 1, "endpoint health row created");
      assert.strictEqual(endpointList.summary.total, 1, "endpoint summary totals populated");
      assert.strictEqual(endpointList.items[0].status, "DEGRADED", "endpoint degraded when success rate below 0.95");
      const expectedP50 = Math.round(percentileCont([100, 200, 300, 400, 500], 0.5)!);
      const expectedP95 = Math.round(percentileCont([100, 200, 300, 400, 500], 0.95)!);
      assert.strictEqual(endpointList.items[0].p50_ms, expectedP50, "endpoint p50 latency");
      assert.strictEqual(endpointList.items[0].p95_ms, expectedP95, "endpoint p95 latency");
    
      await db.delete(alertDeliveryAttempts).where(eq(alertDeliveryAttempts.tenantId, endpointTenant));
      await db.insert(alertDeliveryAttempts).values(
        Array.from({ length: 5 }).map((_, idx) => ({
          tenantId: endpointTenant,
          deliveryId: endpointDelivery.id,
          attemptNo: idx + 1,
          status: "FAILED",
          latencyMs: 0,
          createdAt: new Date(endpointNow.getTime() - idx * 60 * 1000),
        })),
      );
      const endpointAuditContext: any = {
        tenantId: endpointTenant,
        actorType: "API_KEY",
        actorUserId: endpointUser,
        actorApiKeyId: randomUUID(),
        actorLabel: "endpoint-key",
        requestId: randomUUID(),
        ip: null,
        userAgent: null,
      };
      await computeEndpointHealth({ tenantId: endpointTenant, now: endpointNow, window: "24h", auditContext: endpointAuditContext });
      const endpointDown = await listEndpointHealth({ tenantId: endpointTenant, window: "24h" });
      assert.strictEqual(endpointDown.items[0].status, "DOWN", "endpoint down on consecutive failures");
    
      const endpointAuditDown = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.action, "ALERT.ENDPOINT_HEALTH_DOWN"), eq(auditEvents.tenantId, endpointTenant)))
        .limit(1);
      assert(endpointAuditDown.length === 1, "endpoint down audit emitted");
    
      const endpointDestinationKey = makeDestinationKey("WEBHOOK", endpointDelivery.endpoint);
      const [endpointIncident] = await db
        .select()
        .from(incidents)
        .where(and(
          eq(incidents.tenantId, endpointTenant),
          eq(incidents.type, "ENDPOINT_DOWN"),
          eq(incidents.destinationKey, endpointDestinationKey),
        ))
        .limit(1);
      assert(endpointIncident, "endpoint down incident opened");
    
      const endpointTenantB = harness.newTenantId();
      await computeEndpointHealth({ tenantId: endpointTenantB, now: endpointNow, window: "24h" });
      const endpointListB = await listEndpointHealth({ tenantId: endpointTenantB, window: "24h" });
      assert.strictEqual(endpointListB.items.length, 0, "tenant isolation for endpoint health");
      const destinationStateTenant = harness.newTenantId();
      const destinationStateUser = randomUUID();
      const destinationStateNow = new Date("2026-01-15T13:00:00Z");
      const [destinationStateSub] = await db.insert(alertSubscriptions).values({
      tenantId: destinationStateTenant,
      userId: destinationStateUser,
      entityType: "port",
      entityId: portA,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/paused-endpoint",
      isEnabled: true,
      updatedAt: destinationStateNow,
      }).returning();
      const destinationKeyPaused = makeDestinationKey("WEBHOOK", destinationStateSub.endpoint);
      await db.insert(alertDestinationStates).values({
      tenantId: destinationStateTenant,
      destinationType: "WEBHOOK",
      destinationKey: destinationKeyPaused,
      state: "PAUSED",
      reason: "maintenance",
      pausedByUserId: destinationStateUser,
      pausedAt: destinationStateNow,
      createdAt: destinationStateNow,
      updatedAt: destinationStateNow,
      });

      await runAlerts({ day: "2026-01-15", tenantId: destinationStateTenant, userId: destinationStateUser, now: destinationStateNow });
      const pausedDeliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, destinationStateTenant));
      assert.strictEqual(pausedDeliveries.length, 1, "paused destination delivery created");
      assert.strictEqual(pausedDeliveries[0].status, "SKIPPED_DESTINATION_PAUSED", "destination pause skip status");
      assert.strictEqual(pausedDeliveries[0].skipReason, "DESTINATION_PAUSED", "destination pause skip reason");
      assert.strictEqual((pausedDeliveries[0] as any).decision?.gates?.destination_state?.state, "PAUSED", "decision gate records destination pause");
      const pausedAttempts = await db
      .select()
      .from(alertDeliveryAttempts)
      .where(eq(alertDeliveryAttempts.tenantId, destinationStateTenant));
      assert.strictEqual(pausedAttempts.length, 0, "paused destination creates no attempts");
      const pausedDlq = await db
      .select()
      .from(alertDlq)
      .where(eq(alertDlq.tenantId, destinationStateTenant));
      assert.strictEqual(pausedDlq.length, 0, "paused destination creates no dlq");

      const destFilterTenant = harness.newTenantId();
      const destFilterUser = randomUUID();
      const destFilterNow = new Date("2026-02-20T10:00:00Z");
      const [destFilterRun] = await db.insert(alertRuns).values({
      tenantId: destFilterTenant,
      status: "SUCCESS",
      startedAt: destFilterNow,
      finishedAt: destFilterNow,
      }).returning();
      const [destFilterSub] = await db.insert(alertSubscriptions).values({
      tenantId: destFilterTenant,
      userId: destFilterUser,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "HIGH",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/filter-a",
      isEnabled: true,
      updatedAt: destFilterNow,
      }).returning();
      await db.insert(alertDeliveries).values([
      {
      runId: destFilterRun.id,
      tenantId: destFilterTenant,
      userId: destFilterUser,
      subscriptionId: destFilterSub.id,
      clusterId: "FILTER-A",
      entityType: "port",
      entityId: portA,
      day: new Date("2026-02-20"),
      destinationType: "WEBHOOK",
      endpoint: "http://localhost:9999/filter-a",
      status: "FAILED",
      attempts: 1,
      createdAt: destFilterNow,
      },
      {
      runId: destFilterRun.id,
      tenantId: destFilterTenant,
      userId: destFilterUser,
      subscriptionId: destFilterSub.id,
      clusterId: "FILTER-B",
      entityType: "port",
      entityId: portA,
      day: new Date("2026-02-20"),
      destinationType: "WEBHOOK",
      endpoint: "http://localhost:9999/filter-b",
      status: "FAILED",
      attempts: 1,
      createdAt: destFilterNow,
      },
      ]);
      const destinationKeyFilter = makeDestinationKey("WEBHOOK", "http://localhost:9999/filter-a");
      const filtered = await listAlertDeliveries({
      tenantId: destFilterTenant,
      userId: destFilterUser,
      days: 7,
      destinationKey: destinationKeyFilter,
      });
      assert.strictEqual(filtered.items.length, 1, "destination_key filter returns one delivery");
      assert.strictEqual(filtered.items[0].endpoint, "http://localhost:9999/filter-a");
      const filteredSummary = await getAlertDeliveriesSummary({
      tenantId: destFilterTenant,
      userId: destFilterUser,
      days: 7,
      destinationKey: destinationKeyFilter,
      });
      assert.strictEqual(filteredSummary.total, 1, "destination_key filter summary matches");

      const autoPauseTenant = harness.newTenantId();
      const autoPauseOwner = randomUUID();
      const autoPauseNow = new Date("2026-02-17T12:00:00Z");
      await db.insert(tenantUsers).values({
      tenantId: autoPauseTenant,
      userId: autoPauseOwner,
      email: "owner@autopause.test",
      role: "OWNER",
      status: "ACTIVE",
      createdAt: autoPauseNow,
      });
      await db.insert(alertSubscriptions).values({
      tenantId: autoPauseTenant,
      userId: autoPauseOwner,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/auto-paused",
      isEnabled: true,
      updatedAt: autoPauseNow,
      });
      await db.insert(alertEndpointHealth).values({
      tenantId: autoPauseTenant,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: "http://localhost:9999/auto-paused",
      status: "DOWN",
      attemptsTotal: 5,
      attemptsSuccess: 0,
      successRate: 0,
      consecutiveFailures: 5,
      updatedAt: autoPauseNow,
      });

      const autoPauseAuditContext: any = {
      tenantId: autoPauseTenant,
      actorType: "API_KEY",
      actorUserId: autoPauseOwner,
      actorApiKeyId: randomUUID(),
      actorLabel: "auto-pause-key",
      requestId: randomUUID(),
      ip: null,
      userAgent: null,
      };
      await applyAutoPauseFromEndpointHealth({
      tenantId: autoPauseTenant,
      window: "1h",
      now: autoPauseNow,
      auditContext: autoPauseAuditContext,
      });
      const autoPauseRows = await db
      .select()
      .from(alertDestinationStates)
      .where(eq(alertDestinationStates.tenantId, autoPauseTenant));
      assert.strictEqual(autoPauseRows.length, 1, "auto pause row created");
      assert.strictEqual(autoPauseRows[0].state, "AUTO_PAUSED", "auto pause state applied");
      const autoPauseAudit = await db
      .select()
      .from(auditEvents)
      .where(and(eq(auditEvents.tenantId, autoPauseTenant), eq(auditEvents.action, "ALERT.ENDPOINT_AUTO_PAUSED")));
      assert(autoPauseAudit.length >= 1, "auto pause audit emitted");
      const autoPauseDeliveries = await db
      .select()
      .from(alertDeliveries)
      .where(and(
      eq(alertDeliveries.tenantId, autoPauseTenant),
      sql`${alertDeliveries.clusterId} like ${`destination-state:${autoPauseTenant}%`}`,
      ));
      assert.strictEqual(autoPauseDeliveries.length, 1, "auto pause system alert delivered");

      const resumeTenant = harness.newTenantId();
      const resumeOwner = randomUUID();
      const resumeNow = new Date("2026-02-18T10:00:00Z");
      const resumeLater = new Date("2026-02-18T10:05:00Z");
      const resumeAfterCooldown = new Date("2026-02-18T10:12:00Z");
      const originalResumeMinDown = process.env.ENDPOINT_AUTO_RESUME_MIN_DOWN_MINUTES;
      const originalResumeCooldown = process.env.ENDPOINT_AUTO_RESUME_COOLDOWN_MINUTES;
      const originalResumeP95 = process.env.ENDPOINT_AUTO_RESUME_P95_MS_WEBHOOK;
      process.env.ENDPOINT_AUTO_RESUME_MIN_DOWN_MINUTES = "0";
      process.env.ENDPOINT_AUTO_RESUME_COOLDOWN_MINUTES = "10";
      process.env.ENDPOINT_AUTO_RESUME_P95_MS_WEBHOOK = "1000";
      await db.insert(tenantUsers).values({
      tenantId: resumeTenant,
      userId: resumeOwner,
      email: "owner@resume.test",
      role: "OWNER",
      status: "ACTIVE",
      createdAt: resumeNow,
      });
      const resumeEndpoint = "http://localhost:9999/resume-endpoint";
      await db.insert(alertSubscriptions).values({
      tenantId: resumeTenant,
      userId: resumeOwner,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: resumeEndpoint,
      isEnabled: true,
      updatedAt: resumeNow,
      });
      const resumeKey = makeDestinationKey("WEBHOOK", resumeEndpoint);
      await db.insert(alertDestinationStates).values({
      tenantId: resumeTenant,
      destinationType: "WEBHOOK",
      destinationKey: resumeKey,
      state: "AUTO_PAUSED",
      reason: "auto-paused: endpoint down",
      autoPausedAt: resumeNow,
      createdAt: resumeNow,
      updatedAt: resumeNow,
      });
      await db.insert(alertEndpointHealth).values({
      tenantId: resumeTenant,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: resumeEndpoint,
      status: "OK",
      attemptsTotal: 3,
      attemptsSuccess: 3,
      successRate: 1,
      p95Ms: 200,
      consecutiveFailures: 0,
      updatedAt: resumeNow,
      });

      const resumeAuditContext: any = {
      tenantId: resumeTenant,
      actorType: "API_KEY",
      actorUserId: resumeOwner,
      actorApiKeyId: randomUUID(),
      actorLabel: "auto-resume-key",
      requestId: randomUUID(),
      ip: null,
      userAgent: null,
      };
      await applyAutoPauseFromEndpointHealth({
      tenantId: resumeTenant,
      window: "1h",
      now: resumeNow,
      auditContext: resumeAuditContext,
      });
      const [readyRow] = await db
      .select()
      .from(alertDestinationStates)
      .where(eq(alertDestinationStates.tenantId, resumeTenant));
      assert.strictEqual(readyRow?.state, "AUTO_PAUSED", "auto-resume does not flip immediately");
      assert(readyRow?.resumeReadyAt, "resume-ready timestamp set");

      await applyAutoPauseFromEndpointHealth({
      tenantId: resumeTenant,
      window: "1h",
      now: resumeLater,
      auditContext: resumeAuditContext,
      });
      const [stillPausedRow] = await db
      .select()
      .from(alertDestinationStates)
      .where(eq(alertDestinationStates.tenantId, resumeTenant));
      assert.strictEqual(stillPausedRow?.state, "AUTO_PAUSED", "cooldown prevents early auto-resume");

      await applyAutoPauseFromEndpointHealth({
      tenantId: resumeTenant,
      window: "1h",
      now: resumeAfterCooldown,
      auditContext: resumeAuditContext,
      });
      const [resumedRow] = await db
      .select()
      .from(alertDestinationStates)
      .where(eq(alertDestinationStates.tenantId, resumeTenant));
      assert.strictEqual(resumedRow?.state, "ACTIVE", "auto-resume flips state after cooldown");

      const resumeAudit = await db
      .select()
      .from(auditEvents)
      .where(and(eq(auditEvents.tenantId, resumeTenant), eq(auditEvents.action, "ALERT.ENDPOINT_AUTO_RESUMED")));
      assert.strictEqual(resumeAudit.length, 1, "auto-resume audit emitted once");

      await db.delete(alertEndpointHealth).where(eq(alertEndpointHealth.tenantId, resumeTenant));
      await db.insert(alertEndpointHealth).values({
      tenantId: resumeTenant,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: resumeEndpoint,
      status: "DOWN",
      attemptsTotal: 5,
      attemptsSuccess: 0,
      successRate: 0,
      p95Ms: 12000,
      consecutiveFailures: 5,
      updatedAt: new Date(resumeAfterCooldown.getTime() + 60 * 1000),
      });
      await applyAutoPauseFromEndpointHealth({
      tenantId: resumeTenant,
      window: "1h",
      now: new Date(resumeAfterCooldown.getTime() + 60 * 1000),
      auditContext: resumeAuditContext,
      });
      const [cooldownRow] = await db
      .select()
      .from(alertDestinationStates)
      .where(eq(alertDestinationStates.tenantId, resumeTenant));
      assert.strictEqual(cooldownRow?.state, "ACTIVE", "cooldown prevents immediate auto-pause after resume");

      const manualPauseEndpoint = "http://localhost:9999/manual-paused";
      const manualPauseKey = makeDestinationKey("WEBHOOK", manualPauseEndpoint);
      await db.insert(alertDestinationStates).values({
      tenantId: resumeTenant,
      destinationType: "WEBHOOK",
      destinationKey: manualPauseKey,
      state: "PAUSED",
      reason: "manual pause",
      pausedByUserId: resumeOwner,
      pausedAt: resumeNow,
      createdAt: resumeNow,
      updatedAt: resumeNow,
      });
      await db.insert(alertEndpointHealth).values({
      tenantId: resumeTenant,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: manualPauseEndpoint,
      status: "OK",
      attemptsTotal: 3,
      attemptsSuccess: 3,
      successRate: 1,
      p95Ms: 200,
      consecutiveFailures: 0,
      updatedAt: resumeNow,
      });
      await applyAutoPauseFromEndpointHealth({
      tenantId: resumeTenant,
      window: "1h",
      now: resumeNow,
      auditContext: resumeAuditContext,
      });
      const [manualPauseRow] = await db
      .select()
      .from(alertDestinationStates)
      .where(and(
      eq(alertDestinationStates.tenantId, resumeTenant),
      eq(alertDestinationStates.destinationKey, manualPauseKey),
      ));
      assert.strictEqual(manualPauseRow?.state, "PAUSED", "manual pause is never auto-resumed");

      const destinationsTenant = harness.newTenantId();
      const destinationsUser = randomUUID();
      const destinationsNow = new Date("2026-02-20T08:00:00Z");
      const webhookEndpoint = "http://localhost:9999/destinations-a";
      const emailEndpoint = "alerts@destinations.test";
      await db.insert(alertSubscriptions).values([
      {
      tenantId: destinationsTenant,
      userId: destinationsUser,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: webhookEndpoint,
      isEnabled: true,
      updatedAt: destinationsNow,
      },
      {
      tenantId: destinationsTenant,
      userId: destinationsUser,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      channel: "EMAIL",
      endpoint: emailEndpoint,
      isEnabled: true,
      updatedAt: new Date(destinationsNow.getTime() + 1000),
      },
      ]);
      const webhookKey = makeDestinationKey("WEBHOOK", webhookEndpoint);
      const emailKey = makeDestinationKey("EMAIL", emailEndpoint);
      await db.insert(alertDestinationStates).values([
      {
      tenantId: destinationsTenant,
      destinationType: "WEBHOOK",
      destinationKey: webhookKey,
      state: "ACTIVE",
      createdAt: destinationsNow,
      updatedAt: destinationsNow,
      },
      {
      tenantId: destinationsTenant,
      destinationType: "EMAIL",
      destinationKey: emailKey,
      state: "PAUSED",
      reason: "paused",
      createdAt: new Date(destinationsNow.getTime() + 1000),
      updatedAt: new Date(destinationsNow.getTime() + 1000),
      },
      ]);

      const listPage1 = await listDestinations({
      tenantId: destinationsTenant,
      window: "24h",
      limit: 1,
      now: new Date(destinationsNow.getTime() + 2000),
      });
      assert.strictEqual(listPage1.items.length, 1, "destinations list page1 size");
      assert(listPage1.next_cursor, "destinations list cursor set");
      const listPage2 = await listDestinations({
      tenantId: destinationsTenant,
      window: "24h",
      limit: 1,
      cursor: listPage1.next_cursor!,
      now: new Date(destinationsNow.getTime() + 2000),
      });
      assert.strictEqual(listPage2.items.length, 1, "destinations list page2 size");
      assert.notStrictEqual(listPage1.items[0].destination_key, listPage2.items[0].destination_key, "destinations paging no overlap");

      const otherTenantId = harness.newTenantId();
      const otherKey = makeDestinationKey("WEBHOOK", "http://localhost:9999/other-tenant");
      await db.insert(alertDestinationStates).values({
      tenantId: otherTenantId,
      destinationType: "WEBHOOK",
      destinationKey: otherKey,
      state: "ACTIVE",
      createdAt: destinationsNow,
      updatedAt: destinationsNow,
      });
      const listTenantIsolation = await listDestinations({
      tenantId: destinationsTenant,
      window: "24h",
      limit: 10,
      now: destinationsNow,
      });
      assert(!listTenantIsolation.items.some((item) => item.destination_key === otherKey), "destination list tenant isolation");

      const bulkResult = await bulkUpdateDestinationStates({
      tenantId: destinationsTenant,
      userId: destinationsUser,
      destinationKeys: [webhookKey, "missing-key"],
      state: "PAUSED",
      reason: "bulk pause",
      now: destinationsNow,
      });
      const bulkStatuses = new Map(bulkResult.results.map((item) => [item.destination_key, item.status]));
      assert.strictEqual(bulkStatuses.get(webhookKey), "ok", "bulk update marks ok");
      assert.strictEqual(bulkStatuses.get("missing-key"), "not_found", "bulk update marks not_found");

      await db.insert(alertEndpointHealth).values({
      tenantId: destinationsTenant,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: webhookEndpoint,
      status: "DOWN",
      attemptsTotal: 10,
      attemptsSuccess: 2,
      successRate: 0.2,
      p50Ms: 200,
      p95Ms: 900,
      consecutiveFailures: 3,
      lastSuccessAt: destinationsNow,
      lastFailureAt: new Date(destinationsNow.getTime() + 1000),
      updatedAt: new Date(destinationsNow.getTime() + 2000),
      });

      const detailSlaWindowStart = new Date("2026-02-03T11:25:00Z");
      await db.insert(alertDeliverySlaWindows).values({
      tenantId: destinationsTenant,
      destinationType: "WEBHOOK",
      destinationKey: webhookKey,
      window: "24h",
      windowStart: detailSlaWindowStart,
      attemptsTotal: 10,
      attemptsSuccess: 8,
      attemptsFailed: 2,
      latencyP50Ms: 350,
      latencyP95Ms: 5200,
      successRate: 78,
      status: "AT_RISK",
      computedAt: new Date("2026-02-04T11:25:00Z"),
      });

      const [detailRun] = await db.insert(alertRuns).values({
      tenantId: destinationsTenant,
      status: "SUCCESS",
      startedAt: destinationsNow,
      finishedAt: destinationsNow,
      }).returning();
      const [detailSub] = await db
      .select({ id: alertSubscriptions.id })
      .from(alertSubscriptions)
      .where(and(
      eq(alertSubscriptions.tenantId, destinationsTenant),
      eq(alertSubscriptions.endpoint, webhookEndpoint),
      ))
      .limit(1);
      assert(detailSub?.id, "destination detail subscription exists");
      await db.insert(alertDeliveries).values([
      {
      runId: detailRun.id,
      tenantId: destinationsTenant,
      userId: destinationsUser,
      subscriptionId: detailSub.id,
      clusterId: "DEST-DETAIL-1",
      entityType: "port",
      entityId: portA,
      day: new Date("2026-02-04"),
      destinationType: "WEBHOOK",
      endpoint: webhookEndpoint,
      status: "FAILED",
      attempts: 1,
      createdAt: new Date("2026-02-04T11:10:00Z"),
      },
      {
      runId: detailRun.id,
      tenantId: destinationsTenant,
      userId: destinationsUser,
      subscriptionId: detailSub.id,
      clusterId: "DEST-DETAIL-2",
      entityType: "port",
      entityId: portA,
      day: new Date("2026-02-04"),
      destinationType: "WEBHOOK",
      endpoint: webhookEndpoint,
      status: "SENT",
      attempts: 1,
      createdAt: new Date("2026-02-04T11:12:00Z"),
      },
      ]);

      const detail = await getDestinationDetail({
      tenantId: destinationsTenant,
      destinationKey: webhookKey,
      now: destinationsNow,
      });
      assert(detail?.destination_key === webhookKey, "destination detail returns item");
      assert.strictEqual(detail?.endpoint_health?.window, "1h", "destination detail health window");
      assert.strictEqual(detail?.sla?.window, "24h", "destination detail sla window");
      assert.strictEqual(detail?.recent_deliveries?.length, 2, "destination detail recent deliveries");

      const otherDetail = await getDestinationDetail({
      tenantId: otherTenantId,
      destinationKey: webhookKey,
      now: destinationsNow,
      });
      assert.strictEqual(otherDetail, null, "destination detail tenant isolation");

      assert.strictEqual(canTransitionDestinationState("VIEWER", "PAUSED"), false, "viewer cannot change state");
      assert.strictEqual(canTransitionDestinationState("OPERATOR", "DISABLED"), false, "operator cannot disable");
      assert.strictEqual(canTransitionDestinationState("OWNER", "DISABLED"), true, "owner can disable");

      process.env.ENDPOINT_AUTO_RESUME_MIN_DOWN_MINUTES = originalResumeMinDown;
      process.env.ENDPOINT_AUTO_RESUME_COOLDOWN_MINUTES = originalResumeCooldown;
      process.env.ENDPOINT_AUTO_RESUME_P95_MS_WEBHOOK = originalResumeP95;

      const endpointAlertTenant = harness.newTenantId();
      const endpointAlertOwner = randomUUID();
      const endpointAlertNow = new Date("2026-02-16T12:00:00Z");
      await db.insert(tenantUsers).values({
      tenantId: endpointAlertTenant,
      userId: endpointAlertOwner,
      email: "owner@endpoint.test",
      role: "OWNER",
      status: "ACTIVE",
      createdAt: endpointAlertNow,
      });
      const [endpointOwnerSub] = await db.insert(alertSubscriptions).values({
      tenantId: endpointAlertTenant,
      userId: endpointAlertOwner,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "HIGH",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9998/owner-webhook",
      isEnabled: true,
      updatedAt: endpointAlertNow,
      }).returning();
      const [endpointAlertRun] = await db.insert(alertRuns).values({
      tenantId: endpointAlertTenant,
      status: "SUCCESS",
      startedAt: endpointAlertNow,
      finishedAt: endpointAlertNow,
      }).returning();
      const [endpointSourceDelivery] = await db.insert(alertDeliveries).values({
      runId: endpointAlertRun.id,
      tenantId: endpointAlertTenant,
      userId: endpointAlertOwner,
      subscriptionId: endpointOwnerSub.id,
      clusterId: "ENDPOINT-ALERT-SOURCE",
      entityType: "port",
      entityId: portA,
      day: new Date("2026-02-16"),
      destinationType: "WEBHOOK",
      endpoint: "http://localhost:9999/failing-endpoint",
      status: "SENT",
      attempts: 1,
      createdAt: endpointAlertNow,
      }).returning();

      await db.insert(alertDeliveryAttempts).values(
      Array.from({ length: 5 }).map((_, idx) => ({
      tenantId: endpointAlertTenant,
      deliveryId: endpointSourceDelivery.id,
      attemptNo: idx + 1,
      status: "SENT",
      latencyMs: 150,
      createdAt: new Date(endpointAlertNow.getTime() - (10 - idx) * 60 * 1000),
      })),
      );
      await computeEndpointHealth({ tenantId: endpointAlertTenant, now: endpointAlertNow, window: "1h" });

      await db.delete(alertDeliveryAttempts).where(eq(alertDeliveryAttempts.tenantId, endpointAlertTenant));
      await db.insert(alertDeliveryAttempts).values(
      Array.from({ length: 5 }).map((_, idx) => ({
      tenantId: endpointAlertTenant,
      deliveryId: endpointSourceDelivery.id,
      attemptNo: idx + 1,
      status: "FAILED",
      latencyMs: 0,
      createdAt: new Date(endpointAlertNow.getTime() - idx * 60 * 1000),
      })),
      );
      await computeEndpointHealth({ tenantId: endpointAlertTenant, now: endpointAlertNow, window: "1h" });
      const endpointSystemAlerts = await db
      .select()
      .from(alertDeliveries)
      .where(and(
      eq(alertDeliveries.tenantId, endpointAlertTenant),
      sql`${alertDeliveries.clusterId} like ${`endpoint:${endpointAlertTenant}%`}`,
      ));
      assert.strictEqual(endpointSystemAlerts.length, 1, "endpoint down system alert emitted");

      await computeEndpointHealth({ tenantId: endpointAlertTenant, now: endpointAlertNow, window: "1h" });
      const endpointSystemAlertsAgain = await db
      .select()
      .from(alertDeliveries)
      .where(and(
      eq(alertDeliveries.tenantId, endpointAlertTenant),
      sql`${alertDeliveries.clusterId} like ${`endpoint:${endpointAlertTenant}%`}`,
      ));
      assert.strictEqual(endpointSystemAlertsAgain.length, 1, "endpoint system alert deduped");

      await db.delete(alertDeliveryAttempts).where(eq(alertDeliveryAttempts.tenantId, endpointAlertTenant));
      await db.insert(alertDeliveryAttempts).values(
      Array.from({ length: 5 }).map((_, idx) => ({
      tenantId: endpointAlertTenant,
      deliveryId: endpointSourceDelivery.id,
      attemptNo: idx + 1,
      status: "SENT",
      latencyMs: 110,
      createdAt: new Date(endpointAlertNow.getTime() - idx * 60 * 1000),
      })),
      );
      await computeEndpointHealth({ tenantId: endpointAlertTenant, now: endpointAlertNow, window: "1h" });
      const endpointSystemRecovered = await db
      .select()
      .from(alertDeliveries)
      .where(and(
      eq(alertDeliveries.tenantId, endpointAlertTenant),
      sql`${alertDeliveries.clusterId} like ${`endpoint:${endpointAlertTenant}%`}`,
      ));
      assert.strictEqual(endpointSystemRecovered.length, 2, "endpoint recovered system alert emitted");

      const endpointGateTenant = harness.newTenantId();
      const endpointGateUser = randomUUID();
      const endpointGateNow = new Date("2026-01-15T12:30:00Z");
      const [endpointGateSub] = await db.insert(alertSubscriptions).values({
      tenantId: endpointGateTenant,
      userId: endpointGateUser,
      entityType: "port",
      entityId: portA,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/blocked-endpoint",
      isEnabled: true,
      updatedAt: endpointGateNow,
      }).returning();
      await db.insert(alertEndpointHealth).values({
      tenantId: endpointGateTenant,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: endpointGateSub.endpoint,
      status: "DOWN",
      attemptsTotal: 5,
      attemptsSuccess: 0,
      successRate: 0,
      p95Ms: 12000,
      consecutiveFailures: 5,
      updatedAt: endpointGateNow,
      });
      await runAlerts({ day: "2026-01-15", tenantId: endpointGateTenant, userId: endpointGateUser, now: endpointGateNow });
      const endpointGateDeliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, endpointGateTenant));
      assert.strictEqual(endpointGateDeliveries.length, 1, "endpoint gate delivery created");
      assert.strictEqual(endpointGateDeliveries[0].status, "SKIPPED_ENDPOINT_DOWN", "endpoint down suppressed");
      assert.strictEqual(endpointGateDeliveries[0].skipReason, "ENDPOINT_DOWN", "skip reason set");
      assert.strictEqual((endpointGateDeliveries[0] as any).decision?.gates?.endpoint_health?.status, "DOWN");
      assert.strictEqual((endpointGateDeliveries[0] as any).decision?.gates?.endpoint_health?.allowed, false);

      const endpointGateAttempts = await db
      .select()
      .from(alertDeliveryAttempts)
      .where(eq(alertDeliveryAttempts.tenantId, endpointGateTenant));
      assert.strictEqual(endpointGateAttempts.length, 0, "no attempts for endpoint-down suppression");
      const endpointGateDlq = await db
      .select()
      .from(alertDlq)
      .where(eq(alertDlq.tenantId, endpointGateTenant));
      assert.strictEqual(endpointGateDlq.length, 0, "no dlq rows for endpoint-down suppression");


  });
});

