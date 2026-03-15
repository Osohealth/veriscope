import test from "node:test";
import { strict as assert } from "node:assert";
import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { db } from "../../server/db";
import { alertDeliveries, alertDestinationStates, alertEndpointHealth, alertSubscriptions, signals } from "@shared/schema";
import { validateAlertSubscriptionInput } from "../../server/services/alertSubscriptionService";
import { markAlertSent, shouldSendAlert } from "../../server/services/alertDedupe";
import { getAlertCandidates } from "../../server/services/alertQuery";
import { buildWebhookPayload, buildWebhookRequest, computeIdempotencyKey } from "../../server/services/webhookSender";
import { runAlerts } from "../../server/services/alertDispatcher";
import { makeDestinationKey } from "../../server/services/destinationKey";
import { GLOBAL_SCOPE_ENTITY_ID } from "../../server/services/alertScope";
import { createTestHarness } from "../test/harness";

const timingEnabled = process.env.TEST_TIMING === "1";
const phaseTimer = () => {
  let last = timingEnabled ? process.hrtime.bigint() : null;
  return (label: string) => {
    if (!last) return;
    const now = process.hrtime.bigint();
    const ms = Number(now - last) / 1_000_000;
    console.log(`TEST_TIMING: alerts-core ${label} ${ms.toFixed(1)}ms`);
    last = now;
  };
};

test.describe("alerts (core)", () => {
  const harness = createTestHarness({ label: "alerts-core", cleanup: "tenant", schema: "alerts" });

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

  test("subscription validation + dedupe", async () => {
    const phase = phaseTimer();

    const invalidSeverity = validateAlertSubscriptionInput({
      user_id: randomUUID(),
      entity_id: randomUUID(),
      endpoint: "http://localhost:9999/webhook",
      severity_min: "SEVERE",
    }, true);
    assert(!invalidSeverity.ok, "invalid severity fails validation");

    const dedupeKey = {
      tenantId: harness.newTenantId(),
      clusterId: "CLUSTER-TEST-1",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/webhook",
    };
    const now = new Date("2026-02-01T00:00:00Z");
    const firstCheck = await shouldSendAlert({ ...dedupeKey, now });
    assert(firstCheck, "dedupe allows first send");
    await markAlertSent({ ...dedupeKey, now, ttlHours: 24 });
    const secondCheck = await shouldSendAlert({ ...dedupeKey, now: new Date("2026-02-01T10:00:00Z") });
    assert(!secondCheck, "dedupe blocks within ttl");
    const thirdCheck = await shouldSendAlert({ ...dedupeKey, now: new Date("2026-02-02T02:00:00Z") });
    assert(thirdCheck, "dedupe allows after ttl");

    const payload = buildWebhookPayload({
      day: "2026-01-20",
      entityType: "port",
      entityId: "port-a",
      clusterId: "CLUSTER-A",
      clusterSeverity: "HIGH",
      confidenceScore: 0.9,
      confidenceBand: "HIGH",
      clusterSummary: "A",
      metadata: { drivers: [{ metric: "arrivals", value: 1 }] },
    }, { sentAt: new Date("2026-01-20T12:00:00Z"), version: "1.1" });
    assert.strictEqual(payload.payload_version, "1.1");
    assert.strictEqual(payload.sent_at, "2026-01-20T12:00:00.000Z");

    const keyA = computeIdempotencyKey("sub-1", "CLUSTER-A", "2026-01-20");
    const keyB = computeIdempotencyKey("sub-2", "CLUSTER-A", "2026-01-20");
    assert(keyA !== keyB, "idempotency key differs per subscription");

    const request = buildWebhookRequest({
      payload,
      secret: "test_secret",
      subscriptionId: "sub-1",
      clusterId: "CLUSTER-A",
      day: "2026-01-20",
      now: new Date("2026-01-20T12:00:00Z"),
    });
    assert(request.headers["X-Veriscope-Timestamp"], "timestamp header set");
    assert(request.headers["X-Veriscope-Signature"], "signature header set");

    phase("done");
  });

  test("runAlerts dispatch + dedupe", async () => {
    const phase = phaseTimer();

    const tenantId = harness.newTenantId();
    const userId = randomUUID();
    const now = new Date("2026-01-20T12:00:00Z");

    harness.registerTenant(tenantId);

    await db.insert(alertSubscriptions).values({
      tenantId,
      userId,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "HIGH",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/webhook",
      isEnabled: true,
      updatedAt: now,
    });

    await db.insert(signals).values({
      signalType: "PORT_DWELL_SPIKE",
      entityType: "port",
      entityId: "port-a",
      day: new Date(Date.UTC(2026, 0, 20)),
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
      createdAt: now,
    });

    const candidates = await getAlertCandidates({ day: "2026-01-20", severityMin: "HIGH" });
    assert.strictEqual(candidates.length, 1, "candidate selection returns one cluster");

    const stubFetchOk = async () => ({ ok: true, status: 200 }) as any;
    globalThis.fetch = stubFetchOk;

    const run1 = await runAlerts({ day: "2026-01-20", tenantId, userId, now });
    assert.strictEqual(run1.summary.sent_total, 1, "first run sends");

    const run2 = await runAlerts({ day: "2026-01-20", tenantId, userId, now: new Date("2026-01-20T12:10:00Z") });
    assert.strictEqual(run2.summary.sent_total, 0, "second run dedupes");

    const deliveries = await db.select().from(alertDeliveries).where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveries.length, 1, "single delivery created");

    phase("done");
  });

  test("destination + endpoint gates", async () => {
    const phase = phaseTimer();

    const tenantId = harness.newTenantId();
    const userId = randomUUID();
    const now = new Date("2026-01-15T12:30:00Z");

    harness.registerTenant(tenantId);

    const endpoint = "http://localhost:9999/blocked-endpoint";
    const destinationKey = makeDestinationKey("WEBHOOK", endpoint);

    await db.insert(alertSubscriptions).values({
      tenantId,
      userId,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint,
      isEnabled: true,
      updatedAt: now,
    });

    await db.insert(signals).values({
      signalType: "PORT_DWELL_SPIKE",
      entityType: "port",
      entityId: "port-a",
      day: new Date(Date.UTC(2026, 0, 15)),
      severity: "LOW",
      value: 10,
      baseline: 6,
      stddev: 1,
      zscore: 4,
      deltaPct: 60,
      confidenceScore: 0.9,
      confidenceBand: "HIGH",
      method: "zscore_30d",
      clusterId: "CLUSTER-ENDPOINT",
      clusterKey: "CLUSTER-ENDPOINT",
      clusterType: "PORT_DISRUPTION",
      clusterSeverity: "LOW",
      clusterSummary: "Endpoint",
      explanation: "Endpoint",
      metadata: { day: "2026-01-15" },
      createdAt: now,
    });

    await db.insert(alertDestinationStates).values({
      tenantId,
      destinationType: "WEBHOOK",
      destinationKey,
      state: "PAUSED",
      reason: "manual pause",
      createdAt: now,
      updatedAt: now,
    });

    globalThis.fetch = async () => ({ ok: true, status: 200 }) as any;

    await runAlerts({ day: "2026-01-15", tenantId, userId, now });
    const pausedDelivery = await db.select().from(alertDeliveries).where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(pausedDelivery.length, 1, "delivery created for paused destination");
    assert.strictEqual(pausedDelivery[0].status, "SKIPPED_DESTINATION_PAUSED", "paused gate enforced");

    await db.delete(alertDeliveries).where(eq(alertDeliveries.tenantId, tenantId));
    await db.delete(alertDestinationStates).where(and(
      eq(alertDestinationStates.tenantId, tenantId),
      eq(alertDestinationStates.destinationKey, destinationKey),
    ));

    await db.insert(alertEndpointHealth).values({
      tenantId,
      window: "1h",
      destinationType: "WEBHOOK",
      destination: endpoint,
      status: "DOWN",
      attemptsTotal: 5,
      attemptsSuccess: 0,
      successRate: 0,
      p95Ms: 12000,
      consecutiveFailures: 5,
      updatedAt: now,
    });

    await runAlerts({ day: "2026-01-15", tenantId, userId, now });
    const endpointDelivery = await db.select().from(alertDeliveries).where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(endpointDelivery.length, 1, "delivery created for endpoint down");
    assert.strictEqual(endpointDelivery[0].status, "SKIPPED_ENDPOINT_DOWN", "endpoint gate enforced");

    phase("done");
  });
});
