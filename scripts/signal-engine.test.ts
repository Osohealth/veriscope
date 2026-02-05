import { strict as assert } from "node:assert";
import { randomUUID, createHmac } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../server/db";
import { evaluatePortSignalsForDay, listSignals } from "../server/services/signalEngine";
import { buildSignalClusterAlertPayload } from "../server/services/signalAlertService";
import { alertDedupe, alertDeliveries, alertDeliveryAttempts, alertDlq, alertRuns, alertSubscriptions, apiKeys, portDailyBaselines, ports, signals } from "@shared/schema";
import { METRIC_LABELS } from "@shared/metrics";
import { buildSignalResponse } from "../server/services/signalResponse";
import { validateAlertSubscriptionInput } from "../server/services/alertSubscriptionService";
import { markAlertSent, shouldSendAlert } from "../server/services/alertDedupe";
import { getAlertCandidates } from "../server/services/alertQuery";
import { buildWebhookPayload, buildWebhookRequest, computeIdempotencyKey, sendWebhook, WebhookSendError } from "../server/services/webhookSender";
import { renderAlertEmail } from "../server/services/emailSender";
import { runAlerts } from "../server/services/alertDispatcher";
import { getDeliveryHealthByDay, getDeliveryLatency, getEndpointHealth, getDlqHealth, getDlqOverdue } from "../server/services/alertMetrics";
import { listAlertDeliveries } from "../server/services/alertDeliveries";
import { hashApiKey } from "../server/services/apiKeyService";
import { authenticateApiKey } from "../server/middleware/apiKeyAuth";

const buildPort = (id: string, code: string) => ({
  id,
  name: `Test Port ${code}`,
  code,
  unlocode: code,
  country: "Testland",
  countryCode: "TT",
  region: "Test",
  latitude: "0",
  longitude: "0",
  timezone: "UTC",
  type: "container_port",
});

async function main() {
  process.env.API_KEY_PEPPER = "test-pepper";
  delete process.env.ALERTS_API_KEY;
  delete process.env.ALERTS_USER_ID;
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
  const tenantA = randomUUID();
  const tenantB = randomUUID();

  const portIds = [portA, portB, portC, portD, portE, portF];
  const historyDays = 10;

  try {
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "signals" (
        "id" varchar PRIMARY KEY DEFAULT gen_random_uuid(),
        "signal_type" text NOT NULL,
        "entity_type" text NOT NULL,
        "entity_id" varchar NOT NULL,
        "day" date NOT NULL,
        "severity" text NOT NULL,
        "value" double precision NOT NULL,
        "baseline" double precision,
        "stddev" double precision,
        "zscore" double precision,
        "delta_pct" double precision,
        "confidence_score" double precision,
        "confidence_band" text,
        "method" text,
        "cluster_id" text,
        "cluster_type" text,
        "cluster_summary" text,
        "explanation" text NOT NULL,
        "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
        "created_at" timestamptz NOT NULL DEFAULT now()
      );
    `);

    await db.execute(sql`
      CREATE UNIQUE INDEX IF NOT EXISTS "signals_unique"
        ON "signals" ("signal_type", "entity_type", "entity_id", "day");
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "alert_subscriptions" (
        "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "user_id" uuid NOT NULL,
        "scope" text NOT NULL DEFAULT 'PORT',
        "entity_type" text NOT NULL DEFAULT 'port',
        "entity_id" uuid NOT NULL,
        "severity_min" text NOT NULL DEFAULT 'HIGH',
        "confidence_min" text,
        "channel" text NOT NULL DEFAULT 'WEBHOOK',
        "endpoint" text NOT NULL,
        "secret" text,
        "signature_version" text NOT NULL DEFAULT 'v1',
        "is_enabled" boolean NOT NULL DEFAULT true,
        "last_test_at" timestamptz,
        "last_test_status" text,
        "last_test_error" text,
        "created_at" timestamptz NOT NULL DEFAULT now(),
        "updated_at" timestamptz NOT NULL DEFAULT now()
      );
    `);
    await db.execute(sql`
      ALTER TABLE "alert_subscriptions"
        ADD COLUMN IF NOT EXISTS "secret" text;
    `);
    await db.execute(sql`
      ALTER TABLE "alert_subscriptions"
        ADD COLUMN IF NOT EXISTS "signature_version" text NOT NULL DEFAULT 'v1';
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "alert_dedupe" (
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "cluster_id" text NOT NULL,
        "channel" text NOT NULL,
        "endpoint" text NOT NULL,
        "last_sent_at" timestamptz NOT NULL,
        "ttl_hours" int NOT NULL DEFAULT 24
      );
    `);
    await db.execute(sql`
      CREATE UNIQUE INDEX IF NOT EXISTS "alert_dedupe_unique"
        ON "alert_dedupe" ("tenant_id", "cluster_id", "channel", "endpoint");
    `);

    await db.insert(ports).values([
      buildPort(portA, `TSA${portA.slice(0, 6).toUpperCase()}`),
      buildPort(portB, `TSB${portB.slice(0, 6).toUpperCase()}`),
      buildPort(portC, `TSC${portC.slice(0, 6).toUpperCase()}`),
      buildPort(portD, `TSD${portD.slice(0, 6).toUpperCase()}`),
      buildPort(portE, `TSE${portE.slice(0, 6).toUpperCase()}`),
      buildPort(portF, `TSF${portF.slice(0, 6).toUpperCase()}`),
    ]);

    const historyRows = portIds.flatMap((portId) => {
      const rows = [];
      for (let offset = 1; offset <= historyDays; offset += 1) {
        rows.push({
          portId,
          day: new Date(Date.UTC(2026, 0, 15 - offset)),
          arrivals: 5,
          departures: 5,
          uniqueVessels: 5,
          avgDwellHours: 8,
          openCalls: 5,
          arrivals30dAvg: 5,
          arrivals30dStd: 1,
          dwell30dAvg: 8,
          dwell30dStd: 1,
          openCalls30dAvg: 5,
        });
      }
      return rows;
    });

    await db.insert(portDailyBaselines).values([
      ...historyRows,
      {
        portId: portA,
        day: testDay,
        arrivals: 25,
        departures: 20,
        uniqueVessels: 22,
        avgDwellHours: 10,
        openCalls: 2,
        arrivals30dAvg: 15,
        arrivals30dStd: 5,
        dwell30dAvg: 10,
        dwell30dStd: 4,
        openCalls30dAvg: 2,
      },
      {
        portId: portB,
        day: testDay,
        arrivals: 6,
        departures: 5,
        uniqueVessels: 6,
        avgDwellHours: 20,
        openCalls: 3,
        arrivals30dAvg: 6,
        arrivals30dStd: 4,
        dwell30dAvg: 8,
        dwell30dStd: 4,
        openCalls30dAvg: 3,
      },
      {
        portId: portC,
        day: testDay,
        arrivals: 5,
        departures: 5,
        uniqueVessels: 5,
        avgDwellHours: 7,
        openCalls: 18,
        arrivals30dAvg: 5,
        arrivals30dStd: 2,
        dwell30dAvg: 7,
        dwell30dStd: 2,
        openCalls30dAvg: 8,
      },
      {
        portId: portD,
        day: testDay,
        arrivals: 5,
        departures: 5,
        uniqueVessels: 5,
        avgDwellHours: 7,
        openCalls: 10,
        arrivals30dAvg: 5,
        arrivals30dStd: 2,
        dwell30dAvg: 7,
        dwell30dStd: 2,
        openCalls30dAvg: 4,
      },
      {
        portId: portE,
        day: testDay,
        arrivals: 4,
        departures: 4,
        uniqueVessels: 4,
        avgDwellHours: 4,
        openCalls: 2,
        arrivals30dAvg: 4,
        arrivals30dStd: 2,
        dwell30dAvg: 10,
        dwell30dStd: 3,
        openCalls30dAvg: 2,
      },
      {
        portId: portF,
        day: testDay,
        arrivals: 130,
        departures: 100,
        uniqueVessels: 100,
        avgDwellHours: 6,
        openCalls: 6,
        arrivals30dAvg: 100,
        arrivals30dStd: 5,
        dwell30dAvg: 6,
        dwell30dStd: 1,
        openCalls30dAvg: 5,
      },
    ]);

    const firstRun = await evaluatePortSignalsForDay(testDay, { portIds });
    assert.strictEqual(firstRun.upserted, 4, "expected four signals to be upserted");

    const signalRows = await db
      .select()
      .from(signals)
      .where(and(eq(signals.day, testDay), inArray(signals.entityId, portIds)));

    assert.strictEqual(signalRows.length, 4, "signals created for anomaly, dwell, congestion, critical anomaly");

    const signalTypes = new Set(signalRows.map((row) => row.signalType));
    assert(signalTypes.has("PORT_ARRIVALS_ANOMALY"));
    assert(signalTypes.has("PORT_DWELL_SPIKE"));
    assert(signalTypes.has("PORT_CONGESTION_BUILDUP"));

    const arrivalsSignal = signalRows.find((row) => row.signalType === "PORT_ARRIVALS_ANOMALY" && row.entityId === portA);
    assert(arrivalsSignal?.severity === "MEDIUM", "arrival anomaly severity mapped to MEDIUM");
    assert(arrivalsSignal?.explanation, "arrival anomaly explanation populated");
    assert(arrivalsSignal?.confidenceBand, "arrival anomaly confidence band set");
    assert(arrivalsSignal?.confidenceScore !== null, "arrival anomaly confidence score set");
    assert(arrivalsSignal?.method, "arrival anomaly method set");
    assert(arrivalsSignal?.clusterId, "arrival anomaly cluster id set");
    assert(arrivalsSignal?.clusterKey, "arrival anomaly cluster key set");
    assert(arrivalsSignal?.clusterType === "PORT_DISRUPTION", "arrival anomaly cluster type set");
    assert(arrivalsSignal?.clusterSeverity, "arrival anomaly cluster severity set");
    assert(arrivalsSignal?.clusterSummary, "arrival anomaly cluster summary set");

    const dwellSignal = signalRows.find((row) => row.signalType === "PORT_DWELL_SPIKE");
    assert(dwellSignal?.severity === "HIGH", "dwell spike severity mapped to HIGH");
    assert(dwellSignal?.confidenceBand, "dwell spike confidence band set");
    assert(dwellSignal?.confidenceScore !== null, "dwell spike confidence score set");
    assert(dwellSignal?.method, "dwell spike method set");
    assert(dwellSignal?.clusterId, "dwell spike cluster id set");
    assert(dwellSignal?.clusterKey, "dwell spike cluster key set");
    assert(dwellSignal?.clusterType === "PORT_DISRUPTION", "dwell spike cluster type set");
    assert(dwellSignal?.clusterSeverity, "dwell spike cluster severity set");
    assert(dwellSignal?.clusterSummary, "dwell spike cluster summary set");

    const congestionSignal = signalRows.find((row) => row.signalType === "PORT_CONGESTION_BUILDUP");
    assert(congestionSignal?.severity === "HIGH", "congestion severity mapped to HIGH");
    assert(congestionSignal?.confidenceBand, "congestion confidence band set");
    assert(congestionSignal?.confidenceScore !== null, "congestion confidence score set");
    assert(congestionSignal?.method, "congestion method set");
    assert(congestionSignal?.clusterId, "congestion cluster id set");
    assert(congestionSignal?.clusterKey, "congestion cluster key set");
    assert(congestionSignal?.clusterType === "PORT_DISRUPTION", "congestion cluster type set");
    assert(congestionSignal?.clusterSeverity, "congestion cluster severity set");
    assert(congestionSignal?.clusterSummary, "congestion cluster summary set");

    const criticalSignal = signalRows.find((row) => row.entityId === portF && row.signalType === "PORT_ARRIVALS_ANOMALY");
    assert(criticalSignal?.severity === "CRITICAL", "critical anomaly severity mapped to CRITICAL");
    assert(criticalSignal?.confidenceBand, "critical anomaly confidence band set");
    assert(criticalSignal?.confidenceScore !== null, "critical anomaly confidence score set");
    assert(criticalSignal?.method, "critical anomaly method set");
    assert(criticalSignal?.clusterId, "critical anomaly cluster id set");

    const beforeCountResult = await db
      .select({ count: sql<number>`count(*)` })
      .from(signals)
      .where(and(eq(signals.day, testDay), inArray(signals.entityId, portIds)));
    const beforeCount = Number(beforeCountResult[0]?.count ?? 0);

    await evaluatePortSignalsForDay(testDay, { portIds });

    const afterCountResult = await db
      .select({ count: sql<number>`count(*)` })
      .from(signals)
      .where(and(eq(signals.day, testDay), inArray(signals.entityId, portIds)));
    const afterCount = Number(afterCountResult[0]?.count ?? 0);

    assert.strictEqual(afterCount, beforeCount, "signal engine is idempotent");

    const listByPort = await listSignals({ portId: portA, limit: 10, offset: 0 });
    assert.strictEqual(listByPort.total, 1, "filter by port returns one signal");
    assert.strictEqual(listByPort.items.length, 1);

    const listBySeverity = await listSignals({ severity: "HIGH", limit: 10, offset: 0 });
    assert(listBySeverity.items.length >= 2, "severity filter returns high signals");

    const alertPayload = buildSignalClusterAlertPayload({
      day: testDay,
      entityType: "port",
      entityId: portA,
      clusterId: arrivalsSignal?.clusterId ?? null,
      clusterSeverity: arrivalsSignal?.clusterSeverity ?? null,
      confidenceScore: arrivalsSignal?.confidenceScore ?? null,
      confidenceBand: arrivalsSignal?.confidenceBand ?? null,
      clusterSummary: arrivalsSignal?.clusterSummary ?? null,
      metadata: arrivalsSignal?.metadata ?? {},
    });

    assert(alertPayload.event_type === "VERISCOPE_SIGNAL_CLUSTER");
    assert(alertPayload.cluster_id !== undefined);
    assert(alertPayload.cluster_severity !== undefined);
    assert(Array.isArray(alertPayload.top_drivers));
    assert(alertPayload.top_drivers[0]?.metric, "alert payload includes top driver metric");
    assert(alertPayload.top_drivers[0]?.value !== undefined, "alert payload includes top driver value");

    assert(METRIC_LABELS.arrivals, "metric label map includes arrivals");
    assert(METRIC_LABELS.departures, "metric label map includes departures");
    assert(METRIC_LABELS.unique_vessels, "metric label map includes unique_vessels");
    assert(METRIC_LABELS.avg_dwell_hours, "metric label map includes avg_dwell_hours");
    assert(METRIC_LABELS.open_calls, "metric label map includes open_calls");

    await db.insert(signals).values({
      signalType: "PORT_ARRIVALS_ANOMALY",
      entityType: "port",
      entityId: missingPort,
      day: testDay,
      severity: "HIGH",
      value: 20,
      baseline: 10,
      stddev: 2,
      zscore: 5,
      deltaPct: 100,
      confidenceScore: 0.9,
      confidenceBand: "HIGH",
      method: "zscore_30d",
      clusterId: `PORT_DISRUPTION:${missingPort}:${testDay.toISOString().slice(0, 10)}`,
      clusterKey: `PORT_DISRUPTION|${missingPort}|${testDay.toISOString().slice(0, 10)}`,
      clusterType: "PORT_DISRUPTION",
      clusterSeverity: "HIGH",
      clusterSummary: "Arrivals +100.0%",
      explanation: "Synthetic missing port signal.",
      metadata: {
        day: testDay.toISOString().slice(0, 10),
        metric: "arrivals",
        baseline_window: "30d",
        min_history_days: 10,
        data_quality: {
          history_days_used: 10,
          completeness_pct: 33,
          missing_points: 20,
        },
      },
    });

    const portRows = await db
      .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
      .from(ports)
      .where(inArray(ports.id, [portA]));
    const entityMap = new Map(portRows.map((row) => [row.id, { ...row, type: "port" as const }]));

    const signalForPort = signalRows.find((row) => row.entityId === portA)!;
    const withEntity = buildSignalResponse(signalForPort, {
      compat: false,
      includeEntity: true,
      entityMap,
    });
    assert(withEntity.entity, "include_entity includes embedded entity");
    assert.strictEqual(withEntity.entity.id, portA, "embedded entity id matches");

    const withoutEntity = buildSignalResponse(signalForPort, {
      compat: false,
      includeEntity: false,
      entityMap,
    });
    assert(!("entity" in withoutEntity), "default response omits entity");

    const missingSignal = await db
      .select()
      .from(signals)
      .where(eq(signals.entityId, missingPort))
      .limit(1);
    const missingEntityResponse = buildSignalResponse(missingSignal[0], {
      compat: false,
      includeEntity: true,
      entityMap,
    });
    assert(!("entity" in missingEntityResponse), "missing entity omitted");

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

    const candidates = await getAlertCandidates({ day: "2026-01-20", severityMin: "HIGH" });
    assert.strictEqual(candidates.length, 2, "returns one per cluster");
    assert.strictEqual(candidates[0].cluster_id, "CLUSTER-B", "higher severity ranks first");
    assert.strictEqual(candidates[1].cluster_id, "CLUSTER-A");

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

    const originalFetch = globalThis.fetch;
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

    globalThis.fetch = originalFetch;

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "alert_runs" (
        "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "day" date,
        "started_at" timestamptz NOT NULL DEFAULT now(),
        "finished_at" timestamptz,
        "status" text NOT NULL,
        "summary" jsonb,
        "error" jsonb
      );
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "alert_deliveries" (
        "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        "run_id" uuid NOT NULL REFERENCES "alert_runs" ("id"),
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "user_id" uuid NOT NULL,
        "subscription_id" uuid NOT NULL REFERENCES "alert_subscriptions" ("id"),
        "cluster_id" text NOT NULL,
        "entity_type" text NOT NULL,
        "entity_id" text NOT NULL,
        "day" date NOT NULL,
        "destination_type" text NOT NULL,
        "endpoint" text NOT NULL,
        "status" text NOT NULL,
        "attempts" int NOT NULL DEFAULT 0,
        "last_http_status" int,
        "latency_ms" int,
        "error" text,
        "sent_at" timestamptz,
        "is_test" boolean NOT NULL DEFAULT false,
        "created_at" timestamptz NOT NULL DEFAULT now()
      );
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "alert_dlq" (
        "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "delivery_id" uuid NOT NULL REFERENCES "alert_deliveries" ("id") ON DELETE CASCADE,
        "next_attempt_at" timestamptz NOT NULL,
        "attempt_count" int NOT NULL DEFAULT 0,
        "max_attempts" int NOT NULL DEFAULT 10,
        "last_error" text,
        "created_at" timestamptz NOT NULL DEFAULT now()
      );
    `);
    await db.execute(sql`
      CREATE UNIQUE INDEX IF NOT EXISTS "alert_dlq_unique"
        ON "alert_dlq" ("delivery_id");
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "alert_delivery_attempts" (
        "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "delivery_id" uuid NOT NULL REFERENCES "alert_deliveries" ("id") ON DELETE CASCADE,
        "attempt_no" int NOT NULL,
        "status" text NOT NULL,
        "latency_ms" int,
        "http_status" int,
        "error" text,
        "sent_at" timestamptz,
        "created_at" timestamptz NOT NULL DEFAULT now()
      );
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "api_keys" (
        "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
        "user_id" uuid NOT NULL,
        "key_hash" text NOT NULL UNIQUE,
        "name" text,
        "created_at" timestamptz NOT NULL DEFAULT now(),
        "revoked_at" timestamptz
      );
    `);

    await db.insert(alertSubscriptions).values({
      tenantId: tenantA,
      userId: dispatcherSubUserId,
      entityType: "port",
      entityId: portA,
      severityMin: "HIGH",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/webhook",
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
    assert.strictEqual(dispatchRun2.summary.skipped_dedupe_total, 1, "second run dedupes");

    globalThis.fetch = originalFetch;

    globalThis.fetch = async () => {
      return { ok: false, status: 500 } as any;
    };
    const failedRun = await runAlerts({ day: "2026-01-21", tenantId: tenantA, userId: dispatcherSubUserId, now: new Date("2026-01-21T12:30:00Z") });
    assert.strictEqual(failedRun.status, "FAILED", "failed run sets status FAILED");
    assert(failedRun.summary.failed_total >= 1, "failed run increments failed_total");
    globalThis.fetch = originalFetch;

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

    const apiUserA = randomUUID();
    const apiUserB = randomUUID();
    const apiKeyRaw = "test_api_key_1";
    const prevAlertsKey = process.env.ALERTS_API_KEY;
    const prevAlertsUser = process.env.ALERTS_USER_ID;
    process.env.ALERTS_API_KEY = "";
    process.env.ALERTS_USER_ID = "";
    await db.insert(apiKeys).values({
      tenantId: tenantA,
      userId: apiUserA,
      keyHash: hashApiKey(apiKeyRaw),
      name: "test-key",
    });

    let statusCode = 0;
    let jsonBody: any = null;
    let nextCalled = false;
    const resMock = {
      status(code: number) {
        statusCode = code;
        return this;
      },
      json(body: any) {
        jsonBody = body;
        return this;
      },
    } as any;

    await authenticateApiKey({ headers: {} } as any, resMock, () => {
      nextCalled = true;
    });
    assert.strictEqual(statusCode, 401, "missing api key returns 401");
    assert(!nextCalled, "missing api key does not call next");

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
    process.env.ALERTS_API_KEY = prevAlertsKey ?? "";
    process.env.ALERTS_USER_ID = prevAlertsUser ?? "";

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
      subscriptionIds: [subA.id],
      limit: 2,
    });
    assert.strictEqual(page1.items.length, 2, "page1 returns 2");
    const cursorCreatedAt = page1.items[page1.items.length - 1].createdAt as Date;
    const cursorId = page1.items[page1.items.length - 1].id;

    const page2 = await listAlertDeliveries({
      tenantId: tenantA,
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

    console.log("PASS: signal engine tests");
  } finally {
    try {
      await db.delete(signals).where(inArray(signals.entityId, [...portIds, missingPort]));
      await db.delete(portDailyBaselines).where(inArray(portDailyBaselines.portId, portIds));
      await db.delete(ports).where(inArray(ports.id, portIds));
      await db.delete(alertDlq).where(sql`1=1`);
      await db.delete(alertDeliveryAttempts).where(sql`1=1`);
      await db.delete(alertDeliveries).where(sql`1=1`);
      await db.delete(alertSubscriptions).where(eq(alertSubscriptions.userId, subUserId));
      await db.delete(alertDedupe).where(eq(alertDedupe.clusterId, "CLUSTER-TEST-1"));
      await db.delete(alertSubscriptions).where(eq(alertSubscriptions.userId, dispatcherSubUserId));
      await db.delete(apiKeys).where(sql`1=1`);
    } catch (cleanupError) {
      console.warn("Cleanup warning:", (cleanupError as Error).message);
    }
  }
}

main().catch((error) => {
  console.error("Signal engine tests failed:", error);
  process.exit(1);
});
