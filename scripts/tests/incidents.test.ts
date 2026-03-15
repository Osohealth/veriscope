import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { and, eq } from "drizzle-orm";
import { db } from "../../server/db";
import { auditEvents, incidents } from "@shared/schema";
import { getIncidentMetricsV1 } from "../../server/services/incidentMetrics";
import { autoAckIncidents, autoResolveIncidents, runIncidentAutomation } from "../../server/services/incidentAutomationService";
import { createTestHarness } from "../test/harness";

test.describe("incidents", () => {
  const harness = createTestHarness({ label: "incidents", cleanup: "tenant" });

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

  test("incidents core", async () => {
    const metricsTenant = harness.newTenantId();
      const metricsNow = new Date("2026-02-14T00:00:00Z");
      const metricsOpened = new Date(metricsNow.getTime() - 5 * 60 * 60 * 1000);
      const metricsAck2m = new Date(metricsOpened.getTime() + 2 * 60 * 1000);
      const metricsAck5m = new Date(metricsOpened.getTime() + 5 * 60 * 1000);
      const metricsAck10m = new Date(metricsOpened.getTime() + 10 * 60 * 1000);
      const metricsRes30m = new Date(metricsOpened.getTime() + 30 * 60 * 1000);
      const metricsRes60m = new Date(metricsOpened.getTime() + 60 * 60 * 1000);

      await db.insert(incidents).values([
        {
          tenantId: metricsTenant,
          type: "SLA_AT_RISK",
          destinationKey: "metrics-dest-1",
          status: "RESOLVED",
          severity: "HIGH",
          title: "Resolved A",
          summary: "Resolved A",
          openedAt: metricsOpened,
          ackedAt: metricsAck2m,
          resolvedAt: metricsRes30m,
          openedByActorType: "SYSTEM",
          resolvedByActorType: "SYSTEM",
        },
        {
          tenantId: metricsTenant,
          type: "ENDPOINT_DOWN",
          destinationKey: "metrics-dest-2",
          status: "RESOLVED",
          severity: "HIGH",
          title: "Resolved B",
          summary: "Resolved B",
          openedAt: metricsOpened,
          ackedAt: metricsAck10m,
          resolvedAt: metricsRes60m,
          openedByActorType: "SYSTEM",
          resolvedByActorType: "SYSTEM",
        },
        {
          tenantId: metricsTenant,
          type: "SLA_AT_RISK",
          destinationKey: "metrics-dest-3",
          status: "ACKED",
          severity: "MEDIUM",
          title: "Acked only",
          summary: "Acked only",
          openedAt: metricsOpened,
          ackedAt: metricsAck5m,
          openedByActorType: "SYSTEM",
          ackedByActorType: "SYSTEM",
        },
        {
          tenantId: metricsTenant,
          type: "SLA_AT_RISK",
          destinationKey: "metrics-dest-4",
          status: "OPEN",
          severity: "HIGH",
          title: "Open only",
          summary: "Open only",
          openedAt: metricsOpened,
          openedByActorType: "SYSTEM",
        },
      ]);

      const realMetricsNow = Date.now;
      Date.now = () => metricsNow.getTime();
      const metrics = await getIncidentMetricsV1({ tenantId: metricsTenant, days: 30 });
      Date.now = realMetricsNow;

      assert.strictEqual(metrics.open_count, 1, "metrics open_count counts open incidents");
      assert.strictEqual(metrics.mtta_ms.n, 3, "metrics mtta counts acked incidents");
      assert.strictEqual(metrics.mttr_ms.n, 2, "metrics mttr counts resolved incidents");
      assert.strictEqual(metrics.mtta_ms.p50, 5 * 60 * 1000, "metrics mtta p50");
      assert(metrics.mtta_ms.p95 !== null && metrics.mtta_ms.p95 > 9 * 60 * 1000, "metrics mtta p95 range");
      assert.strictEqual(metrics.mttr_ms.p50, 45 * 60 * 1000, "metrics mttr p50");
      assert(metrics.mttr_ms.p95 !== null && metrics.mttr_ms.p95 > 58 * 60 * 1000 && metrics.mttr_ms.p95 < 60 * 60 * 1000, "metrics mttr p95 range");

      const metricsTenantB = harness.newTenantId();
      await db.insert(incidents).values({
        tenantId: metricsTenantB,
        type: "SLA_AT_RISK",
        destinationKey: "metrics-b",
        status: "RESOLVED",
        severity: "HIGH",
        title: "Tenant B",
        summary: "Tenant B",
        openedAt: metricsOpened,
        ackedAt: metricsAck2m,
        resolvedAt: metricsRes30m,
        openedByActorType: "SYSTEM",
        resolvedByActorType: "SYSTEM",
      });
      Date.now = () => metricsNow.getTime();
      const metricsAgain = await getIncidentMetricsV1({ tenantId: metricsTenant, days: 30 });
      Date.now = realMetricsNow;
      assert.strictEqual(metricsAgain.mttr_ms.n, 2, "tenant isolation for metrics");

      const autoTenant = harness.newTenantId();
      const autoNow = new Date("2026-02-16T12:00:00Z");
      const autoOld = new Date(autoNow.getTime() - 40 * 60 * 1000);
      const autoRecent = new Date(autoNow.getTime() - 10 * 60 * 1000);
      const autoDestinationKey = "auto-dest-1";

      const [autoIncident] = await db.insert(incidents).values({
        tenantId: autoTenant,
        type: "SLA_AT_RISK",
        destinationKey: autoDestinationKey,
        status: "OPEN",
        severity: "HIGH",
        title: "Auto Ack",
        summary: "Auto Ack",
        openedAt: autoOld,
        openedByActorType: "SYSTEM",
      }).returning();

      await autoAckIncidents({ tenantId: autoTenant, minutes: 30, now: autoNow });
      const [autoAcked] = await db
        .select()
        .from(incidents)
        .where(eq(incidents.id, autoIncident.id))
        .limit(1);
      assert.strictEqual(autoAcked?.status, "ACKED", "auto-ack updates status");
      assert.strictEqual(autoAcked?.ackedByActorType, "SYSTEM", "auto-ack actor system");

      const autoAckAudit = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.tenantId, autoTenant), eq(auditEvents.action, "INCIDENT.AUTO_ACKED")));
      assert(autoAckAudit.length >= 1, "auto-ack audit emitted");

      await autoAckIncidents({ tenantId: autoTenant, minutes: 30, now: autoNow });
      const autoAckAuditAgain = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.tenantId, autoTenant), eq(auditEvents.action, "INCIDENT.AUTO_ACKED")));
      assert.strictEqual(autoAckAuditAgain.length, autoAckAudit.length, "auto-ack idempotent");

      const [autoResolveIncident] = await db.insert(incidents).values({
        tenantId: autoTenant,
        type: "SLA_AT_RISK",
        destinationKey: autoDestinationKey,
        status: "OPEN",
        severity: "HIGH",
        title: "Auto Resolve",
        summary: "Auto Resolve",
        openedAt: autoRecent,
        openedByActorType: "SYSTEM",
      }).returning();

      await db.insert(auditEvents).values({
        tenantId: autoTenant,
        actorType: "SYSTEM",
        actorUserId: null,
        actorApiKeyId: null,
        actorLabel: "system",
        action: "ALERT.SLA_RECOVERED",
        resourceType: "ALERT_DELIVERY_SLA",
        resourceId: randomUUID(),
        severity: "INFO",
        status: "SUCCESS",
        message: "SLA recovered",
        metadata: { destination_key: autoDestinationKey },
        createdAt: autoNow,
      });

      await autoResolveIncidents({ tenantId: autoTenant, now: autoNow });
      const [autoResolved] = await db
        .select()
        .from(incidents)
        .where(eq(incidents.id, autoResolveIncident.id))
        .limit(1);
      assert.strictEqual(autoResolved?.status, "RESOLVED", "auto-resolve updates status");

      const autoResolveAudit = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.tenantId, autoTenant), eq(auditEvents.action, "INCIDENT.AUTO_RESOLVED")));
      assert(autoResolveAudit.length >= 1, "auto-resolve audit emitted");

      await autoResolveIncidents({ tenantId: autoTenant, now: autoNow });
      const autoResolveAuditAgain = await db
        .select()
        .from(auditEvents)
        .where(and(eq(auditEvents.tenantId, autoTenant), eq(auditEvents.action, "INCIDENT.AUTO_RESOLVED")));
      assert.strictEqual(autoResolveAuditAgain.length, autoResolveAudit.length, "auto-resolve idempotent");

      const autoTenantB = harness.newTenantId();
      await db.insert(incidents).values({
        tenantId: autoTenantB,
        type: "SLA_AT_RISK",
        destinationKey: "auto-b",
        status: "OPEN",
        severity: "HIGH",
        title: "Auto B",
        summary: "Auto B",
        openedAt: autoOld,
        openedByActorType: "SYSTEM",
      });
      await autoAckIncidents({ tenantId: autoTenant, minutes: 30, now: autoNow });
      const tenantBRows = await db.select().from(incidents).where(eq(incidents.tenantId, autoTenantB));
      assert.strictEqual(tenantBRows[0]?.status, "OPEN", "tenant isolation for auto-ack");

      const automationTenant = harness.newTenantId();
      const automationAckOpened = new Date("2026-02-01T00:00:00Z");
      const automationResolveOpened = new Date("2026-02-02T00:00:00Z");
      const automationAckDestination = "automation-ack";
      const automationResolveDestination = "automation-resolve";

      const [automationAckIncident] = await db.insert(incidents).values({
        tenantId: automationTenant,
        type: "SLA_AT_RISK",
        destinationKey: automationAckDestination,
        status: "OPEN",
        severity: "HIGH",
        title: "Automation Ack",
        summary: "Automation Ack",
        openedAt: automationAckOpened,
        openedByActorType: "SYSTEM",
      }).returning();

      const [automationResolveIncident] = await db.insert(incidents).values({
        tenantId: automationTenant,
        type: "SLA_AT_RISK",
        destinationKey: automationResolveDestination,
        status: "OPEN",
        severity: "HIGH",
        title: "Automation Resolve",
        summary: "Automation Resolve",
        openedAt: automationResolveOpened,
        openedByActorType: "SYSTEM",
      }).returning();

      await db.insert(auditEvents).values({
        tenantId: automationTenant,
        actorType: "SYSTEM",
        actorUserId: null,
        actorApiKeyId: null,
        actorLabel: "system",
        action: "ALERT.SLA_RECOVERED",
        resourceType: "ALERT_DELIVERY_SLA",
        resourceId: randomUUID(),
        severity: "INFO",
        status: "SUCCESS",
        message: "SLA recovered",
        metadata: { destination_key: automationResolveDestination },
        createdAt: new Date("2026-02-03T00:00:00Z"),
      });

      await runIncidentAutomation();

      const [automationAcked] = await db
        .select()
        .from(incidents)
        .where(eq(incidents.id, automationAckIncident.id))
        .limit(1);
      assert.strictEqual(automationAcked?.status, "ACKED", "run automation auto-acked");

      const [automationResolved] = await db
        .select()
        .from(incidents)
        .where(eq(incidents.id, automationResolveIncident.id))
        .limit(1);
      assert.strictEqual(automationResolved?.status, "RESOLVED", "run automation auto-resolved");

      await runIncidentAutomation();
      const [automationAckedAgain] = await db
        .select()
        .from(incidents)
        .where(eq(incidents.id, automationAckIncident.id))
        .limit(1);
      const [automationResolvedAgain] = await db
        .select()
        .from(incidents)
        .where(eq(incidents.id, automationResolveIncident.id))
        .limit(1);
      assert.strictEqual(automationAckedAgain?.status, "ACKED", "run automation idempotent ack");
      assert.strictEqual(automationResolvedAgain?.status, "RESOLVED", "run automation idempotent resolve");
  });
});
