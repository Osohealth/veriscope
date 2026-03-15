import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { db } from "../../server/db";
import {
  alertDedupe,
  alertDeliveries,
  alertDeliveryAttempts,
  alertDestinationStates,
  alertDlq,
  alertSubscriptions,
  auditEvents,
  incidentEscalationPolicies,
  incidentEscalations,
  incidents,
  userContactMethods,
} from "@shared/schema";
import {
  getIncidentEscalationSnapshot,
  listIncidentEscalationPolicies,
  runIncidentEscalations,
  upsertIncidentEscalationPolicy,
  validateEscalationPolicyTarget,
} from "../../server/services/incidentEscalationService";
import { validateRoutingPolicyDraft } from "../../server/services/alertRoutingValidationService";
import { getRoutingHealthForPolicy } from "../../server/services/alertRoutingHealthService";
import { hashKey, makeDestinationKey } from "../../server/services/destinationKey";
import { createUserContactMethod } from "../../server/services/userContactMethodsService";
import { listTeamUsersDirectory } from "../../server/services/teamUsersDirectory";
import { upsertTenantSettings } from "../../server/services/tenantSettings";
import { getOpsCounters, resetOpsTelemetry } from "../../server/services/opsTelemetry";
import { cleanDatabase, insertTenantUsers } from "../bootstrap";
import { GLOBAL_SCOPE_ENTITY_ID } from "../../server/services/alertScope";
import { createTestHarness } from "../test/harness";

test.describe("escalations", () => {
  const harness = createTestHarness({ label: "escalations", cleanup: "none", schema: "escalations" });
  const suiteTenants: string[] = [];
  const newTenant = () => {
    const id = randomUUID();
    suiteTenants.push(id);
    return id;
  };

  test.before(async () => {
    await harness.beforeAll();
  });

  test.beforeEach(async (t) => {
    await harness.beforeEach(t);
    globalThis.fetch = (async () => ({ ok: true, status: 200 })) as typeof fetch;
  });

  test.afterEach(async () => {
    await harness.afterEach();
  });

  test.after(async () => {
    if (suiteTenants.length > 0) {
      await cleanDatabase({ tenantIds: suiteTenants });
    }
    await harness.afterAll();
  });

  test("role escalation levels + idempotence", async () => {
    const tenantId = newTenant();
    const userId = randomUUID();
    const now = new Date("2026-02-10T12:00:00Z");
    const openedAt = new Date(now.getTime() - 31 * 60 * 1000);

    await insertTenantUsers({
      tenantId,
      userId,
      email: "escalation@veriscope.dev",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: userId,
    });

    await db.insert(alertSubscriptions).values({
      tenantId,
      userId,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/escalation",
      isEnabled: true,
      updatedAt: now,
    });

    const [incident] = await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "esc-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Escalation Incident",
      summary: "Escalation Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    }).returning();

    await db.insert(incidentEscalationPolicies).values([
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "HIGH",
        level: 1,
        afterMinutes: 15,
        targetType: "ROLE",
        targetRef: "OWNER",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "HIGH",
        level: 2,
        afterMinutes: 30,
        targetType: "ROLE",
        targetRef: "OWNER",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    await runIncidentEscalations({ tenantId, now });
    const [state] = await db
      .select()
      .from(incidentEscalations)
      .where(eq(incidentEscalations.incidentId, incident.id))
      .limit(1);
    assert.strictEqual(state?.currentLevel, 1, "escalation level advanced to 1");

    const deliveries = await db
      .select()
      .from(alertDeliveries)
      .where(and(
        eq(alertDeliveries.tenantId, tenantId),
        sql`${alertDeliveries.clusterId} like ${`incident-escalation:${tenantId}:${incident.id}%`}`,
      ));
    assert.strictEqual(deliveries.length, 1, "escalation delivery emitted");

    const auditRows = await db
      .select()
      .from(auditEvents)
      .where(and(eq(auditEvents.tenantId, tenantId), eq(auditEvents.action, "INCIDENT.ESCALATED")));
    assert.strictEqual(auditRows.length, 1, "escalation audit emitted");

    await runIncidentEscalations({ tenantId, now });
    const deliveriesLevel2 = await db
      .select()
      .from(alertDeliveries)
      .where(and(
        eq(alertDeliveries.tenantId, tenantId),
        sql`${alertDeliveries.clusterId} like ${`incident-escalation:${tenantId}:${incident.id}%`}`,
      ));
    assert.strictEqual(deliveriesLevel2.length, 2, "escalation advanced to level 2 without re-sending level 1");
  });

  test("multi-target idempotence", async () => {
    const tenantId = newTenant();
    const now = new Date("2026-02-12T12:30:00Z");
    const openedAt = new Date(now.getTime() - 2 * 60 * 1000);

    await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "multi-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Multi-target Incident",
      summary: "Multi-target Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    }).returning();

    await db.insert(incidentEscalationPolicies).values([
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "EMAIL",
        targetRef: "ops@veriscope.dev",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "WEBHOOK",
        targetRef: "http://localhost:9999/multi-escalation",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    await runIncidentEscalations({ tenantId, now });
    const deliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveries.length, 2, "multi-target escalation created two deliveries");
    assert.strictEqual(new Set(deliveries.map((row) => row.clusterId)).size, 2, "multi-target cluster ids unique");
    const emailDelivery = deliveries.find((row) => row.destinationType === "EMAIL");
    const webhookDelivery = deliveries.find((row) => row.destinationType === "WEBHOOK");
    assert.strictEqual(emailDelivery?.endpoint, "ops@veriscope.dev", "email direct target endpoint");
    assert.strictEqual(
      emailDelivery?.destinationKey,
      makeDestinationKey("EMAIL", "ops@veriscope.dev"),
      "email destination key",
    );
    assert.strictEqual(webhookDelivery?.endpoint, "http://localhost:9999/multi-escalation", "webhook direct target endpoint");

    await runIncidentEscalations({ tenantId, now });
    const deliveriesAgain = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveriesAgain.length, 2, "multi-target escalation idempotent");
  });

  test("cooldown prevents duplicate deliveries even if dedupe cleared", async () => {
    process.env.ESCALATION_COOLDOWN_MINUTES = "60";
    const tenantId = newTenant();
    const userId = randomUUID();
    const portId = randomUUID();
    const now = new Date("2026-02-12T13:00:00Z");
    const openedAt = new Date(now.getTime() - 5 * 60 * 1000);

    await insertTenantUsers({
      tenantId,
      userId,
      email: "owner@veriscope.dev",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: userId,
    });

    await db.insert(alertSubscriptions).values({
      tenantId,
      userId,
      entityType: "port",
      entityId: portId,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/cooldown",
      isEnabled: true,
      updatedAt: now,
    });

    const [incident] = await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "cooldown-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Cooldown Incident",
      summary: "Cooldown Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    }).returning();

    await db.insert(incidentEscalationPolicies).values({
      tenantId,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 1,
      afterMinutes: 1,
      targetType: "ROLE",
      targetRef: "OWNER",
      enabled: true,
      createdAt: now,
      updatedAt: now,
    });

    await runIncidentEscalations({ tenantId, now });
    const firstDeliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(firstDeliveries.length, 1, "initial escalation created delivery");

    await db.delete(alertDedupe).where(eq(alertDedupe.tenantId, tenantId));

    const secondNow = new Date(now.getTime() + 2 * 60 * 1000);
    await runIncidentEscalations({ tenantId, now: secondNow });
    const deliveriesAgain = await db
      .select()
      .from(alertDeliveries)
      .where(and(
        eq(alertDeliveries.tenantId, tenantId),
        sql`${alertDeliveries.clusterId} like ${`incident-escalation:${tenantId}:${incident.id}%`}`,
      ));
    assert.strictEqual(deliveriesAgain.length, 1, "cooldown prevents duplicate escalation");
  });

  test("current level re-evaluates and increments cooldown counter", async () => {
    const prevCooldown = process.env.ESCALATION_COOLDOWN_MINUTES;
    process.env.ESCALATION_COOLDOWN_MINUTES = "30";
    resetOpsTelemetry();
    const tenantId = newTenant();
    const now = new Date("2026-02-12T13:05:00Z");
    const openedAt = new Date(now.getTime() - 5 * 60 * 1000);

    await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "cooldown-counter-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Cooldown Counter Incident",
      summary: "Cooldown Counter Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    });

    await db.insert(incidentEscalationPolicies).values({
      tenantId,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 1,
      afterMinutes: 1,
      targetType: "EMAIL",
      targetRef: "ops@veriscope.dev",
      enabled: true,
      createdAt: now,
      updatedAt: now,
    });

    await runIncidentEscalations({ tenantId, now });
    const firstDeliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(firstDeliveries.length, 1, "first run created delivery");

    const before = getOpsCounters();
    await runIncidentEscalations({ tenantId, now });
    const after = getOpsCounters();
    assert.strictEqual(
      after.deliveries_blocked_cooldown_total,
      before.deliveries_blocked_cooldown_total + 1,
      "second run increments cooldown blocked counter",
    );

    const deliveriesAgain = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveriesAgain.length, 1, "second run does not create duplicate delivery");

    process.env.ESCALATION_COOLDOWN_MINUTES = prevCooldown;
  });

  test("policy idempotency keyed by policy id (same target)", async () => {
    const tenantId = newTenant();
    const userId = randomUUID();
    const portId = randomUUID();
    const now = new Date("2026-02-12T13:10:00Z");
    const openedAt = new Date(now.getTime() - 5 * 60 * 1000);

    await insertTenantUsers({
      tenantId,
      userId,
      email: "owner@veriscope.dev",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: userId,
    });

    await db.insert(alertSubscriptions).values({
      tenantId,
      userId,
      entityType: "port",
      entityId: portId,
      severityMin: "LOW",
      channel: "WEBHOOK",
      endpoint: "http://localhost:9999/policy-id",
      isEnabled: true,
      updatedAt: now,
    });

    await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "policy-id-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Policy Id Incident",
      summary: "Policy Id Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    });

    await db.insert(incidentEscalationPolicies).values([
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "ROLE",
        targetRef: "OWNER",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "MEDIUM",
        level: 1,
        afterMinutes: 1,
        targetType: "ROLE",
        targetRef: "OWNER",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    await runIncidentEscalations({ tenantId, now });
    const deliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveries.length, 2, "policies with same target emit separate deliveries");
  });

  test("partial success uses DLQ and avoids duplicate deliveries", async () => {
    process.env.ESCALATION_COOLDOWN_MINUTES = "0";
    const tenantId = newTenant();
    const now = new Date("2026-02-12T13:30:00Z");
    const openedAt = new Date(now.getTime() - 5 * 60 * 1000);

    globalThis.fetch = (async () => ({ ok: false, status: 500 })) as typeof fetch;

    await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "partial-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Partial Incident",
      summary: "Partial Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    });

    await db.insert(incidentEscalationPolicies).values([
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "EMAIL",
        targetRef: "ops@veriscope.dev",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "WEBHOOK",
        targetRef: "http://localhost:9999/partial",
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    await runIncidentEscalations({ tenantId, now });
    const deliveries = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveries.length, 2, "partial success writes two deliveries");
    const emailDelivery = deliveries.find((row) => row.destinationType === "EMAIL");
    const webhookDelivery = deliveries.find((row) => row.destinationType === "WEBHOOK");
    assert.strictEqual(emailDelivery?.status, "SENT", "email delivery succeeded");
    assert.strictEqual(webhookDelivery?.status, "FAILED", "webhook delivery failed");

    const dlqRows = await db
      .select()
      .from(alertDlq)
      .where(eq(alertDlq.deliveryId, webhookDelivery?.id ?? ""));
    assert.strictEqual(dlqRows.length, 1, "failed webhook added to DLQ");

    await runIncidentEscalations({ tenantId, now });
    const deliveriesAgain = await db
      .select()
      .from(alertDeliveries)
      .where(eq(alertDeliveries.tenantId, tenantId));
    assert.strictEqual(deliveriesAgain.length, 2, "rerun does not create duplicate deliveries");

    if (dlqRows[0]) {
      await db.update(alertDlq)
        .set({ attemptCount: dlqRows[0].maxAttempts })
        .where(eq(alertDlq.id, dlqRows[0].id));
      await runIncidentEscalations({ tenantId, now });
      const deliveriesFinal = await db
        .select()
        .from(alertDeliveries)
        .where(eq(alertDeliveries.tenantId, tenantId));
      assert.strictEqual(deliveriesFinal.length, 2, "exhausted DLQ does not create new deliveries");
    }
  });

  test("user routing selection + missing contact", async () => {
    const tenantId = newTenant();
    const now = new Date("2026-02-12T12:20:00Z");
    const openedAt = new Date(now.getTime() - 2 * 60 * 1000);

    const webhookUser = randomUUID();
    const emailUser = randomUUID();
    const missingUser = randomUUID();

    await insertTenantUsers([
      {
        tenantId,
        userId: webhookUser,
        email: "target-user@veriscope.dev",
        role: "OPERATOR",
        status: "ACTIVE",
        createdBy: webhookUser,
      },
      {
        tenantId,
        userId: emailUser,
        email: "email-only@veriscope.dev",
        role: "OPERATOR",
        status: "ACTIVE",
        createdBy: emailUser,
      },
      {
        tenantId,
        userId: missingUser,
        email: "missing-contact@veriscope.dev",
        role: "OPERATOR",
        status: "ACTIVE",
        createdBy: missingUser,
      },
    ]);

    await db.insert(userContactMethods).values([
      {
        tenantId,
        userId: webhookUser,
        type: "WEBHOOK",
        value: "http://localhost:9999/user-primary",
        label: "Primary",
        isPrimary: true,
        isVerified: true,
        isActive: true,
        createdAt: now,
      },
      {
        tenantId,
        userId: webhookUser,
        type: "EMAIL",
        value: "user@veriscope.dev",
        label: "Email",
        isPrimary: false,
        isVerified: true,
        isActive: true,
        createdAt: now,
      },
      {
        tenantId,
        userId: emailUser,
        type: "EMAIL",
        value: "only-email@veriscope.dev",
        label: "Primary",
        isPrimary: true,
        isVerified: true,
        isActive: true,
        createdAt: now,
      },
    ]);

    const [incident] = await db.insert(incidents).values({
      tenantId,
      type: "SLA_AT_RISK",
      destinationKey: "user-routing-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "User Routing Incident",
      summary: "User Routing Incident",
      openedAt,
      openedByActorType: "SYSTEM",
    }).returning();

    await db.insert(incidentEscalationPolicies).values([
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "USER",
        targetRef: webhookUser,
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "USER",
        targetRef: emailUser,
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
      {
        tenantId,
        incidentType: "SLA_AT_RISK",
        severityMin: "LOW",
        level: 1,
        afterMinutes: 1,
        targetType: "USER",
        targetRef: missingUser,
        enabled: true,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    await runIncidentEscalations({ tenantId, now });
    const deliveries = await db
      .select()
      .from(alertDeliveries)
      .where(and(
        eq(alertDeliveries.tenantId, tenantId),
        sql`${alertDeliveries.clusterId} like ${`incident-escalation:${tenantId}:${incident.id}%`}`,
      ));
    assert.strictEqual(deliveries.length, 3, "user escalation created three deliveries");

    const webhookDelivery = deliveries.find((row) => row.userId === webhookUser);
    assert.strictEqual(webhookDelivery?.destinationType, "WEBHOOK", "user escalation uses primary webhook");
    assert.strictEqual(webhookDelivery?.endpoint, "http://localhost:9999/user-primary", "user escalation webhook endpoint");

    const emailDelivery = deliveries.find((row) => row.userId === emailUser);
    assert.strictEqual(emailDelivery?.destinationType, "EMAIL", "user escalation fallback to email");
    assert.strictEqual(emailDelivery?.endpoint, "only-email@veriscope.dev", "user escalation email endpoint");

    const skippedDelivery = deliveries.find((row) => row.userId === missingUser);
    assert.strictEqual(skippedDelivery?.status, "SKIPPED_DESTINATION_ROUTING", "missing contact is skipped");
    assert.strictEqual(skippedDelivery?.skipReason, "NO_USER_CONTACT_METHOD", "missing contact skip reason");

    const attempts = await db
      .select()
      .from(alertDeliveryAttempts)
      .where(eq(alertDeliveryAttempts.deliveryId, skippedDelivery?.id ?? ""));
    assert.strictEqual(attempts.length, 0, "missing contact writes no attempts");

    const dlqRows = await db
      .select()
      .from(alertDlq)
      .where(eq(alertDlq.deliveryId, skippedDelivery?.id ?? ""));
    assert.strictEqual(dlqRows.length, 0, "missing contact writes no dlq");
  });

  test("directory + allowlist + validation", async () => {
    const directoryTenant = newTenant();
    const directoryUserA = randomUUID();
    const directoryUserB = randomUUID();
    const directoryUserInactive = randomUUID();
    const directoryOtherTenant = newTenant();

    await insertTenantUsers([
      {
        tenantId: directoryTenant,
        userId: directoryUserA,
        email: "alpha@veriscope.dev",
        displayName: "Alpha User",
        role: "OWNER",
        status: "ACTIVE",
        createdBy: directoryUserA,
        createdAt: new Date("2026-02-12T08:00:00Z"),
      },
      {
        tenantId: directoryTenant,
        userId: directoryUserB,
        email: "beta@veriscope.dev",
        displayName: "Beta User",
        role: "OPERATOR",
        status: "ACTIVE",
        createdBy: directoryUserA,
        createdAt: new Date("2026-02-12T09:00:00Z"),
      },
      {
        tenantId: directoryTenant,
        userId: directoryUserInactive,
        email: "inactive@veriscope.dev",
        displayName: "Inactive User",
        role: "VIEWER",
        status: "DISABLED",
        createdBy: directoryUserA,
        createdAt: new Date("2026-02-12T10:00:00Z"),
      },
      {
        tenantId: directoryOtherTenant,
        userId: randomUUID(),
        email: "other@veriscope.dev",
        displayName: "Other Tenant",
        role: "OWNER",
        status: "ACTIVE",
        createdBy: directoryUserA,
        createdAt: new Date("2026-02-12T11:00:00Z"),
      },
    ]);

    const directoryPage1 = await listTeamUsersDirectory({
      tenantId: directoryTenant,
      limit: 1,
    });
    assert.strictEqual(directoryPage1.items.length, 1, "directory page 1 length");
    assert(directoryPage1.nextCursor, "directory next cursor returned");
    assert.strictEqual(directoryPage1.items[0].status, "ACTIVE", "directory active only");

    const [cursorCreatedAt, cursorId] = Buffer.from(String(directoryPage1.nextCursor), "base64")
      .toString("utf8")
      .split("|");
    const directoryPage2 = await listTeamUsersDirectory({
      tenantId: directoryTenant,
      limit: 2,
      cursorCreatedAt,
      cursorId,
    });
    assert(directoryPage2.items.every((item) => item.user_id !== directoryPage1.items[0].user_id), "directory cursor no overlap");
    assert(directoryPage2.items.every((item) => item.status === "ACTIVE"), "directory filters inactive");

    const allowlistTenant = newTenant();
    await upsertTenantSettings(allowlistTenant, {
      audit_retention_days: 90,
      allowed_email_domains: ["veriscope.dev"],
      allowed_webhook_hosts: ["hooks.veriscope.dev"],
    });

    const prevNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    const emailTarget = await validateEscalationPolicyTarget({
      tenantId: allowlistTenant,
      targetType: "EMAIL",
      targetRef: "alerts@veriscope.dev",
    });
    assert.strictEqual(emailTarget.targetRef, "alerts@veriscope.dev", "email allowlist accepted");

    let emailRejected = false;
    try {
      await validateEscalationPolicyTarget({
        tenantId: allowlistTenant,
        targetType: "EMAIL",
        targetRef: "alerts@other.com",
      });
    } catch {
      emailRejected = true;
    }
    assert(emailRejected, "email allowlist rejects non-allowed domain");

    const webhookTarget = await validateEscalationPolicyTarget({
      tenantId: allowlistTenant,
      targetType: "WEBHOOK",
      targetRef: "https://hooks.veriscope.dev/escalation",
    });
    assert.strictEqual(webhookTarget.targetRef, "https://hooks.veriscope.dev/escalation", "webhook allowlist accepted");

    let webhookRejected = false;
    try {
      await validateEscalationPolicyTarget({
        tenantId: allowlistTenant,
        targetType: "WEBHOOK",
        targetRef: "https://example.com/escalation",
      });
    } catch {
      webhookRejected = true;
    }
    assert(webhookRejected, "webhook allowlist rejects non-allowed host");

    const allowlistUser = randomUUID();
    await insertTenantUsers({
      tenantId: allowlistTenant,
      userId: allowlistUser,
      email: "allowlist-user@veriscope.dev",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: allowlistUser,
    });

    let contactEmailRejected = false;
    try {
      await createUserContactMethod({
        tenantId: allowlistTenant,
        userId: allowlistUser,
        input: { type: "EMAIL", value: "blocked@other.com" },
      });
    } catch {
      contactEmailRejected = true;
    }
    assert(contactEmailRejected, "contact method email allowlist enforced");
    await createUserContactMethod({
      tenantId: allowlistTenant,
      userId: allowlistUser,
      input: { type: "EMAIL", value: "allowed@veriscope.dev" },
    });

    let contactWebhookRejected = false;
    try {
      await createUserContactMethod({
        tenantId: allowlistTenant,
        userId: allowlistUser,
        input: { type: "WEBHOOK", value: "https://example.com/escalation" },
      });
    } catch {
      contactWebhookRejected = true;
    }
    assert(contactWebhookRejected, "contact method webhook allowlist enforced");
    await createUserContactMethod({
      tenantId: allowlistTenant,
      userId: allowlistUser,
      input: { type: "WEBHOOK", value: "https://hooks.veriscope.dev/contact" },
    });

    const userTarget = await validateEscalationPolicyTarget({
      tenantId: directoryTenant,
      targetType: "USER",
      targetRef: directoryUserA,
    });
    assert.strictEqual(userTarget.targetName, "Alpha User", "user target resolves name");

    let inactiveRejected = false;
    try {
      await validateEscalationPolicyTarget({
        tenantId: directoryTenant,
        targetType: "USER",
        targetRef: directoryUserInactive,
      });
    } catch {
      inactiveRejected = true;
    }
    assert(inactiveRejected, "inactive user target rejected");

    process.env.NODE_ENV = prevNodeEnv;

    const validationTenant = newTenant();
    const validationUser = randomUUID();
    const validationUser2 = randomUUID();
    await insertTenantUsers([
      {
        tenantId: validationTenant,
        userId: validationUser,
        email: "validation-owner@veriscope.dev",
        displayName: "Validation Owner",
        role: "OWNER",
        status: "ACTIVE",
        createdBy: validationUser,
      },
      {
        tenantId: validationTenant,
        userId: validationUser2,
        email: "validation-owner-2@veriscope.dev",
        displayName: "Validation Owner 2",
        role: "OWNER",
        status: "ACTIVE",
        createdBy: validationUser2,
      },
    ]);

    const otherTenant = newTenant();
    const missingUser = randomUUID();
    await insertTenantUsers({
      tenantId: otherTenant,
      userId: missingUser,
      email: "other-tenant@veriscope.dev",
      displayName: "Other Tenant",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: missingUser,
    });

    const validationNow = new Date("2026-02-12T12:40:00Z");
    const validationMissingUser = await validateRoutingPolicyDraft({
      tenantId: validationTenant,
      now: validationNow,
      draft: {
        incident_type: "SLA_AT_RISK",
        severity_min: "HIGH",
        level: 1,
        after_minutes: 5,
        targets: [{ target_type: "USER", target_ref: missingUser }],
      },
    });
    assert(validationMissingUser.errors.some((err) => err.code === "USER_NOT_FOUND"), "validation rejects missing user");

    const validationRoleNoRoutes = await validateRoutingPolicyDraft({
      tenantId: validationTenant,
      now: validationNow,
      draft: {
        incident_type: "SLA_AT_RISK",
        severity_min: "HIGH",
        level: 1,
        after_minutes: 5,
        targets: [{ target_type: "ROLE", target_ref: "OWNER" }],
      },
    });
    assert(validationRoleNoRoutes.warnings.some((warn) => warn.code === "ROLE_HAS_RECIPIENTS_BUT_NO_ROUTES"), "role warnings when no routes");

    const validationNormalized = await validateRoutingPolicyDraft({
      tenantId: validationTenant,
      now: validationNow,
      draft: {
        incident_type: "SLA_AT_RISK",
        severity_min: "HIGH",
        level: 1,
        after_minutes: 5,
        targets: [{ target_type: "USER", target_ref: validationUser, target_name: "garbage" }],
      },
    });
    assert.strictEqual(validationNormalized.normalized_policy.targets[0].target_name, "Validation Owner", "validation normalizes user target_name");

    const validationPrevEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    await upsertTenantSettings(validationTenant, {
      audit_retention_days: 90,
      allowed_email_domains: ["veriscope.dev"],
      allowed_webhook_hosts: ["hooks.veriscope.dev"],
    });
    const validationEmailAllowlist = await validateRoutingPolicyDraft({
      tenantId: validationTenant,
      now: validationNow,
      draft: {
        incident_type: "SLA_AT_RISK",
        severity_min: "HIGH",
        level: 1,
        after_minutes: 5,
        targets: [{ target_type: "EMAIL", target_ref: "alerts@other.com" }],
      },
    });
    assert(validationEmailAllowlist.errors.some((err) => err.code === "EMAIL_DOMAIN_NOT_ALLOWED"), "validation enforces email allowlist");
    process.env.NODE_ENV = validationPrevEnv;
  });

  test("routing health + snapshot + disabled policy + ordering", async () => {
    const routingHealthTenant = newTenant();
    const routingHealthUser = randomUUID();
    await insertTenantUsers({
      tenantId: routingHealthTenant,
      userId: routingHealthUser,
      email: "routing-health@veriscope.dev",
      displayName: "Routing Health",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: routingHealthUser,
    });

    const routingHealthPolicy = await upsertIncidentEscalationPolicy({
      tenantId: routingHealthTenant,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 1,
      afterMinutes: 5,
      targetType: "USER",
      targetRef: routingHealthUser,
      enabled: true,
    });

    const routingHealthNoRoutes = await getRoutingHealthForPolicy({
      tenantId: routingHealthTenant,
      policy: routingHealthPolicy,
      now: new Date("2026-02-12T13:00:00Z"),
    });
    assert.strictEqual(routingHealthNoRoutes.routes_total, 0, "routing health routes total empty");
    assert.strictEqual(routingHealthNoRoutes.warnings_count >= 1, true, "routing health warning for no contact methods");

    const pausedWebhook = "https://hooks.veriscope.dev/paused";
    await createUserContactMethod({
      tenantId: routingHealthTenant,
      userId: routingHealthUser,
      input: { type: "WEBHOOK", value: pausedWebhook, is_primary: true },
    });

    const pausedDestKey = makeDestinationKey("WEBHOOK", pausedWebhook);
    await db.insert(alertDestinationStates).values({
      tenantId: routingHealthTenant,
      destinationType: "WEBHOOK",
      destinationKey: pausedDestKey,
      state: "PAUSED",
      createdAt: new Date("2026-02-12T13:05:00Z"),
      updatedAt: new Date("2026-02-12T13:05:00Z"),
    });

    const routingHealthPaused = await getRoutingHealthForPolicy({
      tenantId: routingHealthTenant,
      policy: routingHealthPolicy,
      now: new Date("2026-02-12T13:06:00Z"),
    });
    assert(routingHealthPaused.routes_total >= 1, "routing health routes total after contact method");
    assert.strictEqual(routingHealthPaused.routes_allowed, 0, "routing health blocked routes");
    assert(routingHealthPaused.blocked_reasons.includes("DESTINATION_PAUSED"), "routing health paused reason");

    const readinessUser = randomUUID();
    await insertTenantUsers({
      tenantId: routingHealthTenant,
      userId: readinessUser,
      email: "readiness-user@veriscope.dev",
      displayName: "Readiness User",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: readinessUser,
    });

    const readinessPolicy = await upsertIncidentEscalationPolicy({
      tenantId: routingHealthTenant,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 3,
      afterMinutes: 5,
      targetType: "USER",
      targetRef: readinessUser,
      enabled: true,
    });

    const readinessHealth = await getRoutingHealthForPolicy({
      tenantId: routingHealthTenant,
      policy: readinessPolicy,
      now: new Date("2026-02-12T13:07:00Z"),
    });
    assert.strictEqual(readinessHealth.routes_allowed, 0, "readiness blocked when no routes");

    await createUserContactMethod({
      tenantId: routingHealthTenant,
      userId: readinessUser,
      input: { type: "EMAIL", value: "readiness-user@veriscope.dev", is_primary: true },
    });

    const readinessHealthAfter = await getRoutingHealthForPolicy({
      tenantId: routingHealthTenant,
      policy: readinessPolicy,
      now: new Date("2026-02-12T13:08:00Z"),
    });
    assert(readinessHealthAfter.routes_allowed > 0, "readiness allowed after contact method");

    const disabledTenant = newTenant();
    const disabledUser = randomUUID();
    const disabledNow = new Date("2026-02-12T13:10:00Z");
    const disabledOpened = new Date(disabledNow.getTime() - 10 * 60 * 1000);

    await insertTenantUsers({
      tenantId: disabledTenant,
      userId: disabledUser,
      email: "disabled-user@veriscope.dev",
      displayName: "Disabled User",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: disabledUser,
    });
    await createUserContactMethod({
      tenantId: disabledTenant,
      userId: disabledUser,
      input: { type: "EMAIL", value: "disabled-user@veriscope.dev", is_primary: true },
    });
    await upsertIncidentEscalationPolicy({
      tenantId: disabledTenant,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 1,
      afterMinutes: 1,
      targetType: "USER",
      targetRef: disabledUser,
      enabled: false,
    });
    await db.insert(incidents).values({
      tenantId: disabledTenant,
      type: "SLA_AT_RISK",
      status: "OPEN",
      severity: "HIGH",
      title: "Disabled policy incident",
      summary: "Disabled policy incident",
      openedAt: disabledOpened,
      openedByActorType: "SYSTEM",
    }).returning();
    const disabledRun = await runIncidentEscalations({ tenantId: disabledTenant, now: disabledNow });
    assert.strictEqual(disabledRun.escalated, 0, "disabled policies not escalated");

    const policyTenant = newTenant();
    await upsertIncidentEscalationPolicy({
      tenantId: policyTenant,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 1,
      afterMinutes: 5,
      targetType: "ROLE",
      targetRef: "OWNER",
      enabled: true,
    });
    await upsertIncidentEscalationPolicy({
      tenantId: policyTenant,
      incidentType: "SLA_AT_RISK",
      severityMin: "HIGH",
      level: 1,
      afterMinutes: 7,
      targetType: "ROLE",
      targetRef: "OWNER",
      enabled: true,
    });
    await upsertIncidentEscalationPolicy({
      tenantId: policyTenant,
      incidentType: "ENDPOINT_DOWN",
      severityMin: "LOW",
      level: 1,
      afterMinutes: 10,
      targetType: "ROLE",
      targetRef: "OWNER",
      enabled: true,
    });
    await upsertIncidentEscalationPolicy({
      tenantId: policyTenant,
      incidentType: "ALL",
      severityMin: "MEDIUM",
      level: 2,
      afterMinutes: 20,
      targetType: "ROLE",
      targetRef: "OWNER",
      enabled: true,
    });

    const policyList = await listIncidentEscalationPolicies(policyTenant);
    const policyTypes = policyList.map((row: any) => row.incidentType);
    assert.strictEqual(
      policyList.filter((row: any) => row.incidentType === "SLA_AT_RISK" && row.level === 1).length,
      1,
      "policy upsert is idempotent for level/type",
    );
    assert.deepStrictEqual(policyTypes, ["ALL", "SLA_AT_RISK", "ENDPOINT_DOWN"], "policy ordering is deterministic");

    const snapshotTenant = newTenant();
    const snapshotNow = new Date("2026-02-11T10:00:00Z");
    const snapshotOpened = new Date(snapshotNow.getTime() - 20 * 60 * 1000);

    const [snapshotIncident] = await db.insert(incidents).values({
      tenantId: snapshotTenant,
      type: "SLA_AT_RISK",
      destinationKey: "snapshot-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "Snapshot Incident",
      summary: "Snapshot Incident",
      openedAt: snapshotOpened,
      openedByActorType: "SYSTEM",
    }).returning();

    await db.insert(incidentEscalationPolicies).values([
      {
        tenantId: snapshotTenant,
        incidentType: "SLA_AT_RISK",
        severityMin: "HIGH",
        level: 1,
        afterMinutes: 10,
        targetType: "ROLE",
        targetRef: "OWNER",
        enabled: true,
        createdAt: snapshotNow,
        updatedAt: snapshotNow,
      },
      {
        tenantId: snapshotTenant,
        incidentType: "SLA_AT_RISK",
        severityMin: "HIGH",
        level: 2,
        afterMinutes: 30,
        targetType: "ROLE",
        targetRef: "OWNER",
        enabled: true,
        createdAt: snapshotNow,
        updatedAt: snapshotNow,
      },
    ]);

    await db.insert(incidentEscalations).values({
      tenantId: snapshotTenant,
      incidentId: snapshotIncident.id,
      currentLevel: 1,
      lastEscalatedAt: new Date(snapshotNow.getTime() - 5 * 60 * 1000),
      createdAt: snapshotNow,
      updatedAt: snapshotNow,
    });

    const snapshot = await getIncidentEscalationSnapshot({
      tenantId: snapshotTenant,
      incident: snapshotIncident,
      now: snapshotNow,
    });
    assert.strictEqual(snapshot.has_policy, true, "snapshot has policy");
    assert.strictEqual(snapshot.current_level, 1, "snapshot current level");
    assert.strictEqual(snapshot.next.level, 2, "snapshot next level");
    assert.strictEqual(snapshot.next.after_minutes, 30, "snapshot next after minutes");
    assert.strictEqual(snapshot.next.reason, "NOT_DUE", "snapshot next reason");
    assert.strictEqual(snapshot.next.eta_seconds, 600, "snapshot eta seconds");
    const expectedDue = new Date(snapshotOpened.getTime() + 30 * 60 * 1000).toISOString();
    assert.strictEqual(snapshot.next.due_at, expectedDue, "snapshot due at");

    const noPolicyTenant = newTenant();
    const [noPolicyIncident] = await db.insert(incidents).values({
      tenantId: noPolicyTenant,
      type: "SLA_AT_RISK",
      destinationKey: "no-policy-dest",
      status: "OPEN",
      severity: "HIGH",
      title: "No Policy Incident",
      summary: "No Policy Incident",
      openedAt: snapshotOpened,
      openedByActorType: "SYSTEM",
    }).returning();

    const noPolicySnapshot = await getIncidentEscalationSnapshot({
      tenantId: noPolicyTenant,
      incident: noPolicyIncident,
      now: snapshotNow,
    });
    assert.strictEqual(noPolicySnapshot.has_policy, false, "snapshot no policy");
    assert.strictEqual(noPolicySnapshot.next.reason, "NO_POLICY", "snapshot no policy reason");
  });
});

