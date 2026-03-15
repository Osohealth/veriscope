import { randomUUID, randomBytes } from "node:crypto";
import { eq, and, inArray, sql, desc, or } from "drizzle-orm";
import { db } from "../server/db";
import {
  apiKeys,
  tenantUsers,
  userContactMethods,
  alertSubscriptions,
  alertDedupe,
  incidents,
  incidentEscalations,
  incidentEscalationPolicies,
  alertDeliveries,
  alertDeliveryAttempts,
  alertDlq,
} from "../shared/schema";
import { hashApiKey } from "../server/services/apiKeyService";
import { TENANT_DEMO_ID } from "../server/config/tenancy";
import { DLQ_MAX_ATTEMPTS } from "../server/config/alerting";
import { GLOBAL_SCOPE_ENTITY_ID } from "../server/services/alertScope";
import { validateRoutingPolicyDraft } from "../server/services/alertRoutingValidationService";
import { getRoutingHealthForPolicy } from "../server/services/alertRoutingHealthService";
import { upsertIncidentEscalationPolicy, runIncidentEscalations } from "../server/services/incidentEscalationService";
import { getOpsCounters } from "../server/services/opsTelemetry";

const BASE_URL = process.env.DEMO_BASE_URL ?? "http://localhost:5000";
const API_KEY = process.env.DEMO_API_KEY ?? "vs_demo_key";
const TENANT_ID = process.env.DEMO_TENANT_ID ?? TENANT_DEMO_ID;
const OWNER_USER_ID = process.env.DEMO_OWNER_USER_ID ?? TENANT_ID;
const OWNER_EMAIL = process.env.DEMO_OWNER_EMAIL ?? `demo+${OWNER_USER_ID.slice(0, 6)}@veriscope.dev`;
const DEMO_EMAIL = process.env.DEMO_ALERT_EMAIL ?? "demo+alerts@veriscope.dev";
const DEMO_WEBHOOK_FAIL = process.env.DEMO_ALERT_WEBHOOK ?? "http://127.0.0.1:1/demo";
const DEMO_WEBHOOK_SUCCESS = process.env.DEMO_ALERT_WEBHOOK_SUCCESS ?? `${BASE_URL}/api/dev/webhook-sink`;
const DEMO_MODE = (process.env.DEMO_MODE ?? "baseline").toLowerCase();
const DEMO_CLEANUP = process.env.DEMO_CLEANUP === "1";
const DEMO_REQUIRE_SERVER = process.env.DEMO_REQUIRE_SERVER !== "0";
const DEMO_POLICY_NAME_EMAIL = process.env.DEMO_POLICY_NAME_EMAIL ?? "demo_policy_l1_email";
const DEMO_POLICY_NAME_WEBHOOK = process.env.DEMO_POLICY_NAME_WEBHOOK ?? "demo_policy_l1_webhook";
const DEMO_INCIDENT_TITLE = "Demo escalation incident";
const DEMO_INCIDENT_PREFIX = "demo-escalation:";
const COOLDOWN_MINUTES = Number.parseInt(process.env.ESCALATION_COOLDOWN_MINUTES ?? "30", 10) || 30;

const headers = {
  "Content-Type": "application/json",
  Authorization: `Bearer ${API_KEY}`,
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchJson(url: string, init: RequestInit = {}) {
  const res = await fetch(url, init);
  const text = await res.text();
  let payload: any = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = text;
  }
  if (!res.ok) {
    const message = typeof payload === "string" ? payload : payload?.error || res.statusText;
    const error = new Error(`HTTP ${res.status} ${message}`);
    (error as any).status = res.status;
    (error as any).payload = payload;
    throw error;
  }
  return payload;
}

async function probeServer() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 1500);
  try {
    await fetch(`${BASE_URL}/metrics/ops`, { signal: controller.signal });
    return true;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
}

async function resetOps() {
  return fetchJson(`${BASE_URL}/api/admin/ops/reset`, {
    method: "POST",
    headers,
  });
}

async function ensureTenantUser() {
  await db
    .insert(tenantUsers)
    .values({
      tenantId: TENANT_ID,
      userId: OWNER_USER_ID,
      email: OWNER_EMAIL,
      displayName: "Demo Owner",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: OWNER_USER_ID,
    })
    .onConflictDoNothing();
}

async function ensureApiKey() {
  const keyHash = hashApiKey(API_KEY);
  const existing = await db
    .select()
    .from(apiKeys)
    .where(eq(apiKeys.keyHash, keyHash))
    .limit(1);

  if (existing.length === 0) {
    await db
      .insert(apiKeys)
      .values({
        tenantId: TENANT_ID,
        userId: OWNER_USER_ID,
        keyHash,
        name: "demo-script",
        label: "demo-script",
        role: "OWNER",
        isActive: true,
        createdAt: new Date(),
      })
      .onConflictDoNothing();
    return;
  }

  if (existing[0].revokedAt) {
    await db
      .update(apiKeys)
      .set({ revokedAt: null, isActive: true })
      .where(eq(apiKeys.id, existing[0].id));
  }
}

async function ensureContactMethod() {
  const existing = await db
    .select()
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, TENANT_ID),
      eq(userContactMethods.userId, OWNER_USER_ID),
      eq(userContactMethods.type, "EMAIL"),
      eq(userContactMethods.value, OWNER_EMAIL),
    ))
    .limit(1);

  if (existing.length > 0) return;

  const primaryExists = await db
    .select()
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, TENANT_ID),
      eq(userContactMethods.userId, OWNER_USER_ID),
      eq(userContactMethods.type, "EMAIL"),
      eq(userContactMethods.isPrimary, true),
    ))
    .limit(1);

  await db
    .insert(userContactMethods)
    .values({
      tenantId: TENANT_ID,
      userId: OWNER_USER_ID,
      type: "EMAIL",
      value: OWNER_EMAIL,
      label: "Demo Email",
      isPrimary: primaryExists.length === 0,
      isVerified: true,
      isActive: true,
    })
    .onConflictDoNothing();
}

async function ensureSubscription(channel: "EMAIL" | "WEBHOOK", endpoint: string) {
  const existing = await db
    .select()
    .from(alertSubscriptions)
    .where(and(
      eq(alertSubscriptions.tenantId, TENANT_ID),
      eq(alertSubscriptions.userId, OWNER_USER_ID),
      eq(alertSubscriptions.channel, channel),
      eq(alertSubscriptions.endpoint, endpoint),
      eq(alertSubscriptions.entityId, GLOBAL_SCOPE_ENTITY_ID),
    ))
    .limit(1);

  if (existing.length > 0) return existing[0];

  const secret = channel === "WEBHOOK" ? randomBytes(24).toString("base64url") : null;
  const [row] = await db
    .insert(alertSubscriptions)
    .values({
      tenantId: TENANT_ID,
      userId: OWNER_USER_ID,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "HIGH",
      channel,
      endpoint,
      secret,
      signatureVersion: "v1",
      isEnabled: true,
      updatedAt: new Date(),
    })
    .returning();

  return row;
}

async function ensurePolicy(args: {
  level: number;
  targetType: "EMAIL" | "WEBHOOK";
  targetRef: string;
  targetName: string;
}) {
  const validation = await validateRoutingPolicyDraft({
    tenantId: TENANT_ID,
    now: new Date(),
    draft: {
      incident_type: "SLA_AT_RISK",
      severity_min: "HIGH",
      level: args.level,
      after_minutes: 0,
      include_blocked: true,
      targets: [{
        target_type: args.targetType,
        target_ref: args.targetRef,
        target_name: args.targetName,
      }],
    },
  });

  if (!validation.ok) {
    throw new Error(`Policy validation failed: ${JSON.stringify(validation.errors)}`);
  }

  const normalized = validation.normalized_policy;
  const target = normalized.targets[0];
  const health = await getRoutingHealthForPolicy({
    tenantId: TENANT_ID,
    policy: {
      targetType: target.target_type,
      targetRef: target.target_ref,
    },
    now: new Date(),
  });

  if (health.routes_allowed === 0) {
    throw new Error(`Policy not routable: ${JSON.stringify(health.blocked_reasons ?? [])}`);
  }

  const policy = await upsertIncidentEscalationPolicy({
    tenantId: TENANT_ID,
    incidentType: normalized.incident_type,
    severityMin: normalized.severity_min,
    level: normalized.level,
    afterMinutes: normalized.after_minutes,
    targetType: target.target_type,
    targetRef: target.target_ref,
    targetName: target.target_name ?? null,
    enabled: true,
    lastValidatedAt: new Date(),
    lastRoutingHealth: health,
  });

  if (!policy) {
    throw new Error("Failed to upsert escalation policy");
  }

  return policy;
}

async function insertIncident() {
  const now = new Date();
  const openedAt = new Date(now.getTime() - 2 * 60 * 1000);
  const destinationKey = `${DEMO_INCIDENT_PREFIX}${randomUUID().slice(0, 8)}`;

  const [row] = await db
    .insert(incidents)
    .values({
      tenantId: TENANT_ID,
      type: "SLA_AT_RISK",
      destinationKey,
      status: "OPEN",
      severity: "HIGH",
      title: DEMO_INCIDENT_TITLE,
      summary: "Synthetic incident created by demo script",
      openedAt,
      openedByActorType: "SYSTEM",
      openedByActorId: null,
    })
    .returning();

  if (!row) throw new Error("Failed to insert incident");

  return { incident: row, destinationKey };
}

async function fetchMetrics() {
  const payload = await fetchJson(`${BASE_URL}/metrics/ops`);
  if (payload?.counters) return payload;
  return { counters: payload ?? {} };
}

async function fetchHealth() {
  return fetchJson(`${BASE_URL}/health`);
}

async function runEscalationsViaApi() {
  return fetchJson(`${BASE_URL}/api/admin/incidents/escalations/run`, {
    method: "POST",
    headers,
  });
}

async function cleanupDemoArtifacts() {
  const incidentRows = await db
    .select({ id: incidents.id, destinationKey: incidents.destinationKey })
    .from(incidents)
    .where(and(
      eq(incidents.tenantId, TENANT_ID),
      or(
        eq(incidents.title, DEMO_INCIDENT_TITLE),
        sql`${incidents.destinationKey} like ${DEMO_INCIDENT_PREFIX + "%"}`,
      ),
    ));

  const incidentIds = incidentRows.map((row) => row.id);
  if (incidentIds.length > 0) {
    await db.delete(incidentEscalations)
      .where(and(
        eq(incidentEscalations.tenantId, TENANT_ID),
        inArray(incidentEscalations.incidentId, incidentIds),
      ));
  }

  for (const incidentId of incidentIds) {
    const clusterPrefix = `incident-escalation:${TENANT_ID}:${incidentId}:level:`;
    const deliveryRows = await db
      .select({ id: alertDeliveries.id })
      .from(alertDeliveries)
      .where(and(
        eq(alertDeliveries.tenantId, TENANT_ID),
        sql`${alertDeliveries.clusterId} like ${clusterPrefix + "%"}`,
      ));
    const deliveryIds = deliveryRows.map((row) => row.id);
    if (deliveryIds.length > 0) {
      await db.delete(alertDeliveryAttempts)
        .where(and(
          eq(alertDeliveryAttempts.tenantId, TENANT_ID),
          inArray(alertDeliveryAttempts.deliveryId, deliveryIds),
        ));
      await db.delete(alertDlq)
        .where(and(
          eq(alertDlq.tenantId, TENANT_ID),
          inArray(alertDlq.deliveryId, deliveryIds),
        ));
      await db.delete(alertDeliveries)
        .where(and(
          eq(alertDeliveries.tenantId, TENANT_ID),
          inArray(alertDeliveries.id, deliveryIds),
        ));
    }

    await db.delete(alertDedupe)
      .where(and(
        eq(alertDedupe.tenantId, TENANT_ID),
        sql`${alertDedupe.clusterId} like ${clusterPrefix + "%"}`,
      ));
  }

  if (incidentIds.length > 0) {
    await db.delete(incidents)
      .where(and(eq(incidents.tenantId, TENANT_ID), inArray(incidents.id, incidentIds)));
  }

  await db.delete(alertSubscriptions)
    .where(and(
      eq(alertSubscriptions.tenantId, TENANT_ID),
      eq(alertSubscriptions.userId, OWNER_USER_ID),
      or(
        eq(alertSubscriptions.endpoint, DEMO_EMAIL),
        eq(alertSubscriptions.endpoint, DEMO_WEBHOOK_FAIL),
        eq(alertSubscriptions.endpoint, DEMO_WEBHOOK_SUCCESS),
      ),
    ));

  await db.delete(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, TENANT_ID),
      eq(userContactMethods.userId, OWNER_USER_ID),
      eq(userContactMethods.label, "Demo Email"),
    ));

  await db.delete(incidentEscalationPolicies)
    .where(and(
      eq(incidentEscalationPolicies.tenantId, TENANT_ID),
      inArray(incidentEscalationPolicies.targetName, [DEMO_POLICY_NAME_EMAIL, DEMO_POLICY_NAME_WEBHOOK]),
    ));
}

async function fetchDeliveriesForIncident(incidentId: string, level?: number) {
  const levelFragment = level !== undefined ? `level:${level}` : "level:";
  const prefix = `incident-escalation:${TENANT_ID}:${incidentId}:${levelFragment}`;
  const deliveries = await db
    .select()
    .from(alertDeliveries)
    .where(and(
      eq(alertDeliveries.tenantId, TENANT_ID),
      sql`${alertDeliveries.clusterId} like ${prefix + "%"}`,
    ))
    .orderBy(desc(alertDeliveries.createdAt), desc(alertDeliveries.id));

  if (deliveries.length === 0) return [];

  const deliveryIds = deliveries.map((row) => row.id);

  const attempts = await db
    .select()
    .from(alertDeliveryAttempts)
    .where(and(
      eq(alertDeliveryAttempts.tenantId, TENANT_ID),
      inArray(alertDeliveryAttempts.deliveryId, deliveryIds),
    ));

  const dlqRows = await db
    .select()
    .from(alertDlq)
    .where(and(
      eq(alertDlq.tenantId, TENANT_ID),
      inArray(alertDlq.deliveryId, deliveryIds),
    ));

  const attemptsByDelivery = new Map<string, number>();
  for (const attempt of attempts) {
    attemptsByDelivery.set(attempt.deliveryId, (attemptsByDelivery.get(attempt.deliveryId) ?? 0) + 1);
  }

  const dlqByDelivery = new Map<string, typeof dlqRows[number]>();
  for (const row of dlqRows) {
    dlqByDelivery.set(row.deliveryId, row);
  }

  return deliveries.map((row) => ({
    id: row.id,
    destination_type: row.destinationType,
    destination: row.endpoint,
    status: row.status,
    created_at: row.createdAt,
    attempts: attemptsByDelivery.get(row.id) ?? 0,
    dlq: dlqByDelivery.get(row.id) ?? null,
  }));
}

async function forceDlqBlocked(incidentId: string, level: number) {
  const deliveries = await fetchDeliveriesForIncident(incidentId, level);
  const failed = deliveries.find((delivery) => delivery.status === "FAILED");
  if (!failed || !failed.dlq) return false;

  const baseTime = new Date(Date.now() - (COOLDOWN_MINUTES + 2) * 60 * 1000);
  await db.update(alertDeliveries)
    .set({ createdAt: baseTime })
    .where(and(eq(alertDeliveries.tenantId, TENANT_ID), eq(alertDeliveries.id, failed.id)));

  await db.update(alertDlq)
    .set({
      attemptCount: DLQ_MAX_ATTEMPTS,
      maxAttempts: DLQ_MAX_ATTEMPTS,
      nextAttemptAt: new Date(),
    })
    .where(and(eq(alertDlq.tenantId, TENANT_ID), eq(alertDlq.deliveryId, failed.id)));

  return true;
}

function formatDelta(before: Record<string, number>, after: Record<string, number>) {
  const delta: Record<string, number> = {};
  for (const key of Object.keys(after)) {
    delta[key] = (after as any)[key] - (before as any)[key];
  }
  return delta;
}

async function main() {
  console.log("\n=== Demo Escalation Flow ===\n");
  if (DEMO_CLEANUP) {
    await cleanupDemoArtifacts();
    console.log("[ok] demo cleanup complete");
  }
  await ensureTenantUser();
  await ensureApiKey();
  await ensureContactMethod();
  await ensureSubscription("EMAIL", DEMO_EMAIL);
  const webhookEndpoint = DEMO_MODE === "dlq" ? DEMO_WEBHOOK_FAIL : DEMO_WEBHOOK_SUCCESS;
  await ensureSubscription("WEBHOOK", webhookEndpoint);

  const policyEmail = await ensurePolicy({
    level: 1,
    targetType: "EMAIL",
    targetRef: DEMO_EMAIL,
    targetName: DEMO_POLICY_NAME_EMAIL,
  });
  const policyWebhook = await ensurePolicy({
    level: 1,
    targetType: "WEBHOOK",
    targetRef: webhookEndpoint,
    targetName: DEMO_POLICY_NAME_WEBHOOK,
  });

  const serverUp = await probeServer();
  const requireServer = (DEMO_MODE === "cooldown" || DEMO_MODE === "dlq")
    ? DEMO_REQUIRE_SERVER
    : process.env.DEMO_REQUIRE_SERVER === "1";
  if (!serverUp && requireServer) {
    console.error("\n\u274c Server required for this demo mode.");
    console.error("Start with: npm run dev\n");
    process.exitCode = 1;
    return;
  }
  if (serverUp) {
    try {
      const health = await fetchHealth();
      const appVersion = health?.app_version ?? health?.version ?? "unknown";
      const schemaVersion = health?.ops_schema_version ?? "unknown";
      console.log(`[info] app_version=${appVersion} ops_schema_version=${schemaVersion}`);
    } catch {
      console.warn("[warn] unable to read /health");
    }
  }
  if (serverUp) {
    try {
      await resetOps();
    } catch {
      console.warn("[warn] ops reset not available (server not restarted or dev routes disabled).");
      if (process.env.DEMO_REQUIRE_RESET === "1") {
        throw new Error("Ops reset required but endpoint is unavailable.");
      }
    }
  }
  const metricsBefore = serverUp ? await fetchMetrics() : { counters: getOpsCounters() };

  const { incident } = await insertIncident();
  console.log(`[ok] Incident created: ${incident.id}`);
  console.log(`[ok] Policy upserted: ${policyEmail.id} (L${policyEmail.level}, target EMAIL)`);
  console.log(`[ok] Policy upserted: ${policyWebhook.id} (L${policyWebhook.level}, target WEBHOOK)`);

  const runOnce = async (label: string) => {
    let runResult: any = null;
    if (serverUp) {
      try {
        runResult = await runEscalationsViaApi();
      } catch (error: any) {
        console.warn("[warn] admin run endpoint failed, falling back to direct service call.");
        runResult = await runIncidentEscalations({ tenantId: TENANT_ID, now: new Date() });
      }
    } else {
      console.warn("[warn] server not reachable; running escalations in-process.");
      runResult = await runIncidentEscalations({ tenantId: TENANT_ID, now: new Date() });
    }
    console.log(`[ok] ${label}: escalated=${runResult.escalated ?? 0}`);
    return runResult;
  };

  await runOnce("run #1");

  if (DEMO_MODE === "cooldown") {
    await runOnce("run #2 (cooldown)");
  } else if (DEMO_MODE === "dlq") {
    const forced = await forceDlqBlocked(incident.id, 1);
    if (!forced) {
      console.warn("[warn] no failed delivery + DLQ found for L1; DLQ demo may not block.");
    }
    await runOnce("run #2 (dlq)");
  }

  await sleep(300);

  const deliveries = await fetchDeliveriesForIncident(incident.id);
  if (deliveries.length === 0) {
    console.log("[warn] No deliveries found for incident.");
  } else {
    console.log("\nDeliveries:");
    for (const delivery of deliveries) {
      const dlqStatus = delivery.dlq
        ? `DLQ attempts=${delivery.dlq.attemptCount}/${delivery.dlq.maxAttempts}`
        : "";
      console.log(`- ${delivery.destination_type} ${delivery.destination} -> ${delivery.status} (attempts=${delivery.attempts}) ${dlqStatus}`.trim());
    }
  }

  const metricsAfter = serverUp ? await fetchMetrics() : { counters: getOpsCounters() };
  const delta = formatDelta(metricsBefore.counters ?? {}, metricsAfter.counters ?? {});

  console.log("\nMetrics delta:");
  Object.entries(delta).forEach(([key, value]) => {
    if (value !== 0) {
      console.log(`- ${key}: +${value}`);
    }
  });

  if ((delta as any).deliveries_blocked_cooldown_total > 0) {
    console.log("[info] Blocked by cooldown.");
  }
  if ((delta as any).deliveries_blocked_dlq_total > 0) {
    console.log("[info] Blocked by DLQ.");
  }

  if (Object.values(delta).every((value) => value === 0)) {
    console.log("- (no changes detected)");
  }

  console.log("\n[ok] Demo flow complete.\n");
}

main().catch((error) => {
  console.error("\nDemo flow failed:", error);
  process.exitCode = 1;
});
