import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "http";
import { hashApiKey } from "../../server/services/apiKeyService";
import { requestIdMiddleware, auditContextMiddleware } from "../../server/middleware/requestContext";
import { createTestHarness } from "../test/harness";
import { cleanDatabase, insertApiKeys, insertTenantUsers } from "../bootstrap";
import { db } from "../../server/db";
import { incidents } from "@shared/schema";

const TEST_VERSION = "test-version";
const TEST_SHA = "test-sha";

async function startTestServer() {
  const app = express();
  app.use(express.json());
  app.use(express.urlencoded({ extended: false }));
  app.use(requestIdMiddleware);
  app.use(auditContextMiddleware);
  const { registerRoutes } = await import("../../server/routes");
  const server = await registerRoutes(app);
  await new Promise<void>((resolve) => {
    server.listen(0, () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Failed to bind test server");
  }
  return { server, baseUrl: `http://127.0.0.1:${address.port}` };
}

test.describe("contracts", () => {
  const harness = createTestHarness({ label: "contracts", cleanup: "tenant", schema: "full" });
  const tenantId = randomUUID();
  const userId = randomUUID();
  const apiKey = "vs_contract_key";
  const headers = { Authorization: `Bearer ${apiKey}` };
  let server: Server | null = null;
  let baseUrl = "";

  test.before(async () => {
    process.env.APP_VERSION = TEST_VERSION;
    process.env.BUILD_SHA = TEST_SHA;
    process.env.NODE_ENV = "development";
    await harness.beforeAll();

    await insertTenantUsers({
      tenantId,
      userId,
      email: "contracts@veriscope.dev",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: userId,
    });
    const keyHash = hashApiKey(apiKey);
    await insertApiKeys({
      tenantId,
      userId,
      keyHash,
      name: "contracts",
      label: "contracts",
      role: "OWNER",
      isActive: true,
      createdAt: new Date(),
    });

    const started = await startTestServer();
    server = started.server;
    baseUrl = started.baseUrl;
  });

  test.after(async () => {
    if (server) {
      await new Promise<void>((resolve) => server?.close(() => resolve()));
    }
    await cleanDatabase({ tenantIds: [tenantId] });
    await harness.afterAll();
  });

  test("/metrics/ops shape", async () => {
    const res = await fetch(`${baseUrl}/metrics/ops`);
    assert.equal(res.status, 200);
    const payload = await res.json();
    assert.equal(payload.version, "1");
    assert.equal(payload.app_version, TEST_VERSION);
    assert.equal(payload.build_sha, TEST_SHA);
    assert.ok(payload.ops_schema_version);
    assert.ok(payload.counters);
    assert.ok(payload.delivery_latency_ms);
    assert.ok(payload.escalation_run_duration_ms);
  });

  test("/health includes ops + version", async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert.equal(res.status, 200);
    const payload = await res.json();
    assert.equal(payload.app_version, TEST_VERSION);
    assert.equal(payload.build_sha, TEST_SHA);
    assert.ok(payload.ops_schema_version);
  });

  test("/health/alerts includes ops snapshot", async () => {
    const res = await fetch(`${baseUrl}/health/alerts`, { headers });
    assert.equal(res.status, 200);
    const payload = await res.json();
    assert.ok(payload.ops);
    assert.ok(payload.ops.deliveries);
    assert.ok(payload.ops.latency_ms);
  });

  test("/metrics/ops reset works in dev", async () => {
    process.env.DEV_ROUTES_ENABLED = "true";
    const res = await fetch(`${baseUrl}/api/admin/ops/reset`, { method: "POST", headers });
    assert.equal(res.status, 200);
    const payload = await res.json();
    assert.equal(payload.ok, true);
    process.env.DEV_ROUTES_ENABLED = "";
  });

  test("admin routes blocked in production", async () => {
    process.env.NODE_ENV = "production";
    process.env.DEV_ROUTES_ENABLED = "";
    const res = await fetch(`${baseUrl}/api/admin/ops/reset`, { method: "POST", headers });
    assert.equal(res.status, 404);
    const payload = await res.json();
    assert.equal(payload.error, "NOT_FOUND");
    process.env.NODE_ENV = "development";
  });

  test("missing API key returns UNAUTHORIZED", async () => {
    const res = await fetch(`${baseUrl}/v1/alert-subscriptions`);
    assert.equal(res.status, 401);
    const payload = await res.json();
    assert.equal(payload.error, "UNAUTHORIZED");
  });

  test("viewer role returns FORBIDDEN", async () => {
    const viewerKey = "vs_viewer_key";
    const viewerUserId = randomUUID();
    await insertTenantUsers({
      tenantId,
      userId: viewerUserId,
      email: "viewer@veriscope.dev",
      role: "VIEWER",
      status: "ACTIVE",
      createdBy: viewerUserId,
    });
    await insertApiKeys({
      tenantId,
      userId: viewerUserId,
      keyHash: hashApiKey(viewerKey),
      name: "viewer",
      label: "viewer",
      role: "VIEWER",
      isActive: true,
      createdAt: new Date(),
    });

    const res = await fetch(`${baseUrl}/v1/incident-escalation-policies`, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${viewerKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        incident_type: "SLA_AT_RISK",
        severity_min: "HIGH",
        level: 1,
        after_minutes: 5,
        target_type: "ROLE",
        target_ref: "OWNER",
      }),
    });
    assert.equal(res.status, 403);
    const payload = await res.json();
    assert.equal(payload.error, "FORBIDDEN");
  });

  test("ack/resolve incident updates status", async () => {
    const incidentId = randomUUID();
    const now = new Date();
    await db.insert(incidents).values({
      id: incidentId,
      tenantId,
      type: "PORT_CONGESTION",
      status: "OPEN",
      severity: "HIGH",
      title: "Contract incident",
      summary: "Contract incident test",
      openedAt: now,
      openedByActorType: "API_KEY",
      openedByActorId: userId,
    });

    const ackRes = await fetch(`${baseUrl}/v1/incidents/${incidentId}/ack`, {
      method: "POST",
      headers,
    });
    assert.equal(ackRes.status, 200);

    const acked = await fetch(`${baseUrl}/v1/incidents/${incidentId}`, { headers });
    assert.equal(acked.status, 200);
    const ackedPayload = await acked.json();
    assert.equal(ackedPayload.item.status, "ACKED");

    const ackedList = await fetch(`${baseUrl}/v1/incidents?status=ACKED&limit=10`, { headers });
    assert.equal(ackedList.status, 200);
    const ackedListPayload = await ackedList.json();
    assert.ok(Array.isArray(ackedListPayload.items));
    assert.ok(ackedListPayload.items.find((item: any) => item.id === incidentId));

    const resolveRes = await fetch(`${baseUrl}/v1/incidents/${incidentId}/resolve`, {
      method: "POST",
      headers,
    });
    assert.equal(resolveRes.status, 200);

    const resolved = await fetch(`${baseUrl}/v1/incidents/${incidentId}`, { headers });
    assert.equal(resolved.status, 200);
    const resolvedPayload = await resolved.json();
    assert.equal(resolvedPayload.item.status, "RESOLVED");

    const resolvedList = await fetch(`${baseUrl}/v1/incidents?status=RESOLVED&limit=10`, { headers });
    assert.equal(resolvedList.status, 200);
    const resolvedListPayload = await resolvedList.json();
    assert.ok(Array.isArray(resolvedListPayload.items));
    assert.ok(resolvedListPayload.items.find((item: any) => item.id === incidentId));
  });
});
