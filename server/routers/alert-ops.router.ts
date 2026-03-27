import { randomBytes } from "crypto";
import { Router } from "express";
import { and, eq, inArray } from "drizzle-orm";
import { db } from "../db";
import { alertSubscriptions, ports } from "@shared/schema";
import { authenticateApiKey } from "../middleware/apiKeyAuth";
import { requireRole } from "../auth/requireRole";
import { writeAuditEvent } from "../services/auditLog";
import {
  listAlertSlaThresholds,
  upsertAlertSlaThreshold,
  listAlertSlaWindows,
  getAlertSlaSummary,
} from "../services/alertSlaService";
import {
  listAlertNoiseBudgets,
  upsertAlertNoiseBudget,
} from "../services/alertNoiseBudgetService";
import { listEndpointHealth } from "../services/alertEndpointHealthService";
import {
  listAlertSubscriptionsPage,
  getAlertSubscriptionsSummary,
} from "../services/alertSubscriptions";
import { GLOBAL_SCOPE_ENTITY_ID, normalizeScope } from "../services/alertScopeService";
import { logger } from "../middleware/observability";
import { parseSafeLimit } from "../utils/pagination";

export const alertOpsRouter = Router();

const generateSecret = () => randomBytes(24).toString("base64url");

function isValidWebhookUrl(value: string, allowHttp: boolean): boolean {
  try {
    const u = new URL(value);
    if (u.protocol === "https:") return true;
    if (allowHttp && u.protocol === "http:") return true;
    return false;
  } catch {
    return false;
  }
}

function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

async function guardRole(req: any, res: any, minRole: "OWNER" | "OPERATOR" | "VIEWER"): Promise<boolean> {
  try {
    requireRole(req.auth, minRole);
    return true;
  } catch (err: any) {
    await writeAuditEvent(req.auditContext, {
      action: "AUTH.RBAC_DENIED",
      resourceType: "RBAC",
      status: "DENIED",
      severity: "SECURITY",
      message: "Role is not permitted for this operation.",
      metadata: { path: req.path, method: req.method, role: req.auth?.role, required: minRole },
    }).catch(() => { });
    const status = err?.status ?? 403;
    res.status(status).json({ error: status === 401 ? "UNAUTHORIZED" : "FORBIDDEN", detail: err?.message ?? "Forbidden" });
    return false;
  }
}

// ---- SLA Thresholds ----

// GET /v1/alert-sla-thresholds
alertOpsRouter.get("/v1/alert-sla-thresholds", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = String(req.query?.window ?? "");
    if (window !== "24h" && window !== "7d") {
      return res.status(400).json({ error: "window must be 24h or 7d" });
    }

    const result = await listAlertSlaThresholds(tenantId, window as "24h" | "7d");
    res.json({ version: "1", ...result });
  } catch (err) {
    logger.error("Failed to list SLA thresholds", { err });
    next(err);
  }
});

// PATCH /v1/alert-sla-thresholds
alertOpsRouter.patch("/v1/alert-sla-thresholds", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = String(req.body?.window ?? "");
    if (window !== "24h" && window !== "7d") {
      return res.status(400).json({ error: "window must be 24h or 7d" });
    }

    const destinationType = String(req.body?.destination_type ?? "").toUpperCase();
    if (!["WEBHOOK", "EMAIL"].includes(destinationType)) {
      return res.status(400).json({ error: "destination_type must be WEBHOOK or EMAIL" });
    }

    const p95MsThreshold = Number(req.body?.p95_ms_threshold);
    if (!Number.isFinite(p95MsThreshold) || p95MsThreshold <= 0) {
      return res.status(400).json({ error: "p95_ms_threshold must be a positive number" });
    }

    const successRateThreshold = Number(req.body?.success_rate_threshold);
    if (!Number.isFinite(successRateThreshold) || successRateThreshold < 0 || successRateThreshold > 1) {
      return res.status(400).json({ error: "success_rate_threshold must be between 0 and 1" });
    }

    const update = await upsertAlertSlaThreshold({
      tenantId,
      window: window as "24h" | "7d",
      destinationType: destinationType as "WEBHOOK" | "EMAIL",
      p95MsThreshold,
      successRateThreshold,
    });

    await writeAuditEvent(req.auditContext, {
      action: "ALERT.SLA_THRESHOLDS_UPDATED",
      resourceType: "ALERT_SLA_THRESHOLD",
      severity: "INFO",
      status: "SUCCESS",
      message: "Updated alert SLA thresholds",
      metadata: { window, destination_type: destinationType, from: update.previous, to: update.current },
    }).catch(() => { });

    res.json({
      version: "1",
      window,
      destination_type: destinationType,
      p95_ms_threshold: update.current.p95_ms_threshold,
      success_rate_threshold: update.current.success_rate_threshold,
    });
  } catch (err) {
    logger.error("Failed to update SLA thresholds", { err });
    next(err);
  }
});

// ---- Noise Budgets ----

// GET /v1/alert-noise-budgets
alertOpsRouter.get("/v1/alert-noise-budgets", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = String(req.query?.window ?? "");
    if (window !== "24h" && window !== "7d") {
      return res.status(400).json({ error: "window must be 24h or 7d" });
    }

    const result = await listAlertNoiseBudgets(tenantId, window as "24h" | "7d");
    res.json({ version: "1", ...result });
  } catch (err) {
    logger.error("Failed to list noise budgets", { err });
    next(err);
  }
});

// PATCH /v1/alert-noise-budgets
alertOpsRouter.patch("/v1/alert-noise-budgets", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = String(req.body?.window ?? "");
    if (window !== "24h" && window !== "7d") {
      return res.status(400).json({ error: "window must be 24h or 7d" });
    }

    const destinationType = String(req.body?.destination_type ?? "").toUpperCase();
    if (!["WEBHOOK", "EMAIL"].includes(destinationType)) {
      return res.status(400).json({ error: "destination_type must be WEBHOOK or EMAIL" });
    }

    const maxDeliveries = Number(req.body?.max_deliveries);
    if (!Number.isFinite(maxDeliveries) || maxDeliveries <= 0) {
      return res.status(400).json({ error: "max_deliveries must be a positive number" });
    }

    const update = await upsertAlertNoiseBudget({
      tenantId,
      window: window as "24h" | "7d",
      destinationType: destinationType as "WEBHOOK" | "EMAIL",
      maxDeliveries: Math.floor(maxDeliveries),
    });

    await writeAuditEvent(req.auditContext, {
      action: "ALERT.NOISE_BUDGET_UPDATED",
      resourceType: "ALERT_NOISE_BUDGET",
      severity: "INFO",
      status: "SUCCESS",
      message: "Updated alert noise budget",
      metadata: { window, destination_type: destinationType, from: update.previous, to: update.current },
    }).catch(() => { });

    res.json({
      version: "1",
      window,
      destination_type: destinationType,
      max_deliveries: update.current.max_deliveries,
    });
  } catch (err) {
    logger.error("Failed to update noise budget", { err });
    next(err);
  }
});

// ---- SLA Windows / Summary ----

// GET /v1/alert-slas
alertOpsRouter.get("/v1/alert-slas", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = req.query?.window ? String(req.query.window) : undefined;
    if (window && window !== "24h" && window !== "7d") {
      return res.status(400).json({ error: "window must be 24h or 7d" });
    }

    const limitRaw = Number(req.query?.limit ?? 50);
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;
    const cursor = req.query?.cursor ? String(req.query.cursor) : undefined;
    const destinationType = req.query?.destination_type ? String(req.query.destination_type).toUpperCase() : undefined;

    const result = await listAlertSlaWindows(tenantId, window);
    res.json({ version: "1", ...result });
  } catch (err) {
    logger.error("Failed to list alert SLA windows", { err });
    next(err);
  }
});

// GET /v1/alert-slas/summary
alertOpsRouter.get("/v1/alert-slas/summary", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = req.query?.window ? String(req.query.window) : undefined;
    if (window && window !== "24h" && window !== "7d") {
      return res.status(400).json({ error: "window must be 24h or 7d" });
    }

    const summary = await getAlertSlaSummary(tenantId);
    res.json({ version: "1", ...summary });
  } catch (err) {
    logger.error("Failed to get SLA summary", { err });
    next(err);
  }
});

// ---- Endpoint Health ----

// GET /v1/alert-endpoints
alertOpsRouter.get("/v1/alert-endpoints", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const window = String(req.query?.window ?? "1h");
    if (window !== "1h" && window !== "24h") {
      return res.status(400).json({ error: "window must be 1h or 24h" });
    }

    const result = await listEndpointHealth({ tenantId, window: window as "1h" | "24h" });
    res.json(result);
  } catch (err) {
    logger.error("Failed to list endpoint health", { err });
    next(err);
  }
});

// ---- Alert Subscriptions v1 ----

// GET /v1/alert-subscriptions
alertOpsRouter.get("/v1/alert-subscriptions", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const userId = req.auth?.userId;
    const tenantId = req.auth?.tenantId;
    if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });

    const includeEntity = String(req.query?.include_entity ?? "false") === "true";
    const limitNum = parseSafeLimit(req.query?.limit, 50, 200);
    const cursor = req.query?.cursor;
    let cursorCreatedAt: string | null = null;
    let cursorId: string | null = null;

    if (cursor) {
      try {
        const decoded = Buffer.from(String(cursor), "base64").toString("utf8");
        const [createdAtIso, id] = decoded.split("|");
        const createdAtMs = Date.parse(createdAtIso);
        if (createdAtIso && id && !Number.isNaN(createdAtMs)) {
          cursorCreatedAt = createdAtIso;
          cursorId = id;
        } else {
          return res.status(400).json({ error: "Invalid cursor" });
        }
      } catch {
        return res.status(400).json({ error: "Invalid cursor" });
      }
    }

    const { items: rows } = await listAlertSubscriptionsPage({ tenantId, userId, cursorCreatedAt, cursorId, limit: limitNum });
    const summary = await getAlertSubscriptionsSummary({ tenantId, userId });

    let entityMap = new Map<string, { id: string; type: "port"; name: string; code: string; unlocode: string }>();
    if (includeEntity) {
      const ids = rows.map((row: any) => row.entityId).filter((id: any) => id && id !== GLOBAL_SCOPE_ENTITY_ID);
      if (ids.length) {
        const portRows = await db
          .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
          .from(ports)
          .where(inArray(ports.id, ids));
        for (const port of portRows) {
          entityMap.set(port.id, { id: port.id, type: "port", name: port.name, code: port.code ?? port.unlocode ?? "", unlocode: port.unlocode ?? port.code ?? "" });
        }
      }
    }

    const items = rows.map((row: any) => ({
      id: row.id,
      user_id: row.userId,
      scope: row.scope ?? "PORT",
      destination_type: row.channel,
      destination: row.endpoint,
      entity_type: row.entityType,
      entity_id: row.entityId,
      ...(includeEntity ? { entity: entityMap.get(row.entityId) ?? null } : {}),
      severity_min: row.severityMin,
      min_quality_band: row.minQualityBand ?? null,
      min_quality_score: row.minQualityScore ?? null,
      enabled: row.isEnabled,
      signature_version: row.signatureVersion,
      has_secret: Boolean(row.secret),
      created_at: row.createdAtRaw ?? row.createdAt,
      updated_at: row.updatedAt,
      last_test_at: row.lastTestAt ?? null,
      last_test_status: row.lastTestStatus ?? null,
    }));

    const last = rows.length > 0 ? rows[rows.length - 1] : null;
    const nextCursor = rows.length === limitNum && last?.createdAtRaw
      ? Buffer.from(`${last.createdAtRaw}|${last.id}`).toString("base64")
      : null;

    res.json({ version: "1", items, next_cursor: nextCursor, summary });
  } catch (err) {
    logger.error("Failed to list subscriptions", { err });
    next(err);
  }
});

// POST /v1/alert-subscriptions
alertOpsRouter.post("/v1/alert-subscriptions", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const allowHttp = process.env.NODE_ENV !== "production";
    const userId = req.auth?.userId;
    const tenantId = req.auth?.tenantId;
    if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });

    const destinationType = String(req.body?.destination_type ?? req.body?.channel ?? "WEBHOOK").toUpperCase();
    const destination = String(req.body?.destination ?? req.body?.endpoint ?? "").trim();
    const severityMin = String(req.body?.severity_min ?? "HIGH").toUpperCase();
    const minQualityBandRaw = req.body?.min_quality_band;
    const minQualityScoreRaw = req.body?.min_quality_score;
    const enabled = req.body?.enabled !== false;
    const signatureVersion = String(req.body?.signature_version ?? "v1");
    const providedSecret = req.body?.secret ? String(req.body.secret) : null;
    const scope = normalizeScope(req.body?.scope);
    let entityId = req.body?.entity_id ? String(req.body.entity_id) : null;

    if (!destination) return res.status(400).json({ error: "destination is required" });
    if (!["WEBHOOK", "EMAIL"].includes(destinationType)) {
      return res.status(400).json({ error: "destination_type must be WEBHOOK or EMAIL" });
    }
    if (!["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityMin)) {
      return res.status(400).json({ error: "invalid severity_min" });
    }

    let minQualityBand: string | null = null;
    if (minQualityBandRaw !== undefined && minQualityBandRaw !== null && String(minQualityBandRaw).trim() !== "") {
      const bandValue = String(minQualityBandRaw).toUpperCase();
      if (!["LOW", "MEDIUM", "HIGH"].includes(bandValue)) {
        return res.status(400).json({ error: "invalid min_quality_band" });
      }
      minQualityBand = bandValue;
    }

    let minQualityScore: number | null = null;
    if (minQualityScoreRaw !== undefined && minQualityScoreRaw !== null && String(minQualityScoreRaw).trim() !== "") {
      const scoreValue = Number(minQualityScoreRaw);
      if (!Number.isInteger(scoreValue) || scoreValue < 0 || scoreValue > 100) {
        return res.status(400).json({ error: "invalid min_quality_score" });
      }
      minQualityScore = scoreValue;
    }

    if (destinationType === "WEBHOOK") {
      if (!isValidWebhookUrl(destination, allowHttp)) {
        return res.status(400).json({ error: "invalid webhook url" });
      }
    } else {
      const email = normalizeEmail(destination);
      if (!isValidEmail(email)) return res.status(400).json({ error: "invalid email" });
    }

    const secret = destinationType === "WEBHOOK" ? (providedSecret ?? generateSecret()) : generateSecret();

    if (scope === "GLOBAL") {
      entityId = GLOBAL_SCOPE_ENTITY_ID;
    } else if (!entityId) {
      return res.status(400).json({ error: "entity_id is required when scope=PORT" });
    }

    const [created] = await db
      .insert(alertSubscriptions)
      .values({
        tenantId,
        userId,
        scope,
        entityType: "port",
        entityId: entityId!,
        severityMin,
        minQualityBand,
        minQualityScore,
        channel: destinationType,
        endpoint: destinationType === "EMAIL" ? normalizeEmail(destination) : destination,
        secret,
        signatureVersion,
        isEnabled: enabled,
        updatedAt: new Date(),
      })
      .returning();

    await writeAuditEvent(req.auditContext, {
      action: "ALERT.SUBSCRIPTION_CREATED",
      resourceType: "ALERT_SUBSCRIPTION",
      resourceId: created.id,
      severity: "INFO",
      status: "SUCCESS",
      message: "Alert subscription created",
      metadata: { channel: created.channel, scope: created.scope ?? "PORT", entity_id: created.entityId },
    }).catch(() => { });

    res.status(201).json({
      version: "1",
      id: created.id,
      user_id: created.userId,
      scope: created.scope ?? "PORT",
      destination_type: created.channel,
      destination: created.endpoint,
      entity_type: created.entityType,
      entity_id: created.entityId,
      severity_min: created.severityMin,
      min_quality_band: created.minQualityBand ?? null,
      min_quality_score: created.minQualityScore ?? null,
      enabled: created.isEnabled,
      signature_version: created.signatureVersion,
      has_secret: Boolean(created.secret),
      created_at: created.createdAt?.toISOString?.() ?? null,
      updated_at: created.updatedAt?.toISOString?.() ?? null,
    });
  } catch (err) {
    logger.error("Failed to create subscription", { err });
    next(err);
  }
});

// PATCH /v1/alert-subscriptions/:id
alertOpsRouter.patch("/v1/alert-subscriptions/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const allowHttp = process.env.NODE_ENV !== "production";
    const id = req.params.id;
    const userId = req.auth?.userId;
    const tenantId = req.auth?.tenantId;
    if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });

    const [existing] = await db
      .select()
      .from(alertSubscriptions)
      .where(and(eq(alertSubscriptions.id, id), eq(alertSubscriptions.userId, userId), eq(alertSubscriptions.tenantId, tenantId)))
      .limit(1);
    if (!existing) return res.status(404).json({ error: "subscription not found" });

    const updates: any = { updatedAt: new Date() };

    if (req.body?.scope) {
      const scope = normalizeScope(req.body.scope);
      updates.scope = scope;
      if (scope === "GLOBAL") updates.entityId = GLOBAL_SCOPE_ENTITY_ID;
      if (scope === "PORT" && !req.body?.entity_id) {
        return res.status(400).json({ error: "entity_id is required when scope=PORT" });
      }
    }

    if (req.body?.entity_id) updates.entityId = String(req.body.entity_id);

    if (req.body?.severity_min) {
      const severityMin = String(req.body.severity_min).toUpperCase();
      if (!["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityMin)) {
        return res.status(400).json({ error: "invalid severity_min" });
      }
      updates.severityMin = severityMin;
    }

    if (req.body?.min_quality_band !== undefined) {
      const rawBand = req.body.min_quality_band;
      if (rawBand === null || String(rawBand).trim() === "") {
        updates.minQualityBand = null;
      } else {
        const bandValue = String(rawBand).toUpperCase();
        if (!["LOW", "MEDIUM", "HIGH"].includes(bandValue)) {
          return res.status(400).json({ error: "invalid min_quality_band" });
        }
        updates.minQualityBand = bandValue;
      }
    }

    if (req.body?.min_quality_score !== undefined) {
      const rawScore = req.body.min_quality_score;
      if (rawScore === null || String(rawScore).trim() === "") {
        updates.minQualityScore = null;
      } else {
        const scoreValue = Number(rawScore);
        if (!Number.isInteger(scoreValue) || scoreValue < 0 || scoreValue > 100) {
          return res.status(400).json({ error: "invalid min_quality_score" });
        }
        updates.minQualityScore = scoreValue;
      }
    }

    if (req.body?.enabled !== undefined) updates.isEnabled = Boolean(req.body.enabled);

    if (req.body?.destination) {
      const dest = String(req.body.destination).trim();
      if (existing.channel === "WEBHOOK") {
        if (!isValidWebhookUrl(dest, allowHttp)) return res.status(400).json({ error: "invalid webhook url" });
        updates.endpoint = dest;
      } else {
        const email = normalizeEmail(dest);
        if (!isValidEmail(email)) return res.status(400).json({ error: "invalid email" });
        updates.endpoint = email;
      }
    }

    const [updated] = await db
      .update(alertSubscriptions)
      .set(updates)
      .where(and(eq(alertSubscriptions.id, id), eq(alertSubscriptions.userId, userId), eq(alertSubscriptions.tenantId, tenantId)))
      .returning();

    if (!updated) return res.status(404).json({ error: "subscription not found" });

    let action = "ALERT.SUBSCRIPTION_UPDATED";
    if (updates.isEnabled === false) action = "ALERT.SUBSCRIPTION_DISABLED";
    if (updates.isEnabled === true) action = "ALERT.SUBSCRIPTION_ENABLED";

    await writeAuditEvent(req.auditContext, {
      action,
      resourceType: "ALERT_SUBSCRIPTION",
      resourceId: updated.id,
      severity: "INFO",
      status: "SUCCESS",
      message: "Alert subscription updated",
      metadata: { fields: Object.keys(updates) },
    }).catch(() => { });

    res.json({
      version: "1",
      id: updated.id,
      user_id: updated.userId,
      destination_type: updated.channel,
      destination: updated.endpoint,
      severity_min: updated.severityMin,
      min_quality_band: updated.minQualityBand ?? null,
      min_quality_score: updated.minQualityScore ?? null,
      enabled: updated.isEnabled,
      signature_version: updated.signatureVersion,
      has_secret: Boolean(updated.secret),
      created_at: updated.createdAt?.toISOString?.() ?? null,
      updated_at: updated.updatedAt?.toISOString?.() ?? null,
      last_test_at: (updated as any).lastTestAt?.toISOString?.() ?? null,
      last_test_status: (updated as any).lastTestStatus ?? null,
      last_test_error: (updated as any).lastTestError ?? null,
    });
  } catch (err) {
    logger.error("Failed to update subscription", { err });
    next(err);
  }
});

// POST /v1/alert-subscriptions/:id/rotate-secret
alertOpsRouter.post("/v1/alert-subscriptions/:id/rotate-secret", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const id = req.params.id;
    const userId = req.auth?.userId;
    const tenantId = req.auth?.tenantId;
    if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });

    const newSecret = generateSecret();
    const [updated] = await db
      .update(alertSubscriptions)
      .set({ secret: newSecret, updatedAt: new Date() })
      .where(and(eq(alertSubscriptions.id, id), eq(alertSubscriptions.userId, userId), eq(alertSubscriptions.tenantId, tenantId)))
      .returning();

    if (!updated) return res.status(404).json({ error: "subscription not found" });

    await writeAuditEvent(req.auditContext, {
      action: "ALERT.SUBSCRIPTION_SECRET_ROTATED",
      resourceType: "ALERT_SUBSCRIPTION",
      resourceId: updated.id,
      severity: "SECURITY",
      status: "SUCCESS",
      message: "Subscription secret rotated",
    }).catch(() => { });

    res.json({ version: "1", id: updated.id, new_secret: newSecret });
  } catch (err) {
    logger.error("Failed to rotate subscription secret", { err });
    next(err);
  }
});
