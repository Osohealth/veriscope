import { Router } from "express";
import { and, eq } from "drizzle-orm";
import { db } from "../db";
import { auditEvents } from "@shared/schema";
import { authenticateApiKey } from "../middleware/apiKeyAuth";
import { requireRole } from "../auth/requireRole";
import { writeAuditEvent } from "../services/auditLog";
import { listAuditEvents, getAuditEventsSummary } from "../services/auditEvents";
import {
  createAuditExport,
  getAuditExport,
  fetchAuditEventsForExport,
  verifyExportToken,
  auditEventsToCsv,
  auditEventsToJsonl,
} from "../services/auditExport";
import { getTenantSettings, upsertTenantSettings } from "../services/tenantSettings";
import { logger } from "../middleware/observability";
import { parseSafeLimit } from "../utils/pagination";

export const auditRouter = Router();

const isValidUuid = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

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

// GET /v1/tenant-settings
auditRouter.get("/v1/tenant-settings", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OPERATOR"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });
    const settings = await getTenantSettings(tenantId);
    res.json({ version: "1", ...settings });
  } catch (err) {
    logger.error("Failed to load tenant settings", { err });
    next(err);
  }
});

// PATCH /v1/tenant-settings
auditRouter.patch("/v1/tenant-settings", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const current = await getTenantSettings(tenantId);
    const updated = await upsertTenantSettings(tenantId, {
      audit_retention_days: req.body?.audit_retention_days,
      allowed_email_domains: req.body?.allowed_email_domains,
      allowed_webhook_hosts: req.body?.allowed_webhook_hosts,
    });

    await writeAuditEvent(req.auditContext, {
      action: "SETTINGS.AUDIT_RETENTION_UPDATED",
      resourceType: "TENANT_SETTINGS",
      resourceId: tenantId,
      severity: "SECURITY",
      status: "SUCCESS",
      message: "Audit retention updated",
      metadata: {
        from_days: current.audit_retention_days,
        to_days: updated.audit_retention_days,
        allowed_email_domains: updated.allowed_email_domains,
        allowed_webhook_hosts: updated.allowed_webhook_hosts,
      },
    });

    res.json({ version: "1", ...updated });
  } catch (err: any) {
    res.status(400).json({ error: "Failed to update tenant settings" });
  }
});

// GET /v1/audit-events
auditRouter.get("/v1/audit-events", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const daysRaw = Number(req.query?.days ?? 7);
    const days = Number.isFinite(daysRaw) ? Math.min(Math.max(daysRaw, 1), 365) : 7;
    const limitNum = parseSafeLimit(req.query?.limit, 50, 200);

    const action = req.query?.action ? String(req.query.action).toUpperCase() : undefined;
    let resourceType = req.query?.resource_type ? String(req.query.resource_type).toUpperCase() : undefined;
    if (resourceType === "DESTINATION" || resourceType === "ALERT_DESTINATION") resourceType = "ALERT_DESTINATION";
    const resourceId = req.query?.resource_id ? String(req.query.resource_id) : undefined;
    const actor = req.query?.actor ? String(req.query.actor) : undefined;
    const severityMin = req.query?.severity_min ? String(req.query.severity_min).toUpperCase() : undefined;

    if (resourceId && resourceId.length > 200) return res.status(400).json({ error: "Invalid resource_id" });
    if (severityMin && !["INFO", "WARN", "SECURITY"].includes(severityMin)) {
      return res.status(400).json({ error: "Invalid severity_min" });
    }

    let cursorCreatedAt: string | null = null;
    let cursorId: string | null = null;
    const cursor = req.query?.cursor;
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

    const { items } = await listAuditEvents({ tenantId, days, action, resourceType, resourceId, actor, severityMin, cursorCreatedAt, cursorId, limit: limitNum });
    const summary = await getAuditEventsSummary({ tenantId, days, action, resourceType, resourceId, actor, severityMin });

    const mapped = items.map((row) => ({
      id: row.id,
      tenant_id: row.tenantId,
      actor_type: row.actorType,
      actor_user_id: row.actorUserId ?? null,
      actor_api_key_id: row.actorApiKeyId ?? null,
      actor_label: row.actorLabel ?? null,
      action: row.action,
      resource_type: row.resourceType,
      resource_id: row.resourceId ?? null,
      severity: row.severity,
      status: row.status,
      message: row.message,
      metadata: row.metadata ?? {},
      ip: row.ip ?? null,
      user_agent: row.userAgent ?? null,
      request_id: row.requestId ?? null,
      created_at: row.createdAt,
    }));

    const last = items.length > 0 ? items[items.length - 1] : null;
    const nextCursor =
      items.length === limitNum && last?.createdAt
        ? Buffer.from(`${new Date(last.createdAt as any).toISOString()}|${last.id}`).toString("base64")
        : null;

    res.json({ version: "1", items: mapped, next_cursor: nextCursor, summary });
  } catch (err) {
    logger.error("Failed to fetch audit events", { err });
    next(err);
  }
});

// GET /v1/audit-events/export/:id/download — must come BEFORE /:id
auditRouter.get("/v1/audit-events/export/:id/download", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const exportId = req.params.id;
    const token = String(req.query?.token ?? "");
    if (!token) return res.status(401).json({ error: "token required" });

    const exportRow = await getAuditExport(exportId, tenantId);
    if (!exportRow) return res.status(404).json({ error: "Export not found" });

    const tokenOk = verifyExportToken(tenantId, exportId, token);
    if (!tokenOk.ok) return res.status(401).json({ error: "Invalid token" });
    if (exportRow.expiresAt && new Date(exportRow.expiresAt).getTime() < Date.now()) {
      return res.status(410).json({ error: "Export expired" });
    }

    const filters = exportRow.filters as any;
    const rows = await fetchAuditEventsForExport(
      {
        tenantId,
        days: filters?.days,
        action: filters?.action,
        resourceType: filters?.resourceType,
        resourceId: filters?.resourceId,
        actor: filters?.actor,
        severityMin: filters?.severityMin,
      },
      exportRow.rowCount,
    );

    if (exportRow.format === "csv") {
      const csv = auditEventsToCsv(rows);
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="audit-events-${exportId}.csv"`);
      return res.send(csv);
    }

    const jsonl = auditEventsToJsonl(rows);
    res.setHeader("Content-Type", "application/x-ndjson");
    res.setHeader("Content-Disposition", `attachment; filename="audit-events-${exportId}.jsonl"`);
    return res.send(jsonl);
  } catch (err) {
    logger.error("Failed to download export", { err });
    next(err);
  }
});

// GET /v1/audit-events/:id
auditRouter.get("/v1/audit-events/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });
    const id = req.params.id;
    if (!isValidUuid(id)) return res.status(400).json({ error: "Invalid id" });

    const [row] = await db
      .select()
      .from(auditEvents)
      .where(and(eq(auditEvents.id, id), eq(auditEvents.tenantId, tenantId)))
      .limit(1);
    if (!row) return res.status(404).json({ error: "Audit event not found" });

    res.json({
      version: "1",
      item: {
        id: row.id,
        tenant_id: row.tenantId,
        actor_type: row.actorType,
        actor_user_id: row.actorUserId ?? null,
        actor_api_key_id: row.actorApiKeyId ?? null,
        actor_label: row.actorLabel ?? null,
        action: row.action,
        resource_type: row.resourceType,
        resource_id: row.resourceId ?? null,
        severity: row.severity,
        status: row.status,
        message: row.message,
        metadata: row.metadata ?? {},
        ip: row.ip ?? null,
        user_agent: row.userAgent ?? null,
        request_id: row.requestId ?? null,
        created_at: row.createdAt,
      },
    });
  } catch (err) {
    logger.error("Failed to fetch audit event", { err });
    next(err);
  }
});

// POST /v1/audit-events/export
auditRouter.post("/v1/audit-events/export", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const daysRaw = Number(req.body?.days ?? 7);
    const days = Number.isFinite(daysRaw) ? Math.min(Math.max(daysRaw, 1), 365) : 7;
    const format = String(req.body?.format ?? "csv").toLowerCase();
    if (!["csv", "jsonl"].includes(format)) return res.status(400).json({ error: "format must be csv or jsonl" });

    const action = req.body?.action ? String(req.body.action).toUpperCase() : undefined;
    let resourceType = req.body?.resource_type ? String(req.body.resource_type).toUpperCase() : undefined;
    if (resourceType === "DESTINATION" || resourceType === "ALERT_DESTINATION") resourceType = "ALERT_DESTINATION";
    const resourceId = req.body?.resource_id ? String(req.body.resource_id) : undefined;
    const actor = req.body?.actor ? String(req.body.actor) : undefined;
    const severityMin = req.body?.severity_min ? String(req.body.severity_min).toUpperCase() : undefined;

    if (resourceId && resourceId.length > 200) return res.status(400).json({ error: "Invalid resource_id" });
    if (severityMin && !["INFO", "WARN", "SECURITY"].includes(severityMin)) {
      return res.status(400).json({ error: "Invalid severity_min" });
    }

    const maxRowsRaw = Number(req.body?.max_rows ?? 5000);
    const maxRows = Number.isFinite(maxRowsRaw) ? Math.min(Math.max(Math.floor(maxRowsRaw), 1), 50000) : 5000;

    const filters = { tenantId, days, action, resourceType, resourceId, actor, severityMin };
    const summary = await getAuditEventsSummary(filters);
    if (summary.total > maxRows) return res.status(400).json({ error: "Export too large; narrow filters." });

    const rowCount = Math.min(summary.total, maxRows);
    const exportRow = await createAuditExport({ tenantId, userId, format: format as "csv" | "jsonl", filters, maxRows: rowCount });

    await writeAuditEvent(req.auditContext, {
      action: "AUDIT.EXPORT_REQUESTED",
      resourceType: "AUDIT_EVENTS",
      severity: "SECURITY",
      status: "SUCCESS",
      message: "Audit export requested",
      metadata: { format, filters: { days, action, resourceType, resourceId, actor, severityMin }, max_rows: maxRows, returned_rows: rowCount },
    });

    const downloadUrl = `/v1/audit-events/export/${exportRow.exportId}/download?token=${exportRow.token}`;
    res.json({ version: "1", export_id: exportRow.exportId, status: "READY", download_url: downloadUrl });
  } catch (err) {
    logger.error("Failed to export audit events", { err });
    next(err);
  }
});
