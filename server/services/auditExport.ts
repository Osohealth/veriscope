import { createHmac, timingSafeEqual } from "node:crypto";
import { desc, eq, sql } from "drizzle-orm";
import { db } from "../db";
import { auditEvents, auditExports } from "@shared/schema";
import { buildAuditEventsWhere, type AuditEventFilters } from "./auditEvents";

const DEFAULT_SECRET = "dev-audit-export-secret";
const DEFAULT_EXP_MINUTES = 30;

const resolveExportSecret = () => {
  const secret = process.env.AUDIT_EXPORT_SECRET || process.env.API_KEY_PEPPER;
  if (!secret) {
    if (process.env.NODE_ENV === "development") return DEFAULT_SECRET;
    throw new Error("AUDIT_EXPORT_SECRET is required");
  }
  return secret;
};

export const signExportToken = (tenantId: string, exportId: string, exp: number) => {
  const payload = `${tenantId}|${exportId}|${exp}`;
  const secret = resolveExportSecret();
  const sig = createHmac("sha256", secret).update(payload).digest("hex");
  return `${exp}.${sig}`;
};

export const verifyExportToken = (tenantId: string, exportId: string, token: string) => {
  const [expPart, sig] = token.split(".");
  if (!expPart || !sig) return { ok: false };
  const exp = Number(expPart);
  if (!Number.isFinite(exp)) return { ok: false };
  if (Date.now() > exp * 1000) return { ok: false };
  const expected = signExportToken(tenantId, exportId, exp).split(".")[1];
  const sigBuf = Buffer.from(sig);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length) return { ok: false };
  if (!timingSafeEqual(sigBuf, expBuf)) return { ok: false };
  return { ok: true, exp };
};

export async function createAuditExport(options: {
  tenantId: string;
  userId: string;
  format: "csv" | "jsonl";
  filters: AuditEventFilters;
  maxRows: number;
}) {
  const now = new Date();
  const expiresAt = new Date(now.getTime() + DEFAULT_EXP_MINUTES * 60 * 1000);
  const [row] = await db
    .insert(auditExports)
    .values({
      tenantId: options.tenantId,
      requestedByUserId: options.userId,
      format: options.format,
      filters: options.filters,
      rowCount: options.maxRows,
      createdAt: now,
      expiresAt,
    })
    .returning();

  const exp = Math.floor(expiresAt.getTime() / 1000);
  const token = signExportToken(options.tenantId, row.id, exp);

  return {
    exportId: row.id,
    expiresAt: row.expiresAt,
    token,
  };
}

export async function getAuditExport(exportId: string, tenantId: string) {
  const [row] = await db
    .select()
    .from(auditExports)
    .where(eq(auditExports.id, exportId))
    .limit(1);
  if (!row || row.tenantId !== tenantId) return null;
  return row;
}

export async function fetchAuditEventsForExport(filters: AuditEventFilters, limit: number) {
  const whereClause = buildAuditEventsWhere(filters);
  const rows = await db
    .select()
    .from(auditEvents)
    .where(whereClause)
    .orderBy(desc(auditEvents.createdAt), desc(auditEvents.id))
    .limit(limit);
  return rows;
}

const escapeCsv = (value: string) => {
  if (value.includes('"') || value.includes(",") || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
};

export const auditEventsToCsv = (rows: any[]) => {
  const header = [
    "id",
    "tenant_id",
    "actor_type",
    "actor_user_id",
    "actor_api_key_id",
    "actor_label",
    "action",
    "resource_type",
    "resource_id",
    "severity",
    "status",
    "message",
    "metadata",
    "ip",
    "user_agent",
    "request_id",
    "created_at",
  ];
  const lines = [header.join(",")];
  for (const row of rows) {
    const values = [
      row.id,
      row.tenantId,
      row.actorType,
      row.actorUserId ?? "",
      row.actorApiKeyId ?? "",
      row.actorLabel ?? "",
      row.action,
      row.resourceType,
      row.resourceId ?? "",
      row.severity,
      row.status,
      row.message ?? "",
      JSON.stringify(row.metadata ?? {}),
      row.ip ?? "",
      row.userAgent ?? "",
      row.requestId ?? "",
      row.createdAt instanceof Date ? row.createdAt.toISOString() : row.createdAt ?? "",
    ];
    lines.push(values.map((value) => escapeCsv(String(value ?? ""))).join(","));
  }
  return lines.join("\n");
};

export const auditEventsToJsonl = (rows: any[]) =>
  rows.map((row) => JSON.stringify({
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
    created_at: row.createdAt instanceof Date ? row.createdAt.toISOString() : row.createdAt ?? null,
  })).join("\n");
