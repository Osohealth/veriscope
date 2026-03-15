import { eq } from "drizzle-orm";
import { db } from "../db";
import { tenantSettings } from "@shared/schema";

const DEFAULT_RETENTION_DAYS = 90;
const MIN_RETENTION_DAYS = 7;
const MAX_RETENTION_DAYS = 3650;
const DEFAULT_ALLOWLIST: string[] = [];

export const clampRetentionDays = (value: unknown) => {
  const parsed = typeof value === "string" ? Number(value) : Number(value);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) {
    throw new Error("audit_retention_days must be an integer");
  }
  if (parsed < MIN_RETENTION_DAYS || parsed > MAX_RETENTION_DAYS) {
    throw new Error(`audit_retention_days must be between ${MIN_RETENTION_DAYS} and ${MAX_RETENTION_DAYS}`);
  }
  return parsed;
};

const normalizeAllowlist = (value: unknown, label: string) => {
  if (value === undefined) return undefined;
  if (value === null) return [];
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array of strings`);
  }
  const cleaned = value
    .map((entry) => String(entry).trim().toLowerCase())
    .filter((entry) => entry.length > 0);
  return Array.from(new Set(cleaned));
};

export async function getTenantSettings(tenantId: string) {
  const [row] = await db
    .select()
    .from(tenantSettings)
    .where(eq(tenantSettings.tenantId, tenantId))
    .limit(1);

  if (!row) {
    return {
      tenant_id: tenantId,
      audit_retention_days: DEFAULT_RETENTION_DAYS,
      allowed_email_domains: DEFAULT_ALLOWLIST,
      allowed_webhook_hosts: DEFAULT_ALLOWLIST,
    };
  }

  return {
    tenant_id: row.tenantId,
    audit_retention_days: row.auditRetentionDays ?? DEFAULT_RETENTION_DAYS,
    allowed_email_domains: row.allowedEmailDomains ?? DEFAULT_ALLOWLIST,
    allowed_webhook_hosts: row.allowedWebhookHosts ?? DEFAULT_ALLOWLIST,
  };
}

export async function upsertTenantSettings(tenantId: string, patch: {
  audit_retention_days?: unknown;
  allowed_email_domains?: unknown;
  allowed_webhook_hosts?: unknown;
}) {
  const current = await getTenantSettings(tenantId);
  const auditRetentionDays = patch.audit_retention_days === undefined
    ? current.audit_retention_days
    : clampRetentionDays(patch.audit_retention_days);
  const allowedEmailDomains = normalizeAllowlist(patch.allowed_email_domains, "allowed_email_domains")
    ?? current.allowed_email_domains;
  const allowedWebhookHosts = normalizeAllowlist(patch.allowed_webhook_hosts, "allowed_webhook_hosts")
    ?? current.allowed_webhook_hosts;
  const now = new Date();
  const [row] = await db
    .insert(tenantSettings)
    .values({
      tenantId,
      auditRetentionDays,
      allowedEmailDomains,
      allowedWebhookHosts,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: tenantSettings.tenantId,
      set: { auditRetentionDays, allowedEmailDomains, allowedWebhookHosts, updatedAt: now },
    })
    .returning();

  return {
    tenant_id: row.tenantId,
    audit_retention_days: row.auditRetentionDays,
    allowed_email_domains: row.allowedEmailDomains ?? DEFAULT_ALLOWLIST,
    allowed_webhook_hosts: row.allowedWebhookHosts ?? DEFAULT_ALLOWLIST,
  };
}

export async function getTenantAllowlist(tenantId: string) {
  const settings = await getTenantSettings(tenantId);
  return {
    allowed_email_domains: settings.allowed_email_domains ?? DEFAULT_ALLOWLIST,
    allowed_webhook_hosts: settings.allowed_webhook_hosts ?? DEFAULT_ALLOWLIST,
  };
}
