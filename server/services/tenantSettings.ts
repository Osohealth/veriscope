import { eq } from "drizzle-orm";
import { db } from "../db";
import { tenantSettings } from "@shared/schema";

const DEFAULT_RETENTION_DAYS = 90;
const MIN_RETENTION_DAYS = 7;
const MAX_RETENTION_DAYS = 3650;

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
    };
  }

  return {
    tenant_id: row.tenantId,
    audit_retention_days: row.auditRetentionDays ?? DEFAULT_RETENTION_DAYS,
  };
}

export async function upsertTenantSettings(tenantId: string, patch: { audit_retention_days?: unknown }) {
  const auditRetentionDays = clampRetentionDays(patch.audit_retention_days);
  const now = new Date();
  const [row] = await db
    .insert(tenantSettings)
    .values({
      tenantId,
      auditRetentionDays,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: tenantSettings.tenantId,
      set: { auditRetentionDays, updatedAt: now },
    })
    .returning();

  return {
    tenant_id: row.tenantId,
    audit_retention_days: row.auditRetentionDays,
  };
}
