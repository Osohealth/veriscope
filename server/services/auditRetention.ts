import { sql } from "drizzle-orm";
import { db } from "../db";
import { auditEvents, tenantSettings } from "@shared/schema";

const DEFAULT_RETENTION_DAYS = 90;

type PurgeOptions = {
  tenantId: string;
  retentionDays: number;
  batchSize?: number;
  maxBatches?: number;
};

export async function purgeAuditEventsForTenant(options: PurgeOptions) {
  const batchSize = Math.min(Math.max(options.batchSize ?? 5000, 1), 50000);
  const maxBatches = Math.min(Math.max(options.maxBatches ?? 20, 1), 100);
  const cutoff = new Date(Date.now() - options.retentionDays * 24 * 60 * 60 * 1000);
  let deletedTotal = 0;

  for (let i = 0; i < maxBatches; i += 1) {
    const result = await db.execute(sql`
      WITH rows AS (
        SELECT id
        FROM ${auditEvents}
        WHERE tenant_id = ${options.tenantId}
          AND created_at < ${cutoff}
        ORDER BY created_at ASC
        LIMIT ${batchSize}
      )
      DELETE FROM ${auditEvents}
      WHERE id IN (SELECT id FROM rows)
      RETURNING id;
    `);

    const deletedCount = Array.isArray(result?.rows) ? result.rows.length : 0;
    deletedTotal += deletedCount;
    if (deletedCount < batchSize) break;
  }

  return { deleted: deletedTotal };
}

export async function runAuditRetentionPurge(options?: { batchSizeTotal?: number; tenantId?: string }) {
  const batchSizeTotal = Math.min(Math.max(options?.batchSizeTotal ?? 20000, 1), 200000);
  const tenantIds: string[] = [];

  if (options?.tenantId) {
    tenantIds.push(options.tenantId);
  } else {
    const rows = await db
      .select({ tenantId: tenantSettings.tenantId })
      .from(tenantSettings);
    for (const row of rows) tenantIds.push(row.tenantId);
    if (tenantIds.length === 0) {
      const distinctRows = await db.execute(sql`SELECT DISTINCT tenant_id FROM ${auditEvents}`) as {
        rows?: { tenant_id?: string | null }[];
      };
      for (const row of distinctRows.rows ?? []) {
        if (row.tenant_id) tenantIds.push(row.tenant_id);
      }
    }
  }

  let deletedTotal = 0;
  let tenantsProcessed = 0;

  for (const tenantId of tenantIds) {
    const [setting] = await db
      .select({ auditRetentionDays: tenantSettings.auditRetentionDays })
      .from(tenantSettings)
      .where(sql`${tenantSettings.tenantId} = ${tenantId}`)
      .limit(1);
    const retentionDays = setting?.auditRetentionDays ?? DEFAULT_RETENTION_DAYS;
    const result = await purgeAuditEventsForTenant({
      tenantId,
      retentionDays,
      batchSize: Math.min(5000, batchSizeTotal),
    });
    tenantsProcessed += 1;
    deletedTotal += result.deleted;
  }

  return { tenantsProcessed, deletedTotal };
}
