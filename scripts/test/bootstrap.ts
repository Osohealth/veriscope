import { randomUUID } from "node:crypto";
import { eq, inArray, sql } from "drizzle-orm";
import { db, pool } from "../../server/db";
import {
  alertDedupe,
  alertDeliveries,
  alertDeliveryAttempts,
  alertDeliverySlaWindows,
  alertDestinationStates,
  alertDlq,
  alertEndpointHealth,
  alertNoiseBudgetBreaches,
  alertNoiseBudgets,
  alertQualityGateBreaches,
  alertRuns,
  alertSlaThresholds,
  alertSubscriptions,
  apiKeys,
  auditEvents,
  auditExports,
  incidentEscalationPolicies,
  incidentEscalations,
  incidents,
  portDailyBaselines,
  ports,
  signals,
  tenantSettings,
  tenantUsers,
  userContactMethods,
} from "@shared/schema";
export {
  ensureAlertingOnlySchema,
  ensureAlertingSchema,
  ensureCoreSchema,
  ensureEscalationsOnlySchema,
  ensureEscalationsSchema,
  ensureIncidentsSchema,
  ensureSignalsSchema,
  ensureTestSchema,
} from "./schema";

export const TEST_TENANT_ID = randomUUID();
export const TEST_USER_ID = randomUUID();
export const TEST_API_KEY = "vs_test_key";
export const TEST_API_KEY_PEPPER = "test-pepper";

export function applyTestEnv() {
  process.env.WEBHOOK_RETRY_ATTEMPTS = "1";
  process.env.WEBHOOK_TIMEOUT_MS = "50";
  process.env.DLQ_MAX_ATTEMPTS = "1";
  process.env.ALERT_RATE_LIMIT_PER_ENDPOINT = "9999";
  process.env.API_KEY_PEPPER = TEST_API_KEY_PEPPER;
  delete process.env.ALERTS_API_KEY;
  delete process.env.ALERTS_USER_ID;
}

export function deterministicNow(offsetMinutes = 0, baseIso = "2026-01-01T00:00:00Z") {
  const base = new Date(baseIso);
  base.setMinutes(base.getMinutes() + offsetMinutes);
  return base;
}

export async function withTransientRetry<T>(fn: () => Promise<T>, label: string, retries = 2) {
  const debug = process.env.TEST_DEBUG === "1";
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await fn();
    } catch (error: any) {
      const message = String(error?.message ?? "");
      if (message.includes("Connection terminated unexpectedly") && attempt < retries) {
        if (debug) {
          console.log(`TEST_STEP: retry ${label} after connection drop (${attempt + 1}/${retries})`);
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
        continue;
      }
      throw error;
    }
  }
  throw new Error(`Retry failed for ${label}`);
}

export async function enablePgCrypto() {
  await db.execute(sql`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);
}

type ApiKeyInsert = typeof apiKeys.$inferInsert;
type TenantUserInsert = typeof tenantUsers.$inferInsert;

export async function insertApiKeys(
  values: ApiKeyInsert | ApiKeyInsert[],
  options: { returning?: boolean } = {},
) {
  const rows = Array.isArray(values) ? values : [values];
  const insert = db.insert(apiKeys).values(rows);
  return options.returning ? insert.returning() : insert;
}

export async function insertTenantUsers(
  values: TenantUserInsert | TenantUserInsert[],
  options: { returning?: boolean } = {},
) {
  const rows = Array.isArray(values) ? values : [values];
  const insert = db.insert(tenantUsers).values(rows);
  return options.returning ? insert.returning() : insert;
}

export type AdvisoryLockHandle = {
  tenantId: string;
  key: string;
  client: Awaited<ReturnType<typeof pool.connect>>;
  locked: boolean;
};

export async function tryAdvisoryLock(tenantId: string, key: string): Promise<AdvisoryLockHandle> {
  const client = await pool.connect();
  const lockResult = await client.query(
    "SELECT pg_try_advisory_lock(hashtext($1), hashtext($2)) AS locked",
    [tenantId, key],
  );
  return { tenantId, key, client, locked: Boolean(lockResult.rows?.[0]?.locked) };
}

export async function releaseAdvisoryLock(handle: AdvisoryLockHandle) {
  try {
    await handle.client.query(
      "SELECT pg_advisory_unlock(hashtext($1), hashtext($2))",
      [handle.tenantId, handle.key],
    );
  } finally {
    handle.client.release();
  }
}

export async function cleanDatabase(ctx: {
  portIds?: string[];
  missingPortId?: string;
  alertSubscriptionUserIds?: string[];
  tenantIds?: string[];
} = {}) {
  const timingEnabled = process.env.TEST_TIMING === "1";
  const cleanupStart = timingEnabled ? process.hrtime.bigint() : null;
  const portIds = ctx.portIds ?? [];
  const missingPortId = ctx.missingPortId;
  const subscriptionUsers = ctx.alertSubscriptionUserIds ?? [];
  const tenantIds = ctx.tenantIds ?? [];
  const hasTenantIds = tenantIds.length > 0;

  try {
    if (portIds.length > 0 || missingPortId) {
      const signalIds = missingPortId ? [...portIds, missingPortId] : portIds;
      if (signalIds.length > 0) {
        await db.delete(signals).where(inArray(signals.entityId, signalIds));
      }
      if (portIds.length > 0) {
        await db.execute(sql`DELETE FROM storage_fill_data`);
        for (const portId of portIds) {
          await db.execute(sql`DELETE FROM port_stats WHERE port_id = ${portId}`);
          await db.execute(sql`DELETE FROM port_calls WHERE port_id = ${portId}`);
          await db.execute(sql`DELETE FROM storage_facilities WHERE port_id = ${portId}`);
        }
      }
      if (portIds.length > 0) {
        await db.delete(portDailyBaselines).where(inArray(portDailyBaselines.portId, portIds));
        await db.delete(ports).where(inArray(ports.id, portIds));
      }
    }
    if (hasTenantIds) {
      await db.delete(alertDeliveryAttempts).where(inArray(alertDeliveryAttempts.tenantId, tenantIds));
      await db.delete(alertDlq).where(inArray(alertDlq.tenantId, tenantIds));
      await db.delete(alertDeliveries).where(inArray(alertDeliveries.tenantId, tenantIds));
      await db.delete(alertDeliverySlaWindows).where(inArray(alertDeliverySlaWindows.tenantId, tenantIds));
      await db.delete(alertSlaThresholds).where(inArray(alertSlaThresholds.tenantId, tenantIds));
      await db.delete(alertEndpointHealth).where(inArray(alertEndpointHealth.tenantId, tenantIds));
      await db.delete(alertDestinationStates).where(inArray(alertDestinationStates.tenantId, tenantIds));
      await db.delete(alertNoiseBudgetBreaches).where(inArray(alertNoiseBudgetBreaches.tenantId, tenantIds));
      await db.delete(alertQualityGateBreaches).where(inArray(alertQualityGateBreaches.tenantId, tenantIds));
      await db.delete(alertNoiseBudgets).where(inArray(alertNoiseBudgets.tenantId, tenantIds));
      await db.delete(alertRuns).where(inArray(alertRuns.tenantId, tenantIds));
      await db.delete(alertSubscriptions).where(inArray(alertSubscriptions.tenantId, tenantIds));
      await db.delete(alertDedupe).where(inArray(alertDedupe.tenantId, tenantIds));
      await db.delete(auditEvents).where(inArray(auditEvents.tenantId, tenantIds));
      await db.delete(auditExports).where(inArray(auditExports.tenantId, tenantIds));
      await db.delete(incidentEscalations).where(inArray(incidentEscalations.tenantId, tenantIds));
      await db.delete(incidentEscalationPolicies).where(inArray(incidentEscalationPolicies.tenantId, tenantIds));
      await db.delete(incidents).where(inArray(incidents.tenantId, tenantIds));
      await db.delete(tenantSettings).where(inArray(tenantSettings.tenantId, tenantIds));
      await db.delete(tenantUsers).where(inArray(tenantUsers.tenantId, tenantIds));
      await db.delete(userContactMethods).where(inArray(userContactMethods.tenantId, tenantIds));
      await db.delete(apiKeys).where(inArray(apiKeys.tenantId, tenantIds));
      for (const tenantId of tenantIds) {
        await db.execute(sql`DELETE FROM rate_limit_buckets WHERE tenant_id = ${tenantId}`);
      }
    } else {
      await db.execute(sql`TRUNCATE TABLE ports CASCADE`);
      await db.delete(signals).where(sql`1=1`);
      await db.delete(auditEvents).where(sql`1=1`);
      await db.delete(auditExports).where(sql`1=1`);
      await db.delete(incidentEscalations).where(sql`1=1`);
      await db.delete(incidentEscalationPolicies).where(sql`1=1`);
      await db.delete(incidents).where(sql`1=1`);
      await db.delete(tenantSettings).where(sql`1=1`);
      await db.execute(sql`DELETE FROM rate_limit_buckets`);
      await db.delete(alertDeliverySlaWindows).where(sql`1=1`);
      await db.delete(alertSlaThresholds).where(sql`1=1`);
      await db.delete(alertEndpointHealth).where(sql`1=1`);
      await db.delete(alertDestinationStates).where(sql`1=1`);
      await db.delete(alertNoiseBudgetBreaches).where(sql`1=1`);
      await db.delete(alertQualityGateBreaches).where(sql`1=1`);
      await db.delete(alertNoiseBudgets).where(sql`1=1`);
      await db.delete(alertDeliveryAttempts).where(sql`1=1`);
      await db.delete(alertDlq).where(sql`1=1`);
      await db.delete(alertDeliveries).where(sql`1=1`);
      await db.delete(alertRuns).where(sql`1=1`);
      if (subscriptionUsers.length > 0) {
        for (const userId of subscriptionUsers) {
          await db.delete(alertSubscriptions).where(eq(alertSubscriptions.userId, userId));
        }
      } else {
        await db.delete(alertSubscriptions).where(sql`1=1`);
      }
      await db.delete(alertDedupe).where(sql`1=1`);
      await db.delete(apiKeys).where(sql`1=1`);
      await db.delete(tenantUsers).where(sql`1=1`);
      await db.delete(userContactMethods).where(sql`1=1`);
    }
  } catch (cleanupError) {
    console.warn("Cleanup warning:", (cleanupError as Error).message);
  } finally {
    if (cleanupStart) {
      const elapsedMs = Number(process.hrtime.bigint() - cleanupStart) / 1_000_000;
      const mode = hasTenantIds ? "tenant" : "full";
      const detail = hasTenantIds ? `tenant=${tenantIds.length}` : "all";
      console.log(`TEST_TIMING: cleanup ${mode} ${detail} ${elapsedMs.toFixed(1)}ms`);
    }
  }
}

export async function closePool() {
  await pool.end();
}

export function createTestContext() {
  return {
    async start() {
      applyTestEnv();
      await enablePgCrypto();
      await ensureTestSchema();
    },
    async reset() {
      await cleanDatabase();
    },
    async ping() {
      const result = await db.execute(sql`SELECT 1 as ok`);
      return Boolean(result?.rows?.[0]?.ok ?? true);
    },
    async stop() {
      await closePool();
    },
  };
}
