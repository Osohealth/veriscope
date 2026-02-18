import { and, desc, eq, sql, type SQL } from "drizzle-orm";
import { db } from "../db";
import { alertSubscriptions } from "@shared/schema";

export type AlertSubscriptionFilters = {
  tenantId: string;
  userId: string;
  enabled?: boolean;
  destinationType?: string;
  scope?: "PORT" | "GLOBAL";
  cursorCreatedAt?: string | null;
  cursorId?: string | null;
  limit?: number;
};

export const buildAlertSubscriptionsWhere = (
  filters: AlertSubscriptionFilters,
  options?: { includeCursor?: boolean },
) => {
  const conditions: SQL[] = [];

  conditions.push(eq(alertSubscriptions.tenantId, filters.tenantId));
  conditions.push(eq(alertSubscriptions.userId, filters.userId));

  if (filters.enabled !== undefined) {
    conditions.push(eq(alertSubscriptions.isEnabled, filters.enabled));
  }
  if (filters.destinationType) {
    conditions.push(eq(alertSubscriptions.channel, filters.destinationType));
  }
  if (filters.scope) {
    conditions.push(eq(alertSubscriptions.scope, filters.scope));
  }

  if (options?.includeCursor && filters.cursorCreatedAt && filters.cursorId) {
    conditions.push(
      sql`(${alertSubscriptions.createdAt} < ${filters.cursorCreatedAt}::timestamptz OR (${alertSubscriptions.createdAt} = ${filters.cursorCreatedAt}::timestamptz AND ${alertSubscriptions.id} < ${filters.cursorId}))`,
    );
  }

  return conditions.length > 0 ? and(...conditions) : undefined;
};

export async function listAlertSubscriptionsPage(filters: AlertSubscriptionFilters) {
  const limit = Math.min(Math.max(filters.limit ?? 50, 1), 200);
  const whereClause = buildAlertSubscriptionsWhere(filters, { includeCursor: true });

  const items = await db
    .select({
      id: alertSubscriptions.id,
      tenantId: alertSubscriptions.tenantId,
      userId: alertSubscriptions.userId,
      scope: alertSubscriptions.scope,
      entityType: alertSubscriptions.entityType,
      entityId: alertSubscriptions.entityId,
      severityMin: alertSubscriptions.severityMin,
      confidenceMin: alertSubscriptions.confidenceMin,
      minQualityBand: alertSubscriptions.minQualityBand,
      minQualityScore: alertSubscriptions.minQualityScore,
      channel: alertSubscriptions.channel,
      endpoint: alertSubscriptions.endpoint,
      secret: alertSubscriptions.secret,
      signatureVersion: alertSubscriptions.signatureVersion,
      isEnabled: alertSubscriptions.isEnabled,
      lastTestAt: alertSubscriptions.lastTestAt,
      lastTestStatus: alertSubscriptions.lastTestStatus,
      lastTestError: alertSubscriptions.lastTestError,
      createdAt: alertSubscriptions.createdAt,
      updatedAt: alertSubscriptions.updatedAt,
      createdAtRaw: sql<string>`to_char(${alertSubscriptions.createdAt}, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
    })
    .from(alertSubscriptions)
    .where(whereClause)
    .orderBy(desc(alertSubscriptions.createdAt), desc(alertSubscriptions.id))
    .limit(limit);

  return { items };
}

export async function getAlertSubscriptionsSummary(filters: AlertSubscriptionFilters) {
  const whereClause = buildAlertSubscriptionsWhere(filters);

  const [row] = await db
    .select({
      total: sql<number>`count(*)`,
      enabled: sql<number>`count(*) filter (where ${alertSubscriptions.isEnabled} = true)`,
      disabled: sql<number>`count(*) filter (where ${alertSubscriptions.isEnabled} = false)`,
      webhook: sql<number>`count(*) filter (where ${alertSubscriptions.channel} = 'WEBHOOK')`,
      email: sql<number>`count(*) filter (where ${alertSubscriptions.channel} = 'EMAIL')`,
      scopeGlobal: sql<number>`count(*) filter (where ${alertSubscriptions.scope} = 'GLOBAL')`,
      scopePort: sql<number>`count(*) filter (where ${alertSubscriptions.scope} = 'PORT')`,
    })
    .from(alertSubscriptions)
    .where(whereClause);

  return {
    total: Number(row?.total ?? 0),
    enabled: Number(row?.enabled ?? 0),
    disabled: Number(row?.disabled ?? 0),
    webhook: Number(row?.webhook ?? 0),
    email: Number(row?.email ?? 0),
    scope_global: Number(row?.scopeGlobal ?? 0),
    scope_port: Number(row?.scopePort ?? 0),
  };
}
