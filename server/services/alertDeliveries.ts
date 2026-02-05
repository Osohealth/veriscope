import { and, desc, eq, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries } from "@shared/schema";

export type AlertDeliveryFilters = {
  days?: number;
  day?: Date | null;
  tenantId?: string;
  userId?: string;
  entityId?: string;
  subscriptionId?: string;
  subscriptionIds?: string[];
  runId?: string;
  status?: string;
  destinationType?: string;
  isTest?: boolean;
  cursorCreatedAt?: Date | null;
  cursorId?: string | null;
  limit?: number;
};

export async function listAlertDeliveries(filters: AlertDeliveryFilters) {
  const limit = Math.min(Math.max(filters.limit ?? 50, 1), 200);

  const baseConditions: any[] = [];
  if (filters.days) {
    baseConditions.push(sql`${alertDeliveries.day} >= (current_date - ${filters.days}::int)`);
  }
  if (filters.day) baseConditions.push(eq(alertDeliveries.day, filters.day));
  if (filters.tenantId) baseConditions.push(eq(alertDeliveries.tenantId, filters.tenantId));
  if (filters.userId) baseConditions.push(eq(alertDeliveries.userId, filters.userId));
  if (filters.entityId) baseConditions.push(eq(alertDeliveries.entityId, filters.entityId));
  if (filters.subscriptionId) baseConditions.push(eq(alertDeliveries.subscriptionId, filters.subscriptionId));
  if (filters.subscriptionIds && filters.subscriptionIds.length > 0) {
    baseConditions.push(inArray(alertDeliveries.subscriptionId, filters.subscriptionIds));
  }
  if (filters.runId) baseConditions.push(eq(alertDeliveries.runId, filters.runId));
  if (filters.status) baseConditions.push(eq(alertDeliveries.status, filters.status));
  if (filters.destinationType) baseConditions.push(eq(alertDeliveries.destinationType, filters.destinationType));
  if (filters.isTest !== undefined) baseConditions.push(eq(alertDeliveries.isTest, filters.isTest));

  const conditions = [...baseConditions];
  if (filters.cursorCreatedAt && filters.cursorId) {
    conditions.push(sql`(${alertDeliveries.createdAt} < ${filters.cursorCreatedAt} OR (${alertDeliveries.createdAt} = ${filters.cursorCreatedAt} AND ${alertDeliveries.id} < ${filters.cursorId}))`);
  }

  const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

  const items = await db
    .select()
    .from(alertDeliveries)
    .where(whereClause)
    .orderBy(desc(alertDeliveries.createdAt), desc(alertDeliveries.id))
    .limit(limit);

  const countWhere = baseConditions.length > 0 ? and(...baseConditions) : undefined;
  const totalResult = await db
    .select({ count: sql<number>`count(*)` })
    .from(alertDeliveries)
    .where(countWhere);

  const total = Number(totalResult[0]?.count ?? 0);
  return { items, total };
}
