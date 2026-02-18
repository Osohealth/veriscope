import { and, desc, eq, sql, type SQL } from "drizzle-orm";
import { db } from "../db";
import { auditEvents } from "@shared/schema";

export type AuditEventFilters = {
  tenantId: string;
  days?: number;
  action?: string;
  resourceType?: string;
  resourceId?: string;
  actor?: string;
  severityMin?: string;
  cursorCreatedAt?: string | null;
  cursorId?: string | null;
  limit?: number;
};

const severityRankSql = (value: SQL) => sql`(case ${value}
  when 'SECURITY' then 3
  when 'WARN' then 2
  when 'INFO' then 1
  else 0
end)`;

export const buildAuditEventsWhere = (
  filters: AuditEventFilters,
  options?: { includeCursor?: boolean },
) => {
  const conditions: SQL[] = [];

  conditions.push(eq(auditEvents.tenantId, filters.tenantId));

  if (filters.days) {
    const cutoff = new Date(Date.now() - filters.days * 24 * 60 * 60 * 1000);
    conditions.push(sql`${auditEvents.createdAt} >= ${cutoff}`);
  }
  if (filters.action) conditions.push(eq(auditEvents.action, filters.action));
  if (filters.resourceType) conditions.push(eq(auditEvents.resourceType, filters.resourceType));
  if (filters.resourceId) conditions.push(eq(auditEvents.resourceId, filters.resourceId));

  if (filters.actor) {
    const pattern = `%${filters.actor}%`;
    conditions.push(sql`${auditEvents.actorLabel} ilike ${pattern}`);
  }

  if (filters.severityMin) {
    const requiredRank = severityRankSql(sql`${filters.severityMin}`);
    conditions.push(sql`${severityRankSql(sql`upper(${auditEvents.severity})`)} >= ${requiredRank}`);
  }

  if (options?.includeCursor && filters.cursorCreatedAt && filters.cursorId) {
    conditions.push(
      sql`(${auditEvents.createdAt} < ${filters.cursorCreatedAt}::timestamptz OR (${auditEvents.createdAt} = ${filters.cursorCreatedAt}::timestamptz AND ${auditEvents.id} < ${filters.cursorId}))`,
    );
  }

  return conditions.length > 0 ? and(...conditions) : undefined;
};

export async function listAuditEvents(filters: AuditEventFilters) {
  const limit = Math.min(Math.max(filters.limit ?? 50, 1), 200);
  const whereClause = buildAuditEventsWhere(filters, { includeCursor: true });

  const items = await db
    .select()
    .from(auditEvents)
    .where(whereClause)
    .orderBy(desc(auditEvents.createdAt), desc(auditEvents.id))
    .limit(limit);

  return { items };
}

export async function getAuditEventsSummary(filters: AuditEventFilters) {
  const whereClause = buildAuditEventsWhere(filters);
  const [row] = await db
    .select({
      total: sql<number>`count(*)`,
      security: sql<number>`count(*) filter (where ${auditEvents.severity} = 'SECURITY')`,
      denied: sql<number>`count(*) filter (where ${auditEvents.status} = 'DENIED')`,
      failed: sql<number>`count(*) filter (where ${auditEvents.status} = 'FAILED')`,
    })
    .from(auditEvents)
    .where(whereClause);

  return {
    total: Number(row?.total ?? 0),
    security: Number(row?.security ?? 0),
    denied: Number(row?.denied ?? 0),
    failed: Number(row?.failed ?? 0),
  };
}
