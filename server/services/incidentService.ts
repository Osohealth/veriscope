import { and, desc, eq, isNull, sql, type SQL } from "drizzle-orm";
import { db } from "../db";
import { incidents } from "@shared/schema";
import { writeAuditEvent } from "./auditLog";

export type IncidentStatus = "OPEN" | "ACKED" | "RESOLVED";

export type IncidentFilters = {
  tenantId: string;
  status?: IncidentStatus;
  destinationKey?: string;
  type?: string;
  cursorOpenedAt?: string | null;
  cursorId?: string | null;
  limit?: number;
};

const buildIncidentsWhere = (
  filters: IncidentFilters,
  options?: { includeCursor?: boolean },
) => {
  const conditions: SQL[] = [];

  conditions.push(eq(incidents.tenantId, filters.tenantId));

  if (filters.status) conditions.push(eq(incidents.status, filters.status));
  if (filters.type) conditions.push(eq(incidents.type, filters.type));
  if (filters.destinationKey) conditions.push(eq(incidents.destinationKey, filters.destinationKey));

  if (options?.includeCursor && filters.cursorOpenedAt && filters.cursorId) {
    conditions.push(
      sql`(${incidents.openedAt} < ${filters.cursorOpenedAt}::timestamptz OR (${incidents.openedAt} = ${filters.cursorOpenedAt}::timestamptz AND ${incidents.id} < ${filters.cursorId}))`,
    );
  }

  return conditions.length > 0 ? and(...conditions) : undefined;
};

export async function listIncidents(filters: IncidentFilters) {
  const limit = Math.min(Math.max(filters.limit ?? 50, 1), 200);
  const whereClause = buildIncidentsWhere(filters, { includeCursor: true });

  const items = await db
    .select()
    .from(incidents)
    .where(whereClause)
    .orderBy(desc(incidents.openedAt), desc(incidents.id))
    .limit(limit);

  return { items };
}

export async function getIncidentsSummary(filters: IncidentFilters) {
  const whereClause = buildIncidentsWhere(filters);
  const [row] = await db
    .select({
      open: sql<number>`count(*) filter (where ${incidents.status} = 'OPEN')`,
      acked: sql<number>`count(*) filter (where ${incidents.status} = 'ACKED')`,
      resolved: sql<number>`count(*) filter (where ${incidents.status} = 'RESOLVED')`,
    })
    .from(incidents)
    .where(whereClause);

  return {
    open_count: Number(row?.open ?? 0),
    acked_count: Number(row?.acked ?? 0),
    resolved_count: Number(row?.resolved ?? 0),
  };
}

export async function getIncidentById(tenantId: string, id: string) {
  const [row] = await db
    .select()
    .from(incidents)
    .where(and(eq(incidents.tenantId, tenantId), eq(incidents.id, id)))
    .limit(1);
  return row ?? null;
}

export async function openOrAttachIncident(args: {
  tenantId: string;
  type: string;
  destinationKey?: string | null;
  severity: string;
  title: string;
  summary: string;
  now: Date;
}) {
  const destinationCondition = args.destinationKey
    ? eq(incidents.destinationKey, args.destinationKey)
    : isNull(incidents.destinationKey);

  const existing = await db
    .select()
    .from(incidents)
    .where(and(
      eq(incidents.tenantId, args.tenantId),
      eq(incidents.type, args.type),
      eq(incidents.status, "OPEN"),
      destinationCondition,
    ))
    .limit(1);

  if (existing[0]) return existing[0];

  const [created] = await db
    .insert(incidents)
    .values({
      tenantId: args.tenantId,
      type: args.type,
      destinationKey: args.destinationKey ?? null,
      status: "OPEN",
      severity: args.severity,
      title: args.title,
      summary: args.summary,
      openedAt: args.now,
      openedByActorType: "SYSTEM",
    })
    .returning();

  await writeAuditEvent(undefined, {
    tenantId: args.tenantId,
    actorType: "SYSTEM",
    action: "INCIDENT.OPENED",
    resourceType: "INCIDENT",
    resourceId: created.id,
    status: "SUCCESS",
    severity: "SECURITY",
    message: args.title,
    metadata: {
      type: args.type,
      destination_key: args.destinationKey ?? null,
    },
  });

  return created;
}

export async function resolveIncidentIfExists(args: {
  tenantId: string;
  type: string;
  destinationKey?: string | null;
  now: Date;
}) {
  const destinationCondition = args.destinationKey
    ? eq(incidents.destinationKey, args.destinationKey)
    : isNull(incidents.destinationKey);

  const [incident] = await db
    .select()
    .from(incidents)
    .where(and(
      eq(incidents.tenantId, args.tenantId),
      eq(incidents.type, args.type),
      eq(incidents.status, "OPEN"),
      destinationCondition,
    ))
    .limit(1);

  if (!incident) return null;

  await db
    .update(incidents)
    .set({
      status: "RESOLVED",
      resolvedAt: args.now,
      resolvedByActorType: "SYSTEM",
      resolvedByActorId: null,
    })
    .where(eq(incidents.id, incident.id));

  await writeAuditEvent(undefined, {
    tenantId: args.tenantId,
    actorType: "SYSTEM",
    action: "INCIDENT.RESOLVED",
    resourceType: "INCIDENT",
    resourceId: incident.id,
    status: "SUCCESS",
    severity: "SECURITY",
    message: "Incident resolved",
    metadata: {
      type: args.type,
      destination_key: args.destinationKey ?? null,
    },
  });

  return incident;
}
