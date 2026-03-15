import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { incidents } from "@shared/schema";
import { writeAuditEvent } from "./auditLog";
import { listDestinations } from "./alertDestinationStateService";
import { runIncidentEscalations } from "./incidentEscalationService";
import { logOpsEvent } from "./opsTelemetry";

const clampMinutes = (value?: number) => {
  const parsed = Number.isFinite(Number(value)) ? Number(value) : 30;
  return Math.max(5, Math.min(1440, Math.floor(parsed)));
};

const tryAdvisoryLock = async (tenantId: string, lockKey: string) => {
  const result = await db.execute(sql`
    SELECT pg_try_advisory_lock(hashtext(${tenantId}), hashtext(${lockKey})) AS locked
  `);
  const row = (result as any).rows?.[0] ?? (Array.isArray(result) ? result[0] : undefined);
  return Boolean(row?.locked);
};

const releaseAdvisoryLock = async (tenantId: string, lockKey: string) => {
  await db.execute(sql`
    SELECT pg_advisory_unlock(hashtext(${tenantId}), hashtext(${lockKey}))
  `);
};

const tryTenantAutomationLock = async (lockKey: string) => {
  const result = await db.execute(sql`
    SELECT pg_try_advisory_lock(hashtext(${lockKey})) AS locked
  `);
  const row = (result as any).rows?.[0] ?? (Array.isArray(result) ? result[0] : undefined);
  return Boolean(row?.locked);
};

const releaseTenantAutomationLock = async (lockKey: string) => {
  await db.execute(sql`
    SELECT pg_advisory_unlock(hashtext(${lockKey}))
  `);
};

export async function autoAckIncidents(args: {
  tenantId: string;
  minutes?: number;
  now?: Date;
}) {
  const now = args.now ?? new Date();
  const minutes = clampMinutes(args.minutes);
  const lockKey = "incident_auto_ack";
  const locked = await tryAdvisoryLock(args.tenantId, lockKey);
  if (!locked) {
    return { tenant_id: args.tenantId, minutes, auto_acked: 0, skipped: true };
  }

  try {
    const cutoff = new Date(now.getTime() - minutes * 60 * 1000);
    const result = await db.execute(sql`
      UPDATE incidents
      SET status = 'ACKED',
          acked_at = ${now},
          acked_by_actor_type = 'SYSTEM',
          acked_by_actor_id = NULL
      WHERE tenant_id = ${args.tenantId}
        AND status = 'OPEN'
        AND acked_at IS NULL
        AND opened_at <= ${cutoff}
      RETURNING id, type, destination_key
    `);
    const rows = (result as any).rows ?? result ?? [];

    for (const row of rows) {
      await writeAuditEvent(undefined, {
        tenantId: args.tenantId,
        actorType: "SYSTEM",
        action: "INCIDENT.AUTO_ACKED",
        resourceType: "INCIDENT",
        resourceId: row.id,
        status: "SUCCESS",
        severity: "SECURITY",
        message: "Incident auto-acknowledged",
        metadata: {
          type: row.type,
          destination_key: row.destination_key ?? null,
        },
      });
      logOpsEvent("INCIDENT_ACKED", {
        tenantId: args.tenantId,
        incidentId: row.id,
        type: row.type,
        destinationKey: row.destination_key ?? null,
      });
    }

    return { tenant_id: args.tenantId, minutes, auto_acked: rows.length, skipped: false };
  } finally {
    await releaseAdvisoryLock(args.tenantId, lockKey);
  }
}

export async function autoResolveIncidents(args: {
  tenantId: string;
  now?: Date;
}) {
  const now = args.now ?? new Date();
  const lockKey = "incident_auto_resolve";
  const locked = await tryAdvisoryLock(args.tenantId, lockKey);
  if (!locked) {
    return { tenant_id: args.tenantId, auto_resolved: 0, skipped: true };
  }

  try {
    const slaResult = await db.execute(sql`
      SELECT i.id, i.type, i.destination_key
      FROM incidents i
      WHERE i.tenant_id = ${args.tenantId}
        AND i.type = 'SLA_AT_RISK'
        AND i.status IN ('OPEN', 'ACKED')
        AND i.destination_key IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM audit_events a
          WHERE a.tenant_id = i.tenant_id
            AND a.action = 'ALERT.SLA_RECOVERED'
            AND a.created_at >= i.opened_at
            AND (a.metadata->>'destination_key') = i.destination_key
        )
    `);
    const slaRows = (slaResult as any).rows ?? slaResult ?? [];

    const endpointCandidates = await db
      .select({
        id: incidents.id,
        destinationKey: incidents.destinationKey,
        type: incidents.type,
      })
      .from(incidents)
      .where(and(
        eq(incidents.tenantId, args.tenantId),
        eq(incidents.type, "ENDPOINT_DOWN"),
        inArray(incidents.status, ["OPEN", "ACKED"]),
      ));

    const destinationList = await listDestinations({
      tenantId: args.tenantId,
      window: "1h",
      limit: 10000,
      now,
    });
    const destinationMap = new Map(
      destinationList.items.map((item: any) => [
        item.destination_key,
        {
          state: String(item.state ?? "ACTIVE").toUpperCase(),
          healthStatus: String(item.health?.status ?? "").toUpperCase(),
        },
      ]),
    );

    const endpointRows = endpointCandidates.filter((row) => {
      if (!row.destinationKey) return false;
      const dest = destinationMap.get(row.destinationKey);
      if (!dest) return false;
      return dest.state === "ACTIVE" && dest.healthStatus === "OK";
    });

    const toResolveIds = Array.from(new Set([
      ...slaRows.map((row: any) => row.id),
      ...endpointRows.map((row) => row.id),
    ]));

    if (toResolveIds.length === 0) {
      return { tenant_id: args.tenantId, auto_resolved: 0, skipped: false };
    }

    const updatedRows = await db
      .update(incidents)
      .set({
        status: "RESOLVED",
        resolvedAt: now,
        resolvedByActorType: "SYSTEM",
        resolvedByActorId: null,
      })
      .where(and(eq(incidents.tenantId, args.tenantId), inArray(incidents.id, toResolveIds)))
      .returning({
        id: incidents.id,
        type: incidents.type,
        destination_key: incidents.destinationKey,
      });

    for (const row of updatedRows) {
      await writeAuditEvent(undefined, {
        tenantId: args.tenantId,
        actorType: "SYSTEM",
        action: "INCIDENT.AUTO_RESOLVED",
        resourceType: "INCIDENT",
        resourceId: row.id,
        status: "SUCCESS",
        severity: "SECURITY",
        message: "Incident auto-resolved",
        metadata: {
          type: row.type,
          destination_key: row.destination_key ?? null,
        },
      });
      logOpsEvent("INCIDENT_RESOLVED", {
        tenantId: args.tenantId,
        incidentId: row.id,
        type: row.type,
        destinationKey: row.destination_key ?? null,
      });
    }

    return { tenant_id: args.tenantId, auto_resolved: updatedRows.length, skipped: false };
  } finally {
    await releaseAdvisoryLock(args.tenantId, lockKey);
  }
}

export async function runIncidentAutomation() {
  const tenantsResult = await db.execute(sql`SELECT DISTINCT tenant_id FROM incidents`);
  const tenantRows = (tenantsResult as any).rows ?? tenantsResult ?? [];
  let tenantsProcessed = 0;

  for (const row of tenantRows) {
    const tenantId = row.tenant_id ?? row.tenantId;
    if (!tenantId) continue;
    const lockKey = `incident_automation:${tenantId}`;
    const locked = await tryTenantAutomationLock(lockKey);
    if (!locked) continue;

    try {
      tenantsProcessed += 1;
      const ackResult = await autoAckIncidents({ tenantId });
      const resolveResult = await autoResolveIncidents({ tenantId });
      await runIncidentEscalations({ tenantId, now: new Date() });
      await writeAuditEvent(undefined, {
        tenantId,
        actorType: "SYSTEM",
        action: "INCIDENT.AUTOMATION_RUN",
        resourceType: "SYSTEM",
        resourceId: null,
        status: "SUCCESS",
        severity: "SECURITY",
        message: "Incident automation run",
        metadata: {
          tenants_processed: 1,
          acked: ackResult.auto_acked,
          resolved: resolveResult.auto_resolved,
        },
      });
    } finally {
      await releaseTenantAutomationLock(lockKey);
    }
  }

  return { tenants_processed: tenantsProcessed };
}
