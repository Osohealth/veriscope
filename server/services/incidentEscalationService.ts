import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { incidentEscalationPolicies, incidentEscalations, incidents } from "@shared/schema";
import { SEVERITY_RANK } from "@shared/signalTypes";
import { dispatchIncidentEscalationSystemAlert } from "./alertDispatcher";
import { writeAuditEvent } from "./auditLog";

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

const minutesBetween = (from: Date, to: Date) =>
  Math.floor((to.getTime() - from.getTime()) / 60000);

const severityMeetsMin = (severity: string, minSeverity: string) => {
  const actual = SEVERITY_RANK[String(severity ?? "").toUpperCase()] ?? 0;
  const min = SEVERITY_RANK[String(minSeverity ?? "").toUpperCase()] ?? 0;
  return actual >= min;
};

export async function listIncidentEscalationPolicies(tenantId: string) {
  const items = await db
    .select()
    .from(incidentEscalationPolicies)
    .where(eq(incidentEscalationPolicies.tenantId, tenantId));

  return items.sort((a: any, b: any) => {
    if (a.incidentType !== b.incidentType) return String(a.incidentType).localeCompare(String(b.incidentType));
    if (a.severityMin !== b.severityMin) return String(a.severityMin).localeCompare(String(b.severityMin));
    return Number(a.level ?? 0) - Number(b.level ?? 0);
  });
}

export async function upsertIncidentEscalationPolicy(args: {
  tenantId: string;
  incidentType: string;
  severityMin: string;
  level: number;
  afterMinutes: number;
  targetType: string;
  targetRef: string;
  enabled: boolean;
}) {
  const now = new Date();
  const [row] = await db
    .insert(incidentEscalationPolicies)
    .values({
      tenantId: args.tenantId,
      incidentType: args.incidentType,
      severityMin: args.severityMin,
      level: args.level,
      afterMinutes: args.afterMinutes,
      targetType: args.targetType,
      targetRef: args.targetRef,
      enabled: args.enabled,
      createdAt: now,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: [
        incidentEscalationPolicies.tenantId,
        incidentEscalationPolicies.incidentType,
        incidentEscalationPolicies.severityMin,
        incidentEscalationPolicies.level,
      ],
      set: {
        afterMinutes: args.afterMinutes,
        targetType: args.targetType,
        targetRef: args.targetRef,
        enabled: args.enabled,
        updatedAt: now,
      },
    })
    .returning();

  return row ?? null;
}

export async function deleteIncidentEscalationPolicy(args: { tenantId: string; id: string }) {
  const [row] = await db
    .delete(incidentEscalationPolicies)
    .where(and(eq(incidentEscalationPolicies.tenantId, args.tenantId), eq(incidentEscalationPolicies.id, args.id)))
    .returning();
  return row ?? null;
}

export async function runIncidentEscalations(args: { tenantId: string; now: Date }) {
  const lockKey = "incident_escalations";
  const locked = await tryAdvisoryLock(args.tenantId, lockKey);
  if (!locked) {
    return { skipped: true, escalated: 0 };
  }

  try {
    const openIncidents = await db
      .select()
      .from(incidents)
      .where(and(eq(incidents.tenantId, args.tenantId), eq(incidents.status, "OPEN")));

    if (openIncidents.length === 0) {
      return { skipped: false, escalated: 0 };
    }

    const incidentIds = openIncidents.map((row) => row.id);
    const stateRows = await db
      .select()
      .from(incidentEscalations)
      .where(and(eq(incidentEscalations.tenantId, args.tenantId), inArray(incidentEscalations.incidentId, incidentIds)));
    const stateByIncident = new Map(stateRows.map((row) => [row.incidentId, row]));

    const missing = incidentIds.filter((id) => !stateByIncident.has(id));
    if (missing.length > 0) {
      await db.insert(incidentEscalations)
        .values(missing.map((incidentId) => ({
          tenantId: args.tenantId,
          incidentId,
          currentLevel: 0,
          lastEscalatedAt: null,
          createdAt: args.now,
          updatedAt: args.now,
        })))
        .onConflictDoNothing();

      const reloaded = await db
        .select()
        .from(incidentEscalations)
        .where(and(eq(incidentEscalations.tenantId, args.tenantId), inArray(incidentEscalations.incidentId, missing)));
      reloaded.forEach((row) => stateByIncident.set(row.incidentId, row));
    }

    const policies = await db
      .select()
      .from(incidentEscalationPolicies)
      .where(and(eq(incidentEscalationPolicies.tenantId, args.tenantId), eq(incidentEscalationPolicies.enabled, true)));

    if (policies.length === 0) {
      return { skipped: false, escalated: 0 };
    }

    const policiesByType = new Map<string, typeof policies>();
    for (const policy of policies) {
      const key = String(policy.incidentType);
      const list = policiesByType.get(key) ?? [];
      list.push(policy);
      policiesByType.set(key, list);
    }
    for (const [key, list] of policiesByType) {
      list.sort((a, b) => Number(a.level ?? 0) - Number(b.level ?? 0));
      policiesByType.set(key, list);
    }

    let escalated = 0;

    for (const incident of openIncidents) {
      const state = stateByIncident.get(incident.id);
      const currentLevel = Number(state?.currentLevel ?? 0);
      const nextLevel = currentLevel + 1;
      const typePolicies = policiesByType.get(String(incident.type)) ?? [];
      if (typePolicies.length === 0) continue;

      const nextPolicy = typePolicies.find((policy) => {
        if (Number(policy.level ?? 0) !== nextLevel) return false;
        return severityMeetsMin(String(incident.severity), String(policy.severityMin));
      });
      if (!nextPolicy) continue;

      const ageMinutes = minutesBetween(new Date(incident.openedAt), args.now);
      if (ageMinutes < Number(nextPolicy.afterMinutes ?? 0)) continue;

      const dispatchResult = await dispatchIncidentEscalationSystemAlert({
        tenantId: args.tenantId,
        now: args.now,
        incident: {
          id: incident.id,
          type: incident.type,
          severity: incident.severity,
          destinationKey: incident.destinationKey,
          title: incident.title,
          summary: incident.summary,
          openedAt: incident.openedAt,
        },
        level: nextPolicy.level,
        policy: {
          targetType: String(nextPolicy.targetType),
          targetRef: String(nextPolicy.targetRef),
          afterMinutes: Number(nextPolicy.afterMinutes),
          severityMin: String(nextPolicy.severityMin),
        },
      });

      if (!dispatchResult.didAttempt && !dispatchResult.dedupeBlocked) {
        continue;
      }

      await db
        .update(incidentEscalations)
        .set({
          currentLevel: nextPolicy.level,
          lastEscalatedAt: args.now,
          updatedAt: args.now,
        })
        .where(and(
          eq(incidentEscalations.tenantId, args.tenantId),
          eq(incidentEscalations.incidentId, incident.id),
        ));

      await writeAuditEvent(undefined, {
        tenantId: args.tenantId,
        actorType: "SYSTEM",
        action: "INCIDENT.ESCALATED",
        resourceType: "INCIDENT",
        resourceId: incident.id,
        status: "SUCCESS",
        severity: "SECURITY",
        message: "Incident escalated",
        metadata: {
          incident_type: incident.type,
          incident_severity: incident.severity,
          level: nextPolicy.level,
          target_type: nextPolicy.targetType,
          target_ref: nextPolicy.targetRef,
        },
      });

      escalated += 1;
    }

    return { skipped: false, escalated };
  } finally {
    await releaseAdvisoryLock(args.tenantId, lockKey);
  }
}
