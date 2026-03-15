import { and, eq, inArray, or, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries, alertDlq, incidentEscalationPolicies, incidentEscalations, incidents, tenantUsers } from "@shared/schema";
import { SEVERITY_RANK } from "@shared/signalTypes";
import { dispatchIncidentEscalationSystemAlert } from "./alertDispatcher";
import { writeAuditEvent } from "./auditLog";
import { getTenantAllowlist } from "./tenantSettings";
import { allowlistHostMatches, allowlistMatches } from "./allowlistUtils";
import { resolveEscalationTargets } from "./incidentEscalationRouting";
import { hashKey, makeDestinationKey } from "./destinationKey";
import { incrementOpsCounter, logOpsEvent, recordEscalationRunDuration } from "./opsTelemetry";

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
  const actualKey = String(severity ?? "").toUpperCase() as keyof typeof SEVERITY_RANK;
  const minKey = String(minSeverity ?? "").toUpperCase() as keyof typeof SEVERITY_RANK;
  const actual = SEVERITY_RANK[actualKey] ?? 0;
  const min = SEVERITY_RANK[minKey] ?? 0;
  return actual >= min;
};

const getEscalationCooldownMinutes = () => {
  const raw = process.env.ESCALATION_COOLDOWN_MINUTES;
  if (raw === undefined || raw === "") return 30;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return 30;
  return parsed;
};


export async function validateEscalationPolicyTarget(args: {
  tenantId: string;
  targetType: string;
  targetRef: string;
}) {
  const targetType = String(args.targetType ?? "").toUpperCase();
  if (!["ROLE", "USER", "EMAIL", "WEBHOOK"].includes(targetType)) {
    throw new Error("target_type must be ROLE, USER, EMAIL, or WEBHOOK");
  }

  let targetRef = String(args.targetRef ?? "").trim();
  if (!targetRef) {
    throw new Error("target_ref is required");
  }

  let targetName: string | null = null;

  if (targetType === "ROLE") {
    targetRef = targetRef.toUpperCase();
    if (!["OWNER", "OPERATOR", "VIEWER"].includes(targetRef)) {
      throw new Error("target_ref must be OWNER, OPERATOR, or VIEWER");
    }
  }

  if (targetType === "USER") {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(targetRef)) {
      throw new Error("target_ref must be a user id");
    }
    const [user] = await db
      .select()
      .from(tenantUsers)
      .where(and(
        eq(tenantUsers.tenantId, args.tenantId),
        eq(tenantUsers.userId, targetRef),
        eq(tenantUsers.status, "ACTIVE"),
      ))
      .limit(1);
    if (!user) {
      throw new Error("target_ref must be an active user");
    }
    targetName = user.displayName ?? user.email ?? null;
  }

  if (targetType === "EMAIL") {
    const emailOk = /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(targetRef) && targetRef.length <= 254;
    if (!emailOk) {
      throw new Error("target_ref must be a valid email");
    }
    if (process.env.NODE_ENV === "production") {
      const { allowed_email_domains } = await getTenantAllowlist(args.tenantId);
      const domain = targetRef.split("@")[1]?.toLowerCase() ?? "";
      if (allowed_email_domains.length === 0 || !allowlistMatches(domain, allowed_email_domains)) {
        throw new Error("email domain not allowed");
      }
    }
  }

  if (targetType === "WEBHOOK") {
    try {
      const url = new URL(targetRef);
      const isLocal = url.hostname === "localhost" || url.hostname === "127.0.0.1";
      if (process.env.NODE_ENV === "production") {
        if (url.protocol !== "https:") {
          throw new Error("target_ref must be https in production");
        }
        const { allowed_webhook_hosts } = await getTenantAllowlist(args.tenantId);
        const host = url.hostname.toLowerCase();
        if (allowed_webhook_hosts.length === 0 || !allowlistHostMatches(host, allowed_webhook_hosts)) {
          throw new Error("webhook host not allowed");
        }
      } else if (url.protocol !== "https:" && !isLocal) {
        throw new Error("target_ref must be https or localhost in development");
      }
    } catch (error: any) {
      if (error?.message) throw error;
      throw new Error("target_ref must be a valid URL");
    }
  }

  return { targetType, targetRef, targetName };
}

export async function listIncidentEscalationPolicies(tenantId: string) {
  const items = await db
    .select()
    .from(incidentEscalationPolicies)
    .where(eq(incidentEscalationPolicies.tenantId, tenantId));

  const typeOrder: Record<string, number> = {
    ALL: 0,
    SLA_AT_RISK: 1,
    ENDPOINT_DOWN: 2,
  };

  return items.sort((a: any, b: any) => {
    const typeA = String(a.incidentType ?? "");
    const typeB = String(b.incidentType ?? "");
    const typeRankA = typeOrder[typeA] ?? 99;
    const typeRankB = typeOrder[typeB] ?? 99;
    if (typeRankA !== typeRankB) return typeRankA - typeRankB;
    const levelDiff = Number(a.level ?? 0) - Number(b.level ?? 0);
    if (levelDiff !== 0) return levelDiff;
    const targetTypeDiff = String(a.targetType ?? "").localeCompare(String(b.targetType ?? ""));
    if (targetTypeDiff !== 0) return targetTypeDiff;
    const targetRefDiff = String(a.targetRef ?? "").localeCompare(String(b.targetRef ?? ""));
    if (targetRefDiff !== 0) return targetRefDiff;
    const afterDiff = Number(a.afterMinutes ?? 0) - Number(b.afterMinutes ?? 0);
    if (afterDiff !== 0) return afterDiff;
    return String(a.updatedAt ?? "").localeCompare(String(b.updatedAt ?? ""));
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
  targetName?: string | null;
  enabled: boolean;
  lastValidatedAt?: Date | null;
  lastRoutingHealth?: Record<string, any> | null;
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
      targetName: args.targetName ?? null,
      enabled: args.enabled,
      lastValidatedAt: args.lastValidatedAt ?? null,
      lastRoutingHealth: args.lastRoutingHealth ?? null,
      createdAt: now,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: [
        incidentEscalationPolicies.tenantId,
        incidentEscalationPolicies.incidentType,
        incidentEscalationPolicies.severityMin,
        incidentEscalationPolicies.level,
        incidentEscalationPolicies.targetType,
        incidentEscalationPolicies.targetRef,
      ],
      set: {
        afterMinutes: args.afterMinutes,
        targetType: args.targetType,
        targetRef: args.targetRef,
        targetName: args.targetName ?? null,
        enabled: args.enabled,
        lastValidatedAt: args.lastValidatedAt ?? null,
        lastRoutingHealth: args.lastRoutingHealth ?? null,
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
  const runStart = Date.now();
  const lockKey = "incident_escalations";
  const locked = await tryAdvisoryLock(args.tenantId, lockKey);
  if (!locked) {
    incrementOpsCounter("escalation_runs_skipped_lock_total");
    recordEscalationRunDuration(Date.now() - runStart);
    return { skipped: true, reason: "LOCK_HELD", escalated: 0, processed: 0 };
  }

  try {
    const cooldownMinutes = getEscalationCooldownMinutes();
    const openIncidents = await db
      .select()
      .from(incidents)
      .where(and(eq(incidents.tenantId, args.tenantId), eq(incidents.status, "OPEN")));
    incrementOpsCounter("escalation_runs_total");
    logOpsEvent("ESCALATION_RUN_STARTED", {
      tenantId: args.tenantId,
      open_incidents: openIncidents.length,
      cooldown_minutes: cooldownMinutes,
      now: args.now.toISOString(),
    });

    if (openIncidents.length === 0) {
      logOpsEvent("ESCALATION_RUN_COMPLETED", {
        tenantId: args.tenantId,
        processed: 0,
        escalated: 0,
        skipped: false,
      });
      recordEscalationRunDuration(Date.now() - runStart);
      return { skipped: false, escalated: 0, processed: 0 };
    }
    const processed = openIncidents.length;

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
      logOpsEvent("ESCALATION_RUN_COMPLETED", {
        tenantId: args.tenantId,
        processed,
        escalated: 0,
        skipped: false,
      });
      recordEscalationRunDuration(Date.now() - runStart);
      return { skipped: false, escalated: 0, processed };
    }

    const policiesByType = new Map<string, typeof policies>();
    for (const policy of policies) {
      const key = String(policy.incidentType);
      const list = policiesByType.get(key) ?? ([] as typeof policies);
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
      const incidentSeverity = String(incident.severity ?? "");

      const specificPolicies = (policiesByType.get(String(incident.type)) ?? [])
        .filter((policy) => severityMeetsMin(incidentSeverity, String(policy.severityMin)));
      const fallbackPolicies = (policiesByType.get("ALL") ?? [])
        .filter((policy) => severityMeetsMin(incidentSeverity, String(policy.severityMin)));

      const levelMap = new Map<number, { specific: typeof policies; fallback: typeof policies }>();
      for (const policy of specificPolicies) {
        const level = Number(policy.level ?? 0);
        const entry = levelMap.get(level) ?? { specific: [] as typeof policies, fallback: [] as typeof policies };
        entry.specific.push(policy);
        levelMap.set(level, entry);
      }
      for (const policy of fallbackPolicies) {
        const level = Number(policy.level ?? 0);
        const entry = levelMap.get(level) ?? { specific: [] as typeof policies, fallback: [] as typeof policies };
        entry.fallback.push(policy);
        levelMap.set(level, entry);
      }

      const effectiveByLevel = new Map<number, typeof policies>();
      for (const [level, entry] of levelMap) {
        effectiveByLevel.set(level, entry.specific.length > 0 ? entry.specific : entry.fallback);
      }

      const nextLevel = [...effectiveByLevel.keys()]
        .filter((level) => level > currentLevel)
        .sort((a, b) => a - b)[0];

      const levelsToCheck: number[] = [];
      if (currentLevel > 0 && effectiveByLevel.has(currentLevel)) {
        levelsToCheck.push(currentLevel);
      }
      if (Number.isFinite(nextLevel)) {
        levelsToCheck.push(nextLevel);
      }
      if (levelsToCheck.length === 0) continue;

      const ageMinutes = minutesBetween(new Date(incident.openedAt), args.now);
      let shouldAdvance = false;

      for (const levelToCheck of levelsToCheck) {
        const levelPolicies = effectiveByLevel.get(levelToCheck) ?? [];
        if (levelPolicies.length === 0) continue;

        const duePolicies = levelPolicies.filter((policy) => ageMinutes >= Number(policy.afterMinutes ?? 0));
        if (duePolicies.length === 0) continue;

        const allowAdvance = levelToCheck > currentLevel;

        for (const policy of duePolicies) {
          const policyId = policy.id ? String(policy.id) : null;
          const targetType = String(policy.targetType ?? "").toUpperCase();
          const targetRef = String(policy.targetRef ?? "").trim();
          const targetHash = hashKey(`${targetType}:${targetRef}`);
          const policyTag = policyId ? `:policy:${policyId}` : "";
          const cooldownClusterId = `incident-escalation:${args.tenantId}:${incident.id}:level:${levelToCheck}${policyTag}:target:${targetHash}`;
          const [recentDelivery] = await db
            .select({
              id: alertDeliveries.id,
              createdAt: alertDeliveries.createdAt,
              status: alertDeliveries.status,
              skipReason: alertDeliveries.skipReason,
              decision: alertDeliveries.decision,
            })
            .from(alertDeliveries)
            .where(and(
              eq(alertDeliveries.tenantId, args.tenantId),
              eq(alertDeliveries.clusterId, cooldownClusterId),
              policyId
                ? sql`${alertDeliveries.decision} -> 'gates' -> 'escalation' ->> 'policy_id' = ${policyId}`
                : undefined,
            ))
            .orderBy(sql`${alertDeliveries.createdAt} desc`, sql`${alertDeliveries.id} desc`)
            .limit(1);

          if (recentDelivery) {
            const createdAt = recentDelivery.createdAt instanceof Date
              ? recentDelivery.createdAt
              : new Date(recentDelivery.createdAt);
            const inCooldown = cooldownMinutes > 0
              ? (args.now.getTime() - createdAt.getTime()) < cooldownMinutes * 60 * 1000
              : false;
            const routingBlocked = recentDelivery.skipReason === "NO_USER_CONTACT_METHOD"
              || recentDelivery.status === "SKIPPED_DESTINATION_ROUTING";

            if (inCooldown) {
              if (allowAdvance && !routingBlocked) {
                shouldAdvance = true;
              }
              incrementOpsCounter("deliveries_blocked_cooldown_total");
              logOpsEvent("ESCALATION_COOLDOWN_BLOCKED", {
                tenantId: args.tenantId,
                incidentId: incident.id,
                policyId,
                level: levelToCheck,
                clusterId: cooldownClusterId,
                cooldown_minutes: cooldownMinutes,
              });
              continue;
            }

            if (recentDelivery.status === "FAILED") {
              const [dlqRow] = await db
                .select({
                  attemptCount: alertDlq.attemptCount,
                  maxAttempts: alertDlq.maxAttempts,
                })
                .from(alertDlq)
                .where(and(
                  eq(alertDlq.tenantId, args.tenantId),
                  eq(alertDlq.deliveryId, recentDelivery.id),
                ))
                .limit(1);
              if (dlqRow) {
                if (Number(dlqRow.attemptCount ?? 0) >= Number(dlqRow.maxAttempts ?? 0)) {
                  if (allowAdvance && !routingBlocked) {
                    shouldAdvance = true;
                  }
                  incrementOpsCounter("deliveries_blocked_dlq_total");
                  logOpsEvent("ESCALATION_DLQ_BLOCKED", {
                    tenantId: args.tenantId,
                    incidentId: incident.id,
                    policyId,
                    level: levelToCheck,
                    deliveryId: recentDelivery.id,
                    attempt_count: Number(dlqRow.attemptCount ?? 0),
                    max_attempts: Number(dlqRow.maxAttempts ?? 0),
                  });
                  continue;
                }
                if (allowAdvance && !routingBlocked) {
                  shouldAdvance = true;
                }
                continue;
              }
            }
          }

          let destinations: Array<{ destinationType: "EMAIL" | "WEBHOOK"; destination: string; destinationKey?: string; targetUserId?: string | null }> | undefined;
          let routingGate: {
            allowed: boolean;
            reason?: "NO_USER_CONTACT_METHOD";
            chosen_method_type?: "EMAIL" | "WEBHOOK";
            chosen_method_id?: string;
            target_user_id?: string;
          } | undefined;

          if (String(policy.targetType).toUpperCase() === "USER") {
            const resolved = await resolveEscalationTargets({
              tenantId: args.tenantId,
              userId: String(policy.targetRef),
              targetName: policy.targetName ?? null,
            });
            if (!resolved.ok) {
              destinations = [{
                destinationType: "EMAIL",
                destination: `user:${policy.targetRef}`,
                destinationKey: makeDestinationKey("EMAIL", `user:${policy.targetRef}`),
                targetUserId: String(policy.targetRef),
              }];
              routingGate = {
                allowed: false,
                reason: "NO_USER_CONTACT_METHOD",
                target_user_id: String(policy.targetRef),
              };
            } else {
              destinations = resolved.destinations.map((dest) => ({
                destinationType: dest.destination_type,
                destination: dest.destination,
                destinationKey: dest.destination_key,
                targetUserId: dest.target_user_id,
              }));
              const chosen = resolved.destinations[0];
              routingGate = {
                allowed: true,
                chosen_method_type: chosen.chosen_method_type,
                chosen_method_id: chosen.chosen_method_id,
                target_user_id: chosen.target_user_id,
              };
            }
          }

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
            level: levelToCheck,
            policy: {
              id: policy.id,
              targetType: String(policy.targetType),
              targetRef: String(policy.targetRef),
              afterMinutes: Number(policy.afterMinutes),
              severityMin: String(policy.severityMin),
            },
            destinations,
            routingGate,
          });

          const routingBlocked = routingGate?.allowed === false;
          if (allowAdvance && !routingBlocked && (dispatchResult.didAttempt || dispatchResult.dedupeBlocked)) {
            shouldAdvance = true;
          }
          if (dispatchResult.didAttempt) {
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
                level: levelToCheck,
                target_type: policy.targetType,
                target_ref: policy.targetRef,
                policy_id: policy.id,
              },
            });
            escalated += 1;
          }
        }
      }

      if (!shouldAdvance || !Number.isFinite(nextLevel)) continue;

      await db
        .update(incidentEscalations)
        .set({
          currentLevel: nextLevel,
          lastEscalatedAt: args.now,
          updatedAt: args.now,
        })
        .where(and(
          eq(incidentEscalations.tenantId, args.tenantId),
          eq(incidentEscalations.incidentId, incident.id),
        ));
      logOpsEvent("ESCALATION_LEVEL_ADVANCED", {
        tenantId: args.tenantId,
        incidentId: incident.id,
        level: nextLevel,
      });
    }

    logOpsEvent("ESCALATION_RUN_COMPLETED", {
      tenantId: args.tenantId,
      processed,
      escalated,
      skipped: false,
    });
    if (escalated > 0) {
      incrementOpsCounter("escalation_runs_escalated_total", escalated);
    }
    recordEscalationRunDuration(Date.now() - runStart);
    return { skipped: false, escalated, processed };
  } finally {
    await releaseAdvisoryLock(args.tenantId, lockKey);
  }
}

export async function getIncidentEscalationSnapshot(args: {
  tenantId: string;
  incident: any;
  now: Date;
  limit?: number;
}) {
  const { tenantId, incident, now } = args;
  const limit = Number.isFinite(args.limit) ? Math.max(1, Math.min(Number(args.limit), 50)) : 20;

  const [escalationRow] = await db
    .select()
    .from(incidentEscalations)
    .where(and(
      eq(incidentEscalations.tenantId, tenantId),
      eq(incidentEscalations.incidentId, incident.id),
    ))
    .limit(1);

  const policyRows = await db
    .select()
    .from(incidentEscalationPolicies)
    .where(and(
      eq(incidentEscalationPolicies.tenantId, tenantId),
      eq(incidentEscalationPolicies.enabled, true),
      or(
        eq(incidentEscalationPolicies.incidentType, incident.type),
        eq(incidentEscalationPolicies.incidentType, "ALL"),
      ),
    ));

  const typePriority = (type: string) => (type === String(incident.type) ? 0 : 1);

    const policies = policyRows.sort((a, b) => {
      const typeRank = typePriority(String(a.incidentType)) - typePriority(String(b.incidentType));
      if (typeRank !== 0) return typeRank;
      const targetDiff = String(a.targetType ?? "").localeCompare(String(b.targetType ?? ""));
      if (targetDiff !== 0) return targetDiff;
      const refDiff = String(a.targetRef ?? "").localeCompare(String(b.targetRef ?? ""));
      if (refDiff !== 0) return refDiff;
      const levelDiff = Number(a.level ?? 0) - Number(b.level ?? 0);
      if (levelDiff !== 0) return levelDiff;
      const afterDiff = Number(a.afterMinutes ?? 0) - Number(b.afterMinutes ?? 0);
      if (afterDiff !== 0) return afterDiff;
      return String(a.updatedAt ?? "").localeCompare(String(b.updatedAt ?? ""));
  });

  const hasPolicy = policies.length > 0;
  const currentLevel = Number(escalationRow?.currentLevel ?? 0);
  const incidentSeverity = String(incident.severity ?? "").toUpperCase();

  const specificPolicies = policies.filter((policy) => (
    String(policy.incidentType) === String(incident.type)
    && severityMeetsMin(incidentSeverity, String(policy.severityMin ?? ""))
  ));
  const fallbackPolicies = policies.filter((policy) => (
    String(policy.incidentType) === "ALL"
    && severityMeetsMin(incidentSeverity, String(policy.severityMin ?? ""))
  ));

  const levelMap = new Map<number, { specific: any[]; fallback: any[] }>();
  for (const policy of specificPolicies) {
    const level = Number(policy.level ?? 0);
    const entry = levelMap.get(level) ?? { specific: [], fallback: [] };
    entry.specific.push(policy);
    levelMap.set(level, entry);
  }
  for (const policy of fallbackPolicies) {
    const level = Number(policy.level ?? 0);
    const entry = levelMap.get(level) ?? { specific: [], fallback: [] };
    entry.fallback.push(policy);
    levelMap.set(level, entry);
  }

  const effectiveByLevel = new Map<number, any[]>();
  for (const [level, entry] of levelMap) {
    effectiveByLevel.set(level, entry.specific.length > 0 ? entry.specific : entry.fallback);
  }

  const nextLevelCandidate = [...effectiveByLevel.keys()]
    .filter((level) => level > currentLevel)
    .sort((a, b) => a - b)[0];

  let nextReason = hasPolicy ? "ALREADY_SENT" : "NO_POLICY";
  let nextLevel: number | null = null;
  let nextAfter: number | null = null;
  let nextDueAt: string | null = null;
  let nextEtaSeconds: number | null = null;

  if (hasPolicy && incident.status !== "OPEN") {
    nextReason = "INCIDENT_NOT_OPEN";
  }

  if (hasPolicy && Number.isFinite(nextLevelCandidate)) {
    const levelPolicies = effectiveByLevel.get(nextLevelCandidate as number) ?? [];
    if (levelPolicies.length > 0) {
      nextLevel = Number(nextLevelCandidate);
      nextAfter = Math.min(...levelPolicies.map((policy) => Number(policy.afterMinutes ?? 0)));
    }
  }
  if (hasPolicy && nextLevel !== null && nextAfter !== null) {
    const openedAt = incident.openedAt instanceof Date ? incident.openedAt : new Date(incident.openedAt);
    const dueAt = new Date(openedAt.getTime() + nextAfter * 60 * 1000);
    nextDueAt = dueAt.toISOString();
    nextEtaSeconds = Math.max(0, Math.floor((dueAt.getTime() - now.getTime()) / 1000));
    if (incident.status === "OPEN") {
      nextReason = nextEtaSeconds > 0 ? "NOT_DUE" : "READY";
    }
  }

  const escalationPrefix = `incident-escalation:${tenantId}:${incident.id}:level:`;
  const escalationLike = `${escalationPrefix}%`;
  const deliveryRows = await db
    .select()
    .from(alertDeliveries)
    .where(and(
      eq(alertDeliveries.tenantId, tenantId),
      sql`${alertDeliveries.clusterId} like ${escalationLike}`,
    ))
    .orderBy(sql`${alertDeliveries.createdAt} desc`, sql`${alertDeliveries.id} desc`)
    .limit(limit);

  const parseLevel = (clusterId?: string | null) => {
    if (!clusterId) return null;
    const match = String(clusterId).match(/:level:(\d+)/i);
    return match ? Number(match[1]) : null;
  };

  const deliveries = deliveryRows.map((row: any) => ({
    id: row.id,
    created_at: row.createdAt,
    level: row.decision?.gates?.escalation?.level ?? parseLevel(row.clusterId),
    destination_type: row.destinationType,
    status: row.status,
    destination_key: row.destinationKey ?? null,
    policy_id: row.decision?.gates?.escalation?.policy_id ?? null,
    target_type: row.decision?.gates?.escalation?.target_type ?? null,
    target_ref: row.decision?.gates?.escalation?.target_ref ?? null,
  }));

  return {
    version: "1",
    has_policy: hasPolicy,
    incident_type: incident.type,
    severity: incident.severity,
    current_level: currentLevel,
    last_escalated_at: escalationRow?.lastEscalatedAt ?? null,
    next: {
      level: nextLevel,
      after_minutes: nextAfter,
      due_at: nextDueAt,
      eta_seconds: nextEtaSeconds,
      reason: nextReason,
    },
    policies: policies.map((policy) => ({
      id: policy.id,
      incident_type: policy.incidentType,
      level: policy.level,
      after_minutes: policy.afterMinutes,
      severity_min: policy.severityMin,
      target_type: policy.targetType,
      target_ref: policy.targetRef,
      target_name: policy.targetName ?? null,
    })),
    deliveries,
  };
}
