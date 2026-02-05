import { eq, and, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries, alertDeliveryAttempts, alertDlq, alertRuns, alertSubscriptions, ports } from "@shared/schema";
import { normalizeScope } from "./alertScope";
import { getAlertCandidates } from "./alertQuery";
import { shouldSendAlert, markAlertSent } from "./alertDedupe";
import { buildWebhookPayload, buildWebhookRequest, sendWebhook } from "./webhookSender";
import { renderAlertEmail, sendEmail } from "./emailSender";
import { SEVERITY_RANK } from "@shared/signalTypes";
import { computeNextAttempt } from "./alertDlq";
import { ALERT_DEDUPE_TTL_HOURS, ALERT_RATE_LIMIT_PER_ENDPOINT, DLQ_MAX_ATTEMPTS } from "../config/alerting";
import { TENANT_DEMO_ID } from "../config/tenancy";

type RunAlertsOptions = {
  day?: string;
  tenantId?: string;
  userId?: string;
  now?: Date;
};

export async function runAlerts(options: RunAlertsOptions = {}) {
  const now = options.now ?? new Date();
  const tenantId = options.tenantId ?? TENANT_DEMO_ID;
  const [runRow] = await db
    .insert(alertRuns)
    .values({
      tenantId,
      day: options.day ? new Date(`${options.day}T00:00:00Z`) : null,
      status: "SUCCESS",
      startedAt: now,
    })
    .returning();

  const summary = {
    day: options.day ?? null,
    candidates_total: 0,
    subscriptions: 0,
    matched_total: 0,
    sent_total: 0,
    skipped_dedupe_total: 0,
    failed_total: 0,
    skipped_rate_limit_total: 0,
  };
  const errorDetails: { endpoint: string; cluster_id: string; error_type: string; message: string }[] = [];
  const perEndpointCount = new Map<string, number>();

  try {
    const subscriptions = await db
      .select()
      .from(alertSubscriptions)
      .where(and(
        eq(alertSubscriptions.tenantId, tenantId),
        alertSubscriptions.isEnabled,
      ));

    summary.subscriptions = subscriptions.length;

    for (const sub of subscriptions) {
      if (options.userId && sub.userId !== options.userId) {
        continue;
      }

      const scope = normalizeScope((sub as any).scope);
      const candidates = await getAlertCandidates({
        day: options.day,
        entityType: sub.entityType as "port",
        entityId: scope === "GLOBAL" ? undefined : sub.entityId,
        severityMin: sub.severityMin as any,
      });

      summary.candidates_total += candidates.length;

      for (const candidate of candidates) {
        summary.matched_total += 1;
        if (sub.confidenceMin) {
          const required = SEVERITY_RANK[sub.confidenceMin as any] ?? 0;
          const actual = SEVERITY_RANK[(candidate.confidence_band ?? "LOW") as any] ?? 0;
          if (actual < required) {
            continue;
          }
        }

        const rateKey = sub.id;
        const currentCount = perEndpointCount.get(rateKey) ?? 0;
        if (currentCount >= ALERT_RATE_LIMIT_PER_ENDPOINT) {
          summary.skipped_rate_limit_total += 1;
          await db.insert(alertDeliveries).values({
            runId: runRow.id,
            tenantId,
            userId: sub.userId,
            subscriptionId: sub.id,
            clusterId: String(candidate.cluster_id),
            entityType: candidate.entity_type ?? "port",
            entityId: String(candidate.entity_id),
            day: new Date(String(candidate.day)),
            destinationType: sub.channel,
            endpoint: sub.endpoint,
            status: "SKIPPED_RATE_LIMIT",
            attempts: 0,
            createdAt: now,
          });
          continue;
        }

        const dedupeKey = {
          tenantId,
          clusterId: String(candidate.cluster_id),
          channel: sub.channel,
          endpoint: sub.endpoint,
        };

        const allowed = await shouldSendAlert({ ...dedupeKey, now });
        if (!allowed) {
          summary.skipped_dedupe_total += 1;
          await db.insert(alertDeliveries).values({
            runId: runRow.id,
            tenantId,
            userId: sub.userId,
            subscriptionId: sub.id,
            clusterId: String(candidate.cluster_id),
            entityType: candidate.entity_type ?? "port",
            entityId: String(candidate.entity_id),
            day: new Date(String(candidate.day)),
            destinationType: sub.channel,
            endpoint: sub.endpoint,
            status: "SKIPPED_DEDUPE",
            attempts: 0,
            createdAt: now,
          });
          continue;
        }

        try {
          if (sub.channel === "WEBHOOK") {
            const payload = buildWebhookPayload(candidate, { sentAt: now, version: "1.1" });
            const { body, headers } = buildWebhookRequest({
              payload,
              secret: sub.secret ?? null,
              subscriptionId: sub.id,
              clusterId: String(candidate.cluster_id),
              day: String(candidate.day),
              now,
            });

            const result = await sendWebhook({ endpoint: sub.endpoint, body, headers });
            const attemptLogs = (result as any)?.attemptLogs ?? [];
            const attempts = attemptLogs.length || 1;
            const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

            for (const log of attemptLogs) {
              console.log({
                run_id: runRow.id,
                subscription_id: sub.id,
                cluster_id: candidate.cluster_id,
                endpoint: sub.endpoint,
                attempt: log.attempt,
                status: log.status,
                latency_ms: log.latency_ms,
                http_status: log.http_status,
              });
            }

            const [delivery] = await db.insert(alertDeliveries).values({
              runId: runRow.id,
              tenantId,
              userId: sub.userId,
              subscriptionId: sub.id,
              clusterId: String(candidate.cluster_id),
              entityType: candidate.entity_type ?? "port",
              entityId: String(candidate.entity_id),
              day: new Date(String(candidate.day)),
              destinationType: sub.channel,
              endpoint: sub.endpoint,
              status: "SENT",
              attempts,
              lastHttpStatus: last?.http_status ?? result?.status ?? null,
              latencyMs: last?.latency_ms ?? null,
              sentAt: now,
              createdAt: now,
            }).returning();

            const baseAttempt = Math.max((delivery?.attempts ?? 0) - attempts, 0);
            const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
              .map((log) => ({
                tenantId,
                deliveryId: delivery.id,
                attemptNo: baseAttempt + log.attempt,
                status: log.status === "SUCCESS" ? "SENT" : "FAILED",
                latencyMs: log.latency_ms ?? null,
                httpStatus: log.http_status ?? null,
                error: log.status === "SUCCESS" ? null : "Webhook delivery failed",
                sentAt: log.status === "SUCCESS" ? now : null,
                createdAt: now,
              }));
            if (attemptRows.length) {
              await db.insert(alertDeliveryAttempts).values(attemptRows);
            }
          } else if (sub.channel === "EMAIL") {
            const [port] = await db
              .select({ name: ports.name })
              .from(ports)
              .where(eq(ports.id, candidate.entity_id))
              .limit(1);
            const email = renderAlertEmail({
              signal: candidate,
              entity: port,
              link: `/signals/${candidate.id}`,
            });
            await sendEmail({ to: sub.endpoint, subject: email.subject, text: email.text });
            const [delivery] = await db.insert(alertDeliveries).values({
              runId: runRow.id,
              tenantId,
              userId: sub.userId,
              subscriptionId: sub.id,
              clusterId: String(candidate.cluster_id),
              entityType: candidate.entity_type ?? "port",
              entityId: String(candidate.entity_id),
              day: new Date(String(candidate.day)),
              destinationType: sub.channel,
              endpoint: sub.endpoint,
              status: "SENT",
              attempts: 1,
              sentAt: now,
              createdAt: now,
            }).returning();

            await db.insert(alertDeliveryAttempts).values({
              tenantId,
              deliveryId: delivery.id,
              attemptNo: 1,
              status: "SENT",
              latencyMs: null,
              httpStatus: null,
              error: null,
              sentAt: now,
              createdAt: now,
            });
          }

          await markAlertSent({ ...dedupeKey, now, ttlHours: ALERT_DEDUPE_TTL_HOURS });
          summary.sent_total += 1;
          perEndpointCount.set(rateKey, currentCount + 1);
        } catch (error: any) {
          summary.failed_total += 1;
          errorDetails.push({
            endpoint: sub.endpoint,
            cluster_id: String(candidate.cluster_id),
            error_type: error?.name ?? "ERROR",
            message: error?.message ?? "Alert send failed",
          });

          const attemptLogs = error?.attemptLogs ?? [];
          for (const log of attemptLogs) {
            console.log({
              run_id: runRow.id,
              subscription_id: sub.id,
              cluster_id: candidate.cluster_id,
              endpoint: sub.endpoint,
              attempt: log.attempt,
              status: log.status,
              latency_ms: log.latency_ms,
              http_status: log.http_status,
            });
          }

          const [delivery] = await db.insert(alertDeliveries).values({
            runId: runRow.id,
            tenantId,
            userId: sub.userId,
            subscriptionId: sub.id,
            clusterId: String(candidate.cluster_id),
            entityType: candidate.entity_type ?? "port",
            entityId: String(candidate.entity_id),
            day: new Date(String(candidate.day)),
            destinationType: sub.channel,
            endpoint: sub.endpoint,
            status: "FAILED",
            attempts: attemptLogs.length || 1,
            lastHttpStatus: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.http_status : null,
            latencyMs: attemptLogs.length ? attemptLogs[attemptLogs.length - 1]?.latency_ms : null,
            error: error?.message ?? "Alert send failed",
            createdAt: now,
          }).returning();

          const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "FAILED", latency_ms: null, http_status: null }])
            .map((log) => ({
              tenantId,
              deliveryId: delivery.id,
              attemptNo: log.attempt,
              status: "FAILED",
              latencyMs: log.latency_ms ?? null,
              httpStatus: log.http_status ?? null,
              error: error?.message ?? "Alert send failed",
              sentAt: null,
              createdAt: now,
            }));
          await db.insert(alertDeliveryAttempts).values(attemptRows);

          const nextAttemptAt = computeNextAttempt(now, 1);
          await db.insert(alertDlq).values({
            deliveryId: delivery.id,
            tenantId,
            nextAttemptAt,
            attemptCount: 1,
            maxAttempts: DLQ_MAX_ATTEMPTS,
            lastError: error?.message ?? "Alert send failed",
          }).onConflictDoUpdate({
            target: [alertDlq.deliveryId],
            set: {
              nextAttemptAt,
              attemptCount: sql`${alertDlq.attemptCount} + 1`,
              lastError: error?.message ?? "Alert send failed",
            },
          });
        }
      }
    }

    await db.update(alertRuns)
      .set({
        status: summary.failed_total > 0 ? "FAILED" : "SUCCESS",
        finishedAt: new Date(),
        summary,
        error: errorDetails.length ? errorDetails : null,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

    return { runId: runRow.id, status: summary.failed_total > 0 ? "FAILED" : "SUCCESS", summary };
  } catch (error: any) {
    await db.update(alertRuns)
      .set({
        status: "FAILED",
        finishedAt: new Date(),
        error: errorDetails.length ? errorDetails : [{ message: error.message }],
        summary,
      })
      .where(and(eq(alertRuns.id, runRow.id), eq(alertRuns.tenantId, tenantId)));

    return { runId: runRow.id, status: "FAILED", summary };
  }
}


