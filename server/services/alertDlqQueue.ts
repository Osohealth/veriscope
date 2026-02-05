import { and, eq, lte, sql } from "drizzle-orm";
import { db } from "../db";
import { alertDeliveries, alertDeliveryAttempts, alertDlq, alertSubscriptions } from "@shared/schema";
import { getAlertCandidates } from "./alertQuery";
import { buildWebhookPayload, buildWebhookRequest, sendWebhook } from "./webhookSender";
import { computeNextAttempt } from "./alertDlq";
import { DLQ_MAX_ATTEMPTS } from "../config/alerting";

type RetryDlqOptions = {
  limit?: number;
  now?: Date;
  tenantId?: string;
  userId?: string;
};

export async function retryAlertDlq(options: RetryDlqOptions = {}) {
  const limit = options.limit ?? 50;
  const now = options.now ?? new Date();
  const rows = await db
    .select()
    .from(alertDlq)
    .where(and(
      lte(alertDlq.nextAttemptAt, now),
      options.tenantId ? eq(alertDlq.tenantId, options.tenantId) : undefined,
    ))
    .orderBy(alertDlq.nextAttemptAt)
    .limit(limit);

  let processed = 0;
  let sent = 0;
  let failed = 0;

  for (const dlqRow of rows) {
    processed += 1;
    if (dlqRow.attemptCount >= dlqRow.maxAttempts) {
      await db.update(alertDeliveries)
        .set({
          status: "FAILED",
          error: "DLQ max attempts reached",
        })
        .where(and(
          eq(alertDeliveries.id, dlqRow.deliveryId),
          options.tenantId ? eq(alertDeliveries.tenantId, options.tenantId) : undefined,
        ));
      await db.update(alertDlq)
        .set({
          lastError: "DLQ max attempts reached",
          nextAttemptAt: new Date(now.getTime() + 365 * 24 * 60 * 60 * 1000),
        })
        .where(and(
          eq(alertDlq.id, dlqRow.id),
          options.tenantId ? eq(alertDlq.tenantId, options.tenantId) : undefined,
        ));
      continue;
    }
    const [delivery] = await db
      .select()
      .from(alertDeliveries)
      .where(and(
        eq(alertDeliveries.id, dlqRow.deliveryId),
        options.tenantId ? eq(alertDeliveries.tenantId, options.tenantId) : undefined,
        options.userId ? eq(alertDeliveries.userId, options.userId) : undefined,
      ))
      .limit(1);
    if (!delivery) {
      await db.delete(alertDlq).where(and(
        eq(alertDlq.id, dlqRow.id),
        options.tenantId ? eq(alertDlq.tenantId, options.tenantId) : undefined,
      ));
      continue;
    }

    const [subscription] = await db
      .select()
      .from(alertSubscriptions)
      .where(and(
        eq(alertSubscriptions.id, delivery.subscriptionId),
        options.tenantId ? eq(alertSubscriptions.tenantId, options.tenantId) : undefined,
        options.userId ? eq(alertSubscriptions.userId, options.userId) : undefined,
      ))
      .limit(1);
    if (!subscription) {
      await db.delete(alertDlq).where(and(
        eq(alertDlq.id, dlqRow.id),
        options.tenantId ? eq(alertDlq.tenantId, options.tenantId) : undefined,
      ));
      continue;
    }

    const candidates = await getAlertCandidates({
      day: delivery.day instanceof Date ? delivery.day.toISOString().slice(0, 10) : String(delivery.day),
      entityType: subscription.entityType as "port",
      entityId: String(delivery.entityId),
      severityMin: subscription.severityMin as any,
    });
    const candidate = candidates.find((c) => String(c.cluster_id) === String(delivery.clusterId));
    if (!candidate) {
      await db.update(alertDeliveries)
        .set({ status: "FAILED", error: "Candidate missing" })
        .where(and(
          eq(alertDeliveries.id, delivery.id),
          options.tenantId ? eq(alertDeliveries.tenantId, options.tenantId) : undefined,
        ));
      continue;
    }

    try {
      const payload = buildWebhookPayload(candidate, { sentAt: now, version: "1.1" });
      const { body, headers } = buildWebhookRequest({
        payload,
        secret: subscription.secret ?? null,
        subscriptionId: subscription.id,
        clusterId: String(candidate.cluster_id),
        day: String(candidate.day),
        now,
      });
      const result = await sendWebhook({ endpoint: subscription.endpoint, body, headers });
      const attemptLogs = (result as any)?.attemptLogs ?? [];
      const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

      await db.update(alertDeliveries)
        .set({
          status: "SENT",
          attempts: (delivery.attempts ?? 0) + 1,
          lastHttpStatus: last?.http_status ?? result?.status ?? null,
          latencyMs: last?.latency_ms ?? null,
          sentAt: now,
          error: null,
        })
        .where(and(
          eq(alertDeliveries.id, delivery.id),
          eq(alertDeliveries.tenantId, options.tenantId ?? delivery.tenantId),
        ));

      const baseAttempt = delivery.attempts ?? 0;
      const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
        .map((log) => ({
          deliveryId: delivery.id,
          attemptNo: baseAttempt + log.attempt,
          status: log.status === "SUCCESS" ? "SENT" : "FAILED",
          latencyMs: log.latency_ms ?? null,
          httpStatus: log.http_status ?? null,
          error: log.status === "SUCCESS" ? null : "Retry failed",
          sentAt: log.status === "SUCCESS" ? now : null,
          createdAt: now,
        }));
      if (attemptRows.length) {
        await db.insert(alertDeliveryAttempts).values(attemptRows.map((row) => ({
          ...row,
          tenantId: options.tenantId ?? delivery.tenantId,
        })));
      }

      await db.delete(alertDlq).where(and(
        eq(alertDlq.id, dlqRow.id),
        eq(alertDlq.tenantId, options.tenantId ?? dlqRow.tenantId),
      ));
      sent += 1;
    } catch (error: any) {
      failed += 1;
      const nextAttemptAt = computeNextAttempt(now, dlqRow.attemptCount + 1);
      await db.update(alertDlq)
        .set({
          attemptCount: dlqRow.attemptCount + 1,
          nextAttemptAt,
          lastError: error?.message ?? "Retry failed",
        })
        .where(and(
          eq(alertDlq.id, dlqRow.id),
          eq(alertDlq.tenantId, options.tenantId ?? dlqRow.tenantId),
        ));

      await db.update(alertDeliveries)
        .set({
          status: "FAILED",
          attempts: (delivery.attempts ?? 0) + 1,
          error: error?.message ?? "Retry failed",
        })
        .where(and(
          eq(alertDeliveries.id, delivery.id),
          eq(alertDeliveries.tenantId, options.tenantId ?? delivery.tenantId),
        ));

      await db.insert(alertDeliveryAttempts).values({
        tenantId: options.tenantId ?? delivery.tenantId,
        deliveryId: delivery.id,
        attemptNo: (delivery.attempts ?? 0) + 1,
        status: "FAILED",
        latencyMs: null,
        httpStatus: null,
        error: error?.message ?? "Retry failed",
        sentAt: null,
        createdAt: now,
      });
    }
  }

  return { processed, sent, failed };
}

export async function retryDeliveryById(options: { deliveryId: string; tenantId?: string; userId?: string; now?: Date }) {
  const now = options.now ?? new Date();
  const [delivery] = await db
    .select()
    .from(alertDeliveries)
    .where(and(
      eq(alertDeliveries.id, options.deliveryId),
      options.tenantId ? eq(alertDeliveries.tenantId, options.tenantId) : undefined,
      options.userId ? eq(alertDeliveries.userId, options.userId) : undefined,
    ))
    .limit(1);
  if (!delivery) {
    return { status: "not_found" as const };
  }
  if (delivery.status === "SENT") {
    return { status: "already_sent" as const, delivery };
  }

  const [subscription] = await db
    .select()
    .from(alertSubscriptions)
    .where(and(
      eq(alertSubscriptions.id, delivery.subscriptionId),
      options.tenantId ? eq(alertSubscriptions.tenantId, options.tenantId) : undefined,
      options.userId ? eq(alertSubscriptions.userId, options.userId) : undefined,
    ))
    .limit(1);
  if (!subscription) {
    return { status: "subscription_missing" as const, delivery };
  }

  let [dlqRow] = await db
    .select()
    .from(alertDlq)
    .where(and(
      eq(alertDlq.deliveryId, options.deliveryId),
      options.tenantId ? eq(alertDlq.tenantId, options.tenantId) : undefined,
    ))
    .limit(1);

  if (dlqRow && dlqRow.attemptCount >= dlqRow.maxAttempts) {
    return { status: "terminal" as const, delivery, dlq: dlqRow };
  }

  if (!dlqRow) {
    const [createdDlq] = await db.insert(alertDlq).values({
      deliveryId: options.deliveryId,
      tenantId: options.tenantId ?? delivery.tenantId,
      nextAttemptAt: now,
      attemptCount: 0,
      maxAttempts: DLQ_MAX_ATTEMPTS,
      lastError: null,
    }).returning();
    dlqRow = createdDlq;
  }

  const candidates = await getAlertCandidates({
    day: delivery.day instanceof Date ? delivery.day.toISOString().slice(0, 10) : String(delivery.day),
    entityType: subscription.entityType as "port",
    entityId: String(delivery.entityId),
    severityMin: subscription.severityMin as any,
  });
  const candidate = candidates.find((c) => String(c.cluster_id) === String(delivery.clusterId));
  if (!candidate) {
    await db.update(alertDeliveries)
      .set({ status: "FAILED", error: "Candidate missing" })
      .where(and(
        eq(alertDeliveries.id, options.deliveryId),
        options.tenantId ? eq(alertDeliveries.tenantId, options.tenantId) : undefined,
      ));
    return { status: "candidate_missing" as const, delivery };
  }

  try {
    const payload = buildWebhookPayload(candidate, { sentAt: now, version: "1.1" });
    const { body, headers } = buildWebhookRequest({
      payload,
      secret: subscription.secret ?? null,
      subscriptionId: subscription.id,
      clusterId: String(candidate.cluster_id),
      day: String(candidate.day),
      now,
    });
    const result = await sendWebhook({ endpoint: subscription.endpoint, body, headers });
    const attemptLogs = (result as any)?.attemptLogs ?? [];
    const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;

    await db.update(alertDeliveries)
      .set({
        status: "SENT",
        attempts: (delivery.attempts ?? 0) + 1,
        lastHttpStatus: last?.http_status ?? result?.status ?? null,
        latencyMs: last?.latency_ms ?? null,
        sentAt: now,
        error: null,
      })
      .where(and(
        eq(alertDeliveries.id, options.deliveryId),
        eq(alertDeliveries.tenantId, options.tenantId ?? delivery.tenantId),
      ));

    const baseAttempt = delivery.attempts ?? 0;
    const attemptRows = (attemptLogs.length ? attemptLogs : [{ attempt: 1, status: "SUCCESS", latency_ms: last?.latency_ms ?? null, http_status: last?.http_status ?? result?.status ?? null }])
      .map((log) => ({
        deliveryId: deliveryId,
        attemptNo: baseAttempt + log.attempt,
        status: log.status === "SUCCESS" ? "SENT" : "FAILED",
        latencyMs: log.latency_ms ?? null,
        httpStatus: log.http_status ?? null,
        error: log.status === "SUCCESS" ? null : "Retry failed",
        sentAt: log.status === "SUCCESS" ? now : null,
        createdAt: now,
      }));
    if (attemptRows.length) {
      await db.insert(alertDeliveryAttempts).values(attemptRows.map((row) => ({
        ...row,
        tenantId: options.tenantId ?? delivery.tenantId,
      })));
    }

    await db.delete(alertDlq).where(and(
      eq(alertDlq.deliveryId, options.deliveryId),
      eq(alertDlq.tenantId, options.tenantId ?? dlqRow?.tenantId ?? delivery.tenantId),
    ));
    const [updated] = await db.select().from(alertDeliveries).where(eq(alertDeliveries.id, options.deliveryId)).limit(1);
    return { status: "sent" as const, delivery: updated, dlq: null };
  } catch (error: any) {
    const nextAttemptAt = computeNextAttempt(now, (dlqRow?.attemptCount ?? 0) + 1);
    await db.update(alertDlq)
      .set({
        attemptCount: (dlqRow?.attemptCount ?? 0) + 1,
        nextAttemptAt,
        lastError: error?.message ?? "Retry failed",
      })
      .where(and(
        eq(alertDlq.deliveryId, options.deliveryId),
        eq(alertDlq.tenantId, options.tenantId ?? dlqRow?.tenantId ?? delivery.tenantId),
      ));

    await db.update(alertDeliveries)
      .set({
        status: "FAILED",
        attempts: (delivery.attempts ?? 0) + 1,
        error: error?.message ?? "Retry failed",
      })
      .where(and(
        eq(alertDeliveries.id, options.deliveryId),
        eq(alertDeliveries.tenantId, options.tenantId ?? delivery.tenantId),
      ));

    await db.insert(alertDeliveryAttempts).values({
      tenantId: options.tenantId ?? delivery.tenantId,
      deliveryId: options.deliveryId,
      attemptNo: (delivery.attempts ?? 0) + 1,
      status: "FAILED",
      latencyMs: null,
      httpStatus: null,
      error: error?.message ?? "Retry failed",
      sentAt: null,
      createdAt: now,
    });

    const [updated] = await db.select().from(alertDeliveries).where(eq(alertDeliveries.id, options.deliveryId)).limit(1);
    const [updatedDlq] = await db.select().from(alertDlq).where(eq(alertDlq.deliveryId, options.deliveryId)).limit(1);
    return { status: "failed" as const, delivery: updated, dlq: updatedDlq ?? null };
  }
}
