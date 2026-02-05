import { eq, and } from "drizzle-orm";
import { db } from "../db";
import { alertDedupe } from "@shared/schema";

type AlertDedupeKey = {
  tenantId: string;
  clusterId: string;
  channel: string;
  endpoint: string;
};

type ShouldSendArgs = AlertDedupeKey & {
  now: Date;
};

export async function shouldSendAlert(args: ShouldSendArgs) {
  const existing = await db
    .select()
    .from(alertDedupe)
    .where(and(
      eq(alertDedupe.tenantId, args.tenantId),
      eq(alertDedupe.clusterId, args.clusterId),
      eq(alertDedupe.channel, args.channel),
      eq(alertDedupe.endpoint, args.endpoint),
    ))
    .limit(1);

  if (existing.length === 0) {
    return true;
  }

  const row = existing[0];
  const lastSent = row.lastSentAt instanceof Date ? row.lastSentAt : new Date(row.lastSentAt);
  const ttlMs = Number(row.ttlHours ?? 24) * 60 * 60 * 1000;
  return args.now.getTime() >= lastSent.getTime() + ttlMs;
}

export async function markAlertSent(args: AlertDedupeKey & { now: Date; ttlHours?: number }) {
  const ttl = args.ttlHours ?? 24;
  await db
    .insert(alertDedupe)
    .values({
      tenantId: args.tenantId,
      clusterId: args.clusterId,
      channel: args.channel,
      endpoint: args.endpoint,
      lastSentAt: args.now,
      ttlHours: ttl,
    })
    .onConflictDoUpdate({
      target: [alertDedupe.tenantId, alertDedupe.clusterId, alertDedupe.channel, alertDedupe.endpoint],
      set: {
        lastSentAt: args.now,
        ttlHours: ttl,
      },
    });
}
