import { sql } from "drizzle-orm";
import { db } from "../db";
import { rateLimitBuckets } from "@shared/schema";

export const getWindowStart = (nowMs: number = Date.now()) => {
  return new Date(Math.floor(nowMs / 60000) * 60000);
};

export async function incrementRateLimitBucket(options: {
  tenantId: string;
  keyHash: string;
  scope: string;
  windowStart: Date;
}) {
  const result = await db.execute(sql`
    INSERT INTO ${rateLimitBuckets} (tenant_id, key_hash, scope, window_start, count, updated_at)
    VALUES (${options.tenantId}, ${options.keyHash}, ${options.scope}, ${options.windowStart}, 1, now())
    ON CONFLICT (tenant_id, key_hash, scope, window_start)
    DO UPDATE SET count = ${rateLimitBuckets.count} + 1, updated_at = now()
    RETURNING count;
  `);

  const row = Array.isArray(result?.rows) ? result.rows[0] : undefined;
  return { count: Number(row?.count ?? 1) };
}
