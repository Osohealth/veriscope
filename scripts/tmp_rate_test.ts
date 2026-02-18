import { db } from "../server/db";
import { sql, eq, and } from "drizzle-orm";
import { auditEvents } from "../shared/schema";
import { applyRateLimit } from "../server/middleware/rateLimitMiddleware";
import { hashApiKey } from "../server/services/apiKeyService";
import crypto from "node:crypto";

process.env.RATE_LIMIT_GLOBAL_PER_MIN = "2";
process.env.RATE_LIMIT_EXPORT_PER_MIN = "1";
process.env.RATE_LIMIT_WRITE_PER_MIN = "2";

const tenantId = crypto.randomUUID();
const keyHash = hashApiKey("rate_limit_key_test");
const userId = crypto.randomUUID();

const buildReq = (method: string, path: string) => ({
  method,
  path,
  auth: { tenantId, keyHash, userId, role: "OWNER", apiKeyId: "k", apiKeyName: "k" },
  auditContext: { tenantId, actorType: "API_KEY", actorUserId: userId, actorApiKeyId: "k", actorLabel: "k", requestId: crypto.randomUUID(), ip: null, userAgent: null },
} as any);

const resFactory = () => {
  const res: any = { statusCode: 200, body: null, headers: new Map<string,string>(), setHeader(k:string,v:string){ res.headers.set(k,String(v)); }, status(code:number){ res.statusCode = code; return res; }, json(body:any){ res.body=body; return res; } };
  return res;
};

await db.execute(sql`DELETE FROM rate_limit_buckets`);
await db.delete(auditEvents).where(eq(auditEvents.action, "SECURITY.RATE_LIMIT_EXCEEDED"));

await applyRateLimit(buildReq("GET","/v1/alert-deliveries"), resFactory());
await applyRateLimit(buildReq("GET","/v1/alert-deliveries"), resFactory());
await applyRateLimit(buildReq("GET","/v1/alert-deliveries"), resFactory());

const rows = await db.select().from(auditEvents).where(and(eq(auditEvents.action, "SECURITY.RATE_LIMIT_EXCEEDED"), eq(auditEvents.tenantId, tenantId)));
console.log(rows.length);
