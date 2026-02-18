import { db } from "../db";
import { auditEvents } from "@shared/schema";
import type { AuditContext } from "../middleware/requestContext";

export type AuditSeverity = "INFO" | "WARN" | "SECURITY";
export type AuditStatus = "SUCCESS" | "DENIED" | "FAILED";
export type ActorType = "API_KEY" | "USER" | "SYSTEM";

const isUuid = (value?: string | null) =>
  typeof value === "string" &&
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

export type AuditEventInput = {
  action: string;
  resourceType: string;
  resourceId?: string | null;
  severity?: AuditSeverity;
  status: AuditStatus;
  message: string;
  metadata?: Record<string, unknown>;
  actorType?: ActorType;
  actorUserId?: string | null;
  actorApiKeyId?: string | null;
  actorLabel?: string | null;
  tenantId?: string;
};

export async function writeAuditEvent(ctx: AuditContext | undefined, input: AuditEventInput) {
  const tenantId = input.tenantId ?? ctx?.tenantId;
  if (!tenantId) return;
  try {
    const actorUserId = input.actorUserId ?? ctx?.actorUserId ?? null;
    const actorApiKeyId = input.actorApiKeyId ?? ctx?.actorApiKeyId ?? null;
    await db.insert(auditEvents).values({
      tenantId,
      actorType: input.actorType ?? ctx?.actorType ?? "SYSTEM",
      actorUserId: isUuid(actorUserId) ? actorUserId : null,
      actorApiKeyId: isUuid(actorApiKeyId) ? actorApiKeyId : null,
      actorLabel: input.actorLabel ?? ctx?.actorLabel ?? null,
      action: input.action,
      resourceType: input.resourceType,
      resourceId: input.resourceId ?? null,
      severity: input.severity ?? "INFO",
      status: input.status,
      message: input.message,
      metadata: input.metadata ?? {},
      ip: ctx?.ip ?? null,
      userAgent: ctx?.userAgent ?? null,
      requestId: ctx?.requestId ?? null,
    });
  } catch {
    // Never block product flow on audit write failure.
  }
}
