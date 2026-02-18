import type { Request, Response, NextFunction } from "express";
import crypto from "node:crypto";

export type ActorType = "API_KEY" | "USER" | "SYSTEM";

export type AuditContext = {
  tenantId: string;
  userId?: string | null;
  role?: string | null;
  actorType: ActorType;
  actorUserId?: string | null;
  actorApiKeyId?: string | null;
  actorLabel?: string | null;
  requestId: string;
  ip?: string | null;
  userAgent?: string | null;
};

export const UNKNOWN_TENANT_ID = "00000000-0000-0000-0000-000000000000";

declare global {
  namespace Express {
    interface Request {
      requestId?: string;
      auditContext?: AuditContext;
    }
  }
}

export const requestIdMiddleware = (req: Request, res: Response, next: NextFunction) => {
  const incoming = req.header("X-Request-Id");
  req.requestId = incoming && incoming.trim().length > 0 ? incoming.trim() : crypto.randomUUID();
  res.setHeader("X-Request-Id", req.requestId);
  next();
};

export const buildAuditContext = (req: Request): AuditContext => {
  const auth = req.auth;
  const requestId = req.requestId ?? crypto.randomUUID();
  const userAgent =
    typeof (req as any).header === "function"
      ? (req as any).header("user-agent")
      : ((req as any).headers?.["user-agent"] as string | undefined);
  if (auth?.tenantId) {
    return {
      tenantId: auth.tenantId,
      userId: auth.userId ?? null,
      role: auth.role ?? null,
      actorType: "API_KEY",
      actorUserId: auth.userId ?? null,
      actorApiKeyId: auth.apiKeyId ?? null,
      actorLabel: auth.apiKeyName ?? null,
      requestId,
      ip: req.ip ?? null,
      userAgent: userAgent ?? null,
    };
  }
  return {
    tenantId: UNKNOWN_TENANT_ID,
    actorType: "SYSTEM",
    requestId,
    ip: req.ip ?? null,
    userAgent: userAgent ?? null,
  };
};

export const auditContextMiddleware = (req: Request, _res: Response, next: NextFunction) => {
  req.auditContext = buildAuditContext(req);
  next();
};

export const ensureRequestId = (req: Request) => {
  if (!req.requestId) {
    req.requestId = crypto.randomUUID();
  }
  return req.requestId;
};

export const setAuditContextFromAuth = (req: Request) => {
  req.auditContext = buildAuditContext(req);
};
