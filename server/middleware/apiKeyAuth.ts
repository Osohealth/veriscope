import type { Request, Response, NextFunction } from "express";
import { timingSafeEqual } from "crypto";
import { and, eq, isNull } from "drizzle-orm";
import { db } from "../db";
import { apiKeys, tenantUsers } from "@shared/schema";
import { hashApiKey } from "../services/apiKeyService";
import { resolveTenantId, TENANT_DEMO_ID } from "../config/tenancy";
import type { Role } from "../auth/authTypes";
import { ensureRequestId, setAuditContextFromAuth, UNKNOWN_TENANT_ID } from "./requestContext";
import { writeAuditEvent } from "../services/auditLog";
import { applyRateLimit } from "./rateLimitMiddleware";

export interface ApiKeyAuthContext {
  tenantId: string;
  userId: string;
  role: Role;
  apiKeyId: string;
  apiKeyName?: string | null;
  keyHash: string;
}

declare global {
  namespace Express {
    interface Request {
      auth?: ApiKeyAuthContext;
    }
  }
}

export async function authenticateApiKey(req: Request, res: Response, next: NextFunction) {
  try {
    ensureRequestId(req);
    const authHeader = req.headers.authorization;
    const headerKey = authHeader && authHeader.startsWith("Bearer ") ? authHeader.substring(7) : null;
    const key = headerKey || (req.headers["x-api-key"] as string | undefined);

    if (!key) {
      await writeAuditEvent(req.auditContext, {
        action: "AUTH.API_KEY_DENIED",
        resourceType: "API_KEY",
        status: "DENIED",
        severity: "SECURITY",
        message: "API key missing.",
        metadata: { path: req.path, method: req.method },
        tenantId: UNKNOWN_TENANT_ID,
      });
      return res.status(401).json({ error: "UNAUTHORIZED", detail: "API key required" });
    }

    if (process.env.NODE_ENV === "development") {
      const devKey = process.env.DEMO_API_KEY;
      if (devKey && key === devKey) {
        const tenantId = TENANT_DEMO_ID;
        const userId = "00000000-0000-0000-0000-000000000001";
        const [member] = await db
          .select()
          .from(tenantUsers)
          .where(and(eq(tenantUsers.tenantId, tenantId), eq(tenantUsers.userId, userId)))
          .limit(1);
        if (!member) {
          await db
            .insert(tenantUsers)
            .values({
              tenantId,
              userId,
              email: `demo+${userId.slice(0, 6)}@veriscope.dev`,
              role: "OWNER",
              status: "ACTIVE",
              createdBy: userId,
            })
            .onConflictDoNothing();
        }
        req.auth = {
          tenantId,
          userId,
          role: "OWNER",
          apiKeyId: "dev",
          apiKeyName: "dev",
          keyHash: hashApiKey(key),
        };
        setAuditContextFromAuth(req);
        const allowed = await applyRateLimit(req, res);
        if (!allowed) return;
        return next();
      }
    }

    const envKey = process.env.ALERTS_API_KEY;
    if (envKey) {
      const envUserId = process.env.ALERTS_USER_ID;
      if (!envUserId) {
        return res.status(500).json({ error: "ALERTS_USER_ID is required when ALERTS_API_KEY is set" });
      }
      if (!timingSafeEqual(Buffer.from(hashApiKey(key)), Buffer.from(hashApiKey(envKey)))) {
        await writeAuditEvent(req.auditContext, {
          action: "AUTH.API_KEY_DENIED",
          resourceType: "API_KEY",
          status: "DENIED",
          severity: "SECURITY",
          message: "API key invalid.",
          metadata: { path: req.path, method: req.method },
          tenantId: UNKNOWN_TENANT_ID,
        });
        return res.status(401).json({ error: "UNAUTHORIZED", detail: "Invalid API key" });
      }
      const envRole = (process.env.ALERTS_ROLE || "OWNER").toUpperCase() as Role;
      if (!["OWNER", "OPERATOR", "VIEWER"].includes(envRole)) {
        return res.status(500).json({ error: "Invalid ALERTS_ROLE" });
      }
      const tenantId = resolveTenantId();
      const [member] = await db
        .select()
        .from(tenantUsers)
        .where(and(eq(tenantUsers.tenantId, tenantId), eq(tenantUsers.userId, envUserId)))
        .limit(1);
      if (!member) {
        await db
          .insert(tenantUsers)
          .values({
            tenantId,
            userId: envUserId,
            email: `demo+${envUserId.slice(0, 6)}@veriscope.dev`,
            role: envRole,
            status: "ACTIVE",
            createdBy: envUserId,
          })
          .onConflictDoNothing();
      } else if (String(member.status).toUpperCase() !== "ACTIVE") {
        await writeAuditEvent(req.auditContext, {
          action: "AUTH.MEMBERSHIP_DENIED",
          resourceType: "TENANT_MEMBERSHIP",
          status: "DENIED",
          severity: "SECURITY",
          message: "User membership disabled.",
          metadata: { path: req.path, method: req.method },
          tenantId,
        });
        return res.status(403).json({ error: "FORBIDDEN", detail: "User access disabled" });
      }
      req.auth = {
        tenantId,
        userId: envUserId,
        role: envRole,
        apiKeyId: "env",
        apiKeyName: "env",
        keyHash: hashApiKey(key),
      };
      setAuditContextFromAuth(req);
      const allowed = await applyRateLimit(req, res);
      if (!allowed) return;
      return next();
    }

    const keyHash = hashApiKey(key);
    const [row] = await db
      .select()
      .from(apiKeys)
      .where(and(eq(apiKeys.keyHash, keyHash), isNull(apiKeys.revokedAt)))
      .limit(1);

    if (!row) {
      await writeAuditEvent(req.auditContext, {
        action: "AUTH.API_KEY_DENIED",
        resourceType: "API_KEY",
        status: "DENIED",
        severity: "SECURITY",
        message: "API key invalid or revoked.",
        metadata: { path: req.path, method: req.method },
        tenantId: UNKNOWN_TENANT_ID,
      });
      return res.status(401).json({ error: "UNAUTHORIZED", detail: "Invalid API key" });
    }
    if (row.expiresAt && new Date(row.expiresAt) < new Date()) {
      await writeAuditEvent(req.auditContext, {
        action: "AUTH.API_KEY_DENIED",
        resourceType: "API_KEY",
        status: "DENIED",
        severity: "SECURITY",
        message: "API key expired.",
        metadata: { path: req.path, method: req.method, apiKeyId: row.id },
        tenantId: row.tenantId ?? UNKNOWN_TENANT_ID,
      });
      return res.status(401).json({ error: "UNAUTHORIZED", detail: "API key has expired" });
    }
    let role = (row.role ?? "OWNER").toUpperCase() as Role;
    if (!["OWNER", "OPERATOR", "VIEWER"].includes(role)) {
      return res.status(500).json({ error: "Invalid role on API key" });
    }
    const tenantId = row.tenantId ?? TENANT_DEMO_ID;
    const userId = row.userId ?? TENANT_DEMO_ID;
    const [member] = await db
      .select()
      .from(tenantUsers)
      .where(and(eq(tenantUsers.tenantId, tenantId), eq(tenantUsers.userId, userId)))
      .limit(1);
    if (!member) {
      await writeAuditEvent(req.auditContext, {
        action: "AUTH.MEMBERSHIP_DENIED",
        resourceType: "TENANT_MEMBERSHIP",
        status: "DENIED",
        severity: "SECURITY",
        message: "User not found in tenant.",
        metadata: { path: req.path, method: req.method },
        tenantId,
      });
      return res.status(403).json({ error: "FORBIDDEN", detail: "User not found in tenant" });
    }
    if (String(member.status).toUpperCase() !== "ACTIVE") {
      await writeAuditEvent(req.auditContext, {
        action: "AUTH.MEMBERSHIP_DENIED",
        resourceType: "TENANT_MEMBERSHIP",
        status: "DENIED",
        severity: "SECURITY",
        message: "User membership disabled.",
        metadata: { path: req.path, method: req.method },
        tenantId,
      });
      return res.status(403).json({ error: "FORBIDDEN", detail: "User access disabled" });
    }
    req.auth = { tenantId, userId, role, apiKeyId: row.id, apiKeyName: row.name ?? null, keyHash: row.keyHash };
    setAuditContextFromAuth(req);
    const allowed = await applyRateLimit(req, res);
    if (!allowed) return;
    next();
  } catch (error: any) {
    res.status(500).json({ error: error.message || "Auth failed" });
  }
}
