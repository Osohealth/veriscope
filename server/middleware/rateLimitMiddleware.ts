import type { Request, Response, NextFunction } from "express";
import { getRateLimits, RATE_LIMIT_WINDOW_SECONDS } from "../config/rateLimit";
import { incrementRateLimitBucket, getWindowStart } from "../services/rateLimit";
import { writeAuditEvent } from "../services/auditLog";

const isWriteMethod = (method: string) => ["POST", "PATCH", "DELETE"].includes(method.toUpperCase());

const getScopes = (req: Request, limits: ReturnType<typeof getRateLimits>) => {
  const scopes: Array<{ scope: string; limit: number }> = [
    { scope: "GLOBAL", limit: limits.GLOBAL_PER_MIN },
  ];

  const path = req.path || "";
  if (path.startsWith("/v1/audit-events/export")) {
    scopes.push({ scope: "EXPORT", limit: limits.EXPORT_PER_MIN });
  }

  if (isWriteMethod(req.method || "GET")) {
    scopes.push({ scope: "WRITE", limit: limits.WRITE_PER_MIN });
  }

  return scopes;
};

export const applyRateLimit = async (req: Request, res: Response) => {
  const auth = req.auth as any;
  if (!auth?.tenantId || !auth?.keyHash) return true;

  if (req.path === "/api/dev/webhook-sink") return true;

  const limits = getRateLimits();
  const nowMs = Date.now();
  const windowStart = getWindowStart(nowMs);
  const resetEpoch = Math.floor((windowStart.getTime() + RATE_LIMIT_WINDOW_SECONDS * 1000) / 1000);
  const retryAfter = Math.max(0, resetEpoch - Math.floor(nowMs / 1000));

  const scopes = getScopes(req, limits);

  let globalHeadersSet = false;

  for (const scope of scopes) {
    const { count } = await incrementRateLimitBucket({
      tenantId: auth.tenantId,
      keyHash: auth.keyHash,
      scope: scope.scope,
      windowStart,
    });

    if (scope.scope === "GLOBAL" && !globalHeadersSet) {
      res.setHeader("X-RateLimit-Limit", String(scope.limit));
      res.setHeader("X-RateLimit-Remaining", String(Math.max(0, scope.limit - count)));
      res.setHeader("X-RateLimit-Reset", String(resetEpoch));
      globalHeadersSet = true;
    }

    if (count > scope.limit) {
      if (count === scope.limit + 1) {
        await writeAuditEvent(req.auditContext, {
          action: "SECURITY.RATE_LIMIT_EXCEEDED",
          resourceType: "HTTP",
          status: "DENIED",
          severity: "SECURITY",
          message: "Rate limit exceeded",
          metadata: {
            scope: scope.scope,
            path: req.path,
            method: req.method,
            limit: scope.limit,
            window_start: windowStart.toISOString(),
          },
        });
      }

      res.setHeader("Retry-After", String(retryAfter));
      res.setHeader("X-RateLimit-Limit", String(scope.limit));
      res.setHeader("X-RateLimit-Remaining", "0");
      res.setHeader("X-RateLimit-Reset", String(resetEpoch));

      res.status(429).json({
        error: "Rate limit exceeded",
        scope: scope.scope,
        limit: scope.limit,
        window_seconds: RATE_LIMIT_WINDOW_SECONDS,
        retry_after_seconds: retryAfter,
      });
      return false;
    }
  }

  return true;
};

export const rateLimitMiddleware = async (req: Request, res: Response, next: NextFunction) => {
  const allowed = await applyRateLimit(req, res);
  if (!allowed) return;
  next();
};
