import type { Request, Response, NextFunction } from "express";
import { eq, isNull } from "drizzle-orm";
import { db } from "../db";
import { apiKeys } from "@shared/schema";
import { hashApiKey } from "../services/apiKeyService";
import { resolveTenantId, TENANT_DEMO_ID } from "../config/tenancy";

export interface ApiKeyAuthContext {
  tenantId: string;
  userId: string;
  apiKeyId: string;
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
    const authHeader = req.headers.authorization;
    const headerKey = authHeader && authHeader.startsWith("Bearer ") ? authHeader.substring(7) : null;
    const key = headerKey || (req.headers["x-api-key"] as string | undefined);

    if (!key) {
      return res.status(401).json({ error: "API key required" });
    }

    const envKey = process.env.ALERTS_API_KEY;
    if (envKey) {
      const envUserId = process.env.ALERTS_USER_ID;
      if (!envUserId) {
        return res.status(500).json({ error: "ALERTS_USER_ID is required when ALERTS_API_KEY is set" });
      }
      if (key !== envKey) {
        return res.status(401).json({ error: "Invalid API key" });
      }
      req.auth = { tenantId: resolveTenantId(), userId: envUserId, apiKeyId: "env" };
      return next();
    }

    const keyHash = hashApiKey(key);
    const [row] = await db
      .select()
      .from(apiKeys)
      .where(eq(apiKeys.keyHash, keyHash))
      .where(isNull(apiKeys.revokedAt))
      .limit(1);

    if (!row) {
      return res.status(401).json({ error: "Invalid API key" });
    }

    req.auth = { tenantId: row.tenantId ?? TENANT_DEMO_ID, userId: row.userId ?? TENANT_DEMO_ID, apiKeyId: row.id };
    next();
  } catch (error: any) {
    res.status(500).json({ error: error.message || "Auth failed" });
  }
}
