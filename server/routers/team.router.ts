import { Router } from "express";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { randomUUID } from "node:crypto";
import { db } from "../db";
import { apiKeys, tenantInvites, tenantUsers } from "@shared/schema";
import { authenticateApiKey } from "../middleware/apiKeyAuth";
import { requireRole } from "../auth/requireRole";
import { writeAuditEvent } from "../services/auditLog";
import { listTeamUsersDirectory } from "../services/teamUsersDirectory";
import {
  createUserContactMethod,
  deleteUserContactMethod,
  listUserContactMethods,
  updateUserContactMethod,
} from "../services/userContactMethodsService";
import { generateApiKey, hashApiKey } from "../services/apiKeyService";
import { generateInviteToken, hashInviteToken } from "../services/inviteService";
import { logger } from "../middleware/observability";
import { parseSafeLimit } from "../utils/pagination";

export const teamRouter = Router();

// ---- Private helpers ----

const TEAM_ROLES = ["OWNER", "OPERATOR", "VIEWER"] as const;
const TEAM_STATUSES = ["ACTIVE", "INVITED", "DISABLED"] as const;

const normalizeTeamRole = (value?: string, fallback: (typeof TEAM_ROLES)[number] = "VIEWER") => {
  const role = String(value ?? fallback).toUpperCase();
  return TEAM_ROLES.includes(role as any) ? (role as (typeof TEAM_ROLES)[number]) : fallback;
};

const normalizeTeamStatus = (value?: string, fallback: (typeof TEAM_STATUSES)[number] = "ACTIVE") => {
  const status = String(value ?? fallback).toUpperCase();
  return TEAM_STATUSES.includes(status as any) ? (status as (typeof TEAM_STATUSES)[number]) : fallback;
};

const normalizeEmail = (value: string) => value.trim().toLowerCase();
const isValidEmail = (value: string) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
const isValidUuid = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

// Inline ensureRole — emitting audit on rejection
async function guardRole(
  req: any,
  res: any,
  minRole: (typeof TEAM_ROLES)[number],
): Promise<boolean> {
  try {
    requireRole(req.auth, minRole);
    return true;
  } catch (err: any) {
    await writeAuditEvent(req.auditContext, {
      action: "AUTH.RBAC_DENIED",
      resourceType: "RBAC",
      status: "DENIED",
      severity: "SECURITY",
      message: "Role is not permitted for this operation.",
      metadata: { path: req.path, method: req.method, role: req.auth?.role, required: minRole },
    }).catch(() => { });
    const status = err?.status ?? 403;
    const code = status === 401 ? "UNAUTHORIZED" : "FORBIDDEN";
    res.status(status).json({ error: code, detail: err?.message ?? "Forbidden" });
    return false;
  }
}

const mapMember = (row: typeof tenantUsers.$inferSelect) => ({
  id: row.id,
  user_id: row.userId,
  email: row.email,
  display_name: row.displayName ?? null,
  role: row.role,
  status: row.status,
  created_at: row.createdAt,
  created_by: row.createdBy ?? null,
  revoked_at: row.revokedAt ?? null,
  revoked_by: row.revokedBy ?? null,
});

// GET /v1/me
teamRouter.get("/v1/me", authenticateApiKey, (req, res) => {
  requireRole(req.auth, "VIEWER");
  res.json({
    version: "1",
    tenant_id: req.auth?.tenantId,
    user_id: req.auth?.userId,
    role: req.auth?.role,
  });
});

// GET /v1/team/members
teamRouter.get("/v1/team/members", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const members = await db
      .select()
      .from(tenantUsers)
      .where(eq(tenantUsers.tenantId, tenantId))
      .orderBy(desc(tenantUsers.createdAt));

    res.json({ version: "1", items: members.map(mapMember) });
  } catch (err) {
    logger.error("Failed to list team members", { err });
    next(err);
  }
});

// PATCH /v1/team/members/:id
teamRouter.patch("/v1/team/members/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const memberId = req.params.id;
    const [member] = await db
      .select()
      .from(tenantUsers)
      .where(and(eq(tenantUsers.id, memberId), eq(tenantUsers.tenantId, tenantId)))
      .limit(1);
    if (!member) return res.status(404).json({ error: "Member not found" });

    const updates: Record<string, unknown> = {};

    if (req.body?.role) {
      const nextRole = normalizeTeamRole(req.body.role);
      if (member.role === "OWNER" && nextRole !== "OWNER") {
        const [ownerCount] = await db
          .select({ count: sql`COUNT(*)::int` })
          .from(tenantUsers)
          .where(
            and(
              eq(tenantUsers.tenantId, tenantId),
              eq(tenantUsers.role, "OWNER"),
              eq(tenantUsers.status, "ACTIVE"),
            ),
          );
        if (Number(ownerCount?.count ?? 0) <= 1) {
          return res.status(400).json({ error: "Cannot demote last OWNER" });
        }
      }
      updates.role = nextRole;
    }

    if (req.body?.status) {
      const nextStatus = normalizeTeamStatus(req.body.status);
      if (member.role === "OWNER" && member.status === "ACTIVE" && nextStatus !== "ACTIVE") {
        const [ownerCount] = await db
          .select({ count: sql`COUNT(*)::int` })
          .from(tenantUsers)
          .where(
            and(
              eq(tenantUsers.tenantId, tenantId),
              eq(tenantUsers.role, "OWNER"),
              eq(tenantUsers.status, "ACTIVE"),
            ),
          );
        if (Number(ownerCount?.count ?? 0) <= 1) {
          return res.status(400).json({ error: "Cannot disable last OWNER" });
        }
      }
      updates.status = nextStatus;
      if (nextStatus === "DISABLED") {
        updates.revokedAt = new Date();
        updates.revokedBy = userId;
      }
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: "No valid updates provided" });
    }

    const [updated] = await db
      .update(tenantUsers)
      .set(updates as any)
      .where(and(eq(tenantUsers.id, memberId), eq(tenantUsers.tenantId, tenantId)))
      .returning();

    if (!updated) return res.status(404).json({ error: "Member not found" });

    if (updates.role) {
      await db
        .update(apiKeys)
        .set({ role: String(updates.role) })
        .where(and(eq(apiKeys.tenantId, tenantId), eq(apiKeys.userId, updated.userId)));
      await writeAuditEvent(req.auditContext, {
        action: "TEAM.MEMBER_ROLE_UPDATED",
        resourceType: "TENANT_MEMBER",
        resourceId: updated.id,
        severity: "SECURITY",
        status: "SUCCESS",
        message: `Member role updated to ${updates.role}`,
        metadata: { email: updated.email, from: member.role, to: updates.role },
      });
    }
    if (updates.status === "DISABLED") {
      await db
        .update(apiKeys)
        .set({ revokedAt: new Date() })
        .where(and(eq(apiKeys.tenantId, tenantId), eq(apiKeys.userId, updated.userId)));
      await writeAuditEvent(req.auditContext, {
        action: "TEAM.MEMBER_DISABLED",
        resourceType: "TENANT_MEMBER",
        resourceId: updated.id,
        severity: "SECURITY",
        status: "SUCCESS",
        message: "Member disabled",
        metadata: { email: updated.email },
      });
    }
    if (updates.status === "ACTIVE") {
      await writeAuditEvent(req.auditContext, {
        action: "TEAM.MEMBER_ENABLED",
        resourceType: "TENANT_MEMBER",
        resourceId: updated.id,
        severity: "SECURITY",
        status: "SUCCESS",
        message: "Member enabled",
        metadata: { email: updated.email },
      });
    }

    res.json({ version: "1", item: mapMember(updated) });
  } catch (err) {
    logger.error("Failed to update member", { err });
    next(err);
  }
});

// DELETE /v1/team/members/:id
teamRouter.delete("/v1/team/members/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const memberId = req.params.id;
    const [member] = await db
      .select()
      .from(tenantUsers)
      .where(and(eq(tenantUsers.id, memberId), eq(tenantUsers.tenantId, tenantId)))
      .limit(1);
    if (!member) return res.status(404).json({ error: "Member not found" });

    if (member.role === "OWNER" && member.status === "ACTIVE") {
      const [ownerCount] = await db
        .select({ count: sql`COUNT(*)::int` })
        .from(tenantUsers)
        .where(
          and(
            eq(tenantUsers.tenantId, tenantId),
            eq(tenantUsers.role, "OWNER"),
            eq(tenantUsers.status, "ACTIVE"),
          ),
        );
      if (Number(ownerCount?.count ?? 0) <= 1) {
        return res.status(400).json({ error: "Cannot revoke last OWNER" });
      }
    }

    const [updated] = await db
      .update(tenantUsers)
      .set({ status: "DISABLED", revokedAt: new Date(), revokedBy: userId })
      .where(and(eq(tenantUsers.id, memberId), eq(tenantUsers.tenantId, tenantId)))
      .returning();

    if (updated) {
      await db
        .update(apiKeys)
        .set({ revokedAt: new Date() })
        .where(and(eq(apiKeys.tenantId, tenantId), eq(apiKeys.userId, updated.userId)));
      await writeAuditEvent(req.auditContext, {
        action: "TEAM.MEMBER_REVOKED",
        resourceType: "TENANT_MEMBER",
        resourceId: updated.id,
        severity: "SECURITY",
        status: "SUCCESS",
        message: "Member revoked",
        metadata: { email: updated.email },
      });
    }

    res.json({ version: "1", revoked: true });
  } catch (err) {
    logger.error("Failed to revoke member", { err });
    next(err);
  }
});

// GET /v1/team/users
teamRouter.get("/v1/team/users", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const query = req.query?.query ? String(req.query.query) : undefined;
    const limit = parseSafeLimit(req.query?.limit, 20, 50);
    let cursorCreatedAt: string | null = null;
    let cursorId: string | null = null;

    if (req.query?.cursor) {
      try {
        const decoded = Buffer.from(String(req.query.cursor), "base64").toString("utf8");
        const [createdAtIso, id] = decoded.split("|");
        const createdAtMs = Date.parse(createdAtIso);
        if (createdAtIso && id && !Number.isNaN(createdAtMs)) {
          cursorCreatedAt = createdAtIso;
          cursorId = id;
        } else {
          return res.status(400).json({ error: "Invalid cursor" });
        }
      } catch {
        return res.status(400).json({ error: "Invalid cursor" });
      }
    }

    const { items, nextCursor } = await listTeamUsersDirectory({ tenantId, query, limit, cursorCreatedAt, cursorId });
    res.json({ version: "1", items, next_cursor: nextCursor });
  } catch (err) {
    logger.error("Failed to list team users", { err });
    next(err);
  }
});

// GET /v1/team/invites
teamRouter.get("/v1/team/invites", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const invites = await db
      .select()
      .from(tenantInvites)
      .where(eq(tenantInvites.tenantId, tenantId))
      .orderBy(desc(tenantInvites.createdAt));

    res.json({
      version: "1",
      items: invites.map((row) => ({
        id: row.id,
        email: row.email,
        role: row.role,
        created_at: row.createdAt,
        created_by: row.createdBy,
        expires_at: row.expiresAt,
        accepted_at: row.acceptedAt ?? null,
        accepted_by_user_id: row.acceptedByUserId ?? null,
        revoked_at: row.revokedAt ?? null,
        revoked_by: row.revokedBy ?? null,
      })),
    });
  } catch (err) {
    logger.error("Failed to list invites", { err });
    next(err);
  }
});

// POST /v1/team/invites
teamRouter.post("/v1/team/invites", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const inviterId = req.auth?.userId;
    if (!tenantId || !inviterId) return res.status(401).json({ error: "API key required" });

    const email = normalizeEmail(String(req.body?.email ?? ""));
    if (!isValidEmail(email)) return res.status(400).json({ error: "Invalid email" });

    const role = normalizeTeamRole(req.body?.role, "VIEWER");

    const [pendingInvite] = await db
      .select()
      .from(tenantInvites)
      .where(
        and(
          eq(tenantInvites.tenantId, tenantId),
          eq(tenantInvites.email, email),
          isNull(tenantInvites.acceptedAt),
          isNull(tenantInvites.revokedAt),
        ),
      )
      .limit(1);
    if (pendingInvite) return res.status(409).json({ error: "Invite already pending" });

    const token = generateInviteToken();
    const tokenHash = hashInviteToken(token);
    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);

    const [invite] = await db
      .insert(tenantInvites)
      .values({ tenantId, email, role, tokenHash, expiresAt, createdBy: inviterId })
      .returning();

    const includeToken =
      process.env.NODE_ENV === "development" || process.env.DEV_ROUTES_ENABLED === "true";
    const inviteLink = includeToken
      ? `${req.protocol}://${req.get("host")}/invite/accept?token=${token}`
      : undefined;

    res.status(201).json({
      version: "1",
      id: invite.id,
      email: invite.email,
      role: invite.role,
      expires_at: invite.expiresAt,
      ...(includeToken ? { invite_token: token, invite_link: inviteLink } : {}),
    });

    await writeAuditEvent(req.auditContext, {
      action: "TEAM.INVITE_CREATED",
      resourceType: "TENANT_INVITE",
      resourceId: invite.id,
      severity: "SECURITY",
      status: "SUCCESS",
      message: `Invite created for ${invite.email}`,
      metadata: { email: invite.email, role: invite.role },
    });
  } catch (err) {
    logger.error("Failed to create invite", { err });
    next(err);
  }
});

// POST /v1/team/invites/accept (no API key required — uses invite token)
teamRouter.post("/v1/team/invites/accept", async (req, res, next) => {
  try {
    const token = String(req.body?.token ?? "");
    if (!token) return res.status(400).json({ error: "token is required" });

    const tokenHash = hashInviteToken(token);
    const [invite] = await db
      .select()
      .from(tenantInvites)
      .where(
        and(
          eq(tenantInvites.tokenHash, tokenHash),
          isNull(tenantInvites.acceptedAt),
          isNull(tenantInvites.revokedAt),
        ),
      )
      .limit(1);
    if (!invite) return res.status(400).json({ error: "Invalid or expired invite" });
    if (invite.expiresAt && new Date(invite.expiresAt).getTime() < Date.now()) {
      return res.status(400).json({ error: "Invite expired" });
    }

    const [existingMember] = await db
      .select()
      .from(tenantUsers)
      .where(and(eq(tenantUsers.tenantId, invite.tenantId), eq(tenantUsers.email, invite.email)))
      .limit(1);

    const displayName = req.body?.display_name ? String(req.body.display_name) : null;
    const now = new Date();
    const userId = existingMember?.userId ?? randomUUID();

    if (existingMember) {
      await db
        .update(tenantUsers)
        .set({ status: "ACTIVE", role: invite.role, displayName: displayName ?? existingMember.displayName })
        .where(eq(tenantUsers.id, existingMember.id));
    } else {
      await db.insert(tenantUsers).values({
        tenantId: invite.tenantId,
        userId,
        email: invite.email,
        displayName,
        role: invite.role,
        status: "ACTIVE",
        createdBy: invite.createdBy,
        createdAt: now,
      });
    }

    const rawKey = generateApiKey("vs_user");
    const [apiKeyRow] = await db
      .insert(apiKeys)
      .values({
        tenantId: invite.tenantId,
        userId,
        keyHash: hashApiKey(rawKey),
        name: "invited",
        role: invite.role,
        createdAt: now,
      })
      .returning();

    await db
      .update(tenantInvites)
      .set({ acceptedAt: now, acceptedByUserId: userId })
      .where(eq(tenantInvites.id, invite.id));

    await writeAuditEvent(req.auditContext, {
      action: "TEAM.INVITE_ACCEPTED",
      resourceType: "TENANT_INVITE",
      resourceId: invite.id,
      severity: "SECURITY",
      status: "SUCCESS",
      message: `Invite accepted for ${invite.email}`,
      metadata: { email: invite.email, role: invite.role },
      tenantId: invite.tenantId,
      actorType: "USER",
      actorUserId: userId,
      actorLabel: invite.email,
    });

    res.json({ version: "1", user_id: userId, api_key: rawKey, api_key_id: apiKeyRow.id });
  } catch (err) {
    logger.error("Failed to accept invite", { err });
    next(err);
  }
});

// GET /v1/team/api-keys
teamRouter.get("/v1/team/api-keys", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    const role = req.auth?.role;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const targetUserId = role === "OWNER" && req.query.user_id ? String(req.query.user_id) : userId;
    const keys = await db
      .select()
      .from(apiKeys)
      .where(and(eq(apiKeys.tenantId, tenantId), eq(apiKeys.userId, targetUserId)))
      .orderBy(desc(apiKeys.createdAt));

    res.json({
      version: "1",
      items: keys.map((row) => ({
        id: row.id,
        user_id: row.userId,
        name: row.name ?? null,
        role: row.role ?? null,
        created_at: row.createdAt,
        revoked_at: row.revokedAt ?? null,
      })),
    });
  } catch (err) {
    logger.error("Failed to list API keys", { err });
    next(err);
  }
});

// POST /v1/team/api-keys
teamRouter.post("/v1/team/api-keys", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    const role = req.auth?.role;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const targetUserId = role === "OWNER" && req.body?.user_id ? String(req.body.user_id) : userId;
    const name = req.body?.name ? String(req.body.name) : null;

    const [member] = await db
      .select()
      .from(tenantUsers)
      .where(and(eq(tenantUsers.tenantId, tenantId), eq(tenantUsers.userId, targetUserId)))
      .limit(1);
    if (!member || String(member.status).toUpperCase() !== "ACTIVE") {
      return res.status(400).json({ error: "User is not active in tenant" });
    }

    const rawKey = generateApiKey("vs_user");
    const [created] = await db
      .insert(apiKeys)
      .values({ tenantId, userId: targetUserId, keyHash: hashApiKey(rawKey), name, role: member.role ?? "VIEWER" })
      .returning();

    res.status(201).json({
      version: "1",
      api_key: rawKey,
      item: { id: created.id, user_id: created.userId, name: created.name ?? null, role: created.role ?? null, created_at: created.createdAt, revoked_at: created.revokedAt ?? null },
    });

    await writeAuditEvent(req.auditContext, {
      action: "TEAM.API_KEY_CREATED",
      resourceType: "API_KEY",
      resourceId: created.id,
      severity: "SECURITY",
      status: "SUCCESS",
      message: "API key created",
      metadata: { user_id: created.userId, role: created.role ?? null },
    });
  } catch (err) {
    logger.error("Failed to create API key", { err });
    next(err);
  }
});

// POST /v1/team/api-keys/:id/rotate
teamRouter.post("/v1/team/api-keys/:id/rotate", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    const role = req.auth?.role;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const keyId = req.params.id;
    const [existing] = await db
      .select()
      .from(apiKeys)
      .where(and(eq(apiKeys.id, keyId), eq(apiKeys.tenantId, tenantId)))
      .limit(1);
    if (!existing) return res.status(404).json({ error: "API key not found" });
    if (role !== "OWNER" && existing.userId !== userId) return res.status(403).json({ error: "Forbidden" });

    await db.update(apiKeys).set({ revokedAt: new Date() }).where(eq(apiKeys.id, keyId));

    const rawKey = generateApiKey("vs_user");
    const [created] = await db
      .insert(apiKeys)
      .values({ tenantId, userId: existing.userId, keyHash: hashApiKey(rawKey), name: existing.name ?? "rotated", role: existing.role ?? "VIEWER" })
      .returning();

    res.json({
      version: "1",
      api_key: rawKey,
      item: { id: created.id, user_id: created.userId, name: created.name ?? null, role: created.role ?? null, created_at: created.createdAt, revoked_at: created.revokedAt ?? null },
    });

    await writeAuditEvent(req.auditContext, {
      action: "API_KEY.ROTATED",
      resourceType: "API_KEY",
      resourceId: created.id,
      severity: "SECURITY",
      status: "SUCCESS",
      message: "API key rotated",
      metadata: { user_id: created.userId },
    });
  } catch (err) {
    logger.error("Failed to rotate API key", { err });
    next(err);
  }
});

// DELETE /v1/team/api-keys/:id
teamRouter.delete("/v1/team/api-keys/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    const userId = req.auth?.userId;
    const role = req.auth?.role;
    if (!tenantId || !userId) return res.status(401).json({ error: "API key required" });

    const keyId = req.params.id;
    const [existing] = await db
      .select()
      .from(apiKeys)
      .where(and(eq(apiKeys.id, keyId), eq(apiKeys.tenantId, tenantId)))
      .limit(1);
    if (!existing) return res.status(404).json({ error: "API key not found" });
    if (role !== "OWNER" && existing.userId !== userId) return res.status(403).json({ error: "Forbidden" });

    await db.update(apiKeys).set({ revokedAt: new Date() }).where(eq(apiKeys.id, keyId));

    await writeAuditEvent(req.auditContext, {
      action: "API_KEY.REVOKED",
      resourceType: "API_KEY",
      resourceId: existing.id,
      severity: "SECURITY",
      status: "SUCCESS",
      message: "API key revoked",
      metadata: { user_id: existing.userId },
    });

    res.json({ version: "1", revoked: true });
  } catch (err) {
    logger.error("Failed to revoke API key", { err });
    next(err);
  }
});

// GET /v1/users/:user_id/contact-methods
teamRouter.get("/v1/users/:user_id/contact-methods", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "VIEWER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const userId = String(req.params.user_id ?? "");
    if (!isValidUuid(userId)) return res.status(400).json({ error: "Invalid user id" });

    const [user] = await db
      .select()
      .from(tenantUsers)
      .where(and(eq(tenantUsers.tenantId, tenantId), eq(tenantUsers.userId, userId)))
      .limit(1);
    if (!user) return res.status(404).json({ error: "User not found" });

    const activeOnly = String(req.query?.active_only ?? "true").toLowerCase() !== "false";
    const items = await listUserContactMethods({ tenantId, userId, activeOnly });
    res.json({ version: "1", items });
  } catch (err) {
    logger.error("Failed to list contact methods", { err });
    next(err);
  }
});

// POST /v1/users/:user_id/contact-methods
teamRouter.post("/v1/users/:user_id/contact-methods", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const userId = String(req.params.user_id ?? "");
    if (!isValidUuid(userId)) return res.status(400).json({ error: "Invalid user id" });

    const row = await createUserContactMethod({
      tenantId,
      userId,
      input: {
        type: req.body?.type,
        value: req.body?.value,
        label: req.body?.label ?? null,
        is_primary: req.body?.is_primary,
      },
    });

    await writeAuditEvent(req.auditContext, {
      action: "USER.CONTACT_METHOD_CREATED",
      resourceType: "user",
      resourceId: userId,
      severity: "INFO",
      status: "SUCCESS",
      message: "User contact method created",
      metadata: { contact_method_id: row.id, type: row.type, is_primary: row.isPrimary, is_active: row.isActive },
      tenantId,
    });

    res.json({
      version: "1",
      item: {
        id: row.id,
        tenant_id: row.tenantId,
        user_id: row.userId,
        type: row.type,
        value: row.value,
        label: row.label ?? null,
        is_primary: row.isPrimary,
        is_verified: row.isVerified,
        is_active: row.isActive,
        created_at: row.createdAt,
      },
    });
  } catch (err: any) {
    if (err?.message === "User not found") return res.status(404).json({ error: "User not found" });
    res.status(400).json({ error: "Failed to create contact method" });
  }
});

// PATCH /v1/users/:user_id/contact-methods/:id
teamRouter.patch("/v1/users/:user_id/contact-methods/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const userId = String(req.params.user_id ?? "");
    if (!isValidUuid(userId)) return res.status(400).json({ error: "Invalid user id" });

    const methodId = String(req.params.id ?? "");
    if (!isValidUuid(methodId)) return res.status(400).json({ error: "Invalid contact method id" });

    const row = await updateUserContactMethod({
      tenantId,
      userId,
      id: methodId,
      patch: { label: req.body?.label, is_primary: req.body?.is_primary, is_active: req.body?.is_active },
    });

    await writeAuditEvent(req.auditContext, {
      action: "USER.CONTACT_METHOD_UPDATED",
      resourceType: "user",
      resourceId: userId,
      severity: "INFO",
      status: "SUCCESS",
      message: "User contact method updated",
      metadata: { contact_method_id: row.id, type: row.type, is_primary: row.isPrimary, is_active: row.isActive },
      tenantId,
    });

    res.json({
      version: "1",
      item: {
        id: row.id,
        tenant_id: row.tenantId,
        user_id: row.userId,
        type: row.type,
        value: row.value,
        label: row.label ?? null,
        is_primary: row.isPrimary,
        is_verified: row.isVerified,
        is_active: row.isActive,
        created_at: row.createdAt,
      },
    });
  } catch (err: any) {
    if (err?.message === "Contact method not found") return res.status(404).json({ error: "Contact method not found" });
    res.status(400).json({ error: "Failed to update contact method" });
  }
});

// DELETE /v1/users/:user_id/contact-methods/:id
teamRouter.delete("/v1/users/:user_id/contact-methods/:id", authenticateApiKey, async (req, res, next) => {
  if (!(await guardRole(req, res, "OWNER"))) return;
  try {
    const tenantId = req.auth?.tenantId;
    if (!tenantId) return res.status(401).json({ error: "API key required" });

    const userId = String(req.params.user_id ?? "");
    if (!isValidUuid(userId)) return res.status(400).json({ error: "Invalid user id" });

    const methodId = String(req.params.id ?? "");
    if (!isValidUuid(methodId)) return res.status(400).json({ error: "Invalid contact method id" });

    const row = await deleteUserContactMethod({ tenantId, userId, id: methodId });

    await writeAuditEvent(req.auditContext, {
      action: "USER.CONTACT_METHOD_DELETED",
      resourceType: "user",
      resourceId: userId,
      severity: "INFO",
      status: "SUCCESS",
      message: "User contact method deleted",
      metadata: { contact_method_id: row.id, type: row.type },
      tenantId,
    });

    res.json({ version: "1", ok: true });
  } catch (err: any) {
    if (err?.message === "Contact method not found") return res.status(404).json({ error: "Contact method not found" });
    res.status(400).json({ error: "Failed to delete contact method" });
  }
});
