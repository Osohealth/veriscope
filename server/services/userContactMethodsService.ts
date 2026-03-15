import { and, eq, desc } from "drizzle-orm";
import { db } from "../db";
import { tenantUsers, userContactMethods } from "@shared/schema";
import { getTenantAllowlist } from "./tenantSettings";
import { allowlistHostMatches, allowlistMatches } from "./allowlistUtils";

const EMAIL_REGEX = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

type ContactMethodType = "EMAIL" | "WEBHOOK";

type CreateInput = {
  type: ContactMethodType;
  value: string;
  label?: string | null;
  is_primary?: boolean;
};

type UpdateInput = {
  label?: string | null;
  is_primary?: boolean;
  is_active?: boolean;
};

export async function listUserContactMethods(args: { tenantId: string; userId: string; activeOnly?: boolean }) {
  const filters = [
    eq(userContactMethods.tenantId, args.tenantId),
    eq(userContactMethods.userId, args.userId),
  ];
  if (args.activeOnly !== false) {
    filters.push(eq(userContactMethods.isActive, true));
  }
  const rows = await db
    .select()
    .from(userContactMethods)
    .where(and(...filters))
    .orderBy(desc(userContactMethods.isPrimary), desc(userContactMethods.createdAt));

  return rows.map((row) => ({
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
  }));
}

export async function createUserContactMethod(args: {
  tenantId: string;
  userId: string;
  input: CreateInput;
}) {
  const { tenantId, userId, input } = args;
  const [user] = await db
    .select()
    .from(tenantUsers)
    .where(and(
      eq(tenantUsers.tenantId, tenantId),
      eq(tenantUsers.userId, userId),
    ))
    .limit(1);
  if (!user) {
    throw new Error("User not found");
  }

  const type = String(input.type ?? "").toUpperCase() as ContactMethodType;
  if (!type || (type !== "EMAIL" && type !== "WEBHOOK")) {
    throw new Error("type must be EMAIL or WEBHOOK");
  }

  const value = String(input.value ?? "").trim();
  if (!value) {
    throw new Error("value is required");
  }

  if (type === "EMAIL") {
    if (!EMAIL_REGEX.test(value) || value.length > 254) {
      throw new Error("value must be a valid email");
    }
    if (process.env.NODE_ENV === "production") {
      const { allowed_email_domains } = await getTenantAllowlist(tenantId);
      const domain = value.split("@")[1]?.toLowerCase() ?? "";
      if (allowed_email_domains.length === 0 || !allowlistMatches(domain, allowed_email_domains)) {
        throw new Error("email domain not allowed");
      }
    }
  }

  if (type === "WEBHOOK") {
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      throw new Error("value must be a valid URL");
    }
    const isLocal = url.hostname === "localhost" || url.hostname === "127.0.0.1";
    if (process.env.NODE_ENV === "production") {
      if (url.protocol !== "https:") {
        throw new Error("webhook must be https in production");
      }
      const { allowed_webhook_hosts } = await getTenantAllowlist(tenantId);
      const host = url.hostname.toLowerCase();
      if (allowed_webhook_hosts.length === 0 || !allowlistHostMatches(host, allowed_webhook_hosts)) {
        throw new Error("webhook host not allowed");
      }
    } else if (url.protocol !== "https:" && !isLocal) {
      throw new Error("webhook must be https or localhost in development");
    }
  }

  const isPrimary = Boolean(input.is_primary);
  if (isPrimary) {
    await db
      .update(userContactMethods)
      .set({ isPrimary: false })
      .where(and(
        eq(userContactMethods.tenantId, tenantId),
        eq(userContactMethods.userId, userId),
        eq(userContactMethods.type, type),
      ));
  }

  const [row] = await db
    .insert(userContactMethods)
    .values({
      tenantId,
      userId,
      type,
      value,
      label: input.label ?? null,
      isPrimary,
      isVerified: true,
      isActive: true,
    })
    .returning();

  return row;
}

export async function updateUserContactMethod(args: {
  tenantId: string;
  userId: string;
  id: string;
  patch: UpdateInput;
}) {
  const { tenantId, userId, id, patch } = args;
  const [existing] = await db
    .select()
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.id, id),
      eq(userContactMethods.tenantId, tenantId),
      eq(userContactMethods.userId, userId),
    ))
    .limit(1);
  if (!existing) {
    throw new Error("Contact method not found");
  }

  const updates: any = {};
  if (patch.label !== undefined) updates.label = patch.label;
  if (patch.is_active !== undefined) updates.isActive = Boolean(patch.is_active);
  if (patch.is_primary !== undefined) updates.isPrimary = Boolean(patch.is_primary);

  if (updates.isPrimary) {
    await db
      .update(userContactMethods)
      .set({ isPrimary: false })
      .where(and(
        eq(userContactMethods.tenantId, tenantId),
        eq(userContactMethods.userId, userId),
        eq(userContactMethods.type, existing.type),
      ));
  }

  if (updates.isActive === false) {
    updates.isPrimary = false;
  }

  const [row] = await db
    .update(userContactMethods)
    .set(updates)
    .where(eq(userContactMethods.id, id))
    .returning();

  return row;
}

export async function deleteUserContactMethod(args: { tenantId: string; userId: string; id: string }) {
  const [row] = await db
    .delete(userContactMethods)
    .where(and(
      eq(userContactMethods.id, args.id),
      eq(userContactMethods.tenantId, args.tenantId),
      eq(userContactMethods.userId, args.userId),
    ))
    .returning();
  if (!row) {
    throw new Error("Contact method not found");
  }
  return row;
}
