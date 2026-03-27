import { and, eq } from "drizzle-orm";
import { randomBytes } from "node:crypto";
import { db } from "../db";
import {
  apiKeys,
  tenantUsers,
  userContactMethods,
  alertSubscriptions,
} from "@shared/schema";
import { hashApiKey, generateApiKey } from "./apiKeyService";
import { TENANT_DEMO_ID } from "../config/tenancy";
import { GLOBAL_SCOPE_ENTITY_ID } from "./alertScope";
import { upsertIncidentEscalationPolicy } from "./incidentEscalationService";

const DEMO_TENANT_ID = process.env.DEMO_TENANT_ID ?? TENANT_DEMO_ID;
const DEMO_OWNER_USER_ID = process.env.DEMO_OWNER_USER_ID ?? DEMO_TENANT_ID;
const DEMO_OWNER_EMAIL = process.env.DEMO_OWNER_EMAIL ?? `demo+${DEMO_OWNER_USER_ID.slice(0, 6)}@veriscope.dev`;
const DEMO_BASE_URL = process.env.DEMO_BASE_URL ?? `http://localhost:${process.env.PORT ?? "5000"}`;
const DEMO_ALERT_EMAIL = process.env.DEMO_ALERT_EMAIL ?? "demo+alerts@veriscope.dev";
const DEMO_ALERT_WEBHOOK_SUCCESS = process.env.DEMO_ALERT_WEBHOOK_SUCCESS ?? `${DEMO_BASE_URL}/api/dev/webhook-sink`;
const DEMO_POLICY_NAME_EMAIL = process.env.DEMO_POLICY_NAME_EMAIL ?? "demo_policy_l1_email";
const DEMO_POLICY_NAME_WEBHOOK = process.env.DEMO_POLICY_NAME_WEBHOOK ?? "demo_policy_l1_webhook";

const DEMO_API_KEY_NAME = "demo-server";

const ensureTenantOwner = async () => {
  await db
    .insert(tenantUsers)
    .values({
      tenantId: DEMO_TENANT_ID,
      userId: DEMO_OWNER_USER_ID,
      email: DEMO_OWNER_EMAIL,
      displayName: "Demo Owner",
      role: "OWNER",
      status: "ACTIVE",
      createdBy: DEMO_OWNER_USER_ID,
    })
    .onConflictDoUpdate({
      target: [tenantUsers.tenantId, tenantUsers.userId],
      set: {
        email: DEMO_OWNER_EMAIL,
        displayName: "Demo Owner",
        role: "OWNER",
        status: "ACTIVE",
        revokedAt: null,
        revokedBy: null,
      },
    });
};

const ensureApiKey = async () => {
  const rawKey = process.env.DEMO_API_KEY;
  if (rawKey) {
    const keyHash = hashApiKey(rawKey);
    await db
      .insert(apiKeys)
      .values({
        tenantId: DEMO_TENANT_ID,
        userId: DEMO_OWNER_USER_ID,
        keyHash,
        name: DEMO_API_KEY_NAME,
        label: DEMO_API_KEY_NAME,
        role: "OWNER",
        isActive: true,
        revokedAt: null,
        createdAt: new Date(),
      })
      .onConflictDoUpdate({
        target: [apiKeys.keyHash],
        set: {
          tenantId: DEMO_TENANT_ID,
          userId: DEMO_OWNER_USER_ID,
          name: DEMO_API_KEY_NAME,
          label: DEMO_API_KEY_NAME,
          role: "OWNER",
          isActive: true,
          revokedAt: null,
        },
      });
    return { raw: rawKey, created: false };
  }

  const existing = await db
    .select({ id: apiKeys.id, keyHash: apiKeys.keyHash })
    .from(apiKeys)
    .where(and(eq(apiKeys.tenantId, DEMO_TENANT_ID), eq(apiKeys.name, DEMO_API_KEY_NAME)))
    .limit(1);

  if (existing.length > 0) {
    return { raw: null, created: false };
  }

  const generated = generateApiKey("vs_demo");
  const keyHash = hashApiKey(generated);
  await db.insert(apiKeys).values({
    tenantId: DEMO_TENANT_ID,
    userId: DEMO_OWNER_USER_ID,
    keyHash,
    name: DEMO_API_KEY_NAME,
    label: DEMO_API_KEY_NAME,
    role: "OWNER",
    isActive: true,
    createdAt: new Date(),
  });

  return { raw: generated, created: true };
};

const ensureContactMethod = async () => {
  const existing = await db
    .select()
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, DEMO_TENANT_ID),
      eq(userContactMethods.userId, DEMO_OWNER_USER_ID),
      eq(userContactMethods.type, "EMAIL"),
      eq(userContactMethods.value, DEMO_OWNER_EMAIL),
    ))
    .limit(1);

  if (existing.length > 0) {
    if (!existing[0].isActive || !existing[0].isVerified) {
      await db.update(userContactMethods)
        .set({ isActive: true, isVerified: true })
        .where(eq(userContactMethods.id, existing[0].id));
    }
    return;
  }

  const primaryExists = await db
    .select({ id: userContactMethods.id })
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, DEMO_TENANT_ID),
      eq(userContactMethods.userId, DEMO_OWNER_USER_ID),
      eq(userContactMethods.type, "EMAIL"),
      eq(userContactMethods.isPrimary, true),
    ))
    .limit(1);

  await db.insert(userContactMethods).values({
    tenantId: DEMO_TENANT_ID,
    userId: DEMO_OWNER_USER_ID,
    type: "EMAIL",
    value: DEMO_OWNER_EMAIL,
    label: "Demo Email",
    isPrimary: primaryExists.length === 0,
    isVerified: true,
    isActive: true,
  });
};

const ensureSubscription = async (channel: "EMAIL" | "WEBHOOK", endpoint: string) => {
  const now = new Date();
  const secret = randomBytes(24).toString("base64url");

  await db
    .insert(alertSubscriptions)
    .values({
      tenantId: DEMO_TENANT_ID,
      userId: DEMO_OWNER_USER_ID,
      scope: "GLOBAL",
      entityType: "port",
      entityId: GLOBAL_SCOPE_ENTITY_ID,
      severityMin: "HIGH",
      channel,
      endpoint,
      secret,
      signatureVersion: "v1",
      isEnabled: true,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: [
        alertSubscriptions.tenantId,
        alertSubscriptions.userId,
        alertSubscriptions.channel,
        alertSubscriptions.endpoint,
        alertSubscriptions.entityId,
      ],
      set: {
        isEnabled: true,
        updatedAt: now,
        ...(secret ? { secret } : {}),
      },
    });
};

const ensurePolicies = async () => {
  await upsertIncidentEscalationPolicy({
    tenantId: DEMO_TENANT_ID,
    incidentType: "SLA_AT_RISK",
    severityMin: "HIGH",
    level: 1,
    afterMinutes: 0,
    targetType: "EMAIL",
    targetRef: DEMO_ALERT_EMAIL,
    targetName: DEMO_POLICY_NAME_EMAIL,
    enabled: true,
  });

  await upsertIncidentEscalationPolicy({
    tenantId: DEMO_TENANT_ID,
    incidentType: "SLA_AT_RISK",
    severityMin: "HIGH",
    level: 1,
    afterMinutes: 0,
    targetType: "WEBHOOK",
    targetRef: DEMO_ALERT_WEBHOOK_SUCCESS,
    targetName: DEMO_POLICY_NAME_WEBHOOK,
    enabled: true,
  });
};

export async function seedDemoServer() {
  await ensureTenantOwner();
  const apiKeyResult = await ensureApiKey();
  await ensureContactMethod();
  await ensureSubscription("EMAIL", DEMO_ALERT_EMAIL);
  await ensureSubscription("WEBHOOK", DEMO_ALERT_WEBHOOK_SUCCESS);
  await ensurePolicies();

  if (apiKeyResult?.created && apiKeyResult.raw) {
    console.log(`DEMO_API_KEY=${apiKeyResult.raw}`);
  }
}
