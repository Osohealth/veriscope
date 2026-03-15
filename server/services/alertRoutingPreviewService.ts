import { and, desc, eq, inArray } from "drizzle-orm";
import { db } from "../db";
import { alertEndpointHealth, tenantUsers, userContactMethods } from "@shared/schema";
import { getTenantAllowlist } from "./tenantSettings";
import { allowlistHostMatches, allowlistMatches } from "./allowlistUtils";
import { makeDestinationKey } from "./destinationKey";
import { getDestinationGate } from "./alertDestinationGate";
import { selectContactMethod } from "./alertRoutingResolver";

type PreviewTargetType = "ROLE" | "USER";
type PreviewTargetRole = "OWNER" | "OPERATOR" | "VIEWER";

type PreviewRecipient = {
  user_id: string;
  display_name?: string | null;
  email?: string | null;
};

type PreviewRoute = {
  user_id?: string;
  destination_type: "WEBHOOK" | "EMAIL";
  destination: string;
  destination_key: string;
  contact_method_id?: string;
  contact_method_type?: "WEBHOOK" | "EMAIL";
  contact_method_primary?: boolean;
  allowed: boolean;
  gate?: {
    state?: "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
    reason?: "DESTINATION_DISABLED" | "DESTINATION_PAUSED" | "DESTINATION_AUTO_PAUSED";
    ready_to_resume?: boolean;
    endpoint_health?: "OK" | "DEGRADED" | "DOWN" | null;
  };
  allowlist?: { ok: boolean; rule?: string };
};

export type RoutingPreviewResult = {
  ok: boolean;
  reason?:
    | "NO_RECIPIENTS"
    | "NO_USER_CONTACT_METHOD"
    | "DESTINATION_NOT_ALLOWED"
    | "DESTINATION_DISABLED"
    | "DESTINATION_PAUSED"
    | "DESTINATION_AUTO_PAUSED"
    | "ENDPOINT_DOWN"
    | "UNKNOWN";
  resolved: {
    recipients: PreviewRecipient[];
    routes: PreviewRoute[];
    warnings: string[];
  };
};

const ROLE_VALUES: PreviewTargetRole[] = ["OWNER", "OPERATOR", "VIEWER"];

type GateReason = Exclude<NonNullable<PreviewRoute["gate"]>["reason"], undefined>;

const pickBlockedReason = (routes: PreviewRoute[]) => {
  const hasReason = (reason: GateReason) =>
    routes.some((route) => route.gate?.reason === reason);

  if (hasReason("DESTINATION_DISABLED")) return "DESTINATION_DISABLED";
  if (hasReason("DESTINATION_PAUSED")) return "DESTINATION_PAUSED";
  if (hasReason("DESTINATION_AUTO_PAUSED")) return "DESTINATION_AUTO_PAUSED";
  if (routes.some((route) => route.gate?.endpoint_health === "DOWN")) return "ENDPOINT_DOWN";
  if (routes.some((route) => route.allowlist?.ok === false)) return "DESTINATION_NOT_ALLOWED";
  return "UNKNOWN";
};

const isValidUserId = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

export async function previewRouting(args: {
  tenantId: string;
  targetType: PreviewTargetType;
  targetRef: string;
  includeBlocked: boolean;
  now: Date;
}): Promise<RoutingPreviewResult> {
  const targetType = String(args.targetType ?? "").toUpperCase() as PreviewTargetType;
  const targetRef = String(args.targetRef ?? "").trim();

  let recipients: PreviewRecipient[] = [];
  if (targetType === "ROLE") {
    if (!ROLE_VALUES.includes(targetRef.toUpperCase() as PreviewTargetRole)) {
      return { ok: false, reason: "NO_RECIPIENTS", resolved: { recipients: [], routes: [], warnings: [] } };
    }
    const role = targetRef.toUpperCase();
    const rows = await db
      .select()
      .from(tenantUsers)
      .where(and(
        eq(tenantUsers.tenantId, args.tenantId),
        eq(tenantUsers.status, "ACTIVE"),
        eq(tenantUsers.role, role),
      ))
      .orderBy(desc(tenantUsers.createdAt));
    recipients = rows.map((row) => ({
      user_id: row.userId,
      display_name: row.displayName ?? null,
      email: row.email ?? null,
    }));
  } else if (targetType === "USER") {
    if (!isValidUserId(targetRef)) {
      return { ok: false, reason: "NO_RECIPIENTS", resolved: { recipients: [], routes: [], warnings: [] } };
    }
    const [row] = await db
      .select()
      .from(tenantUsers)
      .where(and(
        eq(tenantUsers.tenantId, args.tenantId),
        eq(tenantUsers.userId, targetRef),
        eq(tenantUsers.status, "ACTIVE"),
      ))
      .limit(1);
    if (row) {
      recipients = [{
        user_id: row.userId,
        display_name: row.displayName ?? null,
        email: row.email ?? null,
      }];
    }
  }

  if (recipients.length === 0) {
    return {
      ok: false,
      reason: "NO_RECIPIENTS",
      resolved: { recipients: [], routes: [], warnings: [] },
    };
  }

  const userIds = recipients.map((r) => r.user_id);
  const methods = await db
    .select()
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, args.tenantId),
      inArray(userContactMethods.userId, userIds),
      eq(userContactMethods.isActive, true),
    ))
    .orderBy(desc(userContactMethods.isPrimary), desc(userContactMethods.createdAt));

  const methodsByUser = new Map<string, any[]>();
  for (const method of methods) {
    const list = methodsByUser.get(method.userId) ?? [];
    list.push(method);
    methodsByUser.set(method.userId, list);
  }

  const warningsSet = new Set<string>();
  const routes: PreviewRoute[] = [];
  const allowlist = process.env.NODE_ENV === "production"
    ? await getTenantAllowlist(args.tenantId)
    : { allowed_email_domains: [], allowed_webhook_hosts: [] };

  for (const recipient of recipients) {
    const userMethods = methodsByUser.get(recipient.user_id) ?? [];
    const chosen = selectContactMethod(userMethods);
    if (!chosen) {
      warningsSet.add("NO_USER_CONTACT_METHOD");
      continue;
    }

    const destinationType = String(chosen.type ?? "").toUpperCase() as "WEBHOOK" | "EMAIL";
    const destination = String(chosen.value ?? "");
    const destinationKey = makeDestinationKey(destinationType, destination);

    let allowlistOk = true;
    let allowlistRule: string | undefined;

    if (destinationType === "EMAIL" && process.env.NODE_ENV === "production") {
      const domain = destination.split("@")[1]?.toLowerCase() ?? "";
      if (allowlist.allowed_email_domains.length === 0 || !allowlistMatches(domain, allowlist.allowed_email_domains)) {
        allowlistOk = false;
        allowlistRule = "email domain not allowed";
      }
    }

    if (destinationType === "WEBHOOK") {
      try {
        const url = new URL(destination);
        const isLocal = url.hostname === "localhost" || url.hostname === "127.0.0.1";
        if (process.env.NODE_ENV === "production") {
          if (url.protocol !== "https:") {
            allowlistOk = false;
            allowlistRule = "webhook must be https in production";
          } else if (
            allowlist.allowed_webhook_hosts.length === 0 ||
            !allowlistHostMatches(url.hostname.toLowerCase(), allowlist.allowed_webhook_hosts)
          ) {
            allowlistOk = false;
            allowlistRule = "webhook host not allowed";
          }
        } else if (url.protocol !== "https:" && !isLocal) {
          allowlistOk = false;
          allowlistRule = "webhook must be https or localhost in development";
        }
      } catch {
        allowlistOk = false;
        allowlistRule = "webhook URL invalid";
      }
    }

    const gate = await getDestinationGate({
      tenantId: args.tenantId,
      destinationKey,
      now: args.now,
    });

    let endpointHealth: "OK" | "DEGRADED" | "DOWN" | null = null;
    const [health] = await db
      .select()
      .from(alertEndpointHealth)
      .where(and(
        eq(alertEndpointHealth.tenantId, args.tenantId),
        eq(alertEndpointHealth.window, "1h"),
        eq(alertEndpointHealth.destinationType, destinationType),
        eq(alertEndpointHealth.destination, destination),
      ))
      .limit(1);
    if (health?.status) {
      endpointHealth = String(health.status).toUpperCase() as "OK" | "DEGRADED" | "DOWN";
    }

    const allowed = allowlistOk && gate.allowed && endpointHealth !== "DOWN";

    routes.push({
      user_id: recipient.user_id,
      destination_type: destinationType,
      destination,
      destination_key: destinationKey,
      contact_method_id: chosen.id,
      contact_method_type: destinationType,
      contact_method_primary: Boolean(chosen.isPrimary),
      allowed,
      gate: {
        state: gate.state,
        reason: gate.reason,
        ready_to_resume: gate.readyToResume ?? undefined,
        endpoint_health: endpointHealth,
      },
      allowlist: { ok: allowlistOk, rule: allowlistRule },
    });
  }

  if (targetType === "ROLE" && recipients.length > 0 && routes.length === 0) {
    warningsSet.add(`ROLE_HAS_RECIPIENTS_BUT_NO_ROUTES:${recipients.length}`);
  }

  const warnings = Array.from(warningsSet);
  const visibleRoutes = args.includeBlocked ? routes : routes.filter((route) => route.allowed);
  const hasAllowed = routes.some((route) => route.allowed);

  if (hasAllowed) {
    return {
      ok: true,
      resolved: {
        recipients,
        routes: visibleRoutes,
        warnings,
      },
    };
  }

  if (routes.length === 0) {
    return {
      ok: false,
      reason: "NO_USER_CONTACT_METHOD",
      resolved: {
        recipients,
        routes: visibleRoutes,
        warnings,
      },
    };
  }

  return {
    ok: false,
    reason: pickBlockedReason(routes),
    resolved: {
      recipients,
      routes: visibleRoutes,
      warnings,
    },
  };
}
