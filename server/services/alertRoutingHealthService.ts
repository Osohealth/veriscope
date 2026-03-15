import { and, desc, eq } from "drizzle-orm";
import { db } from "../db";
import { alertEndpointHealth, tenantUsers } from "@shared/schema";
import { allowlistHostMatches, allowlistMatches } from "./allowlistUtils";
import { getTenantAllowlist } from "./tenantSettings";
import { getDestinationGate } from "./alertDestinationGate";
import { makeDestinationKey } from "./destinationKey";
import { listActiveUserContactMethods, selectContactMethod } from "./alertRoutingResolver";

export type RoutingHealth = {
  routes_total: number;
  routes_allowed: number;
  routes_blocked: number;
  blocked_reasons: string[];
  warnings_count: number;
};

const MAX_ROUTES_DEFAULT = 10;
const MAX_RECIPIENTS_DEFAULT = 25;

type PolicyRow = {
  targetType: string;
  targetRef: string;
};

const countTopReasons = (reasons: Map<string, number>, limit = 3) =>
  [...reasons.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([reason]) => reason);

const recordReason = (reasons: Map<string, number>, reason: string | null) => {
  if (!reason) return;
  reasons.set(reason, (reasons.get(reason) ?? 0) + 1);
};

const normalizeRole = (value: string) => value.toUpperCase();

const evaluateRoute = async (args: {
  tenantId: string;
  destinationType: "WEBHOOK" | "EMAIL";
  destination: string;
  destinationKey: string;
  now: Date;
}): Promise<{ allowed: boolean; reason: string | null }> => {
  let allowlistOk = true;
  let allowlistReason: string | null = null;

  if (args.destinationType === "EMAIL" && process.env.NODE_ENV === "production") {
    const { allowed_email_domains } = await getTenantAllowlist(args.tenantId);
    const domain = args.destination.split("@")[1]?.toLowerCase() ?? "";
    if (allowed_email_domains.length === 0 || !allowlistMatches(domain, allowed_email_domains)) {
      allowlistOk = false;
      allowlistReason = "DESTINATION_NOT_ALLOWED";
    }
  }

  if (args.destinationType === "WEBHOOK") {
    try {
      const url = new URL(args.destination);
      const isLocal = url.hostname === "localhost" || url.hostname === "127.0.0.1";
      if (process.env.NODE_ENV === "production") {
        if (url.protocol !== "https:") {
          allowlistOk = false;
          allowlistReason = "DESTINATION_NOT_ALLOWED";
        } else {
          const { allowed_webhook_hosts } = await getTenantAllowlist(args.tenantId);
          const host = url.hostname.toLowerCase();
          if (allowed_webhook_hosts.length === 0 || !allowlistHostMatches(host, allowed_webhook_hosts)) {
            allowlistOk = false;
            allowlistReason = "DESTINATION_NOT_ALLOWED";
          }
        }
      } else if (url.protocol !== "https:" && !isLocal) {
        allowlistOk = false;
        allowlistReason = "DESTINATION_NOT_ALLOWED";
      }
    } catch {
      allowlistOk = false;
      allowlistReason = "DESTINATION_NOT_ALLOWED";
    }
  }

  const gate = await getDestinationGate({
    tenantId: args.tenantId,
    destinationKey: args.destinationKey,
    now: args.now,
  });

  let endpointDown = false;
  if (args.destinationType === "WEBHOOK") {
    const [health] = await db
      .select()
      .from(alertEndpointHealth)
      .where(and(
        eq(alertEndpointHealth.tenantId, args.tenantId),
        eq(alertEndpointHealth.window, "1h"),
        eq(alertEndpointHealth.destinationType, args.destinationType),
        eq(alertEndpointHealth.destination, args.destination),
      ))
      .limit(1);
    endpointDown = String(health?.status ?? "").toUpperCase() === "DOWN";
  }

  if (!allowlistOk) return { allowed: false, reason: allowlistReason };
  if (!gate.allowed) return { allowed: false, reason: gate.reason ?? "DESTINATION_NOT_ALLOWED" };
  if (endpointDown) return { allowed: false, reason: "ENDPOINT_DOWN" };
  return { allowed: true, reason: null };
};

export async function getRoutingHealthForPolicy(args: {
  tenantId: string;
  policy: PolicyRow;
  now: Date;
  maxRoutes?: number;
  maxRecipients?: number;
}): Promise<RoutingHealth> {
  const maxRoutes = args.maxRoutes ?? MAX_ROUTES_DEFAULT;
  const maxRecipients = args.maxRecipients ?? MAX_RECIPIENTS_DEFAULT;
  const reasons = new Map<string, number>();
  let warningsCount = 0;
  let routesTotal = 0;
  let routesAllowed = 0;
  let routesBlocked = 0;

  const targetType = String(args.policy.targetType ?? "").toUpperCase();
  const targetRef = String(args.policy.targetRef ?? "").trim();

  const addRouteResult = (allowed: boolean, reason: string | null) => {
    routesTotal += 1;
    if (allowed) routesAllowed += 1;
    else {
      routesBlocked += 1;
      recordReason(reasons, reason);
    }
  };

  const handleUserRoute = async (userId: string) => {
    const methods = await listActiveUserContactMethods({ tenantId: args.tenantId, userId });
    const chosen = selectContactMethod(methods);
    if (!chosen) {
      warningsCount += 1;
      return;
    }

    const destinationType = chosen.type as "WEBHOOK" | "EMAIL";
    const destination = chosen.value;
    const destinationKey = makeDestinationKey(destinationType, destination);
    const result = await evaluateRoute({
      tenantId: args.tenantId,
      destinationType,
      destination,
      destinationKey,
      now: args.now,
    });
    addRouteResult(result.allowed, result.reason);
  };

  if (targetType === "USER") {
    if (routesTotal < maxRoutes) {
      await handleUserRoute(targetRef);
    }
  } else if (targetType === "ROLE") {
    const role = normalizeRole(targetRef);
    const rows = await db
      .select()
      .from(tenantUsers)
      .where(and(
        eq(tenantUsers.tenantId, args.tenantId),
        eq(tenantUsers.status, "ACTIVE"),
        eq(tenantUsers.role, role),
      ))
      .orderBy(desc(tenantUsers.createdAt))
      .limit(maxRecipients);

    for (const row of rows) {
      if (routesTotal >= maxRoutes) break;
      await handleUserRoute(row.userId);
    }

    if (rows.length > 0 && routesTotal === 0) {
      warningsCount += 1;
    }
  } else if (targetType === "EMAIL" || targetType === "WEBHOOK") {
    const destinationType = targetType as "WEBHOOK" | "EMAIL";
    const destination = targetRef;
    const destinationKey = makeDestinationKey(destinationType, destination);
    const result = await evaluateRoute({
      tenantId: args.tenantId,
      destinationType,
      destination,
      destinationKey,
      now: args.now,
    });
    addRouteResult(result.allowed, result.reason);
  }

  return {
    routes_total: routesTotal,
    routes_allowed: routesAllowed,
    routes_blocked: routesBlocked,
    blocked_reasons: countTopReasons(reasons, 3),
    warnings_count: warningsCount,
  };
}
