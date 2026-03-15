import { and, eq } from "drizzle-orm";
import { db } from "../db";
import { alertEndpointHealth, tenantUsers } from "@shared/schema";
import { normalizeWebhookUrl, makeDestinationKey } from "./destinationKey";
import { getDestinationGate } from "./alertDestinationGate";
import { previewRouting } from "./alertRoutingPreviewService";
import { validateEscalationPolicyTarget } from "./incidentEscalationService";

type DraftTarget = {
  target_type?: string;
  target_ref?: string;
  target_name?: string | null;
};

type DraftPolicy = {
  incident_type?: string;
  severity_min?: string;
  level?: number;
  after_minutes?: number;
  targets?: DraftTarget[];
  include_blocked?: boolean;
};

type ValidationError = { code: string; path: string; message: string };
type ValidationWarning = { code: string; path: string; message: string; details?: Record<string, any> };

type NormalizedTarget = {
  target_type: string;
  target_ref: string;
  target_name?: string | null;
};

type PreviewRoute = {
  target_type: string;
  target_ref: string;
  recipient_label?: string | null;
  resolved_contact_type?: string | null;
  resolved_contact?: string | null;
  allowed: boolean;
  blocked_reason: string | null;
  warnings: string[];
};

export async function validateRoutingPolicyDraft(args: {
  tenantId: string;
  draft: DraftPolicy;
  now: Date;
}) {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  const incidentType = String(args.draft.incident_type ?? "").toUpperCase();
  const severityMin = String(args.draft.severity_min ?? "").toUpperCase();

  if (!["ALL", "SLA_AT_RISK", "ENDPOINT_DOWN"].includes(incidentType)) {
    errors.push({
      code: "INVALID_INCIDENT_TYPE",
      path: "incident_type",
      message: "incident_type must be ALL, SLA_AT_RISK, or ENDPOINT_DOWN",
    });
  }

  if (!["LOW", "MEDIUM", "HIGH", "CRITICAL", "ALL"].includes(severityMin)) {
    errors.push({
      code: "INVALID_SEVERITY",
      path: "severity_min",
      message: "severity_min must be LOW, MEDIUM, HIGH, CRITICAL, or ALL",
    });
  }

  const levelRaw = Number(args.draft.level);
  if (!Number.isFinite(levelRaw) || levelRaw < 1 || levelRaw > 10) {
    errors.push({
      code: "INVALID_LEVEL",
      path: "level",
      message: "level must be between 1 and 10",
    });
  }

  const afterRaw = Number(args.draft.after_minutes);
  if (!Number.isFinite(afterRaw) || afterRaw < 0 || afterRaw > 10080) {
    errors.push({
      code: "INVALID_AFTER_MINUTES",
      path: "after_minutes",
      message: "after_minutes must be between 0 and 10080",
    });
  }

  const draftTargets = Array.isArray(args.draft.targets) ? args.draft.targets : [];
  if (draftTargets.length === 0) {
    errors.push({
      code: "TARGETS_REQUIRED",
      path: "targets",
      message: "At least one target is required",
    });
  }

  const normalizedTargets: NormalizedTarget[] = [];
  const targetPreviewRoutes: PreviewRoute[] = [];
  let routesAllowed = 0;
  let routesBlocked = 0;

  for (let idx = 0; idx < draftTargets.length; idx += 1) {
    const target = draftTargets[idx] ?? {};
    const targetType = String(target.target_type ?? "").toUpperCase();
    const targetRefInput = String(target.target_ref ?? "").trim();
    const pathBase = `targets[${idx}]`;

    if (!targetType) {
      errors.push({ code: "TARGET_TYPE_REQUIRED", path: `${pathBase}.target_type`, message: "target_type is required" });
      continue;
    }
    if (!targetRefInput) {
      errors.push({ code: "TARGET_REF_REQUIRED", path: `${pathBase}.target_ref`, message: "target_ref is required" });
      continue;
    }

    let validated;
    try {
      validated = await validateEscalationPolicyTarget({
        tenantId: args.tenantId,
        targetType,
        targetRef: targetRefInput,
      });
    } catch (error: any) {
      const message = String(error?.message ?? "Invalid target");
      let code = "INVALID_TARGET";
      if (message.includes("valid email")) code = "INVALID_EMAIL";
      else if (message.includes("email domain not allowed")) code = "EMAIL_DOMAIN_NOT_ALLOWED";
      else if (message.includes("valid URL")) code = "INVALID_URL";
      else if (message.includes("webhook host not allowed")) code = "WEBHOOK_HOST_NOT_ALLOWED";
      else if (message.includes("https")) code = "WEBHOOK_HTTPS_REQUIRED";
      else if (message.includes("user id")) code = "USER_ID_INVALID";
      else if (message.includes("active user")) code = "USER_NOT_FOUND";
      else if (message.includes("OWNER") || message.includes("OPERATOR") || message.includes("VIEWER")) code = "INVALID_ROLE";
      errors.push({ code, path: `${pathBase}.target_ref`, message });
      continue;
    }

    let normalizedRef = validated.targetRef;
    if (targetType === "ROLE") normalizedRef = validated.targetRef.toUpperCase();
    if (targetType === "EMAIL") normalizedRef = validated.targetRef.trim().toLowerCase();
    if (targetType === "WEBHOOK") normalizedRef = normalizeWebhookUrl(validated.targetRef);

    const normalizedTarget: NormalizedTarget = {
      target_type: targetType,
      target_ref: normalizedRef,
      target_name: validated.targetName ?? target.target_name ?? null,
    };

    const dedupeKey = `${normalizedTarget.target_type}:${normalizedTarget.target_ref}`;
    if (normalizedTargets.some((entry) => `${entry.target_type}:${entry.target_ref}` === dedupeKey)) {
      continue;
    }
    normalizedTargets.push(normalizedTarget);

    if (targetType === "ROLE" || targetType === "USER") {
      const preview = await previewRouting({
        tenantId: args.tenantId,
        targetType: targetType as "ROLE" | "USER",
        targetRef: normalizedTarget.target_ref,
        includeBlocked: args.draft.include_blocked !== false,
        now: args.now,
      });

      const previewWarnings = preview.resolved?.warnings ?? [];
      for (const warning of previewWarnings) {
        if (warning.startsWith("ROLE_HAS_RECIPIENTS_BUT_NO_ROUTES")) {
          const count = Number(warning.split(":")[1] ?? "0");
          warnings.push({
            code: "ROLE_HAS_RECIPIENTS_BUT_NO_ROUTES",
            path: pathBase,
            message: `Role has ${count || "some"} recipients but no routes.`,
            details: { count },
          });
        } else if (warning === "NO_USER_CONTACT_METHOD") {
          warnings.push({
            code: "NO_USER_CONTACT_METHOD",
            path: pathBase,
            message: "User has no contact methods.",
          });
        }
      }

      const routes = preview.resolved?.routes ?? [];
      for (const route of routes) {
        const blockedReason =
          route.gate?.reason ??
          (route.gate?.endpoint_health === "DOWN"
            ? "ENDPOINT_DOWN"
            : route.allowlist?.ok === false
              ? "DESTINATION_NOT_ALLOWED"
              : null);
        targetPreviewRoutes.push({
          target_type: targetType,
          target_ref: normalizedTarget.target_ref,
          recipient_label:
            preview.resolved?.recipients?.find((r) => r.user_id === route.user_id)?.display_name ??
            preview.resolved?.recipients?.find((r) => r.user_id === route.user_id)?.email ??
            null,
          resolved_contact_type: route.contact_method_type ?? null,
          resolved_contact: route.destination,
          allowed: route.allowed,
          blocked_reason: route.allowed ? null : blockedReason,
          warnings: [],
        });
        if (route.allowed) routesAllowed += 1;
        else routesBlocked += 1;
      }
      continue;
    }

    const destinationType = targetType === "WEBHOOK" ? "WEBHOOK" : "EMAIL";
    const destination = normalizedTarget.target_ref;
    const destinationKey = makeDestinationKey(destinationType, destination);

    const gate = await getDestinationGate({
      tenantId: args.tenantId,
      destinationKey: destinationKey,
      now: args.now,
    });

    let endpointHealth: string | null = null;
    if (destinationType === "WEBHOOK") {
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
        endpointHealth = String(health.status).toUpperCase();
      }
    }

    const allowed = gate.allowed && endpointHealth !== "DOWN";
    const blockedReason =
      gate.reason ??
      (endpointHealth === "DOWN" ? "ENDPOINT_DOWN" : null);

    targetPreviewRoutes.push({
      target_type: targetType,
      target_ref: normalizedTarget.target_ref,
      recipient_label: normalizedTarget.target_name ?? null,
      resolved_contact_type: destinationType,
      resolved_contact: destination,
      allowed,
      blocked_reason: allowed ? null : blockedReason,
      warnings: [],
    });

    if (allowed) routesAllowed += 1;
    else routesBlocked += 1;
  }

  const targetsValid = normalizedTargets.length;
  const targetsTotal = draftTargets.length;

  if (routesAllowed === 0 && routesBlocked > 0) {
    warnings.push({
      code: "ALL_ROUTES_BLOCKED",
      path: "targets",
      message: "All resolved routes are blocked.",
    });
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    normalized_policy: errors.length === 0 ? {
      incident_type: incidentType,
      severity_min: severityMin,
      level: Number.isFinite(levelRaw) ? Math.floor(levelRaw) : levelRaw,
      after_minutes: Number.isFinite(afterRaw) ? Math.floor(afterRaw) : afterRaw,
      targets: normalizedTargets,
    } : null,
    preview: {
      routes: targetPreviewRoutes,
      summary: {
        targets_total: targetsTotal,
        targets_valid: targetsValid,
        routes_total: targetPreviewRoutes.length,
        routes_allowed: routesAllowed,
        routes_blocked: routesBlocked,
      },
    },
  };
}
