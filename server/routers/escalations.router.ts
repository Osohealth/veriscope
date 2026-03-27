import { Router } from "express";
import { z } from "zod";
import { authenticateApiKey } from "../middleware/apiKeyAuth";
import { requireRole } from "../auth/requireRole";
import { writeAuditEvent } from "../services/auditLog";
import {
    listIncidentEscalationPolicies,
    upsertIncidentEscalationPolicy,
    deleteIncidentEscalationPolicy,
} from "../services/incidentEscalationService";
import { previewRouting } from "../services/alertRoutingPreviewService";
import { validateRoutingPolicyDraft } from "../services/alertRoutingValidationService";
import { getRoutingHealthForPolicy } from "../services/alertRoutingHealthService";
import { logger } from "../middleware/observability";

export const escalationsRouter = Router();

const patchPolicySchema = z.object({
    incident_type: z.string().min(1),
    severity_min: z.enum(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
    level: z.number().int().min(1),
    after_minutes: z.number().int().min(0),
    target_type: z.string().min(1),
    target_ref: z.string().min(1),
    target_name: z.string().nullable().optional(),
    enabled: z.boolean().optional(),
});

function isValidUuid(v: unknown): v is string {
    return typeof v === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
}

async function guardRole(req: any, res: any, minRole: "OWNER" | "OPERATOR" | "VIEWER"): Promise<boolean> {
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
        res.status(status).json({ error: status === 401 ? "UNAUTHORIZED" : "FORBIDDEN", detail: err?.message ?? "Forbidden" });
        return false;
    }
}

function mapPolicy(policy: any) {
    return {
        id: policy.id,
        tenant_id: policy.tenantId,
        enabled: policy.enabled,
        level: policy.level,
        after_minutes: policy.afterMinutes,
        incident_type: policy.incidentType,
        severity_min: policy.severityMin,
        target_type: policy.targetType,
        target_ref: policy.targetRef,
        target_name: policy.targetName ?? null,
        last_validated_at: policy.lastValidatedAt ?? null,
        last_routing_health: policy.lastRoutingHealth ?? null,
        created_at: policy.createdAt,
        updated_at: policy.updatedAt,
    };
}

// GET /v1/incident-escalation-policies
escalationsRouter.get("/v1/incident-escalation-policies", authenticateApiKey, async (req, res, next) => {
    if (!(await guardRole(req, res, "VIEWER"))) return;
    try {
        const tenantId = req.auth?.tenantId;
        if (!tenantId) return res.status(401).json({ error: "API key required" });

        const now = new Date();
        const items = await listIncidentEscalationPolicies(tenantId);
        const mapped = await Promise.all(
            items.map(async (policy: any) => ({
                ...mapPolicy(policy),
                routing_health: await getRoutingHealthForPolicy({ tenantId, policy, now }).catch(() => null),
            })),
        );

        res.json({ version: "1", items: mapped });
    } catch (err) {
        logger.error("Failed to list escalation policies", { err });
        next(err);
    }
});

// PATCH /v1/incident-escalation-policies — upsert a single escalation step
escalationsRouter.patch("/v1/incident-escalation-policies", authenticateApiKey, async (req, res, next) => {
    if (!(await guardRole(req, res, "OWNER"))) return;
    try {
        const tenantId = req.auth?.tenantId;
        if (!tenantId) return res.status(401).json({ error: "API key required" });

        const parsed = patchPolicySchema.safeParse(req.body);
        if (!parsed.success) {
            return res.status(400).json({ error: "INVALID_BODY", errors: parsed.error.flatten().fieldErrors });
        }
        const body = parsed.data;

        const now = new Date();
        const validation = await validateRoutingPolicyDraft({
            tenantId,
            now,
            draft: {
                incident_type: body.incident_type,
                severity_min: body.severity_min,
                level: body.level,
                after_minutes: body.after_minutes,
                include_blocked: true,
                targets: [{
                    target_type: body.target_type,
                    target_ref: body.target_ref,
                    target_name: body.target_name ?? null,
                }],
            },
        });

        if (!validation.ok) {
            return res.status(400).json({
                error: "POLICY_INVALID",
                errors: validation.errors,
                warnings: validation.warnings,
            });
        }

        const normalized = (validation as any).normalized_policy;
        const normalizedTarget = normalized?.targets?.[0];
        if (!normalized || !normalizedTarget) {
            return res.status(400).json({
                error: "POLICY_INVALID",
                errors: [{ code: "TARGETS_REQUIRED", path: "targets", message: "At least one valid target is required" }],
                warnings: validation.warnings,
            });
        }

        const enabled = body.enabled !== undefined ? Boolean(body.enabled) : true;
        const health = await getRoutingHealthForPolicy({
            tenantId,
            policy: { targetType: normalizedTarget.target_type, targetRef: normalizedTarget.target_ref },
            now,
        });

        if (enabled && (health as any).routes_allowed === 0) {
            return res.status(400).json({
                error: "POLICY_NOT_ROUTABLE",
                blocked_reasons: (health as any).blocked_reasons ?? [],
                warnings: validation.warnings,
            });
        }

        const policy = await upsertIncidentEscalationPolicy({
            tenantId,
            incidentType: normalized.incident_type,
            severityMin: normalized.severity_min,
            level: normalized.level,
            afterMinutes: normalized.after_minutes,
            targetType: normalizedTarget.target_type,
            targetRef: normalizedTarget.target_ref,
            targetName: normalizedTarget.target_name ?? null,
            enabled,
            lastValidatedAt: now,
            lastRoutingHealth: health as any,
        });

        if (!policy) return res.status(500).json({ error: "Failed to upsert escalation policy" });

        await writeAuditEvent(req.auditContext, {
            action: "INCIDENT.ESCALATION_POLICY_UPSERTED",
            resourceType: "INCIDENT_ESCALATION_POLICY",
            resourceId: policy.id,
            severity: "INFO",
            status: "SUCCESS",
            message: "Incident escalation policy updated",
            metadata: {
                incident_type: normalized.incident_type,
                severity_min: normalized.severity_min,
                level: normalized.level,
                after_minutes: normalized.after_minutes,
                target_type: normalizedTarget.target_type,
                target_ref: normalizedTarget.target_ref,
                enabled,
            },
        }).catch(() => { });

        res.json({ version: "1", warnings: validation.warnings, item: mapPolicy(policy) });
    } catch (err) {
        logger.error("Failed to upsert escalation policy", { err });
        next(err);
    }
});

// DELETE /v1/incident-escalation-policies/:id
escalationsRouter.delete("/v1/incident-escalation-policies/:id", authenticateApiKey, async (req, res, next) => {
    if (!(await guardRole(req, res, "OWNER"))) return;
    try {
        const tenantId = req.auth?.tenantId;
        if (!tenantId) return res.status(401).json({ error: "API key required" });

        const id = String(req.params.id ?? "");
        if (!isValidUuid(id)) return res.status(400).json({ error: "Invalid policy id" });

        const deleted = await deleteIncidentEscalationPolicy({ tenantId, id });
        if (!deleted) return res.status(404).json({ error: "Policy not found" });

        await writeAuditEvent(req.auditContext, {
            action: "INCIDENT.ESCALATION_POLICY_DELETED",
            resourceType: "INCIDENT_ESCALATION_POLICY",
            resourceId: (deleted as any).id,
            severity: "INFO",
            status: "SUCCESS",
            message: "Incident escalation policy deleted",
            metadata: {
                incident_type: (deleted as any).incidentType,
                severity_min: (deleted as any).severityMin,
                level: (deleted as any).level,
                after_minutes: (deleted as any).afterMinutes,
                target_type: (deleted as any).targetType,
                target_ref: (deleted as any).targetRef,
            },
        }).catch(() => { });

        res.json({ version: "1", ok: true });
    } catch (err) {
        logger.error("Failed to delete escalation policy", { err });
        next(err);
    }
});

// POST /v1/routing/preview
escalationsRouter.post("/v1/routing/preview", authenticateApiKey, async (req, res, next) => {
    if (!(await guardRole(req, res, "VIEWER"))) return;
    try {
        const tenantId = req.auth?.tenantId;
        if (!tenantId) return res.status(401).json({ error: "API key required" });

        const targetType = String(req.body?.target_type ?? "").toUpperCase();
        const targetRef = String(req.body?.target_ref ?? "").trim();
        const errors: Array<{ field: string; message: string }> = [];

        if (!["USER", "ROLE"].includes(targetType)) {
            errors.push({ field: "target_type", message: "target_type must be USER or ROLE" });
        }
        if (!targetRef) {
            errors.push({ field: "target_ref", message: "target_ref is required" });
        } else if (targetType === "USER") {
            if (!isValidUuid(targetRef)) {
                errors.push({ field: "target_ref", message: "target_ref must be a user_id UUID" });
            }
        } else if (targetType === "ROLE") {
            if (!["OWNER", "OPERATOR", "VIEWER"].includes(targetRef.toUpperCase())) {
                errors.push({ field: "target_ref", message: "target_ref must be OWNER, OPERATOR, or VIEWER" });
            }
        }

        if (errors.length > 0) {
            return res.status(400).json({ error: "VALIDATION_ERROR", details: errors });
        }

        const includeBlocked = String(req.query?.include_blocked ?? "true").toLowerCase() !== "false";
        const result = await previewRouting({
            tenantId,
            targetType: targetType as "ROLE" | "USER",
            targetRef,
            includeBlocked,
            now: new Date(),
        });

        res.json({ version: "1", result });
    } catch (err) {
        logger.error("Failed to preview routing", { err });
        next(err);
    }
});

// POST /v1/routing/validate-policy
escalationsRouter.post("/v1/routing/validate-policy", authenticateApiKey, async (req, res, next) => {
    if (!(await guardRole(req, res, "VIEWER"))) return;
    try {
        const tenantId = req.auth?.tenantId;
        if (!tenantId) return res.status(401).json({ error: "API key required" });

        const result = await validateRoutingPolicyDraft({
            tenantId,
            draft: req.body ?? {},
            now: new Date(),
        });

        res.json({
            version: "1",
            ok: result.ok,
            errors: result.errors,
            warnings: result.warnings,
            normalized_policy: (result as any).normalized_policy,
            preview: (result as any).preview,
        });
    } catch (err) {
        logger.error("Failed to validate routing policy", { err });
        next(err);
    }
});
