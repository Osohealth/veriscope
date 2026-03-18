import { randomBytes } from "node:crypto";
import { Router } from "express";
import { and, desc, eq, inArray, or, sql } from "drizzle-orm";
import { db } from "../db";
import {
    alertDeliveries,
    alertDeliveryAttempts,
    alertDlq,
    alertRuns,
    alertSubscriptions,
    ports,
    signals,
} from "@shared/schema";
import { authenticateApiKey } from "../middleware/apiKeyAuth";
import { optionalAuth } from "../middleware/rbac";
import { listAlertDeliveries } from "../services/alertDeliveriesService";
import {
    getDeliveryHealthByDay,
    getDeliveryLatency,
    getEndpointHealth,
    getDlqHealth,
    getDlqOverdue,
} from "../services/alertMetricsService";
import { buildWebhookRequest, sendWebhook } from "../services/webhookService";
import { sendEmail } from "../services/emailService";
import { GLOBAL_SCOPE_ENTITY_ID, normalizeScope } from "../services/alertScopeService";
import { TENANT_DEMO_ID } from "../config/tenancy";
import { buildSignalClusterAlertPayload } from "../services/signalAlertService";
import { parseSignalDay, formatSignalDay } from "../services/signalEngineService";
import { parseSafeLimit } from "../utils/pagination";
import { SEVERITY_RANK, type SignalSeverity, type ConfidenceBand } from "@shared/signalTypes";
import { storage } from "../storage";
import { retryDeliveryById } from "../services/alertDlqQueueService";
import { parsePaginationParams, paginateArray } from "../utils/pagination";
import { logger } from "../middleware/observability";

// ── Module-level state ──────────────────────────────────────────────────────
const subscriptionTestRateLimit = new Map<string, number[]>();

const normalizeEmail = (v: string) => v.trim().toLowerCase();
const isValidEmail = (v: string) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v);

const BLOCKED_IP_RANGES = [
    /^10\./,
    /^172\.(1[6-9]|2[0-9]|3[01])\./,
    /^192\.168\./,
    /^127\./,
    /^169\.254\./,
    /^::1$/,
    /^fc00:/,
    /^fe80:/,
    /^0\./,
    /^100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\./,
];

const isBlockedHost = (h: string) =>
    BLOCKED_IP_RANGES.some((re) => re.test(h));

function isValidWebhookUrl(value: string, allowHttp: boolean): boolean {
    let url: URL;
    try {
        url = new URL(value);
    } catch {
        return false;
    }
    if (!["http:", "https:"].includes(url.protocol)) return false;
    if (!allowHttp && url.protocol === "http:") return false;
    const host = url.hostname;
    if (isBlockedHost(host)) return false;
    if (!/^[a-zA-Z0-9.\-]+$/.test(host)) return false;
    return true;
}

const generateSecret = () => randomBytes(24).toString("base64url");

// ── Router ──────────────────────────────────────────────────────────────────
export const alertsRouter = Router();

// GET /v1/alert-deliveries
alertsRouter.get("/v1/alert-deliveries", authenticateApiKey, async (req, res) => {
    try {
        const {
            day,
            days,
            port,
            subscription_id,
            run_id,
            status,
            destination,
            severity_min,
            is_test,
            include_entity,
            cursor,
            limit = "50",
        } = req.query;

        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId)
            return res.status(401).json({ error: "API key required" });

        const dayExact = day ? parseSignalDay(String(day)) : null;
        if (day && !dayExact) {
            return res.status(400).json({ error: "day must be YYYY-MM-DD" });
        }

        let resolvedPortId: string | undefined;
        const portQuery = port ? String(port).trim() : undefined;
        if (portQuery) {
            const normalized = portQuery.toLowerCase();
            const exactMatches = await db
                .select()
                .from(ports)
                .where(
                    or(
                        sql`lower(${ports.id}) = ${normalized}`,
                        sql`lower(${ports.code}) = ${normalized}`,
                        sql`lower(${ports.unlocode}) = ${normalized}`,
                        sql`lower(${ports.name}) = ${normalized}`
                    )
                )
                .limit(2);
            if (exactMatches.length === 1) {
                resolvedPortId = exactMatches[0].id;
            } else if (exactMatches.length > 1) {
                return res
                    .status(400)
                    .json({ error: "Ambiguous port query (exact match)" });
            } else {
                const partialMatches = await db
                    .select()
                    .from(ports)
                    .where(sql`${ports.name} ILIKE ${`%${portQuery}%`}`)
                    .limit(2);
                if (partialMatches.length === 0) {
                    return res
                        .status(404)
                        .json({ error: "Port not found for port filter" });
                }
                if (partialMatches.length > 1) {
                    return res
                        .status(400)
                        .json({ error: "Ambiguous port query (partial match)" });
                }
                resolvedPortId = partialMatches[0].id;
            }
        }

        const limitNum = Math.min(parseInt(String(limit)) || 50, 200);
        const daysNum = days
            ? Math.min(Math.max(parseInt(String(days)) || 30, 1), 365)
            : undefined;
        const includeEntity =
            String(include_entity ?? "false").toLowerCase() === "true";

        let cursorCreatedAt: Date | null = null;
        let cursorId: string | null = null;
        if (cursor) {
            try {
                const decoded = Buffer.from(String(cursor), "base64").toString("utf8");
                const [createdAtIso, id] = decoded.split("|");
                const createdAt = new Date(createdAtIso);
                if (createdAtIso && id && !Number.isNaN(createdAt.getTime())) {
                    cursorCreatedAt = createdAt;
                    cursorId = id;
                } else {
                    return res.status(400).json({ error: "Invalid cursor" });
                }
            } catch {
                return res.status(400).json({ error: "Invalid cursor" });
            }
        }

        const { items, total } = await listAlertDeliveries({
            days: daysNum,
            day: dayExact ?? null,
            tenantId,
            userId,
            entityId: resolvedPortId,
            subscriptionId: subscription_id ? String(subscription_id) : undefined,
            runId: run_id ? String(run_id) : undefined,
            status:
                status && String(status).toUpperCase() === "DLQ"
                    ? undefined
                    : status
                        ? String(status)
                        : undefined,
            destinationType: destination
                ? String(destination).toUpperCase()
                : undefined,
            isTest:
                typeof is_test === "string"
                    ? String(is_test).toLowerCase() === "true"
                    : undefined,
            cursorCreatedAt,
            cursorId,
            limit: limitNum,
        });

        const deliverySubscriptionIds = Array.from(
            new Set(items.map((row) => row.subscriptionId))
        );
        const subscriptionRows =
            deliverySubscriptionIds.length > 0
                ? await db
                    .select({
                        id: alertSubscriptions.id,
                        scope: alertSubscriptions.scope,
                        entityId: alertSubscriptions.entityId,
                        entityType: alertSubscriptions.entityType,
                    })
                    .from(alertSubscriptions)
                    .where(
                        and(
                            inArray(alertSubscriptions.id, deliverySubscriptionIds),
                            eq(alertSubscriptions.tenantId, tenantId)
                        )
                    )
                : [];
        const subscriptionMap = new Map(
            subscriptionRows.map((row) => [row.id, row])
        );

        const deliveryIds = items.map((row) => row.id);
        const dlqRows =
            deliveryIds.length > 0
                ? await db
                    .select({
                        deliveryId: alertDlq.deliveryId,
                        attemptCount: alertDlq.attemptCount,
                        maxAttempts: alertDlq.maxAttempts,
                        nextAttemptAt: alertDlq.nextAttemptAt,
                    })
                    .from(alertDlq)
                    .where(
                        and(
                            inArray(alertDlq.deliveryId, deliveryIds),
                            eq(alertDlq.tenantId, tenantId)
                        )
                    )
                : [];
        const dlqMap = new Map(dlqRows.map((row) => [row.deliveryId, row]));

        const clusterIds = Array.from(
            new Set(items.map((row) => row.clusterId).filter(Boolean))
        ) as string[];
        const signalRows =
            clusterIds.length > 0
                ? await db
                    .select({
                        id: signals.id,
                        clusterId: signals.clusterId,
                        clusterType: signals.clusterType,
                        clusterSeverity: signals.clusterSeverity,
                        clusterSummary: signals.clusterSummary,
                        confidenceScore: signals.confidenceScore,
                        confidenceBand: signals.confidenceBand,
                        method: signals.method,
                        entityId: signals.entityId,
                        day: signals.day,
                        metadata: signals.metadata,
                        createdAt: signals.createdAt,
                    })
                    .from(signals)
                    .where(inArray(signals.clusterId, clusterIds))
                : [];

        const signalMap = new Map<string, (typeof signalRows)[number]>();
        const rankSignal = (row: (typeof signalRows)[number]) => {
            const severity = String(row.clusterSeverity ?? "LOW").toUpperCase();
            const rank =
                SEVERITY_RANK[severity as keyof typeof SEVERITY_RANK] ?? 0;
            const confidence = Number(row.confidenceScore ?? 0);
            return { rank, confidence };
        };
        for (const row of signalRows) {
            const key = `${row.clusterId}|${row.entityId}|${String(row.day)}`;
            const existing = signalMap.get(key);
            if (!existing) {
                signalMap.set(key, row);
                continue;
            }
            const currentRank = rankSignal(existing);
            const nextRank = rankSignal(row);
            if (nextRank.rank > currentRank.rank) {
                signalMap.set(key, row);
            } else if (
                nextRank.rank === currentRank.rank &&
                nextRank.confidence > currentRank.confidence
            ) {
                signalMap.set(key, row);
            }
        }

        let entityMap: Map<
            string,
            { id: string; name: string; code: string; unlocode: string | null }
        > | null = null;
        if (includeEntity) {
            const portIds = Array.from(
                new Set(items.map((row) => row.entityId))
            );
            if (portIds.length > 0) {
                const portRows = await db
                    .select({
                        id: ports.id,
                        name: ports.name,
                        code: ports.code,
                        unlocode: ports.unlocode,
                    })
                    .from(ports)
                    .where(inArray(ports.id, portIds));
                entityMap = new Map(portRows.map((row) => [row.id, row]));
            } else {
                entityMap = new Map();
            }
        }

        const mapped = items.map((row) => {
            const entityRow = includeEntity ? entityMap?.get(row.entityId) : undefined;
            const key = `${row.clusterId}|${row.entityId}|${String(row.day)}`;
            const signalRow = signalMap.get(key);
            const alertPayload = signalRow
                ? buildSignalClusterAlertPayload({
                    day: signalRow.day,
                    entityType: "port",
                    entityId: signalRow.entityId,
                    clusterId: signalRow.clusterId,
                    clusterSeverity: signalRow.clusterSeverity as SignalSeverity | null,
                    confidenceScore: signalRow.confidenceScore,
                    confidenceBand: signalRow.confidenceBand as ConfidenceBand | null,
                    clusterSummary: signalRow.clusterSummary,
                    metadata: signalRow.metadata ?? {},
                })
                : null;
            const dlqRow = dlqMap.get(row.id);
            const dlqPending = Boolean(dlqRow);
            const dlqTerminal = dlqRow
                ? dlqRow.attemptCount >= dlqRow.maxAttempts
                : false;
            const subRow = subscriptionMap.get(row.subscriptionId);
            return {
                id: row.id,
                run_id: row.runId,
                subscription_id: row.subscriptionId,
                scope: subRow?.scope ?? "PORT",
                cluster_id: row.clusterId,
                cluster_type: signalRow?.clusterType ?? null,
                cluster_summary: signalRow?.clusterSummary ?? null,
                cluster_severity: signalRow?.clusterSeverity ?? null,
                confidence_score: signalRow?.confidenceScore ?? null,
                confidence_band: signalRow?.confidenceBand ?? null,
                method: signalRow?.method ?? null,
                entity_type: row.entityType,
                entity_id: row.entityId,
                day:
                    String(row.day),
                destination_type: row.destinationType,
                endpoint: row.endpoint,
                status: row.status,
                is_test: row.isTest,
                dlq_pending: dlqPending,
                dlq_terminal: dlqTerminal,
                dlq_attempts: dlqRow?.attemptCount ?? null,
                dlq_next_attempt_at: dlqRow?.nextAttemptAt ?? null,
                alert_payload: alertPayload,
                attempts: row.attempts,
                last_http_status: row.lastHttpStatus,
                latency_ms: row.latencyMs,
                error: row.error,
                sent_at: row.sentAt,
                created_at: row.createdAt,
                ...(entityRow
                    ? {
                        entity: {
                            id: entityRow.id,
                            type: "port",
                            name: entityRow.name,
                            code: entityRow.code,
                            unlocode: entityRow.unlocode,
                        },
                    }
                    : {}),
            };
        });

        let filtered = mapped;
        const statusParam = status ? String(status).toUpperCase() : undefined;
        if (statusParam === "DLQ") {
            filtered = filtered.filter((row) => row.dlq_pending);
        } else if (statusParam === "SKIPPED") {
            filtered = filtered.filter((row) =>
                String(row.status).startsWith("SKIPPED")
            );
        }
        if (severity_min) {
            const required =
                SEVERITY_RANK[
                String(severity_min).toUpperCase() as keyof typeof SEVERITY_RANK
                ] ?? 0;
            filtered = filtered.filter((row) => {
                const severity = String(row.cluster_severity ?? "LOW").toUpperCase();
                const rank =
                    SEVERITY_RANK[severity as keyof typeof SEVERITY_RANK] ?? 0;
                return rank >= required;
            });
        }

        const last = items.length > 0 ? items[items.length - 1] : null;
        const nextCursor =
            items.length === limitNum && last?.createdAt
                ? Buffer.from(
                    `${new Date(last.createdAt as any).toISOString()}|${last.id}`
                ).toString("base64")
                : null;

        res.json({ version: "1", items: filtered, total, next_cursor: nextCursor });
    } catch (error) {
        logger.error("V1 alert deliveries error", { error });
        res.status(500).json({ error: "Failed to fetch alert deliveries" });
    }
});

// GET /v1/alert-deliveries/:id
alertsRouter.get(
    "/v1/alert-deliveries/:id",
    authenticateApiKey,
    async (req, res) => {
        try {
            const id = req.params.id;
            const includeEntity =
                String(req.query.include_entity ?? "false").toLowerCase() === "true";
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });

            const [delivery] = await db
                .select()
                .from(alertDeliveries)
                .innerJoin(
                    alertSubscriptions,
                    eq(alertDeliveries.subscriptionId, alertSubscriptions.id)
                )
                .where(
                    and(
                        eq(alertDeliveries.id, id),
                        eq(alertSubscriptions.userId, userId),
                        eq(alertSubscriptions.tenantId, tenantId)
                    )
                )
                .limit(1);
            if (!delivery)
                return res.status(404).json({ error: "Delivery not found" });

            const deliveryRow = (delivery as any).alert_deliveries ?? delivery;
            const subscription = (delivery as any).alert_subscriptions;

            const [dlqRow] = await db
                .select({
                    deliveryId: alertDlq.deliveryId,
                    attemptCount: alertDlq.attemptCount,
                    maxAttempts: alertDlq.maxAttempts,
                    nextAttemptAt: alertDlq.nextAttemptAt,
                    lastError: alertDlq.lastError,
                })
                .from(alertDlq)
                .where(
                    and(
                        eq(alertDlq.deliveryId, deliveryRow.id),
                        eq(alertDlq.tenantId, tenantId)
                    )
                )
                .limit(1);

            const attempts = await db
                .select()
                .from(alertDeliveryAttempts)
                .where(
                    and(
                        eq(alertDeliveryAttempts.deliveryId, deliveryRow.id),
                        eq(alertDeliveryAttempts.tenantId, tenantId)
                    )
                )
                .orderBy(alertDeliveryAttempts.createdAt);

            const [signalRow] = await db
                .select({
                    id: signals.id,
                    clusterId: signals.clusterId,
                    clusterType: signals.clusterType,
                    clusterSeverity: signals.clusterSeverity,
                    clusterSummary: signals.clusterSummary,
                    confidenceScore: signals.confidenceScore,
                    confidenceBand: signals.confidenceBand,
                    method: signals.method,
                    entityId: signals.entityId,
                    day: signals.day,
                    metadata: signals.metadata,
                    createdAt: signals.createdAt,
                })
                .from(signals)
                .where(eq(signals.clusterId, deliveryRow.clusterId))
                .limit(1);

            const alertPayload = signalRow
                ? buildSignalClusterAlertPayload({
                    day: signalRow.day,
                    entityType: "port",
                    entityId: signalRow.entityId,
                    clusterId: signalRow.clusterId,
                    clusterSeverity: signalRow.clusterSeverity as SignalSeverity | null,
                    confidenceScore: signalRow.confidenceScore,
                    confidenceBand: signalRow.confidenceBand as ConfidenceBand | null,
                    clusterSummary: signalRow.clusterSummary,
                    metadata: signalRow.metadata ?? {},
                })
                : null;

            let entity = null as any;
            if (includeEntity) {
                if (
                    subscription?.scope === "GLOBAL" ||
                    deliveryRow.entityId === GLOBAL_SCOPE_ENTITY_ID
                ) {
                    entity = {
                        id: GLOBAL_SCOPE_ENTITY_ID,
                        type: "port",
                        name: "All ports",
                        code: "ALL",
                        unlocode: "ALL",
                    };
                } else {
                    const [portRow] = await db
                        .select({
                            id: ports.id,
                            name: ports.name,
                            code: ports.code,
                            unlocode: ports.unlocode,
                        })
                        .from(ports)
                        .where(eq(ports.id, deliveryRow.entityId))
                        .limit(1);
                    if (portRow) {
                        entity = {
                            id: portRow.id,
                            type: "port",
                            name: portRow.name,
                            code: portRow.code,
                            unlocode: portRow.unlocode,
                        };
                    }
                }
            }

            const response = {
                id: deliveryRow.id,
                run_id: deliveryRow.runId,
                subscription_id: deliveryRow.subscriptionId,
                scope: subscription?.scope ?? "PORT",
                cluster_id: deliveryRow.clusterId,
                cluster_type: signalRow?.clusterType ?? null,
                cluster_summary: signalRow?.clusterSummary ?? null,
                cluster_severity: signalRow?.clusterSeverity ?? null,
                confidence_score: signalRow?.confidenceScore ?? null,
                confidence_band: signalRow?.confidenceBand ?? null,
                method: signalRow?.method ?? null,
                entity_type: deliveryRow.entityType,
                entity_id: deliveryRow.entityId,
                day:
                    String(deliveryRow.day),
                destination_type: deliveryRow.destinationType,
                endpoint: deliveryRow.endpoint,
                status: deliveryRow.status,
                is_test: deliveryRow.isTest,
                dlq_pending: Boolean(dlqRow),
                dlq_terminal: dlqRow
                    ? dlqRow.attemptCount >= dlqRow.maxAttempts
                    : false,
                dlq_attempts: dlqRow?.attemptCount ?? null,
                dlq_next_attempt_at: dlqRow?.nextAttemptAt ?? null,
                alert_payload: alertPayload,
                attempts: deliveryRow.attempts,
                last_http_status: deliveryRow.lastHttpStatus,
                latency_ms: deliveryRow.latencyMs,
                error: deliveryRow.error,
                sent_at: deliveryRow.sentAt,
                created_at: deliveryRow.createdAt,
                attempt_history: attempts.map((row) => ({
                    attempt_no: row.attemptNo,
                    status: row.status,
                    latency_ms: row.latencyMs,
                    http_status: row.httpStatus,
                    error: row.error,
                    sent_at: row.sentAt,
                    created_at: row.createdAt,
                })),
                ...(entity ? { entity } : {}),
            };

            res.json({ version: "1", item: response });
        } catch (error: any) {
            res.status(500).json({ error: error.message || "Failed to load delivery" });
        }
    }
);

// GET /v1/alert-subscriptions
alertsRouter.get(
    "/v1/alert-subscriptions",
    authenticateApiKey,
    async (req, res) => {
        try {
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });
            const includeEntity =
                String(req.query?.include_entity ?? "false") === "true";
            const limitNum = Math.min(
                parseInt(String(req.query?.limit ?? "50")) || 50,
                200
            );
            const cursor = req.query?.cursor;
            let cursorCreatedAt: string | null = null;
            let cursorId: string | null = null;
            if (cursor) {
                try {
                    const decoded = Buffer.from(String(cursor), "base64").toString(
                        "utf8"
                    );
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

            const conditions = [
                eq(alertSubscriptions.tenantId, tenantId),
                eq(alertSubscriptions.userId, userId),
            ] as any[];
            if (cursorCreatedAt && cursorId) {
                conditions.push(
                    sql`(${alertSubscriptions.createdAt} < ${cursorCreatedAt}::timestamptz OR (${alertSubscriptions.createdAt} = ${cursorCreatedAt}::timestamptz AND ${alertSubscriptions.id} < ${cursorId}))`
                );
            }

            const rows = await db
                .select({
                    id: alertSubscriptions.id,
                    tenantId: alertSubscriptions.tenantId,
                    userId: alertSubscriptions.userId,
                    scope: alertSubscriptions.scope,
                    entityType: alertSubscriptions.entityType,
                    entityId: alertSubscriptions.entityId,
                    severityMin: alertSubscriptions.severityMin,
                    confidenceMin: alertSubscriptions.confidenceMin,
                    channel: alertSubscriptions.channel,
                    endpoint: alertSubscriptions.endpoint,
                    secret: alertSubscriptions.secret,
                    signatureVersion: alertSubscriptions.signatureVersion,
                    isEnabled: alertSubscriptions.isEnabled,
                    lastTestAt: alertSubscriptions.lastTestAt,
                    lastTestStatus: alertSubscriptions.lastTestStatus,
                    lastTestError: alertSubscriptions.lastTestError,
                    createdAt: alertSubscriptions.createdAt,
                    updatedAt: alertSubscriptions.updatedAt,
                    createdAtRaw: sql<string>`to_char(${alertSubscriptions.createdAt}, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
                })
                .from(alertSubscriptions)
                .where(and(...conditions))
                .orderBy(
                    desc(alertSubscriptions.createdAt),
                    desc(alertSubscriptions.id)
                )
                .limit(limitNum);

            let entityMap = new Map<
                string,
                { id: string; type: "port"; name: string; code: string; unlocode: string }
            >();
            if (includeEntity) {
                const ids = rows
                    .map((row) => row.entityId)
                    .filter((id) => id && id !== GLOBAL_SCOPE_ENTITY_ID);
                if (ids.length) {
                    const portRows = await db
                        .select({
                            id: ports.id,
                            name: ports.name,
                            code: ports.code,
                            unlocode: ports.unlocode,
                        })
                        .from(ports)
                        .where(inArray(ports.id, ids));
                    for (const port of portRows) {
                        entityMap.set(port.id, {
                            id: port.id,
                            type: "port",
                            name: port.name,
                            code: port.code ?? port.unlocode ?? "",
                            unlocode: port.unlocode ?? port.code ?? "",
                        });
                    }
                }
            }

            const items = rows.map((row) => ({
                id: row.id,
                user_id: row.userId,
                scope: row.scope ?? "PORT",
                destination_type: row.channel,
                destination: row.endpoint,
                entity_type: row.entityType,
                entity_id: row.entityId,
                ...(includeEntity
                    ? {
                        entity:
                            row.scope === "GLOBAL" ||
                                row.entityId === GLOBAL_SCOPE_ENTITY_ID
                                ? {
                                    id: GLOBAL_SCOPE_ENTITY_ID,
                                    type: "port",
                                    name: "All ports",
                                    code: "ALL",
                                    unlocode: "ALL",
                                }
                                : entityMap.get(row.entityId) ?? null,
                    }
                    : {}),
                severity_min: row.severityMin,
                enabled: row.isEnabled,
                signature_version: row.signatureVersion,
                has_secret: Boolean(row.secret),
                created_at: row.createdAt?.toISOString?.() ?? row.createdAt,
                updated_at: row.updatedAt?.toISOString?.() ?? row.updatedAt,
                last_test_at: row.lastTestAt?.toISOString?.() ?? null,
                last_test_status: row.lastTestStatus ?? null,
                last_test_error: row.lastTestError ?? null,
            }));

            const last = rows.length > 0 ? rows[rows.length - 1] : null;
            const nextCursor =
                rows.length === limitNum && last?.createdAtRaw
                    ? Buffer.from(`${last.createdAtRaw}|${last.id}`).toString("base64")
                    : null;

            res.json({ version: "1", items, next_cursor: nextCursor });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to list subscriptions" });
        }
    }
);

// POST /v1/alert-subscriptions
alertsRouter.post(
    "/v1/alert-subscriptions",
    authenticateApiKey,
    async (req, res) => {
        try {
            const allowHttp = process.env.NODE_ENV !== "production";
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });
            const destinationType = String(
                req.body?.destination_type ?? req.body?.channel ?? "WEBHOOK"
            ).toUpperCase();
            const destination = String(
                req.body?.destination ?? req.body?.endpoint ?? ""
            ).trim();
            const severityMin = String(req.body?.severity_min ?? "HIGH").toUpperCase();
            const enabled = req.body?.enabled !== false;
            const signatureVersion = String(req.body?.signature_version ?? "v1");
            const providedSecret = req.body?.secret
                ? String(req.body?.secret)
                : null;
            const scope = normalizeScope(req.body?.scope);
            let entityId = req.body?.entity_id
                ? String(req.body?.entity_id)
                : null;

            if (!destination)
                return res.status(400).json({ error: "destination is required" });
            if (!["WEBHOOK", "EMAIL"].includes(destinationType))
                return res
                    .status(400)
                    .json({ error: "destination_type must be WEBHOOK or EMAIL" });
            if (!["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityMin))
                return res.status(400).json({ error: "invalid severity_min" });

            if (destinationType === "WEBHOOK") {
                if (!isValidWebhookUrl(destination, allowHttp))
                    return res.status(400).json({ error: "invalid webhook url" });
            } else {
                const email = normalizeEmail(destination);
                if (!isValidEmail(email))
                    return res.status(400).json({ error: "invalid email" });
            }

            const secret =
                destinationType === "WEBHOOK"
                    ? (providedSecret ?? generateSecret())
                    : null;

            if (scope === "GLOBAL") {
                entityId = GLOBAL_SCOPE_ENTITY_ID;
            } else if (!entityId) {
                return res
                    .status(400)
                    .json({ error: "entity_id is required when scope=PORT" });
            }

            const [created] = await db
                .insert(alertSubscriptions)
                .values({
                    tenantId,
                    userId,
                    scope,
                    entityType: "port",
                    entityId,
                    severityMin,
                    channel: destinationType,
                    endpoint:
                        destinationType === "EMAIL"
                            ? normalizeEmail(destination)
                            : destination,
                    secret,
                    signatureVersion,
                    isEnabled: enabled,
                    updatedAt: new Date(),
                })
                .returning();

            res.status(201).json({
                version: "1",
                id: created.id,
                user_id: created.userId,
                scope: created.scope ?? "PORT",
                destination_type: created.channel,
                destination: created.endpoint,
                entity_type: created.entityType,
                entity_id: created.entityId,
                severity_min: created.severityMin,
                enabled: created.isEnabled,
                signature_version: created.signatureVersion,
                has_secret: Boolean(created.secret),
                created_at: created.createdAt?.toISOString?.(),
                updated_at: created.updatedAt?.toISOString?.(),
            });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to create subscription" });
        }
    }
);

// PATCH /v1/alert-subscriptions/:id
alertsRouter.patch(
    "/v1/alert-subscriptions/:id",
    authenticateApiKey,
    async (req, res) => {
        try {
            const id = req.params.id;
            const allowHttp = process.env.NODE_ENV !== "production";
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });
            const [existing] = await db
                .select()
                .from(alertSubscriptions)
                .where(
                    and(
                        eq(alertSubscriptions.id, id),
                        eq(alertSubscriptions.userId, userId),
                        eq(alertSubscriptions.tenantId, tenantId)
                    )
                )
                .limit(1);
            if (!existing)
                return res.status(404).json({ error: "subscription not found" });

            const updates: any = { updatedAt: new Date() };

            if (req.body?.scope) {
                const scope = normalizeScope(req.body.scope);
                updates.scope = scope;
                if (scope === "GLOBAL") updates.entityId = GLOBAL_SCOPE_ENTITY_ID;
                if (scope === "PORT" && !req.body?.entity_id) {
                    return res
                        .status(400)
                        .json({ error: "entity_id is required when scope=PORT" });
                }
            }

            if (req.body?.entity_id) updates.entityId = String(req.body.entity_id);

            if (req.body?.severity_min) {
                const severityMin = String(req.body.severity_min).toUpperCase();
                if (!["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityMin))
                    return res.status(400).json({ error: "invalid severity_min" });
                updates.severityMin = severityMin;
            }

            if (req.body?.enabled !== undefined)
                updates.isEnabled = Boolean(req.body.enabled);

            if (req.body?.destination) {
                const destination = String(req.body.destination).trim();
                if (existing.channel === "WEBHOOK") {
                    if (!isValidWebhookUrl(destination, allowHttp))
                        return res.status(400).json({ error: "invalid webhook url" });
                    updates.endpoint = destination;
                } else {
                    const email = normalizeEmail(destination);
                    if (!isValidEmail(email))
                        return res.status(400).json({ error: "invalid email" });
                    updates.endpoint = email;
                }
            }

            const [updated] = await db
                .update(alertSubscriptions)
                .set(updates)
                .where(
                    and(
                        eq(alertSubscriptions.id, id),
                        eq(alertSubscriptions.userId, userId),
                        eq(alertSubscriptions.tenantId, tenantId)
                    )
                )
                .returning();

            if (!updated)
                return res.status(404).json({ error: "subscription not found" });

            res.json({
                version: "1",
                id: updated.id,
                user_id: updated.userId,
                destination_type: updated.channel,
                destination: updated.endpoint,
                severity_min: updated.severityMin,
                enabled: updated.isEnabled,
                signature_version: updated.signatureVersion,
                has_secret: Boolean(updated.secret),
                created_at: updated.createdAt?.toISOString?.(),
                updated_at: updated.updatedAt?.toISOString?.(),
                last_test_at: updated.lastTestAt?.toISOString?.() ?? null,
                last_test_status: updated.lastTestStatus ?? null,
                last_test_error: updated.lastTestError ?? null,
            });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to update subscription" });
        }
    }
);

// POST /v1/alert-subscriptions/:id/rotate-secret
alertsRouter.post(
    "/v1/alert-subscriptions/:id/rotate-secret",
    authenticateApiKey,
    async (req, res) => {
        try {
            const id = req.params.id;
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });
            const newSecret = generateSecret();
            const [updated] = await db
                .update(alertSubscriptions)
                .set({ secret: newSecret, updatedAt: new Date() })
                .where(
                    and(
                        eq(alertSubscriptions.id, id),
                        eq(alertSubscriptions.userId, userId),
                        eq(alertSubscriptions.tenantId, tenantId)
                    )
                )
                .returning();
            if (!updated)
                return res.status(404).json({ error: "subscription not found" });
            res.json({ version: "1", rotated: true, secret: newSecret });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to rotate secret" });
        }
    }
);

// POST /v1/alert-subscriptions/:id/test
alertsRouter.post(
    "/v1/alert-subscriptions/:id/test",
    authenticateApiKey,
    async (req, res) => {
        try {
            const id = req.params.id;
            const mode = String(req.body?.mode ?? "synthetic");
            const severity = String(req.body?.severity ?? "HIGH").toUpperCase();
            const now = new Date();
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });

            const [subscription] = await db
                .select()
                .from(alertSubscriptions)
                .where(
                    and(
                        eq(alertSubscriptions.id, id),
                        eq(alertSubscriptions.userId, userId),
                        eq(alertSubscriptions.tenantId, tenantId)
                    )
                )
                .limit(1);
            if (!subscription)
                return res.status(404).json({ error: "subscription not found" });

            const rateKey = subscription.id;
            const windowMs = 60_000;
            const timestamps = (
                subscriptionTestRateLimit.get(rateKey) ?? []
            ).filter((ts) => now.getTime() - ts < windowMs);
            if (timestamps.length >= 5)
                return res.status(429).json({ error: "rate limit exceeded" });
            timestamps.push(now.getTime());
            subscriptionTestRateLimit.set(rateKey, timestamps);

            const payload = {
                event_type: "TEST_ALERT",
                sent_at: now.toISOString(),
                subscription_id: subscription.id,
                severity,
                mode,
                sample: req.body?.include_sample_signal
                    ? { note: "Sample signal included" }
                    : undefined,
            };

            let status: "SENT" | "FAILED" = "SENT";
            let latencyMs: number | null = null;
            let httpStatus: number | null = null;
            let errorMessage: string | null = null;

            if (subscription.channel === "WEBHOOK") {
                const { body, headers } = buildWebhookRequest({
                    payload,
                    secret: subscription.secret ?? null,
                    subscriptionId: subscription.id,
                    clusterId: `TEST:${subscription.id}`,
                    day: now.toISOString().slice(0, 10),
                    now,
                });
                try {
                    const result = await sendWebhook({
                        endpoint: subscription.endpoint,
                        body,
                        headers,
                    });
                    const attemptLogs = (result as any)?.attemptLogs ?? [];
                    const last = attemptLogs.length
                        ? attemptLogs[attemptLogs.length - 1]
                        : null;
                    latencyMs = last?.latency_ms ?? null;
                    httpStatus =
                        last?.http_status ?? (result as any)?.status ?? null;
                } catch (error: any) {
                    status = "FAILED";
                    errorMessage = error?.message ?? "Test delivery failed";
                }
            } else {
                try {
                    await sendEmail({
                        to: subscription.endpoint,
                        subject: "[Veriscope] TEST ALERT",
                        text: "Test alert delivery.",
                    });
                } catch (error: any) {
                    status = "FAILED";
                    errorMessage = error?.message ?? "Test email failed";
                }
            }

            const [testRun] = await db
                .insert(alertRuns)
                .values({
                    tenantId,
                    status: "TEST",
                    startedAt: now,
                    finishedAt: now,
                    summary: { mode: "test" },
                })
                .returning();

            const [delivery] = await db
                .insert(alertDeliveries)
                .values({
                    runId: testRun.id,
                    tenantId,
                    userId: subscription.userId,
                    subscriptionId: subscription.id,
                    clusterId: `TEST:${subscription.id}`,
                    entityType: subscription.entityType,
                    entityId: subscription.entityId,
                    day: now.toISOString().slice(0, 10),
                    destinationType: subscription.channel,
                    endpoint: subscription.endpoint,
                    status,
                    attempts: 1,
                    lastHttpStatus: httpStatus,
                    latencyMs: latencyMs,
                    error: errorMessage,
                    sentAt: status === "SENT" ? now : null,
                    isTest: true,
                    createdAt: now,
                })
                .returning();

            await db.insert(alertDeliveryAttempts).values({
                tenantId,
                deliveryId: delivery.id,
                attemptNo: 1,
                status,
                latencyMs: latencyMs,
                httpStatus: httpStatus,
                error: errorMessage,
                sentAt: status === "SENT" ? now : null,
                createdAt: now,
            });

            await db
                .update(alertSubscriptions)
                .set({
                    lastTestAt: now,
                    lastTestStatus: status,
                    lastTestError: errorMessage,
                    updatedAt: now,
                })
                .where(
                    and(
                        eq(alertSubscriptions.id, subscription.id),
                        eq(alertSubscriptions.tenantId, tenantId)
                    )
                );

            res.json({
                version: "1",
                status,
                latency_ms: latencyMs,
                http_status: httpStatus,
                error: errorMessage,
                delivery_id: delivery?.id,
            });
        } catch (error: any) {
            res.status(500).json({ error: error.message || "Failed to send test" });
        }
    }
);

// POST /api/alerts/retry-delivery/:delivery_id
alertsRouter.post(
    "/api/alerts/retry-delivery/:delivery_id",
    authenticateApiKey,
    async (req, res) => {
        try {
            const deliveryId = req.params.delivery_id;
            const [ownership] = await db
                .select({
                    userId: alertSubscriptions.userId,
                    tenantId: alertSubscriptions.tenantId,
                })
                .from(alertDeliveries)
                .innerJoin(
                    alertSubscriptions,
                    eq(alertDeliveries.subscriptionId, alertSubscriptions.id)
                )
                .where(eq(alertDeliveries.id, deliveryId))
                .limit(1);
            if (
                !ownership ||
                ownership.userId !== req.auth?.userId ||
                ownership.tenantId !== req.auth?.tenantId
            ) {
                return res.status(404).json({ error: "Delivery not found" });
            }
            const result = await retryDeliveryById({
                deliveryId,
                tenantId: req.auth?.tenantId,
                userId: req.auth?.userId,
                now: new Date(),
            });
            if (result.status === "not_found")
                return res.status(404).json({ error: "Delivery not found" });
            if (result.status === "already_sent")
                return res.status(409).json({ error: "Delivery already sent" });
            if (result.status === "terminal")
                return res.status(409).json({ error: "Delivery is terminal" });
            res.json({
                version: "1",
                delivery: result.delivery,
                dlq: result.dlq ?? null,
                status: result.status,
            });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to retry delivery" });
        }
    }
);

// GET /api/alerts/metrics
alertsRouter.get(
    "/api/alerts/metrics",
    authenticateApiKey,
    async (req, res) => {
        try {
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });
            const daysRaw = Number(req.query.days ?? 30);
            const days = Number.isFinite(daysRaw) ? daysRaw : 30;
            const [deliveryHealth, latency, endpointHealth] = await Promise.all([
                getDeliveryHealthByDay(days, { tenantId, userId }),
                getDeliveryLatency(days, { tenantId, userId }),
                getEndpointHealth(days, { tenantId, userId }),
            ]);
            res.json({
                version: "1",
                days,
                delivery_health: deliveryHealth,
                latency,
                endpoint_health: endpointHealth,
            });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to fetch alert metrics" });
        }
    }
);

// GET /api/alerts/dlq-health
alertsRouter.get(
    "/api/alerts/dlq-health",
    authenticateApiKey,
    async (req, res) => {
        try {
            const userId = req.auth?.userId;
            const tenantId = req.auth?.tenantId;
            if (!userId || !tenantId)
                return res.status(401).json({ error: "API key required" });
            const limit = Math.min(Number(req.query.limit ?? 20), 200);
            const [health, overdue] = await Promise.all([
                getDlqHealth({ tenantId, userId }),
                getDlqOverdue(Number.isFinite(limit) ? limit : 20, {
                    tenantId,
                    userId,
                }),
            ]);
            res.json({ version: "1", health, overdue });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to fetch dlq health" });
        }
    }
);

// ── Communications ───────────────────────────────────────────────────────────

alertsRouter.get(
    "/api/communications",
    optionalAuth,
    async (req, res) => {
        try {
            const { userId } = req.query;
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 100 });

            const communications = await storage.getCommunications(
                userId as string | undefined,
                500
            );

            if (usePagination) {
                res.json(paginateArray(communications, pagination));
            } else {
                res.json(communications);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch communications" });
        }
    }
);

alertsRouter.get(
    "/api/communications/unread",
    optionalAuth,
    async (req, res) => {
        try {
            const { userId } = req.query;
            const unread = await storage.getUnreadCommunications(
                (userId as string) || "default"
            );
            res.json(unread);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch unread communications" });
        }
    }
);

alertsRouter.post(
    "/api/communications",
    optionalAuth,
    async (req, res) => {
        try {
            const communication = await storage.createCommunication(req.body);
            res.status(201).json(communication);
        } catch (error) {
            res.status(500).json({ error: "Failed to create communication" });
        }
    }
);

alertsRouter.patch(
    "/api/communications/:id/read",
    optionalAuth,
    async (req, res) => {
        try {
            const { id } = req.params;
            const communication = await storage.markCommunicationAsRead(id);
            res.json(communication);
        } catch (error) {
            res.status(500).json({ error: "Failed to mark communication as read" });
        }
    }
);

// ── Watchlists ───────────────────────────────────────────────────────────────

alertsRouter.get("/api/watchlists", optionalAuth, async (req, res) => {
    try {
        const userId = (req as any).user?.id || "demo-user";
        const watchlists = await storage.getWatchlists(userId);
        res.json(watchlists);
    } catch (error) {
        res.status(500).json({ error: "Failed to fetch watchlists" });
    }
});

alertsRouter.post("/api/watchlists", optionalAuth, async (req, res) => {
    try {
        const userId = (req as any).user?.id || "demo-user";
        const { name, type, items, alertSettings, isDefault } = req.body;

        if (!name || !type || !items) {
            return res
                .status(400)
                .json({ error: "Name, type, and items are required" });
        }

        const watchlist = await storage.createWatchlist({
            userId,
            name,
            type,
            items,
            alertSettings,
            isDefault: isDefault || false,
        });
        res.status(201).json(watchlist);
    } catch (error) {
        res.status(500).json({ error: "Failed to create watchlist" });
    }
});

alertsRouter.get("/api/watchlists/:id", optionalAuth, async (req, res) => {
    try {
        const watchlist = await storage.getWatchlistById(req.params.id);
        if (!watchlist)
            return res.status(404).json({ error: "Watchlist not found" });
        res.json(watchlist);
    } catch (error) {
        res.status(500).json({ error: "Failed to fetch watchlist" });
    }
});

alertsRouter.patch("/api/watchlists/:id", optionalAuth, async (req, res) => {
    try {
        const { name, items, alertSettings, isDefault } = req.body;
        const watchlist = await storage.updateWatchlist(req.params.id, {
            name,
            items,
            alertSettings,
            isDefault,
        });
        res.json(watchlist);
    } catch (error) {
        res.status(500).json({ error: "Failed to update watchlist" });
    }
});

alertsRouter.delete("/api/watchlists/:id", optionalAuth, async (req, res) => {
    try {
        await storage.deleteWatchlist(req.params.id);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Failed to delete watchlist" });
    }
});

// ── Alert Rules ──────────────────────────────────────────────────────────────

alertsRouter.get("/api/alert-rules", optionalAuth, async (req, res) => {
    try {
        const userId = (req as any).user?.id || "demo-user";
        const rules = await storage.getAlertRules(userId);
        res.json(rules);
    } catch (error) {
        logger.error("Error fetching alert rules", { error });
        res.status(500).json({ error: "Failed to fetch alert rules" });
    }
});

alertsRouter.post("/api/alert-rules", optionalAuth, async (req, res) => {
    try {
        const userId = (req as any).user?.id || "demo-user";
        const {
            name,
            type,
            conditions,
            channels,
            cooldownMinutes,
            watchlistId,
            isActive,
            severity,
            isMuted,
        } = req.body;

        if (!name || !type || !conditions || !channels) {
            return res
                .status(400)
                .json({ error: "Name, type, conditions, and channels are required" });
        }

        const rule = await storage.createAlertRule({
            userId,
            name,
            type,
            conditions,
            channels,
            cooldownMinutes: cooldownMinutes || 60,
            watchlistId,
            isActive: isActive !== false,
            severity: severity || "medium",
            isMuted: isMuted || false,
        });
        res.status(201).json(rule);
    } catch (error) {
        logger.error("Error creating alert rule", { error });
        res.status(500).json({ error: "Failed to create alert rule" });
    }
});

alertsRouter.get("/api/alert-rules/:id", optionalAuth, async (req, res) => {
    try {
        const rule = await storage.getAlertRuleById(req.params.id);
        if (!rule) return res.status(404).json({ error: "Alert rule not found" });
        res.json(rule);
    } catch (error) {
        res.status(500).json({ error: "Failed to fetch alert rule" });
    }
});

alertsRouter.patch("/api/alert-rules/:id", optionalAuth, async (req, res) => {
    try {
        const {
            name,
            conditions,
            channels,
            cooldownMinutes,
            isActive,
            watchlistId,
            severity,
            isMuted,
            snoozedUntil,
        } = req.body;
        const rule = await storage.updateAlertRule(req.params.id, {
            name,
            conditions,
            channels,
            cooldownMinutes,
            isActive,
            watchlistId,
            severity,
            isMuted,
            snoozedUntil: snoozedUntil ? new Date(snoozedUntil) : undefined,
        });
        res.json(rule);
    } catch (error) {
        res.status(500).json({ error: "Failed to update alert rule" });
    }
});

alertsRouter.post(
    "/api/alert-rules/:id/snooze",
    optionalAuth,
    async (req, res) => {
        try {
            const { hours } = req.body;
            if (!hours || hours < 1 || hours > 168) {
                return res
                    .status(400)
                    .json({ error: "Hours must be between 1 and 168 (7 days)" });
            }
            const snoozedUntil = new Date(Date.now() + hours * 60 * 60 * 1000);
            const rule = await storage.updateAlertRule(req.params.id, {
                snoozedUntil,
            });
            res.json(rule);
        } catch (error) {
            res.status(500).json({ error: "Failed to snooze alert rule" });
        }
    }
);

alertsRouter.post(
    "/api/alert-rules/:id/unsnooze",
    optionalAuth,
    async (req, res) => {
        try {
            const rule = await storage.updateAlertRule(req.params.id, {
                snoozedUntil: null as any,
            });
            res.json(rule);
        } catch (error) {
            res.status(500).json({ error: "Failed to unsnooze alert rule" });
        }
    }
);

alertsRouter.post(
    "/api/alert-rules/:id/mute",
    optionalAuth,
    async (req, res) => {
        try {
            const { muted } = req.body;
            const rule = await storage.updateAlertRule(req.params.id, {
                isMuted: muted !== false,
            });
            res.json(rule);
        } catch (error) {
            res.status(500).json({ error: "Failed to mute/unmute alert rule" });
        }
    }
);

alertsRouter.delete("/api/alert-rules/:id", optionalAuth, async (req, res) => {
    try {
        await storage.deleteAlertRule(req.params.id);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Failed to delete alert rule" });
    }
});
