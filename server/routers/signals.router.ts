import { Router } from "express";
import { and, eq, inArray, or, sql } from "drizzle-orm";
import { db } from "../db";
import { ports, signals } from "@shared/schema";
import {
    listSignals,
    getSignalById,
    parseSignalDay,
    formatSignalDay,
    getYesterdayUtcDay,
} from "../services/signalEngineService";
import { buildSignalResponse, type SignalEntity } from "../services/signalResponseService";
import { optionalAuth, authenticate, requirePermission } from "../middleware/rbac";
import { publicDataRateLimiter } from "../middleware/rateLimiter";
import { parseSafeLimit, parsePaginationParams, paginateArray } from "../utils/pagination";
import { cacheService, CACHE_KEYS, CACHE_TTL } from "../services/cacheService";
import { storage } from "../storage";
import { SEVERITY_RANK } from "@shared/signalTypes";
import { logger } from "../middleware/observability";

export const signalsRouter = Router();

// V1 Signals - List with filters
signalsRouter.get("/v1/signals", publicDataRateLimiter, optionalAuth, async (req, res) => {
    try {
        const {
            port_id,
            port,
            signal_type,
            severity,
            severity_min,
            clustered,
            include_entity,
            day,
            day_from,
            day_to,
            limit = "50",
            offset = "0",
        } = req.query;

        const dayExact = day ? parseSignalDay(String(day)) : null;
        let dayFrom = dayExact ?? (day_from ? parseSignalDay(String(day_from)) : null);
        let dayTo = dayExact ?? (day_to ? parseSignalDay(String(day_to)) : null);

        if ((day && !dayExact) || (day_from && !dayFrom) || (day_to && !dayTo)) {
            return res.status(400).json({ error: "day/day_from/day_to must be YYYY-MM-DD" });
        }

        const limitNum = parseSafeLimit(limit, 50, 500);
        const offsetNum = parseSafeLimit(offset, 0, 100000);
        const severityMin = severity_min ? String(severity_min).toUpperCase() : undefined;
        const clusteredParam = clustered ? String(clustered).toLowerCase() : undefined;
        const clusteredFlag =
            clusteredParam === undefined ? true : !["false", "0", "no"].includes(clusteredParam);
        const includeEntity = String(include_entity ?? "false").toLowerCase() === "true";

        let resolvedPortId = port_id ? String(port_id) : undefined;
        const portQuery = port ? String(port).trim() : undefined;
        if (!resolvedPortId && portQuery) {
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
                return res.status(400).json({ error: "Ambiguous port query (exact match)" });
            } else {
                const partialMatches = await db
                    .select()
                    .from(ports)
                    .where(sql`${ports.name} ILIKE ${`%${portQuery}%`}`)
                    .limit(2);

                if (partialMatches.length === 0) {
                    return res.status(404).json({ error: "Port not found for port filter" });
                }
                if (partialMatches.length > 1) {
                    return res.status(400).json({ error: "Ambiguous port query (partial match)" });
                }
                resolvedPortId = partialMatches[0].id;
            }
        }

        if (!day && !day_from && !day_to) {
            const conditions = [] as any[];
            if (resolvedPortId) {
                conditions.push(eq(signals.entityType, "port"));
                conditions.push(eq(signals.entityId, resolvedPortId));
            }
            if (signal_type) {
                conditions.push(eq(signals.signalType, String(signal_type).toUpperCase()));
            }
            if (severity) {
                conditions.push(eq(signals.severity, String(severity).toUpperCase()));
            }
            if (severityMin) {
                const rank = SEVERITY_RANK[severityMin as keyof typeof SEVERITY_RANK] ?? 0;
                if (rank > 0) {
                    conditions.push(
                        sql`CASE ${signals.severity}
                WHEN 'LOW' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'HIGH' THEN 3
                WHEN 'CRITICAL' THEN 4
                ELSE 0
              END >= ${rank}`
                    );
                }
            }

            const whereSql =
                conditions.length > 0 ? sql`${sql.join(conditions, sql` AND `)}` : sql`1=1`;
            const latestResult = await db.execute(sql`
        SELECT max(day) AS max_day
        FROM ${signals}
        WHERE ${whereSql}
      `);
            const latestDay = (latestResult as any).rows?.[0]?.max_day;
            if (latestDay) {
                const latestDate = latestDay instanceof Date ? latestDay : new Date(latestDay);
                dayFrom = latestDate;
                dayTo = latestDate;
            }
        }

        const { items, total } = await listSignals({
            portId: resolvedPortId,
            signalType: signal_type ? String(signal_type).toUpperCase() : undefined,
            severity: severity ? String(severity).toUpperCase() : undefined,
            severityMin,
            dayFrom,
            dayTo,
            limit: limitNum,
            offset: offsetNum,
            clustered: clusteredFlag,
        });

        let entityMap: Map<string, SignalEntity> | null = null;
        if (includeEntity) {
            const portIds = Array.from(
                new Set(
                    items
                        .filter((signal: any) => signal.entityType === "port")
                        .map((signal: any) => signal.entityId as string)
                )
            );
            if (portIds.length > 0) {
                const portRows = await db
                    .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
                    .from(ports)
                    .where(inArray(ports.id, portIds as string[]));
                entityMap = new Map<string, SignalEntity>(
                    portRows.map((row): [string, SignalEntity] => [
                        row.id,
                        { id: row.id, type: "port", name: row.name, code: row.code, unlocode: row.unlocode ?? "" },
                    ])
                );
            } else {
                entityMap = new Map();
            }
        }

        const compat = String(req.query.compat ?? "false").toLowerCase() === "true";
        const mapped = (items as any[]).map((signal: any) =>
            buildSignalResponse(signal, {
                compat,
                includeEntity,
                entityMap: entityMap ?? new Map<string, SignalEntity>(),
            })
        );

        res.json({ items: mapped, total });
    } catch (error) {
        logger.error("V1 signals list error", { error });
        res.status(500).json({ error: "Failed to fetch signals" });
    }
});

// V1 Signals - Get by ID
signalsRouter.get("/v1/signals/:id", optionalAuth, async (req, res) => {
    try {
        const signal = await getSignalById(req.params.id);
        if (!signal) {
            return res.status(404).json({ error: "Signal not found" });
        }

        const compat = String(req.query.compat ?? "false").toLowerCase() === "true";
        const includeEntity =
            String(req.query.include_entity ?? "false").toLowerCase() === "true";
        let entityMap: Map<string, SignalEntity> | undefined;
        if (includeEntity && signal.entityType === "port") {
            const portRow = await db
                .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
                .from(ports)
                .where(eq(ports.id, signal.entityId))
                .limit(1);
            if (portRow.length > 0) {
                entityMap = new Map([
                    [
                        portRow[0].id,
                        {
                            id: portRow[0].id,
                            type: "port" as const,
                            name: portRow[0].name,
                            code: portRow[0].code,
                            unlocode: portRow[0].unlocode ?? "",
                        },
                    ],
                ]);
            }
        }

        res.json(buildSignalResponse(signal, { compat, includeEntity, entityMap }));
    } catch (error) {
        logger.error("V1 signal detail error", { error });
        res.status(500).json({ error: "Failed to fetch signal" });
    }
});

// API signals endpoint with caching and optional pagination
signalsRouter.get(
    "/api/signals",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 50 });

            const sigs = await cacheService.getOrSet(
                CACHE_KEYS.ACTIVE_SIGNALS,
                () => storage.getActiveSignals(),
                CACHE_TTL.SHORT
            );

            if (usePagination) {
                res.json(paginateArray(sigs, pagination));
            } else {
                res.json(sigs);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch signals" });
        }
    }
);
