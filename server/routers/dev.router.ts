import { Router } from "express";
import { randomBytes, randomUUID } from "node:crypto";
import { and, desc, eq, inArray, or, sql } from "drizzle-orm";
import { logger } from "../middleware/observability";
import { db } from "../db";
import {
    alertDedupe,
    alertDeliveries,
    alertDlq,
    alertRuns,
    alertSubscriptions,
    apiKeys,
    portCalls,
    portDailyBaselines,
    ports,
    vessels,
} from "@shared/schema";
import { parseSignalDay, formatSignalDay } from "../services/signalEngineService";
import { TENANT_DEMO_ID } from "../config/tenancy";
import { cacheService, CACHE_KEYS } from "../services/cacheService";
import { validateAlertSubscriptionInput } from "../services/alertSubscriptionService";
import { runAlerts } from "../services/alertDispatcherService";
import { retryAlertDlq } from "../services/alertDlqQueueService";
import { authenticate, requireAdmin } from "../middleware/rbac";
import { authenticateApiKey } from "../middleware/apiKeyAuth";
import { generateApiKey, hashApiKey } from "../services/apiKeyService";

const DEMO_USER_ID = "00000000-0000-0000-0000-000000000001";

export const devRouter = Router();

// Dev-only: seed an anomaly baseline row for deterministic signals
devRouter.post("/api/dev/seed-anomaly", authenticate, requireAdmin, async (req, res) => {
    try {
        if (process.env.NODE_ENV === "production") {
            const expectedToken = process.env.DEV_SEED_TOKEN;
            if (!expectedToken) {
                return res.status(403).json({ error: "Dev seeding disabled in production" });
            }
            const providedToken =
                (req.query.token as string | undefined) ||
                (req.headers["x-dev-seed-token"] as string | undefined);
            if (!providedToken || providedToken !== expectedToken) {
                return res.status(403).json({ error: "Invalid dev seed token" });
            }
        }

        const dayParam = req.query.day as string | undefined;
        const parsedDay = parseSignalDay(dayParam);
        if (!dayParam || !parsedDay) {
            return res.status(400).json({ error: "day is required as YYYY-MM-DD" });
        }

        const requestedPortId = req.query.port_id as string | undefined;
        let port = null as null | { id: string; name: string | null; code: string | null; unlocode: string | null };

        if (requestedPortId) {
            const [found] = await db
                .select()
                .from(ports)
                .where(eq(ports.id, requestedPortId))
                .limit(1);
            if (!found) return res.status(404).json({ error: "Port not found" });
            port = found;
        } else {
            const [rotterdam] = await db
                .select()
                .from(ports)
                .where(
                    or(
                        eq(ports.unlocode, "NLRTM"),
                        eq(ports.code, "RTM"),
                        sql`${ports.name} ILIKE ${"%" + "rotterdam" + "%"}`
                    )
                )
                .limit(1);
            if (rotterdam) {
                port = rotterdam;
            } else {
                const [firstPort] = await db
                    .select()
                    .from(ports)
                    .orderBy(desc(ports.name))
                    .limit(1);
                port = firstPort ?? null;
            }
        }

        if (!port) {
            return res.status(400).json({ error: "No ports available to seed" });
        }

        const historyDays = 10;
        const historyRows = Array.from({ length: historyDays }, (_, index) => {
            const day = new Date(parsedDay);
            day.setUTCDate(day.getUTCDate() - (index + 1));
            return {
                portId: port!.id,
                day: formatSignalDay(day),
                arrivals: 100,
                departures: 90,
                uniqueVessels: 80,
                avgDwellHours: 6,
                openCalls: 10,
                arrivals30dAvg: 100,
                arrivals30dStd: 10,
                dwell30dAvg: 6,
                dwell30dStd: 1,
                openCalls30dAvg: 10,
                updatedAt: new Date(),
            };
        });

        if (historyRows.length > 0) {
            await db
                .insert(portDailyBaselines)
                .values(historyRows)
                .onConflictDoUpdate({
                    target: [portDailyBaselines.portId, portDailyBaselines.day],
                    set: {
                        arrivals: sql`excluded.arrivals`,
                        departures: sql`excluded.departures`,
                        uniqueVessels: sql`excluded.unique_vessels`,
                        avgDwellHours: sql`excluded.avg_dwell_hours`,
                        openCalls: sql`excluded.open_calls`,
                        arrivals30dAvg: sql`excluded.arrivals_30d_avg`,
                        arrivals30dStd: sql`excluded.arrivals_30d_std`,
                        dwell30dAvg: sql`excluded.dwell_30d_avg`,
                        dwell30dStd: sql`excluded.dwell_30d_std`,
                        openCalls30dAvg: sql`excluded.open_calls_30d_avg`,
                        updatedAt: sql`excluded.updated_at`,
                    },
                });
        }

        const seed = {
            portId: port.id,
            day: formatSignalDay(parsedDay!),

            arrivals: 60,
            departures: 20,
            uniqueVessels: 18,
            avgDwellHours: 12,
            openCalls: 40,
            arrivals30dAvg: 100,
            arrivals30dStd: 10,
            dwell30dAvg: 6,
            dwell30dStd: 1,
            openCalls30dAvg: 10,
            updatedAt: new Date(),
        };

        const [baseline] = await db
            .insert(portDailyBaselines)
            .values(seed)
            .onConflictDoUpdate({
                target: [portDailyBaselines.portId, portDailyBaselines.day],
                set: seed,
            })
            .returning();

        res.json({
            message: "Seeded anomaly baseline row",
            day: formatSignalDay(parsedDay),
            port: { id: port.id, name: port.name, code: port.code, unlocode: port.unlocode },
            baseline,
        });
        cacheService.invalidate(CACHE_KEYS.PORT_STATS(port.id));
        cacheService.invalidate(CACHE_KEYS.ACTIVE_SIGNALS);
    } catch (error: any) {
        res.status(500).json({ error: error.message || "Failed to seed anomaly" });
    }
});

// Dev-only: webhook sink for demo success
devRouter.post("/api/dev/webhook-sink", authenticate, requireAdmin, (req, res) => {
    res.json({ ok: true });
});

// Dev-only: seed alert subscriptions for demo
devRouter.post(
    "/api/dev/alert-subscriptions/seed",
    authenticate,
    requireAdmin,
    async (req, res) => {
        try {
            if (process.env.NODE_ENV !== "development") {
                return res.status(404).json({ error: "Not found" });
            }

            const [port] = await db
                .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
                .from(ports)
                .where(
                    or(
                        eq(ports.unlocode, "NLRTM"),
                        eq(ports.code, "RTM"),
                        sql`${ports.name} ILIKE ${"%" + "rotterdam" + "%"}`
                    )
                )
                .limit(1);

            if (!port) {
                return res
                    .status(400)
                    .json({ error: "No ports available to seed subscriptions" });
            }

            const host = req.get("host") || "localhost:5000";
            const baseUrl = `${req.protocol}://${host}`;
            const demoUserId = DEMO_USER_ID;
            const demoTenantId = TENANT_DEMO_ID;

            await db.delete(alertDlq).where(eq(alertDlq.tenantId, demoTenantId));
            await db.delete(alertDeliveries).where(eq(alertDeliveries.tenantId, demoTenantId));
            await db.delete(alertDedupe).where(eq(alertDedupe.tenantId, demoTenantId));
            await db.delete(alertRuns).where(eq(alertRuns.tenantId, demoTenantId));
            await db
                .delete(alertSubscriptions)
                .where(
                    and(
                        eq(alertSubscriptions.userId, demoUserId),
                        eq(alertSubscriptions.tenantId, demoTenantId)
                    )
                );

            const seedSubscriptions = [
                {
                    tenantId: demoTenantId,
                    userId: demoUserId,
                    scope: "PORT",
                    entityType: "port",
                    entityId: port.id,
                    severityMin: "HIGH",
                    channel: "WEBHOOK",
                    endpoint: `${baseUrl}/api/dev/webhook-sink`,
                    isEnabled: true,
                    updatedAt: new Date(),
                },
                {
                    tenantId: demoTenantId,
                    userId: demoUserId,
                    scope: "PORT",
                    entityType: "port",
                    entityId: port.id,
                    severityMin: "HIGH",
                    channel: "WEBHOOK",
                    endpoint: "http://localhost:9999/webhook",
                    isEnabled: true,
                    updatedAt: new Date(),
                },
                {
                    tenantId: demoTenantId,
                    userId: demoUserId,
                    scope: "PORT",
                    entityType: "port",
                    entityId: port.id,
                    severityMin: "HIGH",
                    channel: "EMAIL",
                    endpoint: "alerts@veriscope.dev",
                    isEnabled: true,
                    updatedAt: new Date(),
                },
            ];

            const created = await db
                .insert(alertSubscriptions)
                .values(seedSubscriptions)
                .onConflictDoNothing()
                .returning();

            res.json({ ok: true, subscriptions_created: created.length });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to seed alert subscriptions" });
        }
    }
);

// Dev-only: seed Rotterdam port calls for a week (>=30 vessels)
devRouter.post(
    "/api/dev/seed-rotterdam-week",
    authenticate,
    requireAdmin,
    async (req, res) => {
        try {
            if (process.env.NODE_ENV !== "development") {
                return res.status(404).json({ error: "Not found" });
            }

            const daysRaw = Number(req.query.days ?? 7);
            const vesselsRaw = Number(req.query.vessels ?? 30);
            const days = Math.max(7, Math.min(31, Number.isFinite(daysRaw) ? daysRaw : 7));
            const vesselCount = Math.max(
                30,
                Math.min(200, Number.isFinite(vesselsRaw) ? vesselsRaw : 30)
            );

            const [rotterdam] = await db
                .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
                .from(ports)
                .where(
                    or(
                        eq(ports.code, "NLRTM"),
                        eq(ports.unlocode, "NLRTM"),
                        sql`lower(${ports.name}) = ${"rotterdam"}`
                    )
                )
                .limit(1);

            if (!rotterdam) {
                return res.status(404).json({ error: "Rotterdam port not found" });
            }

            const todayUtc = new Date();
            const startUtc = new Date(
                Date.UTC(
                    todayUtc.getUTCFullYear(),
                    todayUtc.getUTCMonth(),
                    todayUtc.getUTCDate() - (days - 1)
                )
            );

            const mmsiList = Array.from(
                { length: vesselCount },
                (_, idx) => String(200000000 + idx)
            );
            const existing = await db
                .select({ id: vessels.id, mmsi: vessels.mmsi })
                .from(vessels)
                .where(inArray(vessels.mmsi, mmsiList));

            const existingMap = new Map(existing.map((row) => [row.mmsi, row.id]));
            const newVessels = mmsiList
                .filter((mmsi) => !existingMap.has(mmsi))
                .map((mmsi, idx) => ({
                    id: randomUUID(),
                    mmsi,
                    name: `DEV Rotterdam Vessel ${mmsi.slice(-4)}`,
                    vesselType: idx % 2 === 0 ? "container" : "tanker",
                    flag: "NL",
                    owner: "Dev Fleet",
                    operator: "Dev Ops",
                    buildYear: 2000 + (idx % 20),
                }));

            if (newVessels.length > 0) {
                await db.insert(vessels).values(newVessels);
                for (const vessel of newVessels) {
                    existingMap.set(vessel.mmsi, vessel.id);
                }
            }

            const seedTag = "dev_rotterdam_week";
            await db.execute(sql`
        DELETE FROM port_calls
        WHERE port_id = ${rotterdam.id}
          AND metadata->>'seed' = ${seedTag}
          AND arrival_time >= ${startUtc}
      `);

            const calls = mmsiList.map((mmsi, idx) => {
                const vesselId = existingMap.get(mmsi)!;
                const dayOffset = idx % days;
                const arrival = new Date(startUtc);
                arrival.setUTCDate(startUtc.getUTCDate() + dayOffset);
                arrival.setUTCHours(6 + (idx % 12), (idx * 7) % 60, 0, 0);
                const dwellHours = 8 + (idx % 24);
                const shouldDepart = idx % 5 !== 0;
                const departure = shouldDepart
                    ? new Date(arrival.getTime() + dwellHours * 3600 * 1000)
                    : null;
                return {
                    vesselId,
                    portId: rotterdam.id,
                    callType: "arrival",
                    status: shouldDepart ? "completed" : "in_progress",
                    arrivalTime: arrival,
                    departureTime: departure,
                    berthNumber: `B-${(idx % 20) + 1}`,
                    purpose:
                        idx % 3 === 0 ? "loading" : idx % 3 === 1 ? "discharging" : "bunkering",
                    waitTimeHours: String((idx % 6) + 1),
                    berthTimeHours: String(dwellHours),
                    metadata: { seed: seedTag },
                };
            });

            await db.insert(portCalls).values(calls);

            res.json({
                port: rotterdam,
                days,
                vessels: vesselCount,
                callsInserted: calls.length,
                startDate: startUtc.toISOString().slice(0, 10),
            });
        } catch (error: any) {
            res
                .status(500)
                .json({ error: error.message || "Failed to seed Rotterdam week data" });
        }
    }
);

// Dev/admin: create alert subscription
devRouter.post(
    "/api/dev/alert-subscriptions",
    authenticate,
    requireAdmin,
    async (req, res) => {
        try {
            if (
                process.env.NODE_ENV === "production" &&
                process.env.DEV_ROUTES_ENABLED !== "true"
            ) {
                return res.status(404).json({ error: "Not found" });
            }
            const allowHttp = process.env.NODE_ENV !== "production";
            const validation = validateAlertSubscriptionInput(req.body ?? {}, allowHttp);
            if (!validation.ok) {
                return res.status(400).json({ error: validation.errors![0] });
            }
            const [created] = await db
                .insert(alertSubscriptions)
                .values({ tenantId: TENANT_DEMO_ID, ...validation.value, updatedAt: new Date() } as any)
                .returning();
            res.json(created);
        } catch (error: any) {
            res.status(500).json({ error: error.message || "Failed to create subscription" });
        }
    }
);

// Dev/admin: list alert subscriptions
devRouter.get(
    "/api/dev/alert-subscriptions",
    authenticate,
    requireAdmin,
    async (req, res) => {
        try {
            if (
                process.env.NODE_ENV === "production" &&
                process.env.DEV_ROUTES_ENABLED !== "true"
            ) {
                return res.status(404).json({ error: "Not found" });
            }
            const { user_id } = req.query;
            if (!user_id) {
                return res.status(400).json({ error: "user_id is required" });
            }
            const rows = await db
                .select()
                .from(alertSubscriptions)
                .where(
                    and(
                        eq(alertSubscriptions.userId, String(user_id)),
                        eq(alertSubscriptions.tenantId, TENANT_DEMO_ID)
                    )
                );
            res.json({ items: rows });
        } catch (error: any) {
            res.status(500).json({ error: error.message || "Failed to list subscriptions" });
        }
    }
);

// Dev-only: manually trigger alert run
devRouter.post("/api/alerts/run", authenticateApiKey, async (req, res) => {
    if (process.env.NODE_ENV !== "development") {
        return res.status(404).json({ error: "Not found" });
    }
    try {
        const dayParam = req.query.day as string | undefined;
        const parsedDay = dayParam ? parseSignalDay(dayParam) : null;
        if (dayParam && !parsedDay) {
            return res.status(400).json({ error: "day must be YYYY-MM-DD" });
        }
        const result = await runAlerts({
            day: parsedDay ? formatSignalDay(parsedDay) : undefined,
            userId: req.auth?.userId,
            tenantId: req.auth?.tenantId,
        });
        res.json({
            run_id: result.runId,
            status: result.status,
            summary: result.summary,
        });
    } catch (error: any) {
        res.status(500).json({ error: error.message || "Failed to run alerts" });
    }
});

// Dev-only: retry DLQ
devRouter.post("/api/alerts/retry-dlq", authenticateApiKey, async (req, res) => {
    if (process.env.NODE_ENV !== "development") {
        return res.status(404).json({ error: "Not found" });
    }
    try {
        const limit = Math.min(Number(req.query.limit ?? 50), 200);
        const result = await retryAlertDlq({
            limit: Number.isFinite(limit) ? limit : 50,
            now: new Date(),
            tenantId: req.auth?.tenantId,
            userId: req.auth?.userId,
        });
        res.json(result);
    } catch (error: any) {
        res.status(500).json({ error: error.message || "Failed to retry dlq" });
    }
});

/** Seed a demo API key in development if none exists (startup side effect). */
export function seedDemoApiKeyIfDev(): void {
    if (process.env.NODE_ENV !== "development") return;
    setTimeout(async () => {
        try {
            const [existing] = await db
                .select()
                .from(apiKeys)
                .where(eq(apiKeys.userId, DEMO_USER_ID))
                .limit(1);
            if (!existing) {
                const rawKey = generateApiKey("vs_demo");
                await db.insert(apiKeys).values({
                    tenantId: TENANT_DEMO_ID,
                    userId: DEMO_USER_ID,
                    keyHash: hashApiKey(rawKey),
                    name: "dev-demo",
                });
                // NOTE: In development, retrieve the demo key from the database directly
                // instead of logging it. Use GET /api/cache/stats (admin) or check DB.
            }
        } catch (error) {
            logger.warn("Failed to seed demo API key", { error: (error as Error).message });
        }
    }, 0);
}
