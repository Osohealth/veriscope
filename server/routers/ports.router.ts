import { Router } from "express";
import { storage } from "../storage";
import { insertPortCallSchema, insertContainerOperationSchema } from "@shared/schema";
import { rotterdamDataService } from "../services/rotterdamDataService";
import { optionalAuth, authenticate, requirePermission } from "../middleware/rbac";
import { publicDataRateLimiter } from "../middleware/rateLimiter";
import {
    parsePaginationParams,
    paginateArray,
    parseGeoQueryParams,
    filterByGeoRadius,
    parseSafeLimit,
} from "../utils/pagination";
import { cacheService, CACHE_KEYS, CACHE_TTL } from "../services/cacheService";
import { logger } from "../middleware/observability";

export const portsRouter = Router();

// V1 Ports - List
portsRouter.get("/v1/ports", publicDataRateLimiter, optionalAuth, async (req, res) => {
    try {
        const { q, country_code, limit = "50" } = req.query;
        const ports = await storage.getPorts();

        let filtered = ports;

        if (q) {
            const search = String(q).toLowerCase();
            filtered = filtered.filter(
                (p) =>
                    p.name.toLowerCase().includes(search) ||
                    p.code.toLowerCase().includes(search) ||
                    (p.unlocode && p.unlocode.toLowerCase().includes(search))
            );
        }

        if (country_code) {
            const cc = String(country_code).toUpperCase();
            filtered = filtered.filter(
                (p) =>
                    p.countryCode === cc || p.country.toUpperCase().includes(cc)
            );
        }

        const limitNum = parseSafeLimit(limit, 50, 500);
        filtered = filtered.slice(0, limitNum);

        res.json({
            items: filtered.map((p) => ({
                id: p.id,
                name: p.name,
                unlocode: p.unlocode || p.code,
                country_code:
                    p.countryCode || p.country?.substring(0, 2).toUpperCase(),
                latitude: parseFloat(String(p.latitude)),
                longitude: parseFloat(String(p.longitude)),
                timezone: p.timezone || "UTC",
            })),
            total: filtered.length,
        });
    } catch (error) {
        logger.error("V1 ports list error", { error });
        res.status(500).json({ error: "Failed to fetch ports" });
    }
});

// V1 Ports - Get by ID with 7-day KPIs
portsRouter.get("/v1/ports/:port_id", optionalAuth, async (req, res) => {
    try {
        const { port_id } = req.params;
        const ports = await storage.getPorts();
        const port = ports.find((p) => p.id === port_id);

        if (!port) {
            return res.status(404).json({ error: "Port not found" });
        }

        const { getPortMetrics7d } = await import("../services/portStatisticsService");
        const metrics = await getPortMetrics7d(port_id);

        res.json({
            id: port.id,
            name: port.name,
            unlocode: port.unlocode || port.code,
            country_code:
                port.countryCode || port.country?.substring(0, 2).toUpperCase(),
            latitude: parseFloat(String(port.latitude)),
            longitude: parseFloat(String(port.longitude)),
            timezone: port.timezone || "UTC",
            metrics_7d: {
                arrivals: metrics.arrivals,
                departures: metrics.departures,
                unique_vessels: metrics.unique_vessels,
                avg_dwell_hours: metrics.avg_dwell_hours,
                median_dwell_hours: metrics.median_dwell_hours,
                open_calls: metrics.open_calls,
            },
        });
    } catch (error) {
        logger.error("V1 port detail error", { error });
        res.status(500).json({ error: "Failed to fetch port" });
    }
});

// V1 Ports - Get port calls
portsRouter.get("/v1/ports/:port_id/calls", optionalAuth, async (req, res) => {
    try {
        const { port_id } = req.params;
        const { start_time, end_time, limit = "100" } = req.query;

        const startDate = start_time
            ? new Date(String(start_time))
            : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
        const endDate = end_time ? new Date(String(end_time)) : new Date();

        const portCalls = await storage.getPortCallsByPort(
            port_id,
            startDate,
            endDate
        );
        const limitNum = parseSafeLimit(limit, 100, 500);

        const items = await Promise.all(
            portCalls.slice(0, limitNum).map(async (call) => {
                const vessel = await storage.getVessel(call.vesselId);

                let dwellHours = null;
                if (call.departureTime && call.arrivalTime) {
                    const arrival = new Date(call.arrivalTime);
                    const departure = new Date(call.departureTime);
                    dwellHours =
                        Math.round(
                            ((departure.getTime() - arrival.getTime()) /
                                (1000 * 60 * 60)) *
                            10
                        ) / 10;
                }

                return {
                    id: call.id,
                    vessel_id: call.vesselId,
                    vessel_name: vessel?.name || "Unknown",
                    arrival_time_utc: call.arrivalTime,
                    departure_time_utc: call.departureTime,
                    dwell_hours: dwellHours,
                };
            })
        );

        res.json({ items });
    } catch (error) {
        logger.error("V1 port calls error", { error });
        res.status(500).json({ error: "Failed to fetch port calls" });
    }
});

// V1 Ports - Daily arrivals/departures time series (last 7 days)
portsRouter.get(
    "/v1/ports/:port_id/daily_stats",
    optionalAuth,
    async (req, res) => {
        try {
            const { port_id } = req.params;
            const { getDailyArrivalsTimeSeries } = await import(
                "../services/portStatisticsService"
            );
            const timeSeries = await getDailyArrivalsTimeSeries(port_id);
            res.json({ port_id, items: timeSeries });
        } catch (error) {
            logger.error("V1 port daily stats error", { error });
            res.status(500).json({ error: "Failed to fetch daily statistics" });
        }
    }
);

// V1 Ports - Top busy ports (last 7 days)
portsRouter.get("/v1/ports/stats/top_busy", optionalAuth, async (req, res) => {
    try {
        const { limit = "20" } = req.query;
        const { getTopBusyPorts } = await import("../services/portStatisticsService");
        const topPorts = await getTopBusyPorts(
            parseSafeLimit(limit, 20, 100)
        );
        res.json({ items: topPorts });
    } catch (error) {
        logger.error("V1 top busy ports error", { error });
        res.status(500).json({ error: "Failed to fetch top busy ports" });
    }
});

// API ports endpoint with caching and optional pagination (public access)
portsRouter.get("/api/ports", publicDataRateLimiter, optionalAuth, async (req, res) => {
    try {
        const usePagination = req.query.paginate === "true";
        const pagination = parsePaginationParams(req, { limit: 100 });
        const geoParams = parseGeoQueryParams(req);

        const ports = await cacheService.getOrSet(
            CACHE_KEYS.PORTS,
            () => storage.getPorts(),
            CACHE_TTL.LONG
        );

        let result = ports;

        if (geoParams) {
            result = filterByGeoRadius(ports, geoParams);
        }

        if (usePagination) {
            res.json(paginateArray(result, pagination));
        } else {
            res.json(result);
        }
    } catch (error) {
        res.status(500).json({ error: "Failed to fetch ports" });
    }
});

// Port statistics endpoint with caching
portsRouter.get(
    "/api/ports/:portId/stats",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const { portId } = req.params;
            const stats = await cacheService.getOrSet(
                CACHE_KEYS.PORT_STATS(portId),
                () => storage.getLatestPortStats(portId),
                CACHE_TTL.MEDIUM
            );
            res.json(stats);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch port statistics" });
        }
    }
);

// Port delay events endpoint
portsRouter.get(
    "/api/ports/:portId/delays",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const { portId } = req.params;
            const { limit } = req.query;
            const delays = await storage.getPortDelayEvents(
                portId,
                parseSafeLimit(limit, 50, 500)
            );
            res.json(delays);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch port delay events" });
        }
    }
);

// Rotterdam data endpoints
portsRouter.get(
    "/api/rotterdam-data",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const { month } = req.query;

            if (month) {
                const data = rotterdamDataService.getDataByMonth(month as string);
                const stats = rotterdamDataService.getAggregatedStats(month as string);
                res.json({ data, stats });
            } else {
                const data = rotterdamDataService.getAllData();
                const stats = rotterdamDataService.getAggregatedStats();
                res.json({ data, stats });
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch Rotterdam data" });
        }
    }
);

portsRouter.get(
    "/api/rotterdam-data/months",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const months = rotterdamDataService.getAvailableMonths();
            res.json(months);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch available months" });
        }
    }
);

portsRouter.get(
    "/api/rotterdam-data/latest",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const latest = rotterdamDataService.getLatestData();
            res.json(latest);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch latest Rotterdam data" });
        }
    }
);

// Real-time Port of Rotterdam arrivals/departures
portsRouter.get(
    "/api/rotterdam/arrivals",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const arrivals = await rotterdamDataService.getExpectedArrivals();
            res.json({
                port: "NLRTM",
                portName: "Port of Rotterdam",
                count: arrivals.length,
                arrivals,
                lastUpdated: new Date().toISOString(),
            });
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch Rotterdam arrivals" });
        }
    }
);

portsRouter.get(
    "/api/rotterdam/departures",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const departures = await rotterdamDataService.getRecentDepartures();
            res.json({
                port: "NLRTM",
                portName: "Port of Rotterdam",
                count: departures.length,
                departures,
                lastUpdated: new Date().toISOString(),
            });
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch Rotterdam departures" });
        }
    }
);

portsRouter.get(
    "/api/rotterdam/vessels-at-port",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const vessels = await rotterdamDataService.getVesselsAtPort();
            res.json({
                port: "NLRTM",
                portName: "Port of Rotterdam",
                atBerth: vessels.atBerth,
                atAnchor: vessels.atAnchor,
                totalAtBerth: vessels.atBerth.length,
                totalAtAnchor: vessels.atAnchor.length,
                lastUpdated: new Date().toISOString(),
            });
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch vessels at Rotterdam" });
        }
    }
);

portsRouter.get(
    "/api/rotterdam/activity-summary",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const summary = await rotterdamDataService.getPortActivitySummary();
            res.json({
                port: "NLRTM",
                portName: "Port of Rotterdam",
                ...summary,
            });
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch Rotterdam activity summary" });
        }
    }
);

// Port Calls endpoints with optional pagination
portsRouter.get(
    "/api/port-calls",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const { portId, vesselId } = req.query;
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 100 });

            const calls = await storage.getPortCalls(
                portId as string | undefined,
                vesselId as string | undefined,
                500
            );

            if (usePagination) {
                res.json(paginateArray(calls, pagination));
            } else {
                res.json(calls);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch port calls" });
        }
    }
);

portsRouter.post(
    "/api/port-calls",
    authenticate,
    requirePermission("write:ports"),
    async (req, res) => {
        try {
            const parsed = insertPortCallSchema.safeParse(req.body);
            if (!parsed.success) {
                return res.status(400).json({ error: parsed.error.errors[0].message });
            }
            const portCall = await storage.createPortCall(parsed.data);
            res.status(201).json(portCall);
        } catch (error) {
            res.status(500).json({ error: "Failed to create port call" });
        }
    }
);

// Container Operations endpoints with optional pagination
portsRouter.get(
    "/api/container-operations",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const { portId, vesselId } = req.query;
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 100 });

            const operations = await storage.getContainerOperations(
                portId as string | undefined,
                vesselId as string | undefined,
                500
            );

            if (usePagination) {
                res.json(paginateArray(operations, pagination));
            } else {
                res.json(operations);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch container operations" });
        }
    }
);

portsRouter.get(
    "/api/container-operations/stats/:portId",
    authenticate,
    requirePermission("read:ports"),
    async (req, res) => {
        try {
            const { portId } = req.params;
            const stats = await storage.getContainerStatsByPort(portId);
            res.json(stats);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch container stats" });
        }
    }
);

portsRouter.post(
    "/api/container-operations",
    authenticate,
    requirePermission("write:ports"),
    async (req, res) => {
        try {
            const parsed = insertContainerOperationSchema.safeParse(req.body);
            if (!parsed.success) {
                return res.status(400).json({ error: parsed.error.errors[0].message });
            }
            const operation = await storage.createContainerOperation(parsed.data);
            res.status(201).json(operation);
        } catch (error) {
            res.status(500).json({ error: "Failed to create container operation" });
        }
    }
);
