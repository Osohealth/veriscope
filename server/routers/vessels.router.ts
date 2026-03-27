import { Router } from "express";
import { storage } from "../storage";
import { insertBunkeringEventSchema } from "@shared/schema";
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

export const vesselsRouter = Router();

// V1 Vessels - List
vesselsRouter.get("/v1/vessels", publicDataRateLimiter, optionalAuth, async (req, res) => {
    try {
        const { mmsi, imo, name } = req.query;
        const vessels = await storage.getVessels();

        let filtered = vessels;

        if (mmsi) {
            filtered = filtered.filter((v) => v.mmsi === String(mmsi));
        } else if (imo) {
            filtered = filtered.filter((v) => v.imo === String(imo));
        } else if (name) {
            const search = String(name).toLowerCase();
            filtered = filtered.filter((v) => v.name.toLowerCase().includes(search));
        }

        res.json({
            items: filtered.map((v) => ({
                id: v.id,
                mmsi: v.mmsi,
                imo: v.imo,
                name: v.name,
                vessel_type: v.vesselType,
                flag: v.flag,
            })),
        });
    } catch (error) {
        logger.error("V1 vessels list error", { error });
        res.status(500).json({ error: "Failed to fetch vessels" });
    }
});

// V1 Vessels - Get latest position
vesselsRouter.get(
    "/v1/vessels/:vessel_id/latest_position",
    optionalAuth,
    async (req, res) => {
        try {
            const { vessel_id } = req.params;
            const vessel = await storage.getVessel(vessel_id);

            if (!vessel) {
                return res.status(404).json({ error: "Vessel not found" });
            }

            const positions = await storage.getVesselPositions(vessel_id);
            const latest = positions.sort(
                (a, b) =>
                    new Date(b.timestamp || 0).getTime() -
                    new Date(a.timestamp || 0).getTime()
            )[0];

            if (!latest) {
                return res.status(404).json({ error: "No position data available" });
            }

            res.json({
                vessel_id: vessel.id,
                mmsi: vessel.mmsi,
                timestamp_utc: latest.timestampUtc || latest.timestamp,
                latitude: parseFloat(String(latest.latitude)),
                longitude: parseFloat(String(latest.longitude)),
                sog_knots: latest.sogKnots
                    ? parseFloat(String(latest.sogKnots))
                    : latest.speed
                        ? parseFloat(String(latest.speed))
                        : null,
                cog_deg: latest.cogDeg
                    ? parseFloat(String(latest.cogDeg))
                    : latest.course
                        ? parseFloat(String(latest.course))
                        : null,
            });
        } catch (error) {
            logger.error("V1 vessel latest position error", { error });
            res.status(500).json({ error: "Failed to fetch vessel position" });
        }
    }
);

// V1 Vessels - Get positions (GeoJSON)
vesselsRouter.get(
    "/v1/vessels/positions",
    publicDataRateLimiter,
    optionalAuth,
    async (req, res) => {
        try {
            const { bbox, since_minutes = "60", limit = "2000" } = req.query;

            if (!bbox) {
                return res
                    .status(400)
                    .json({ error: "bbox parameter is required (minLon,minLat,maxLon,maxLat)" });
            }

            const [minLon, minLat, maxLon, maxLat] = String(bbox)
                .split(",")
                .map(parseFloat);
            const sinceMinutes = parseSafeLimit(since_minutes, 60, 1440);
            const limitNum = parseSafeLimit(limit, 2000, 5000);
            const sinceTime = new Date(Date.now() - sinceMinutes * 60 * 1000);

            const vessels = await storage.getVessels();
            const features: any[] = [];

            for (const vessel of vessels.slice(0, 100)) {
                const positions = await storage.getVesselPositions(vessel.id);
                const recentPositions = positions.filter((p) => {
                    const posTime = new Date(p.timestampUtc || p.timestamp || 0);
                    const lat = parseFloat(String(p.latitude));
                    const lon = parseFloat(String(p.longitude));
                    return (
                        posTime >= sinceTime &&
                        lat >= minLat &&
                        lat <= maxLat &&
                        lon >= minLon &&
                        lon <= maxLon
                    );
                });

                const latest = recentPositions.sort(
                    (a, b) =>
                        new Date(b.timestamp || 0).getTime() -
                        new Date(a.timestamp || 0).getTime()
                )[0];

                if (latest) {
                    features.push({
                        type: "Feature",
                        geometry: {
                            type: "Point",
                            coordinates: [
                                parseFloat(String(latest.longitude)),
                                parseFloat(String(latest.latitude)),
                            ],
                        },
                        properties: {
                            vessel_id: vessel.id,
                            mmsi: vessel.mmsi,
                            name: vessel.name,
                            sog_knots: latest.sogKnots
                                ? parseFloat(String(latest.sogKnots))
                                : latest.speed
                                    ? parseFloat(String(latest.speed))
                                    : null,
                            cog_deg: latest.cogDeg
                                ? parseFloat(String(latest.cogDeg))
                                : latest.course
                                    ? parseFloat(String(latest.course))
                                    : null,
                            timestamp_utc: latest.timestampUtc || latest.timestamp,
                        },
                    });
                }

                if (features.length >= limitNum) break;
            }

            res.json({ type: "FeatureCollection", features });
        } catch (error) {
            logger.error("V1 vessels positions error", { error });
            res.status(500).json({ error: "Failed to fetch vessel positions" });
        }
    }
);

// API vessels endpoint with caching, optional pagination, and geo-filtering
vesselsRouter.get(
    "/api/vessels",
    authenticate,
    requirePermission("read:vessels"),
    async (req, res) => {
        try {
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 100 });
            const geoParams = parseGeoQueryParams(req);

            const vessels = await cacheService.getOrSet(
                CACHE_KEYS.VESSELS,
                () => storage.getVessels(),
                CACHE_TTL.MEDIUM
            );

            const latestPositions = await storage.getLatestVesselPositions();
            const positionMap = new Map<string, any>();
            for (const pos of latestPositions) {
                if (
                    !positionMap.has(pos.vesselId) ||
                    new Date(pos.timestamp || 0) >
                    new Date(positionMap.get(pos.vesselId).timestamp || 0)
                ) {
                    positionMap.set(pos.vesselId, pos);
                }
            }

            let result = vessels.map((vessel: any) => {
                const position = positionMap.get(vessel.id);
                return {
                    ...vessel,
                    position: position
                        ? {
                            latitude: position.latitude,
                            longitude: position.longitude,
                            speedOverGround: position.speedOverGround || 0,
                            navigationStatus: position.navigationStatus || "unknown",
                            timestamp: position.timestamp,
                        }
                        : null,
                };
            });

            if (geoParams) {
                result = filterByGeoRadius(result, geoParams);
            }

            if (usePagination) {
                res.json(paginateArray(result, pagination));
            } else {
                res.json(result);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch vessels" });
        }
    }
);

// Vessel delay snapshots endpoint
vesselsRouter.get(
    "/api/vessels/delays",
    authenticate,
    requirePermission("read:vessels"),
    async (req, res) => {
        try {
            const { vesselId, portId, limit } = req.query;
            const snapshots = await storage.getVesselDelaySnapshots(
                vesselId as string | undefined,
                portId as string | undefined,
                parseSafeLimit(limit, 50, 500)
            );
            res.json(snapshots);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch vessel delay snapshots" });
        }
    }
);

// Bunkering Events endpoints with optional pagination
vesselsRouter.get(
    "/api/bunkering-events",
    authenticate,
    requirePermission("read:vessels"),
    async (req, res) => {
        try {
            const { vesselId, portId } = req.query;
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 100 });

            const events = await storage.getBunkeringEvents(
                vesselId as string | undefined,
                portId as string | undefined,
                500
            );

            if (usePagination) {
                res.json(paginateArray(events, pagination));
            } else {
                res.json(events);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch bunkering events" });
        }
    }
);

vesselsRouter.get(
    "/api/bunkering-events/stats/:vesselId",
    authenticate,
    requirePermission("read:vessels"),
    async (req, res) => {
        try {
            const { vesselId } = req.params;
            const stats = await storage.getBunkeringStatsByVessel(vesselId);
            res.json(stats);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch bunkering stats" });
        }
    }
);

vesselsRouter.post(
    "/api/bunkering-events",
    authenticate,
    requirePermission("write:vessels"),
    async (req, res) => {
        try {
            const parsed = insertBunkeringEventSchema.safeParse(req.body);
            if (!parsed.success) {
                return res.status(400).json({ error: parsed.error.errors[0].message });
            }
            const event = await storage.createBunkeringEvent(parsed.data);
            res.status(201).json(event);
        } catch (error) {
            res.status(500).json({ error: "Failed to create bunkering event" });
        }
    }
);

// Storage sites endpoint with caching and optional pagination (public access)
vesselsRouter.get(
    "/api/storage/sites",
    publicDataRateLimiter,
    optionalAuth,
    async (req, res) => {
        try {
            const { portId } = req.query;
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 50 });

            const cacheKey = CACHE_KEYS.STORAGE_SITES(portId as string);
            const sites = await cacheService.getOrSet(
                cacheKey,
                async () => {
                    const rawSites = await storage.getStorageSites(portId as string);
                    return Promise.all(
                        rawSites.map(async (site) => {
                            const fillData = await storage.getLatestStorageFill(site.id);
                            return { ...site, fillData };
                        })
                    );
                },
                CACHE_TTL.MEDIUM
            );

            if (usePagination) {
                res.json(paginateArray(sites, pagination));
            } else {
                res.json(sites);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch storage sites" });
        }
    }
);

// Floating storage endpoint (public access)
vesselsRouter.get(
    "/api/storage/floating",
    publicDataRateLimiter,
    optionalAuth,
    async (req, res) => {
        try {
            const { getFloatingStorageStats } = await import(
                "../services/storageDataService"
            );
            const data = await getFloatingStorageStats();
            res.json(data);
        } catch (error) {
            logger.error("Error fetching floating storage", { error });
            res.status(500).json({ error: "Failed to fetch floating storage data" });
        }
    }
);

// SPR reserves endpoint (public access)
vesselsRouter.get(
    "/api/storage/spr",
    publicDataRateLimiter,
    optionalAuth,
    async (req, res) => {
        try {
            const { getSprStats } = await import("../services/storageDataService");
            const data = await getSprStats();
            res.json(data);
        } catch (error) {
            logger.error("Error fetching SPR data", { error });
            res.status(500).json({ error: "Failed to fetch SPR reserves data" });
        }
    }
);

// Storage time series endpoint (public access)
vesselsRouter.get(
    "/api/storage/timeseries",
    publicDataRateLimiter,
    optionalAuth,
    async (req, res) => {
        try {
            const { metricType, region, storageType, weeks } = req.query;
            const { getStorageTimeSeriesData } = await import(
                "../services/storageDataService"
            );
            const data = await getStorageTimeSeriesData({
                metricType: metricType as string,
                region: region as string,
                storageType: storageType as string,
                weeks: parseSafeLimit(weeks, 52, 260),
            });
            res.json(data);
        } catch (error) {
            logger.error("Error fetching storage time series", { error });
            res
                .status(500)
                .json({ error: "Failed to fetch storage time series data" });
        }
    }
);

// Market delay impact endpoint
vesselsRouter.get(
    "/api/market/delays/impact",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { portId, commodityId, limit } = req.query;
            const impacts = await storage.getMarketDelayImpacts(
                portId as string | undefined,
                commodityId as string | undefined,
                parseSafeLimit(limit, 20, 200)
            );
            res.json(impacts);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch market delay impacts" });
        }
    }
);

// Delay-adjusted predictions endpoint
vesselsRouter.get(
    "/api/predictions/delay-adjusted",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { portId, commodityCode } = req.query;

            const impacts = await storage.getMarketDelayImpacts(
                portId as string | undefined,
                undefined,
                1
            );
            const latestImpact = impacts[0];

            const predictions = await storage.getLatestPredictions(
                commodityCode as string
            );
            const basePrediction = predictions[0];

            if (!basePrediction || !latestImpact) {
                return res.json({
                    delayAdjusted: false,
                    prediction: basePrediction,
                    message: "No delay impact data available",
                });
            }

            const priceImpact = parseFloat(latestImpact.priceImpact || "0");
            const basePrice = parseFloat(basePrediction.predictedPrice);
            const adjustedPrice = basePrice + priceImpact;

            res.json({
                delayAdjusted: true,
                basePrediction: basePrediction,
                delayImpact: latestImpact,
                adjustedPrediction: {
                    ...basePrediction,
                    predictedPrice: adjustedPrice.toFixed(2),
                    adjustmentReason: `Adjusted for ${latestImpact.vesselCount} delayed vessels carrying ${latestImpact.totalDelayedVolume} tons`,
                },
            });
        } catch (error) {
            res
                .status(500)
                .json({ error: "Failed to fetch delay-adjusted predictions" });
        }
    }
);

// Trade flows endpoint
vesselsRouter.get(
    "/api/trade-flows",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { limit } = req.query;
            const flows = await storage.getActiveTradeFlows();

            const enrichedFlows = await Promise.all(
                flows.slice(0, parseSafeLimit(limit, 50, 500)).map(async (flow) => {
                    const [legs, stsEvents, splits] = await Promise.all([
                        storage.getCargoLegsByTradeFlow(flow.id),
                        storage.getSTSEventsByTradeFlow(flow.id),
                        storage.getCargoSplitsByTradeFlow(flow.id),
                    ]);

                    return { ...flow, cargoChain: legs, stsEvents, splits };
                })
            );

            res.json(enrichedFlows);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch trade flows" });
        }
    }
);

// Single trade flow with complete cargo chain
vesselsRouter.get(
    "/api/trade-flows/:flowId",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { flowId } = req.params;
            const flows = await storage.getActiveTradeFlows();
            const flow = flows.find((f) => f.id === flowId);

            if (!flow) {
                return res.status(404).json({ error: "Trade flow not found" });
            }

            const [legs, stsEvents, splits, vessel, commodity] = await Promise.all([
                storage.getCargoLegsByTradeFlow(flowId),
                storage.getSTSEventsByTradeFlow(flowId),
                storage.getCargoSplitsByTradeFlow(flowId),
                storage.getVesselByMMSI(flow.vesselId),
                storage
                    .getCommodities()
                    .then((c) => c.find((com) => com.id === flow.commodityId)),
            ]);

            res.json({
                ...flow,
                vessel,
                commodity,
                cargoChain: legs,
                stsEvents,
                splits,
            });
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch trade flow details" });
        }
    }
);

// STS Events endpoints
vesselsRouter.get(
    "/api/sts-events",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { vesselId, limit } = req.query;

            if (vesselId) {
                const events = await storage.getSTSEventsByVessel(vesselId as string);
                res.json(events);
            } else {
                const events = await storage.getSTSEvents(parseSafeLimit(limit, 50, 500));
                res.json(events);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch STS events" });
        }
    }
);

// Flow Forecasts endpoints
vesselsRouter.get(
    "/api/flow-forecasts",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { originPortId, destinationPortId, limit } = req.query;

            if (originPortId && destinationPortId) {
                const forecasts = await storage.getFlowForecastsByRoute(
                    originPortId as string,
                    destinationPortId as string
                );
                res.json(forecasts);
            } else {
                const forecasts = await storage.getActiveFlowForecasts();
                res.json(forecasts.slice(0, parseSafeLimit(limit, 20, 200)));
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch flow forecasts" });
        }
    }
);

// Cargo splits endpoint
vesselsRouter.get(
    "/api/cargo-splits",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { tradeFlowId, limit } = req.query;

            if (tradeFlowId) {
                const splits = await storage.getCargoSplitsByTradeFlow(
                    tradeFlowId as string
                );
                res.json(splits);
            } else {
                const splits = await storage.getCargoSplits(
                    parseSafeLimit(limit, 50, 500)
                );
                res.json(splits);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch cargo splits" });
        }
    }
);
