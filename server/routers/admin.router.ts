import { Router } from "express";
import type { WebSocketServer } from "ws";
import { aisService } from "../services/aisService";
import { signalsService } from "../services/signalsService";
import { predictionService } from "../services/predictionService";
import { delayService } from "../services/delayService";
import { mockDataService } from "../services/mockDataService";
import { portCallService } from "../services/portCallService";
import {
    getPortDailyBaselines,
    startPortDailyBaselineScheduler,
} from "../services/portDailyBaselineService";
import { logger } from "../middleware/observability";
import {
    evaluatePortSignalsForDay,
    formatSignalDay,
    getYesterdayUtcDay,
    parseSignalDay,
} from "../services/signalEngineService";
import { getAuthenticatedUser, createRepository, listRepositories } from "../services/githubService";
import { authenticate, optionalAuth, requireRole, requireAdmin } from "../middleware/rbac";
import { cacheService } from "../services/cacheService";
import { wsManager } from "../services/wsManagerService";
import { parseSafeLimit } from "../utils/pagination";

type InitResult = { portCount: number; startedAt: string; completedAt: string };

let initInProgress: Promise<InitResult> | null = null;
let initCompleted: InitResult | null = null;

export function createAdminRouter(wss: WebSocketServer): Router {
    const router = Router();

    // Port daily baselines (internal debugging, admin only)
    router.get("/api/baselines/ports/:portId", authenticate, requireAdmin, async (req, res) => {
        try {
            const { portId } = req.params;
            const days = parseSafeLimit(req.query.days, 30, 365);
            const items = await getPortDailyBaselines(portId, days);
            res.json({ items });
        } catch (error: any) {
            logger.error("Failed to fetch baselines", { error });
            res.status(500).json({ error: "Failed to fetch baselines" });
        }
    });

    // Signal engine manual run (admin only)
    router.post("/api/signals/run", authenticate, requireAdmin, async (req, res) => {
        try {
            const dayParam = req.query.day as string | undefined;
            const parsedDay = dayParam ? parseSignalDay(dayParam) : null;
            if (dayParam && !parsedDay) {
                return res.status(400).json({ error: "day must be YYYY-MM-DD" });
            }
            const targetDay = parsedDay ?? getYesterdayUtcDay();
            const result = await evaluatePortSignalsForDay(targetDay);
            res.json({ day: formatSignalDay(targetDay), count: result.upserted });
        } catch (error: any) {
            logger.error("Failed to run signal engine", { error });
            res.status(500).json({ error: "Failed to run signal engine" });
        }
    });

    // GitHub integration
    router.get("/api/github/user", authenticate, requireAdmin, async (req, res) => {
        try {
            const user = await getAuthenticatedUser();
            res.json({
                login: user.login,
                name: user.name,
                avatar_url: user.avatar_url,
                html_url: user.html_url,
            });
        } catch (error: any) {
            logger.error("GitHub user fetch failed", { error });
            res.status(500).json({ error: "GitHub API error" });
        }
    });

    router.get("/api/github/repos", authenticate, requireAdmin, async (req, res) => {
        try {
            const repos = await listRepositories();
            res.json(
                repos.map((r) => ({
                    name: r.name,
                    full_name: r.full_name,
                    html_url: r.html_url,
                    private: r.private,
                    updated_at: r.updated_at,
                }))
            );
        } catch (error: any) {
            logger.error("GitHub repos list failed", { error });
            res.status(500).json({ error: "GitHub API error" });
        }
    });

    router.post("/api/github/repos", authenticate, requireAdmin, async (req, res) => {
        try {
            const { name, description, isPrivate } = req.body;
            if (!name) {
                return res.status(400).json({ error: "Repository name is required" });
            }
            const repo = await createRepository(name, description || "", isPrivate || false);
            res.json({
                name: repo.name,
                full_name: repo.full_name,
                html_url: repo.html_url,
                clone_url: repo.clone_url,
                ssh_url: repo.ssh_url,
            });
        } catch (error: any) {
            logger.error("GitHub repo creation failed", { error });
            res.status(500).json({ error: "GitHub API error" });
        }
    });

    // WebSocket stats
    router.get("/api/ws/stats", authenticate, requireRole("admin", "operator"), (req, res) => {
        res.json(wsManager.getStats());
    });

    // AIS stream status
    router.get("/api/ais/status", authenticate, requireRole("admin", "operator"), (req, res) => {
        res.json(aisService.getStatus());
    });

    // Cache stats
    router.get("/api/cache/stats", authenticate, requireRole("admin"), (req, res) => {
        res.json({
            ...cacheService.getStats(),
            hitRate: `${(cacheService.getHitRate() * 100).toFixed(2)}%`,
        });
    });

    // Import CSV data
    router.post(
        "/api/import-csv",
        authenticate,
        requireRole("admin", "operator"),
        async (req, res) => {
            try {
                const { importAllCSVData } = await import("../services/csvImportService");
                logger.info("Starting CSV import");
                await importAllCSVData();
                res.json({ message: "CSV data imported successfully" });
            } catch (error: any) {
                logger.error("CSV import error", { error });
                res.status(500).json({ error: "Failed to import CSV data" });
            }
        }
    );

    // Initialize services and start mock data generation (admin only)
    router.post("/api/init", authenticate, requireAdmin, async (req, res) => {
        try {
            if (initCompleted) {
                return res.json({
                    message: "Veriscope services already initialized",
                    portCount: initCompleted.portCount,
                    initializedAt: initCompleted.completedAt,
                    status: "already_initialized",
                });
            }

            const waitingForInit = !!initInProgress;
            if (!initInProgress) {
                initInProgress = (async () => {
                    const startedAt = new Date().toISOString();
                    logger.info("Initializing Veriscope services");

                    const {
                        seedGlobalPorts,
                        getPortCount,
                        seedPortCalls,
                        getPortCallCount,
                    } = await import("../services/portSeedService");
                    await seedGlobalPorts();
                    const portCount = await getPortCount();
                    logger.info(`Total ports in database: ${portCount}`);

                    await mockDataService.initializeBaseData();

                    await seedPortCalls();
                    const portCallCount = await getPortCallCount();
                    logger.info(`Total port calls in database: ${portCallCount}`);

                    const {
                        initializeRefineryAois,
                        generateMockSatelliteData,
                    } = await import("../services/refinerySatelliteService");
                    await initializeRefineryAois();
                    await generateMockSatelliteData();

                    const { initializeStorageData } = await import("../services/storageDataService");
                    await initializeStorageData();

                    aisService.startSimulation(wss);
                    signalsService.startMonitoring(wss);
                    predictionService.startPredictionService();
                    delayService.start(wss);
                    portCallService.start();
                    startPortDailyBaselineScheduler();

                    const result = {
                        portCount,
                        startedAt,
                        completedAt: new Date().toISOString(),
                    };
                    initCompleted = result;
                    cacheService.clear();
                    return result;
                })();

                initInProgress.finally(() => {
                    initInProgress = null;
                });
            }

            const result = await initInProgress;
            res.json({
                message: "Veriscope services initialized successfully",
                portCount: result.portCount,
                initializedAt: result.completedAt,
                status: waitingForInit ? "initialized_after_wait" : "initialized",
            });
        } catch (error) {
            logger.error("Initialization error", { error });
            res.status(500).json({ error: "Failed to initialize services" });
        }
    });

    return router;
}
