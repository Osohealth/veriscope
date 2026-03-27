import { Router } from "express";
import { storage } from "../storage";
import { insertCrudeGradeSchema, insertLngCargoSchema } from "@shared/schema";
import { optionalAuth, authenticate, requirePermission, requireRole } from "../middleware/rbac";
import { parseSafeLimit, parsePaginationParams, paginateArray } from "../utils/pagination";
import { cacheService, CACHE_KEYS, CACHE_TTL } from "../services/cacheService";
import { logger } from "../middleware/observability";

export const commoditiesRouter = Router();

// ── Helpers (kept local — only used in export routes) ────────────────────────

/** Validates an ISO 8601 date string (YYYY-MM-DD). Returns the value if valid, null otherwise. */
function validateIsoDate(value: unknown): string | null {
    if (typeof value !== "string") return null;
    return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;
}

function requireIsoDate(value: unknown, name: string): { ok: true; value: string } | { ok: false; error: string } {
    const v = validateIsoDate(value);
    if (!v) return { ok: false, error: `${name} must be a valid ISO 8601 date (YYYY-MM-DD)` };
    return { ok: true, value: v };
}

function sanitizeCsvCell(raw: string): string {
    // Prevent CSV formula injection (OWASP: Excel/Sheets DDE attacks)
    if (/^[=+\-@\t\r|]/.test(raw)) {
        return `'${raw}`;
    }
    return raw;
}

function generateCSV(data: any[], columns: string[]): string {
    if (!data || data.length === 0) {
        return columns.join(",") + "\n";
    }
    const header = columns.join(",");
    const rows = data.map((row) =>
        columns
            .map((col) => {
                const value = row[col];
                if (value === null || value === undefined) return "";
                let str: string;
                if (typeof value === "object") {
                    str = JSON.stringify(value);
                } else {
                    str = String(value);
                }
                str = sanitizeCsvCell(str);
                if (str.includes(",") || str.includes('"') || str.includes("\n")) {
                    return `"${str.replace(/"/g, '""')}"`;
                }
                return str;
            })
            .join(",")
    );
    return [header, ...rows].join("\n");
}

// ── Predictions ──────────────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/predictions",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { target } = req.query;
            const usePagination = req.query.paginate === "true";
            const pagination = parsePaginationParams(req, { limit: 50 });

            const predictions = await cacheService.getOrSet(
                CACHE_KEYS.LATEST_PREDICTIONS(target as string),
                () => storage.getLatestPredictions(target as string),
                CACHE_TTL.MEDIUM
            );

            if (usePagination) {
                res.json(paginateArray(predictions, pagination));
            } else {
                res.json(predictions);
            }
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch predictions" });
        }
    }
);

// ── Crude & Products Pack ────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/crude-grades",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { category, limit } = req.query;
            const grades = await storage.getCrudeGrades(
                category as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(grades);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch crude grades" });
        }
    }
);

commoditiesRouter.post(
    "/api/crude-grades",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const parsed = insertCrudeGradeSchema.safeParse(req.body);
            if (!parsed.success) {
                return res.status(400).json({ error: parsed.error.errors[0].message });
            }
            const grade = await storage.createCrudeGrade(parsed.data);
            res.status(201).json(grade);
        } catch (error) {
            res.status(500).json({ error: "Failed to create crude grade" });
        }
    }
);

// ── LNG/LPG Pack ─────────────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/lng-cargoes",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { cargoType, portId, limit } = req.query;
            const cargoes = await storage.getLngCargoes(
                cargoType as string | undefined,
                portId as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(cargoes);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch LNG cargoes" });
        }
    }
);

commoditiesRouter.get(
    "/api/lng-cargoes/diversions",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { limit } = req.query;
            const cargoes = await storage.getDiversionCargoes(
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(cargoes);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch diversion cargoes" });
        }
    }
);

commoditiesRouter.post(
    "/api/lng-cargoes",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const parsed = insertLngCargoSchema.safeParse(req.body);
            if (!parsed.success) {
                return res.status(400).json({ error: parsed.error.errors[0].message });
            }
            const cargo = await storage.createLngCargo(parsed.data);
            res.status(201).json(cargo);
        } catch (error) {
            res.status(500).json({ error: "Failed to create LNG cargo" });
        }
    }
);

// ── Dry Bulk Pack ─────────────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/dry-bulk-fixtures",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { commodityType, vesselSize, limit } = req.query;
            const fixtures = await storage.getDryBulkFixtures(
                commodityType as string | undefined,
                vesselSize as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(fixtures);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch dry bulk fixtures" });
        }
    }
);

commoditiesRouter.post(
    "/api/dry-bulk-fixtures",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const fixture = await storage.createDryBulkFixture(req.body);
            res.status(201).json(fixture);
        } catch (error) {
            res.status(500).json({ error: "Failed to create dry bulk fixture" });
        }
    }
);

// ── Petrochem Pack ────────────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/petrochem-products",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { category, region, limit } = req.query;
            const products = await storage.getPetrochemProducts(
                category as string | undefined,
                region as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(products);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch petrochem products" });
        }
    }
);

commoditiesRouter.post(
    "/api/petrochem-products",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const product = await storage.createPetrochemProduct(req.body);
            res.status(201).json(product);
        } catch (error) {
            res.status(500).json({ error: "Failed to create petrochem product" });
        }
    }
);

// ── Agri & Biofuel Pack ───────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/agri-biofuel-flows",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { commodityType, flowType, limit } = req.query;
            const flows = await storage.getAgriBiofuelFlows(
                commodityType as string | undefined,
                flowType as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(flows);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch agri/biofuel flows" });
        }
    }
);

commoditiesRouter.get(
    "/api/agri-biofuel-flows/sustainable",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { limit } = req.query;
            const flows = await storage.getSustainableBiofuelFlows(
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(flows);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch sustainable biofuel flows" });
        }
    }
);

commoditiesRouter.post(
    "/api/agri-biofuel-flows",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const flow = await storage.createAgriBiofuelFlow(req.body);
            res.status(201).json(flow);
        } catch (error) {
            res.status(500).json({ error: "Failed to create agri/biofuel flow" });
        }
    }
);

// ── Refinery / Plant Intelligence ─────────────────────────────────────────────

commoditiesRouter.get(
    "/api/refineries",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { region, maintenanceStatus, limit } = req.query;
            const refineries = await storage.getRefineries(
                region as string | undefined,
                maintenanceStatus as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(refineries);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch refineries" });
        }
    }
);

commoditiesRouter.get(
    "/api/refineries/:refineryCode",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const refinery = await storage.getRefineryByCode(req.params.refineryCode);
            if (!refinery)
                return res.status(404).json({ error: "Refinery not found" });
            res.json(refinery);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch refinery" });
        }
    }
);

commoditiesRouter.post(
    "/api/refineries",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const refinery = await storage.createRefinery(req.body);
            res.status(201).json(refinery);
        } catch (error) {
            res.status(500).json({ error: "Failed to create refinery" });
        }
    }
);

// ── Supply & Demand Balances ──────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/supply-demand-balances",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { commodity, region, period, limit } = req.query;
            const balances = await storage.getSupplyDemandBalances(
                commodity as string | undefined,
                region as string | undefined,
                period as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(balances);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch supply & demand balances" });
        }
    }
);

commoditiesRouter.get(
    "/api/supply-demand-balances/latest",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { commodity, region, limit } = req.query;
            const balances = await storage.getLatestBalances(
                commodity as string | undefined,
                region as string | undefined,
                parseSafeLimit(limit, 10, 200)
            );
            res.json(balances);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch latest balances" });
        }
    }
);

commoditiesRouter.post(
    "/api/supply-demand-balances",
    authenticate,
    requirePermission("write:storage"),
    async (req, res) => {
        try {
            const balance = await storage.createSupplyDemandBalance(req.body);
            res.status(201).json(balance);
        } catch (error) {
            res.status(500).json({ error: "Failed to create supply & demand balance" });
        }
    }
);

// ── Research & Insight Layer ──────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/research-reports",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { category, subcategory, limit } = req.query;
            const reports = await storage.getResearchReports(
                category as string | undefined,
                subcategory as string | undefined,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(reports);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch research reports" });
        }
    }
);

commoditiesRouter.get(
    "/api/research-reports/published",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { limit } = req.query;
            const reports = await storage.getPublishedReports(
                parseSafeLimit(limit, 10, 100)
            );
            res.json(reports);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch published reports" });
        }
    }
);

commoditiesRouter.get(
    "/api/research-reports/:reportId",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const report = await storage.getReportById(req.params.reportId);
            if (!report) return res.status(404).json({ error: "Report not found" });
            res.json(report);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch report" });
        }
    }
);

commoditiesRouter.post(
    "/api/research-reports",
    authenticate,
    requirePermission("write:signals"),
    async (req, res) => {
        try {
            const report = await storage.createResearchReport(req.body);
            res.status(201).json(report);
        } catch (error) {
            res.status(500).json({ error: "Failed to create research report" });
        }
    }
);

// ── CSV-based Data Endpoints ──────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/refinery/units",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { plant } = req.query;
            const units = await storage.getRefineryUnits(plant as string);
            res.json(units);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch refinery units" });
        }
    }
);

commoditiesRouter.get(
    "/api/refinery/utilization",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { startDate, endDate, plant } = req.query;
            const sd = requireIsoDate(startDate, "startDate");
            const ed = requireIsoDate(endDate, "endDate");
            if (!sd.ok) return res.status(400).json({ error: sd.error });
            if (!ed.ok) return res.status(400).json({ error: ed.error });
            const utilization = await storage.getRefineryUtilization(
                sd.value,
                ed.value,
                plant as string
            );
            res.json(utilization);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch refinery utilization" });
        }
    }
);

commoditiesRouter.get(
    "/api/refinery/crack-spreads",
    authenticate,
    requirePermission("read:storage"),
    async (req, res) => {
        try {
            const { startDate, endDate } = req.query;
            const sd = requireIsoDate(startDate, "startDate");
            const ed = requireIsoDate(endDate, "endDate");
            if (!sd.ok) return res.status(400).json({ error: sd.error });
            if (!ed.ok) return res.status(400).json({ error: ed.error });
            const spreads = await storage.getRefineryCrackSpreads(
                sd.value,
                ed.value
            );
            res.json(spreads);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch crack spreads" });
        }
    }
);

commoditiesRouter.get(
    "/api/supply-demand/models-daily",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { startDate, endDate, region } = req.query;
            const sd = requireIsoDate(startDate, "startDate");
            const ed = requireIsoDate(endDate, "endDate");
            if (!sd.ok) return res.status(400).json({ error: sd.error });
            if (!ed.ok) return res.status(400).json({ error: ed.error });
            const models = await storage.getSdModelsDaily(
                sd.value,
                ed.value,
                region as string
            );
            res.json(models);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch S&D models" });
        }
    }
);

commoditiesRouter.get(
    "/api/supply-demand/forecasts-weekly",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { startDate, endDate, region } = req.query;
            const sd = requireIsoDate(startDate, "startDate");
            const ed = requireIsoDate(endDate, "endDate");
            if (!sd.ok) return res.status(400).json({ error: sd.error });
            if (!ed.ok) return res.status(400).json({ error: ed.error });
            const forecasts = await storage.getSdForecastsWeekly(
                sd.value,
                ed.value,
                region as string
            );
            res.json(forecasts);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch S&D forecasts" });
        }
    }
);

commoditiesRouter.get(
    "/api/research-insights/daily",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { startDate, endDate, limit } = req.query;
            const sd = requireIsoDate(startDate, "startDate");
            const ed = requireIsoDate(endDate, "endDate");
            if (!sd.ok) return res.status(400).json({ error: sd.error });
            if (!ed.ok) return res.status(400).json({ error: ed.error });
            const insights = await storage.getResearchInsightsDaily(
                sd.value,
                ed.value,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(insights);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch research insights" });
        }
    }
);

// ── ML Price Predictions ───────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/ml-predictions",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { commodityType, limit } = req.query;
            const predictions = await storage.getMlPredictions(
                commodityType as string,
                parseSafeLimit(limit, 10, 200)
            );
            res.json(predictions);
        } catch (error) {
            logger.error("Error fetching ML predictions", { error });
            res.status(500).json({ error: "Failed to fetch ML predictions" });
        }
    }
);

commoditiesRouter.get(
    "/api/ml-predictions/latest/:commodityType",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { commodityType } = req.params;
            const prediction = await storage.getLatestMlPrediction(commodityType);
            if (!prediction)
                return res
                    .status(404)
                    .json({ error: "No prediction found for this commodity" });
            res.json(prediction);
        } catch (error) {
            logger.error("Error fetching latest prediction", { error });
            res.status(500).json({ error: "Failed to fetch latest prediction" });
        }
    }
);

commoditiesRouter.post(
    "/api/ml-predictions/generate",
    authenticate,
    requirePermission("write:predictions"),
    async (req, res) => {
        try {
            const { commodityType, currentPrice } = req.body;
            if (!commodityType)
                return res.status(400).json({ error: "commodityType is required" });

            const { mlPredictionService } = await import(
                "../services/mlPredictionService"
            );
            const prediction = await mlPredictionService.generatePrediction(
                commodityType,
                currentPrice || 80
            );
            if (!prediction)
                return res.status(500).json({ error: "Failed to generate prediction" });
            res.json(prediction);
        } catch (error) {
            logger.error("Error generating prediction", { error });
            res.status(500).json({ error: "Failed to generate prediction" });
        }
    }
);

// ── Data Quality Endpoints ─────────────────────────────────────────────────────

commoditiesRouter.get(
    "/api/data-quality/scores",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { entityId, limit } = req.query;
            const { dataQualityService } = await import(
                "../services/dataQualityService"
            );
            const scores = await dataQualityService.getLatestQualityScores(
                entityId as string | undefined,
                parseSafeLimit(limit, 10, 200)
            );
            res.json(scores);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch data quality scores" });
        }
    }
);

commoditiesRouter.get(
    "/api/data-quality/streams",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { dataQualityService } = await import(
                "../services/dataQualityService"
            );
            const streams = await dataQualityService.getAllStreamHealth();
            res.json(streams);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch stream health" });
        }
    }
);

commoditiesRouter.get(
    "/api/data-quality/streams/:streamName",
    authenticate,
    requirePermission("read:signals"),
    async (req, res) => {
        try {
            const { streamName } = req.params;
            const { dataQualityService } = await import(
                "../services/dataQualityService"
            );
            const health = await dataQualityService.getStreamHealth(streamName);
            res.json(health);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch stream health" });
        }
    }
);

// ── Model Registry & ML Credibility ───────────────────────────────────────────

commoditiesRouter.get(
    "/api/models",
    authenticate,
    requirePermission("read:models"),
    async (req, res) => {
        try {
            const { status } = req.query;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const models = await modelRegistryService.listModels(
                status as string | undefined
            );
            res.json(models);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch models" });
        }
    }
);

commoditiesRouter.get(
    "/api/models/:modelId",
    authenticate,
    requirePermission("read:models"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const model = await modelRegistryService.getModel(modelId);
            if (!model) return res.status(404).json({ error: "Model not found" });
            res.json(model);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch model" });
        }
    }
);

commoditiesRouter.post(
    "/api/models",
    authenticate,
    requirePermission("write:models"),
    async (req, res) => {
        try {
            const {
                modelName,
                version,
                modelType,
                features,
                hyperparameters,
                trainingMetrics,
                validationMetrics,
                status,
            } = req.body;

            if (!modelName || typeof modelName !== "string" || modelName.trim() === "")
                return res
                    .status(400)
                    .json({ error: "modelName is required and must be a non-empty string" });
            if (!version || typeof version !== "string" || version.trim() === "")
                return res
                    .status(400)
                    .json({ error: "version is required and must be a non-empty string" });
            if (status && !["active", "deprecated", "archived"].includes(status))
                return res
                    .status(400)
                    .json({ error: "status must be one of: active, deprecated, archived" });

            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const model = await modelRegistryService.createModel({
                modelName: modelName.trim(),
                version: version.trim(),
                modelType,
                features,
                hyperparameters,
                trainingMetrics,
                validationMetrics,
                status,
            });
            if (!model)
                return res.status(400).json({ error: "Failed to create model" });
            res.status(201).json(model);
        } catch (error) {
            res.status(500).json({ error: "Failed to create model" });
        }
    }
);

commoditiesRouter.post(
    "/api/models/:modelId/activate",
    authenticate,
    requirePermission("write:models"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const model = await modelRegistryService.activateModel(modelId);
            if (!model) return res.status(404).json({ error: "Model not found" });
            res.json(model);
        } catch (error) {
            res.status(500).json({ error: "Failed to activate model" });
        }
    }
);

commoditiesRouter.post(
    "/api/models/:modelId/deprecate",
    authenticate,
    requirePermission("write:models"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const model = await modelRegistryService.deprecateModel(modelId);
            if (!model) return res.status(404).json({ error: "Model not found" });
            res.json(model);
        } catch (error) {
            res.status(500).json({ error: "Failed to deprecate model" });
        }
    }
);

commoditiesRouter.get(
    "/api/models/:modelId/predictions",
    authenticate,
    requirePermission("read:predictions"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const { limit } = req.query;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const predictions = await modelRegistryService.getPredictions(
                modelId,
                parseSafeLimit(limit, 100, 1000)
            );
            res.json(predictions);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch predictions" });
        }
    }
);

commoditiesRouter.post(
    "/api/models/:modelId/predictions",
    authenticate,
    requirePermission("write:predictions"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const {
                target,
                predictionDate,
                predictedValue,
                confidenceLevel,
                horizon,
                featuresUsed,
            } = req.body;

            if (!target || typeof target !== "string" || target.trim() === "")
                return res.status(400).json({ error: "target is required and must be a non-empty string" });
            if (!predictionDate)
                return res.status(400).json({ error: "predictionDate is required" });
            const parsedDate = new Date(predictionDate);
            if (isNaN(parsedDate.getTime()))
                return res.status(400).json({ error: "predictionDate must be a valid date" });
            if (
                predictedValue === undefined ||
                typeof predictedValue !== "number" ||
                isNaN(predictedValue)
            )
                return res.status(400).json({ error: "predictedValue is required and must be a number" });
            if (
                confidenceLevel !== undefined &&
                (typeof confidenceLevel !== "number" ||
                    confidenceLevel <= 0 ||
                    confidenceLevel >= 1)
            )
                return res.status(400).json({ error: "confidenceLevel must be a number between 0 and 1" });

            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const prediction =
                await modelRegistryService.generatePredictionWithConfidence(
                    modelId,
                    target.trim(),
                    parsedDate,
                    predictedValue,
                    confidenceLevel || 0.95,
                    horizon,
                    featuresUsed
                );
            if (!prediction)
                return res
                    .status(400)
                    .json({ error: "Failed to create prediction. Model may not exist." });
            res.status(201).json(prediction);
        } catch (error) {
            res.status(500).json({ error: "Failed to create prediction" });
        }
    }
);

commoditiesRouter.post(
    "/api/predictions/:predictionId/actual",
    authenticate,
    requirePermission("write:predictions"),
    async (req, res) => {
        try {
            const { predictionId } = req.params;
            const { actualValue } = req.body;

            if (
                actualValue === undefined ||
                typeof actualValue !== "number" ||
                isNaN(actualValue)
            )
                return res.status(400).json({ error: "actualValue is required and must be a number" });

            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const prediction = await modelRegistryService.recordActualValue(
                predictionId,
                actualValue
            );
            if (!prediction)
                return res.status(404).json({ error: "Prediction not found" });
            res.json(prediction);
        } catch (error) {
            res.status(500).json({ error: "Failed to record actual value" });
        }
    }
);

commoditiesRouter.get(
    "/api/models/:modelId/backtest",
    authenticate,
    requirePermission("read:models"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const { startDate, endDate } = req.query;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const results = await modelRegistryService.getBacktestResults(modelId);
            if (!results) return res.status(404).json({ error: "Model not found" });
            res.json(results);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch backtest results" });
        }
    }
);

commoditiesRouter.get(
    "/api/models/:modelId/drift",
    authenticate,
    requirePermission("read:models"),
    async (req, res) => {
        try {
            const { modelId } = req.params;
            const { modelRegistryService } = await import(
                "../services/modelRegistryService"
            );
            const metrics = await modelRegistryService.getDriftMetrics(modelId);
            if (!metrics) return res.status(404).json({ error: "Model not found" });
            res.json(metrics);
        } catch (error) {
            res.status(500).json({ error: "Failed to fetch drift metrics" });
        }
    }
);

// ── Refinery Satellite Monitoring ─────────────────────────────────────────────

commoditiesRouter.get("/api/refinery/aois", optionalAuth, async (req, res) => {
    try {
        const { getAois } = await import("../services/refinerySatelliteService");
        const aois = await getAois();
        res.json(aois);
    } catch (error) {
        logger.error("Error fetching AOIs", { error });
        res.status(500).json({ error: "Failed to fetch AOIs" });
    }
});

commoditiesRouter.get(
    "/api/refinery/aois/:code",
    optionalAuth,
    async (req, res) => {
        try {
            const { getAoiByCode } = await import(
                "../services/refinerySatelliteService"
            );
            const aoi = await getAoiByCode(req.params.code);
            if (!aoi) return res.status(404).json({ error: "AOI not found" });
            res.json(aoi);
        } catch (error) {
            logger.error("Error fetching AOI", { error });
            res.status(500).json({ error: "Failed to fetch AOI" });
        }
    }
);

commoditiesRouter.get(
    "/api/refinery/activity/latest",
    optionalAuth,
    async (req, res) => {
        try {
            const { getLatestActivityIndex } = await import(
                "../services/refinerySatelliteService"
            );
            const aoiCode = (req.query.aoi as string) || "rotterdam_full";
            const latest = await getLatestActivityIndex(aoiCode);
            if (!latest)
                return res.status(404).json({ error: "No activity data found" });
            res.json(latest);
        } catch (error) {
            logger.error("Error fetching latest activity", { error });
            res.status(500).json({ error: "Failed to fetch latest activity" });
        }
    }
);

commoditiesRouter.get(
    "/api/refinery/activity/timeline",
    optionalAuth,
    async (req, res) => {
        try {
            const { getActivityTimeline } = await import(
                "../services/refinerySatelliteService"
            );
            const aoiCode = (req.query.aoi as string) || "rotterdam_full";
            const weeks = parseSafeLimit(req.query.weeks, 12, 52);
            const timeline = await getActivityTimeline(aoiCode, weeks);
            res.json(timeline);
        } catch (error) {
            logger.error("Error fetching activity timeline", { error });
            res.status(500).json({ error: "Failed to fetch activity timeline" });
        }
    }
);

commoditiesRouter.get(
    "/api/refinery/observations",
    optionalAuth,
    async (req, res) => {
        try {
            const { getRecentObservations } = await import(
                "../services/refinerySatelliteService"
            );
            const aoiCode = (req.query.aoi as string) || "rotterdam_full";
            const limit = parseSafeLimit(req.query.limit, 10, 100);
            const observations = await getRecentObservations(aoiCode, limit);
            res.json(observations);
        } catch (error) {
            logger.error("Error fetching observations", { error });
            res.status(500).json({ error: "Failed to fetch observations" });
        }
    }
);

commoditiesRouter.get(
    "/api/refinery/summary",
    optionalAuth,
    async (req, res) => {
        try {
            const { getSummaryStats } = await import(
                "../services/refinerySatelliteService"
            );
            const summary = await getSummaryStats();
            res.json(summary);
        } catch (error) {
            logger.error("Error fetching summary", { error });
            res.status(500).json({ error: "Failed to fetch summary" });
        }
    }
);

commoditiesRouter.post(
    "/api/refinery/refresh",
    authenticate,
    async (req, res) => {
        try {
            const { refreshSatelliteData } = await import(
                "../services/refinerySatelliteService"
            );
            const result = await refreshSatelliteData();
            res.json(result);
        } catch (error) {
            logger.error("Error refreshing satellite data", { error });
            res.status(500).json({ error: "Failed to refresh satellite data" });
        }
    }
);

// ── CSV Export ───────────────────────────────────────────────────────────────

commoditiesRouter.get("/api/export/vessels", optionalAuth, async (req, res) => {
    try {
        const vessels = await storage.getVessels();
        const csv = generateCSV(vessels, [
            "id",
            "mmsi",
            "name",
            "imo",
            "vesselType",
            "flag",
            "owner",
            "operator",
            "buildYear",
            "deadweight",
            "length",
            "beam",
            "draft",
            "capacity",
        ]);
        res.setHeader("Content-Type", "text/csv");
        res.setHeader("Content-Disposition", "attachment; filename=vessels.csv");
        res.send(csv);
    } catch (error) {
        res.status(500).json({ error: "Failed to export vessels" });
    }
});

commoditiesRouter.get("/api/export/ports", optionalAuth, async (req, res) => {
    try {
        const ports = await storage.getPorts();
        const csv = generateCSV(ports, [
            "id",
            "name",
            "code",
            "country",
            "region",
            "latitude",
            "longitude",
            "type",
            "capacity",
            "depth",
            "operationalStatus",
        ]);
        res.setHeader("Content-Type", "text/csv");
        res.setHeader("Content-Disposition", "attachment; filename=ports.csv");
        res.send(csv);
    } catch (error) {
        res.status(500).json({ error: "Failed to export ports" });
    }
});

commoditiesRouter.get(
    "/api/export/signals",
    optionalAuth,
    async (req, res) => {
        try {
            const sigs = await storage.getActiveSignals(500);
            const csv = generateCSV(sigs, [
                "id",
                "type",
                "title",
                "description",
                "frequency",
                "isActive",
                "lastTriggered",
                "createdAt",
            ]);
            res.setHeader("Content-Type", "text/csv");
            res.setHeader("Content-Disposition", "attachment; filename=signals.csv");
            res.send(csv);
        } catch (error) {
            res.status(500).json({ error: "Failed to export signals" });
        }
    }
);

commoditiesRouter.get(
    "/api/export/predictions",
    optionalAuth,
    async (req, res) => {
        try {
            const predictions = await storage.getPredictions();
            const csv = generateCSV(predictions, [
                "id",
                "commodityId",
                "marketId",
                "timeframe",
                "currentPrice",
                "predictedPrice",
                "confidence",
                "direction",
                "validUntil",
                "createdAt",
            ]);
            res.setHeader("Content-Type", "text/csv");
            res.setHeader(
                "Content-Disposition",
                "attachment; filename=predictions.csv"
            );
            res.send(csv);
        } catch (error) {
            res.status(500).json({ error: "Failed to export predictions" });
        }
    }
);
