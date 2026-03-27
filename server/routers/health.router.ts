import { Router } from "express";
import { sql, eq } from "drizzle-orm";
import { db } from "../db";
import { alertRuns } from "@shared/schema";
import { TENANT_DEMO_ID } from "../config/tenancy";
import { metricsCollector, getHealthStatus, setDbHealth, logger } from "../middleware/observability";
import { storage } from "../storage";
import { WEBHOOK_TIMEOUT_MS } from "../config/alerting";
import { authenticateApiKey } from "../middleware/apiKeyAuth";

export const healthRouter = Router();

healthRouter.get("/health", (req, res) => {
    const status = getHealthStatus();
    res
        .status(
            status.status === "healthy" ? 200 : status.status === "degraded" ? 200 : 503
        )
        .json(status);
});

healthRouter.get("/health/alerts", async (req, res) => {
    try {
        await db.execute(sql`SELECT 1`);

        const [runRow] = await db
            .insert(alertRuns)
            .values({
                tenantId: TENANT_DEMO_ID,
                status: "SUCCESS",
                startedAt: new Date(),
                finishedAt: new Date(),
            })
            .returning();
        if (runRow?.id) {
            await db.delete(alertRuns).where(eq(alertRuns.id, runRow.id));
        }

        await db.execute(sql`SELECT 1 FROM alert_deliveries LIMIT 1`);
        await db.execute(sql`SELECT 1 FROM alert_dlq LIMIT 1`);

        res.json({ status: "ok" });
    } catch (error: any) {
        logger.error("Alert health check failed", { error });
        res
            .status(500)
            .json({ status: "error", error: "Alert health check failed" });
    }
});

healthRouter.get("/health/webhooks", (req, res) => {
    try {
        if (typeof fetch !== "function") {
            return res.status(500).json({ status: "error", error: "fetch is unavailable" });
        }
        res.json({ status: "ok", timeout_ms: WEBHOOK_TIMEOUT_MS });
    } catch (error: any) {
        logger.error("Webhook health check failed", { error });
        res
            .status(500)
            .json({ status: "error", error: "Webhook health check failed" });
    }
});

healthRouter.get("/ready", async (req, res) => {
    try {
        await storage.getPorts();
        setDbHealth(true);
        res.json({ status: "ready", timestamp: new Date().toISOString() });
    } catch {
        setDbHealth(false);
        res.status(503).json({ status: "not_ready", timestamp: new Date().toISOString() });
    }
});

healthRouter.get("/live", (req, res) => {
    res.json({ status: "alive", timestamp: new Date().toISOString() });
});

healthRouter.get("/api/status/data", async (req, res) => {
    try {
        const [vessels, ports] = await Promise.all([storage.getVessels(), storage.getPorts()]);
        res.json({
            status: "ok",
            timestamp: new Date().toISOString(),
            counts: { vessels: vessels.length, ports: ports.length },
            environment: process.env.NODE_ENV || "development",
        });
    } catch (error: any) {
        logger.error("Data status check failed", { error });
        res.status(500).json({ status: "error", error: "Internal server error", timestamp: new Date().toISOString() });
    }
});

healthRouter.get("/metrics", authenticateApiKey, (req, res) => {
    res.json(metricsCollector.getMetrics());
});

