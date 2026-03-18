import type { Express } from "express";
import { createServer, type Server } from "http";
import { WebSocketServer } from "ws";
import swaggerUi from "swagger-ui-express";

import { requestTrackingMiddleware, logger } from "./middleware/observability";
import { wsManager } from "./services/wsManagerService";
import { openApiSpec } from "./openapi";

import { healthRouter } from "./routers/health.router";
import { authRouter } from "./routers/auth.router";
import { devRouter, seedDemoApiKeyIfDev } from "./routers/dev.router";
import { createAdminRouter } from "./routers/admin.router";
import { mockDataService } from "./services/mockDataService";
import { signalsRouter } from "./routers/signals.router";
import { portsRouter } from "./routers/ports.router";
import { alertsRouter } from "./routers/alerts.router";
import { vesselsRouter } from "./routers/vessels.router";
import { commoditiesRouter } from "./routers/commodities.router";

export async function registerRoutes(app: Express): Promise<Server> {
  const httpServer = createServer(app);

  // Apply observability middleware
  app.use(requestTrackingMiddleware);

  // OpenAPI documentation
  app.use("/docs", swaggerUi.serve, swaggerUi.setup(openApiSpec));
  app.get("/openapi.json", (req, res) => {
    res.json(openApiSpec);
  });

  // WebSocket setup
  const wss = new WebSocketServer({ server: httpServer, path: "/ws" });
  wsManager.initialize(wss);

  // Seed dev API key on startup (no-op in production)
  seedDemoApiKeyIfDev();

  // Seed base data (admin user, ports, vessels) — idempotent, safe to run on every boot
  if (process.env.NODE_ENV !== "production") {
    mockDataService.initializeBaseData().catch((err) => {
      logger.error("Failed to seed base data", { error: err });
    });
  }

  // Mount feature routers
  app.use("/", healthRouter);
  app.use("/", authRouter);
  if (process.env.NODE_ENV !== "production") {
    app.use("/", devRouter);
  }
  app.use("/", createAdminRouter(wss));
  app.use("/", signalsRouter);
  app.use("/", portsRouter);
  app.use("/", alertsRouter);
  app.use("/", vesselsRouter);
  app.use("/", commoditiesRouter);

  return httpServer;
}
