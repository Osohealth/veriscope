import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { requestIdMiddleware, auditContextMiddleware } from "./middleware/requestContext";
import { startIncidentAutomationScheduler } from "./services/incidentAutomationScheduler";
import { seedDemoServer } from "./services/demoServerSeed";

if (process.env.DEMO_SERVER === "1") {
  process.env.DEV_ROUTES_ENABLED = "true";
  log("[demo] DEMO_SERVER=1 (DEV_ROUTES_ENABLED forced true)");
}

if (process.env.NODE_ENV === "production" && process.env.TEST_SCHEMA_PROFILE) {
  throw new Error("TEST_SCHEMA_PROFILE must not be set in production");
}
if (process.env.NODE_ENV === "production" && process.env.DEV_ROUTES_ENABLED === "true") {
  console.warn("WARNING: DEV_ROUTES_ENABLED is true in production. This should be disabled.");
}

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(requestIdMiddleware);
app.use(auditContextMiddleware);

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "…";
      }

      log(logLine);
    }
  });

  next();
});

(async () => {
  if (process.env.DEMO_SERVER === "1") {
    await seedDemoServer();
  }
  const server = await registerRoutes(app);
  startIncidentAutomationScheduler();

  // Ensure unmatched API routes return JSON, not the Vite HTML fallback.
  app.use("/api", (_req, res) => {
    res.status(404).json({ version: "1", ok: false, error: "NOT_FOUND" });
  });

  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    res.status(status).json({ message });
    throw err;
  });

  // importantly only setup vite in development and after
  // setting up all the other routes so the catch-all route
  // doesn't interfere with the other routes
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  // ALWAYS serve the app on the port specified in the environment variable PORT
  // Other ports are firewalled. Default to 5000 if not specified.
  // this serves both the API and the client.
  // It is the only port that is not firewalled.
  const port = parseInt(process.env.PORT || '5000', 10);
  server.listen({
    port,
    host: "0.0.0.0",
    ...(process.platform !== "win32" ? { reusePort: true } : {}),
  }, () => {
    log(`serving on port ${port}`);
  });
})();
