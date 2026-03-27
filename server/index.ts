import "dotenv/config";
import express, { type Request, Response, NextFunction } from "express";
import helmet from "helmet";
import cookieParser from "cookie-parser";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { AppError } from "./utils/AppError";
import { logger } from "./middleware/observability";
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
if (process.env.NODE_ENV === "production" && process.env.DEV_ROUTES_ENABLED === "true" && process.env.DEMO_SERVER !== "1") {
  throw new Error("DEV_ROUTES_ENABLED must not be 'true' in production — this is a security risk. Unset it or use DEMO_SERVER=1 for intentional demo deployments.");
}

// Startup guard: validate ALERTS_ROLE early so misconfiguration is caught before serving traffic
const _alertsRole = process.env.ALERTS_ROLE;
if (_alertsRole && !["OWNER", "OPERATOR", "VIEWER"].includes(_alertsRole.toUpperCase())) {
  throw new Error(`ALERTS_ROLE must be one of: OWNER, OPERATOR, VIEWER. Got: "${_alertsRole}"`);
}

const app = express();
app.set('trust proxy', process.env.TRUSTED_PROXIES
  ? process.env.TRUSTED_PROXIES.split(',').map((s: string) => s.trim())
  : false);
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: process.env.NODE_ENV === "production"
        ? ["'self'"]
        : ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "blob:", "https:"],
      connectSrc: ["'self'", "ws:", "wss:"],
    },
  },
  crossOriginEmbedderPolicy: false,
}));
app.use(cookieParser());
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
    res.status(404).json({ version: "1", ok: false, error: "Unknown API route" });
  });

  // Global error handler — sanitises all 500 responses so stack traces
  // and internal DB error messages are never sent to the client.
  app.use((err: any, req: Request, res: Response, _next: NextFunction) => {
    const requestId = (req as any).requestId;

    if (err instanceof AppError) {
      // Structured AppError: return the full code+message for 4xx, sanitise 5xx
      if (err.status >= 500) {
        logger.error(`[${requestId}] AppError 5xx`, { error: err, requestId });
        return res.status(err.status).json({ version: "1", ok: false, error: { code: "INTERNAL", message: "Internal server error" }, requestId });
      }
      return res.status(err.status).json({ version: "1", ok: false, error: { code: err.code, message: err.message } });
    }

    const status = err.status || err.statusCode || 500;
    if (status >= 500) {
      // Log full error internally but never expose it
      logger.error(`[${requestId}] Unhandled error`, { error: err, requestId });
      return res.status(500).json({ version: "1", ok: false, error: "Internal server error", requestId });
    }
    // Client errors (4xx) can pass through their message safely
    res.status(status).json({ version: "1", ok: false, error: err.message || "Request error" });
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
    ...(process.platform === "linux" ? { reusePort: true } : {}),
  }, () => {
    log(`serving on port ${port}`);
  });

  // Graceful shutdown: stop accepting new connections, drain existing ones,
  // then allow the process to exit cleanly. Kubernetes / Railway send SIGTERM.
  const shutdown = (signal: string) => {
    log(`Received ${signal} — shutting down gracefully`);
    server.close(() => {
      log("HTTP server closed");
      process.exit(0);
    });
    // Force exit if draining takes too long
    setTimeout(() => {
      log("Shutdown timeout — forcing exit");
      process.exit(1);
    }, 10_000).unref();
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
})();
