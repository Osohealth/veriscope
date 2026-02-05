import type { Express } from "express";
import { createServer, type Server } from "http";
import { WebSocketServer } from "ws";
import swaggerUi from "swagger-ui-express";
import { randomBytes, randomUUID } from "node:crypto";
import { and, desc, eq, inArray, or, sql } from "drizzle-orm";
import { storage } from "./storage";
import { aisService } from "./services/aisService";
import { signalsService } from "./services/signalsService";
import { predictionService } from "./services/predictionService";
import { delayService } from "./services/delayService";
import { mockDataService } from "./services/mockDataService";
import { portCallService } from "./services/portCallService";
import { rotterdamDataService } from "./services/rotterdamDataService";
import { getPortDailyBaselines, startPortDailyBaselineScheduler } from "./services/portDailyBaselineService";
import { evaluatePortSignalsForDay, formatSignalDay, getSignalById, getYesterdayUtcDay, listSignals, parseSignalDay } from "./services/signalEngine";
import { buildSignalResponse, type SignalEntity } from "./services/signalResponse";
import { buildSignalClusterAlertPayload } from "./services/signalAlertService";
import { validateAlertSubscriptionInput } from "./services/alertSubscriptionService";
import { runAlerts } from "./services/alertDispatcher";
import { retryAlertDlq, retryDeliveryById } from "./services/alertDlqQueue";
import { listAlertDeliveries } from "./services/alertDeliveries";
import { getDeliveryHealthByDay, getDeliveryLatency, getEndpointHealth, getDlqHealth, getDlqOverdue } from "./services/alertMetrics";
import { buildWebhookRequest, sendWebhook } from "./services/webhookSender";
import { sendEmail } from "./services/emailSender";
import { GLOBAL_SCOPE_ENTITY_ID, normalizeScope } from "./services/alertScope";
import { TENANT_DEMO_ID } from "./config/tenancy";
import { authService } from "./services/authService";
import { sessionService } from "./services/sessionService";
import { auditService } from "./services/auditService";
import { requestTrackingMiddleware, metricsCollector, getHealthStatus, setWsHealth, setDbHealth } from "./middleware/observability";
import { authRateLimiter, apiRateLimiter } from "./middleware/rateLimiter";
import { wsManager } from "./services/wsManager";
import { authenticate, optionalAuth, requirePermission, requireRole, requireAdmin, requireSelfOrAdmin, logAccess } from "./middleware/rbac";
import { cacheService, CACHE_KEYS, CACHE_TTL } from "./services/cacheService";
import { parsePaginationParams, paginateArray, parseGeoQueryParams, filterByGeoRadius } from "./utils/pagination";
import { openApiSpec } from "./openapi";
import { getAuthenticatedUser, createRepository, listRepositories } from "./services/githubService";
import { db } from "./db";
import { alertDedupe, alertDeliveries, alertDeliveryAttempts, alertDlq, alertRuns, alertSubscriptions, apiKeys, portCalls, portDailyBaselines, ports, signals, vessels } from "@shared/schema";
import { SEVERITY_RANK } from "@shared/signalTypes";
import { WEBHOOK_TIMEOUT_MS } from "./config/alerting";
import { authenticateApiKey } from "./middleware/apiKeyAuth";
import { generateApiKey, hashApiKey } from "./services/apiKeyService";

type InitResult = {
  portCount: number;
  startedAt: string;
  completedAt: string;
};

let initInProgress: Promise<InitResult> | null = null;
let initCompleted: InitResult | null = null;

const DEMO_USER_ID = "00000000-0000-0000-0000-000000000001";
const subscriptionTestRateLimit = new Map<string, number[]>();

const normalizeEmail = (value: string) => value.trim().toLowerCase();
const isValidEmail = (value: string) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
const isValidWebhookUrl = (value: string, allowHttp: boolean) => {
  try {
    const parsed = new URL(value);
    if (!allowHttp && parsed.protocol !== "https:") return false;
    if (allowHttp && !["https:", "http:"].includes(parsed.protocol)) return false;
    if (parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1") {
      return allowHttp;
    }
    return true;
  } catch {
    return false;
  }
};
const generateSecret = () => randomBytes(24).toString("base64url");

export async function registerRoutes(app: Express): Promise<Server> {
  const httpServer = createServer(app);
  
  // Apply observability middleware
  app.use(requestTrackingMiddleware);
  
  // ===== HEALTH & OBSERVABILITY ENDPOINTS =====
  
  app.get('/health', (req, res) => {
    const status = getHealthStatus();
    res.status(status.status === 'healthy' ? 200 : status.status === 'degraded' ? 200 : 503).json(status);
  });

  app.get('/health/alerts', async (req, res) => {
    try {
      await db.execute(sql`SELECT 1`);

      const [runRow] = await db
        .insert(alertRuns)
        .values({ tenantId: TENANT_DEMO_ID, status: "SUCCESS", startedAt: new Date(), finishedAt: new Date() })
        .returning();
      if (runRow?.id) {
        await db.delete(alertRuns).where(eq(alertRuns.id, runRow.id));
      }

      await db.execute(sql`SELECT 1 FROM alert_deliveries LIMIT 1`);
      await db.execute(sql`SELECT 1 FROM alert_dlq LIMIT 1`);

      res.json({ status: "ok" });
    } catch (error: any) {
      res.status(500).json({ status: "error", error: error.message || "Alert health check failed" });
    }
  });

  app.get('/health/webhooks', (req, res) => {
    try {
      if (typeof fetch !== "function") {
        return res.status(500).json({ status: "error", error: "fetch is unavailable" });
      }
      res.json({ status: "ok", timeout_ms: WEBHOOK_TIMEOUT_MS });
    } catch (error: any) {
      res.status(500).json({ status: "error", error: error.message || "Webhook health check failed" });
    }
  });

  if (process.env.NODE_ENV === "development") {
    setTimeout(async () => {
      try {
        const demoUserId = "00000000-0000-0000-0000-000000000001";
        const [existing] = await db
          .select()
          .from(apiKeys)
          .where(eq(apiKeys.userId, demoUserId))
          .limit(1);
        if (!existing) {
          const rawKey = generateApiKey("vs_demo");
          await db.insert(apiKeys).values({
            tenantId: TENANT_DEMO_ID,
            userId: demoUserId,
            keyHash: hashApiKey(rawKey),
            name: "dev-demo",
          });
          console.log(`DEMO_API_KEY=${rawKey}`);
        }
      } catch (error) {
        console.warn("Failed to seed demo API key:", (error as Error).message);
      }
    }, 0);
  }

  app.get('/ready', async (req, res) => {
    try {
      await storage.getPorts();
      setDbHealth(true);
      res.json({ status: 'ready', timestamp: new Date().toISOString() });
    } catch {
      setDbHealth(false);
      res.status(503).json({ status: 'not_ready', timestamp: new Date().toISOString() });
    }
  });

  app.get('/live', (req, res) => {
    res.json({ status: 'alive', timestamp: new Date().toISOString() });
  });

  // Data status endpoint - shows counts for debugging production data
  app.get('/api/status/data', async (req, res) => {
    try {
      const [vessels, ports] = await Promise.all([
        storage.getVessels(),
        storage.getPorts()
      ]);
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        counts: {
          vessels: vessels.length,
          ports: ports.length
        },
        environment: process.env.NODE_ENV || 'development'
      });
    } catch (error: any) {
      res.status(500).json({
        status: 'error',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  // Port daily baselines (internal debugging)
  app.get('/api/baselines/ports/:portId', optionalAuth, async (req, res) => {
    try {
      const { portId } = req.params;
      const daysParam = parseInt(req.query.days as string);
      const days = Number.isFinite(daysParam) ? daysParam : 30;
      const items = await getPortDailyBaselines(portId, days);
      res.json({ items });
    } catch (error: any) {
      res.status(500).json({ error: error.message || 'Failed to fetch baselines' });
    }
  });

  // Signal engine manual run (internal)
  app.post('/api/signals/run', optionalAuth, async (req, res) => {
    try {
      const dayParam = req.query.day as string | undefined;
      const parsedDay = dayParam ? parseSignalDay(dayParam) : null;

      if (dayParam && !parsedDay) {
        return res.status(400).json({ error: 'day must be YYYY-MM-DD' });
      }

      const targetDay = parsedDay ?? getYesterdayUtcDay();
      const result = await evaluatePortSignalsForDay(targetDay);
      res.json({ day: formatSignalDay(targetDay), count: result.upserted });
    } catch (error: any) {
      res.status(500).json({ error: error.message || 'Failed to run signal engine' });
    }
  });

  // Dev-only: seed an anomaly baseline row for deterministic signals
  app.post('/api/dev/seed-anomaly', async (req, res) => {
    try {
      if (process.env.NODE_ENV === 'production') {
        const expectedToken = process.env.DEV_SEED_TOKEN;
        if (!expectedToken) {
          return res.status(403).json({ error: 'Dev seeding disabled in production' });
        }
        const providedToken = (req.query.token as string | undefined) || (req.headers['x-dev-seed-token'] as string | undefined);
        if (!providedToken || providedToken !== expectedToken) {
          return res.status(403).json({ error: 'Invalid dev seed token' });
        }
      }

      const dayParam = req.query.day as string | undefined;
      const parsedDay = parseSignalDay(dayParam);
      if (!dayParam || !parsedDay) {
        return res.status(400).json({ error: 'day is required as YYYY-MM-DD' });
      }

      const requestedPortId = req.query.port_id as string | undefined;
      let port = null as null | { id: string; name: string | null; code: string | null; unlocode: string | null };

      if (requestedPortId) {
        const [found] = await db.select().from(ports).where(eq(ports.id, requestedPortId)).limit(1);
        if (!found) {
          return res.status(404).json({ error: 'Port not found' });
        }
        port = found;
      } else {
        const [rotterdam] = await db
          .select()
          .from(ports)
          .where(or(
            eq(ports.unlocode, 'NLRTM'),
            eq(ports.code, 'RTM'),
            sql`${ports.name} ILIKE ${'%rotterdam%'}`
          ))
          .limit(1);

        if (rotterdam) {
          port = rotterdam;
        } else {
          const [firstPort] = await db.select().from(ports).orderBy(desc(ports.name)).limit(1);
          port = firstPort ?? null;
        }
      }

      if (!port) {
        return res.status(400).json({ error: 'No ports available to seed' });
      }

      const historyDays = 10;
      const historyRows = Array.from({ length: historyDays }, (_, index) => {
        const day = new Date(parsedDay);
        day.setUTCDate(day.getUTCDate() - (index + 1));
        return {
          portId: port.id,
          day,
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
        day: parsedDay,
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
        message: 'Seeded anomaly baseline row',
        day: formatSignalDay(parsedDay),
        port: {
          id: port.id,
          name: port.name,
          code: port.code,
          unlocode: port.unlocode,
        },
        baseline,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message || 'Failed to seed anomaly' });
    }
  });

  // Dev-only: webhook sink for demo success
  app.post('/api/dev/webhook-sink', (req, res) => {
    if (process.env.NODE_ENV === "production" && process.env.DEV_ROUTES_ENABLED !== "true") {
      return res.status(404).json({ error: "Not found" });
    }
    res.json({ ok: true });
  });

  // Dev-only: seed alert subscriptions for demo
  app.post('/api/dev/alert-subscriptions/seed', async (req, res) => {
    try {
      if (process.env.NODE_ENV !== "development") {
        return res.status(404).json({ error: "Not found" });
      }
      if (process.env.NODE_ENV === 'production') {
        const expectedToken = process.env.DEV_SEED_TOKEN;
        if (!expectedToken) {
          return res.status(403).json({ error: 'Dev seeding disabled in production' });
        }
        const providedToken = (req.query.token as string | undefined) || (req.headers['x-dev-seed-token'] as string | undefined);
        if (!providedToken || providedToken !== expectedToken) {
          return res.status(403).json({ error: 'Invalid dev seed token' });
        }
      }

      const [port] = await db
        .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
        .from(ports)
        .where(or(
          eq(ports.unlocode, 'NLRTM'),
          eq(ports.code, 'RTM'),
          sql`${ports.name} ILIKE ${'%rotterdam%'}`
        ))
        .limit(1);

      if (!port) {
        return res.status(400).json({ error: 'No ports available to seed subscriptions' });
      }

      const host = req.get('host') || 'localhost:5000';
      const baseUrl = `${req.protocol}://${host}`;
      const demoUserId = "00000000-0000-0000-0000-000000000001";
      const demoTenantId = TENANT_DEMO_ID;

      // Hard reset demo alert data for deterministic results.
      await db.delete(alertDlq).where(eq(alertDlq.tenantId, demoTenantId));
      await db.delete(alertDeliveries).where(eq(alertDeliveries.tenantId, demoTenantId));
      await db.delete(alertDedupe).where(eq(alertDedupe.tenantId, demoTenantId));
      await db.delete(alertRuns).where(eq(alertRuns.tenantId, demoTenantId));
      await db.delete(alertSubscriptions).where(and(eq(alertSubscriptions.userId, demoUserId), eq(alertSubscriptions.tenantId, demoTenantId)));

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
      res.status(500).json({ error: error.message || 'Failed to seed alert subscriptions' });
    }
  });

  // Dev-only: seed Rotterdam port calls for a week (>=30 vessels)
    app.post('/api/dev/seed-rotterdam-week', async (req, res) => {
      try {
        if (process.env.NODE_ENV !== "development") {
          return res.status(404).json({ error: "Not found" });
        }
        if (process.env.NODE_ENV === 'production') {
          const expectedToken = process.env.DEV_SEED_TOKEN;
        if (!expectedToken) {
          return res.status(403).json({ error: 'Dev seeding disabled in production' });
        }
        const providedToken = (req.query.token as string | undefined) || (req.headers['x-dev-seed-token'] as string | undefined);
        if (!providedToken || providedToken !== expectedToken) {
          return res.status(403).json({ error: 'Invalid dev seed token' });
        }
      }

      const daysRaw = Number(req.query.days ?? 7);
      const vesselsRaw = Number(req.query.vessels ?? 30);
      const days = Math.max(7, Math.min(31, Number.isFinite(daysRaw) ? daysRaw : 7));
      const vesselCount = Math.max(30, Math.min(200, Number.isFinite(vesselsRaw) ? vesselsRaw : 30));

      const [rotterdam] = await db
        .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
        .from(ports)
        .where(or(
          eq(ports.code, 'NLRTM'),
          eq(ports.unlocode, 'NLRTM'),
          sql`lower(${ports.name}) = 'rotterdam'`,
        ))
        .limit(1);

      if (!rotterdam) {
        return res.status(404).json({ error: 'Rotterdam port not found' });
      }

      const todayUtc = new Date();
      const startUtc = new Date(Date.UTC(
        todayUtc.getUTCFullYear(),
        todayUtc.getUTCMonth(),
        todayUtc.getUTCDate() - (days - 1),
      ));

      const mmsiList = Array.from({ length: vesselCount }, (_, idx) => String(200000000 + idx));
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
          vesselType: idx % 2 === 0 ? 'container' : 'tanker',
          flag: 'NL',
          owner: 'Dev Fleet',
          operator: 'Dev Ops',
          buildYear: 2000 + (idx % 20),
        }));

      if (newVessels.length > 0) {
        await db.insert(vessels).values(newVessels);
        for (const vessel of newVessels) {
          existingMap.set(vessel.mmsi, vessel.id);
        }
      }

      const seedTag = 'dev_rotterdam_week';
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
        const departure = shouldDepart ? new Date(arrival.getTime() + dwellHours * 3600 * 1000) : null;

        return {
          vesselId,
          portId: rotterdam.id,
          callType: 'arrival',
          status: shouldDepart ? 'completed' : 'in_progress',
          arrivalTime: arrival,
          departureTime: departure,
          berthNumber: `B-${(idx % 20) + 1}`,
          purpose: idx % 3 === 0 ? 'loading' : idx % 3 === 1 ? 'discharging' : 'bunkering',
          waitTimeHours: (idx % 6) + 1,
          berthTimeHours: dwellHours,
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
      res.status(500).json({ error: error.message || 'Failed to seed Rotterdam week data' });
    }
  });

  // Dev/admin: alert subscriptions
  app.post('/api/dev/alert-subscriptions', optionalAuth, async (req, res) => {
    try {
      if (process.env.NODE_ENV === "production" && process.env.DEV_ROUTES_ENABLED !== "true") {
        return res.status(404).json({ error: "Not found" });
      }
      const allowHttp = process.env.NODE_ENV !== "production";
      const validation = validateAlertSubscriptionInput(req.body ?? {}, allowHttp);
      if (!validation.ok) {
        return res.status(400).json({ error: validation.errors[0] });
      }

      const [created] = await db
        .insert(alertSubscriptions)
        .values({
          tenantId: TENANT_DEMO_ID,
          ...validation.value,
          updatedAt: new Date(),
        })
        .returning();

      res.json(created);
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to create subscription" });
    }
  });

  app.get('/api/dev/alert-subscriptions', optionalAuth, async (req, res) => {
    try {
      if (process.env.NODE_ENV === "production" && process.env.DEV_ROUTES_ENABLED !== "true") {
        return res.status(404).json({ error: "Not found" });
      }
      const { user_id } = req.query;
      if (!user_id) {
        return res.status(400).json({ error: "user_id is required" });
      }
      const rows = await db
        .select()
        .from(alertSubscriptions)
        .where(and(
          eq(alertSubscriptions.userId, String(user_id)),
          eq(alertSubscriptions.tenantId, TENANT_DEMO_ID),
        ));
      res.json({ items: rows });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to list subscriptions" });
    }
  });

  app.post('/api/alerts/run', authenticateApiKey, async (req, res) => {
      if (process.env.NODE_ENV !== "development") {
        return res.status(404).json({ error: "Not found" });
      }
      try {
        const dayParam = req.query.day as string | undefined;
        const parsedDay = dayParam ? parseSignalDay(dayParam) : null;
        if (dayParam && !parsedDay) {
          return res.status(400).json({ error: 'day must be YYYY-MM-DD' });
        }
        const result = await runAlerts({
          day: parsedDay ? formatSignalDay(parsedDay) : undefined,
          userId: req.auth?.userId,
          tenantId: req.auth?.tenantId,
        });
        res.json({ run_id: result.runId, status: result.status, summary: result.summary });
      } catch (error: any) {
        res.status(500).json({ error: error.message || 'Failed to run alerts' });
      }
    });

    app.post('/api/alerts/retry-dlq', authenticateApiKey, async (req, res) => {
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
      res.status(500).json({ error: error.message || 'Failed to retry dlq' });
    }
  });

    app.post('/api/alerts/retry-delivery/:delivery_id', authenticateApiKey, async (req, res) => {
      try {
        const deliveryId = req.params.delivery_id;
        const [ownership] = await db
          .select({ userId: alertSubscriptions.userId, tenantId: alertSubscriptions.tenantId })
          .from(alertDeliveries)
          .innerJoin(alertSubscriptions, eq(alertDeliveries.subscriptionId, alertSubscriptions.id))
          .where(eq(alertDeliveries.id, deliveryId))
          .limit(1);
        if (!ownership || ownership.userId !== req.auth?.userId || ownership.tenantId !== req.auth?.tenantId) {
          return res.status(404).json({ error: "Delivery not found" });
        }
        const result = await retryDeliveryById({
          deliveryId,
          tenantId: req.auth?.tenantId,
          userId: req.auth?.userId,
          now: new Date(),
        });
        if (result.status === "not_found") return res.status(404).json({ error: "Delivery not found" });
        if (result.status === "already_sent") return res.status(409).json({ error: "Delivery already sent" });
        if (result.status === "terminal") return res.status(409).json({ error: "Delivery is terminal" });
      res.json({ version: "1", delivery: result.delivery, dlq: result.dlq ?? null, status: result.status });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to retry delivery" });
    }
  });

  app.get('/api/alerts/metrics', authenticateApiKey, async (req, res) => {
    try {
      const userId = req.auth?.userId;
      const tenantId = req.auth?.tenantId;
      if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
      const daysRaw = Number(req.query.days ?? 30);
      const days = Number.isFinite(daysRaw) ? daysRaw : 30;
      const [deliveryHealth, latency, endpointHealth] = await Promise.all([
        getDeliveryHealthByDay(days, { tenantId, userId }),
        getDeliveryLatency(days, { tenantId, userId }),
        getEndpointHealth(days, { tenantId, userId }),
      ]);
      res.json({
        version: "1",
        days,
        delivery_health: deliveryHealth,
        latency,
        endpoint_health: endpointHealth,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to fetch alert metrics" });
    }
  });

  app.get('/api/alerts/dlq-health', authenticateApiKey, async (req, res) => {
    try {
      const userId = req.auth?.userId;
      const tenantId = req.auth?.tenantId;
      if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
      const limit = Math.min(Number(req.query.limit ?? 20), 200);
      const [health, overdue] = await Promise.all([
        getDlqHealth({ tenantId, userId }),
        getDlqOverdue(Number.isFinite(limit) ? limit : 20, { tenantId, userId }),
      ]);
      res.json({ version: "1", health, overdue });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to fetch dlq health" });
    }
  });

  app.get('/metrics', (req, res) => {
    res.json(metricsCollector.getMetrics());
  });

  // ===== GITHUB INTEGRATION ENDPOINTS =====
  
  app.get('/api/github/user', async (req, res) => {
    try {
      const user = await getAuthenticatedUser();
      res.json({
        login: user.login,
        name: user.name,
        avatar_url: user.avatar_url,
        html_url: user.html_url
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/github/repos', async (req, res) => {
    try {
      const repos = await listRepositories();
      res.json(repos.map(r => ({
        name: r.name,
        full_name: r.full_name,
        html_url: r.html_url,
        private: r.private,
        updated_at: r.updated_at
      })));
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/github/repos', async (req, res) => {
    try {
      const { name, description, isPrivate } = req.body;
      if (!name) {
        return res.status(400).json({ error: 'Repository name is required' });
      }
      const repo = await createRepository(name, description || '', isPrivate || false);
      res.json({
        name: repo.name,
        full_name: repo.full_name,
        html_url: repo.html_url,
        clone_url: repo.clone_url,
        ssh_url: repo.ssh_url
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });
  
  // OpenAPI documentation
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(openApiSpec));
  app.get('/openapi.json', (req, res) => {
    res.json(openApiSpec);
  });
  
  // WebSocket setup with message schema versioning, throttling, and topics
  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
  wsManager.initialize(wss);
  
  // WebSocket stats endpoint
  app.get('/api/ws/stats', authenticate, requireRole('admin', 'operator'), (req, res) => {
    res.json(wsManager.getStats());
  });

  // AIS stream status endpoint
  app.get('/api/ais/status', authenticate, requireRole('admin', 'operator'), (req, res) => {
    res.json(aisService.getStatus());
  });

  // ===== AUTHENTICATION ENDPOINTS =====
  
  app.post('/api/auth/register', authRateLimiter, async (req, res) => {
    try {
      const { email, password, name, full_name, organization_name } = req.body;
      const fullName = full_name || name;
      
      if (!email || !password || !fullName) {
        return res.status(400).json({ error: 'Email, password, and name are required' });
      }
      
      if (password.length < 6) {
        return res.status(400).json({ error: 'Password must be at least 6 characters' });
      }
      
      const result = await authService.register(email, password, fullName, organization_name);
      
      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }
      
      res.status(201).json({ message: 'Registration successful', ...result.data });
    } catch (error: any) {
      console.error('Registration error:', error);
      res.status(500).json({ error: 'Registration failed' });
    }
  });

  app.post('/api/auth/login', authRateLimiter, async (req, res) => {
    try {
      const { email, password } = req.body;
      
      if (!email || !password) {
        return res.status(400).json({ error: 'Email and password are required' });
      }
      
      const result = await authService.login(email, password);
      
      if (!result.success || !result.data) {
        await auditService.logLogin('', false, req, result.error);
        return res.status(401).json({ error: result.error });
      }
      
      await auditService.logLogin(result.data.user.id, true, req);
      
      res.json({ 
        message: 'Login successful', 
        ...result.data
      });
    } catch (error: any) {
      console.error('Login error:', error);
      res.status(500).json({ error: 'Login failed' });
    }
  });

  app.post('/api/auth/refresh', async (req, res) => {
    try {
      const { refreshToken, refresh_token } = req.body;
      const token = refreshToken || refresh_token;
      
      if (!token) {
        return res.status(400).json({ error: 'Refresh token required' });
      }
      
      const result = await authService.refreshTokens(token);
      
      if (!result.success || !result.data) {
        return res.status(401).json({ error: result.error || 'Invalid or expired refresh token' });
      }
      
      res.json(result.data);
    } catch (error: any) {
      console.error('Token refresh error:', error);
      res.status(500).json({ error: 'Token refresh failed' });
    }
  });

  app.post('/api/auth/logout', async (req, res) => {
    try {
      const { refreshToken } = req.body;
      const authHeader = req.headers.authorization;
      
      if (refreshToken) {
        sessionService.revokeRefreshToken(refreshToken);
      }
      
      if (authHeader?.startsWith('Bearer ')) {
        const token = authHeader.substring(7);
        const payload = sessionService.verifyToken(token);
        if (payload) {
          await auditService.logLogout(payload.userId, req);
        }
      }
      
      res.json({ message: 'Logged out successfully' });
    } catch (error: any) {
      console.error('Logout error:', error);
      res.status(500).json({ error: 'Logout failed' });
    }
  });

  // ===== PHASE ONE V1 API ENDPOINTS =====
  
  // V1 Auth - Register
  app.post('/v1/auth/register', authRateLimiter, async (req, res) => {
    try {
      const { email, password, full_name, organization_name } = req.body;
      
      if (!email || !password || !full_name) {
        return res.status(400).json({ error: 'Email, password, and full_name are required' });
      }
      
      if (password.length < 8) {
        return res.status(400).json({ error: 'Password must be at least 8 characters' });
      }
      
      const result = await authService.register(email, password, full_name, organization_name);
      
      if (!result.success || !result.data) {
        return res.status(400).json({ error: result.error });
      }
      
      res.status(201).json({
        access_token: result.data.accessToken,
        refresh_token: result.data.refreshToken,
        token_type: result.data.tokenType,
        user: {
          id: result.data.user.id,
          email: result.data.user.email,
          full_name: result.data.user.fullName,
          role: result.data.user.role,
        }
      });
    } catch (error: any) {
      console.error('V1 Registration error:', error);
      res.status(500).json({ error: 'Registration failed' });
    }
  });

  // V1 Auth - Login
  app.post('/v1/auth/login', authRateLimiter, async (req, res) => {
    try {
      const { email, password } = req.body;
      
      if (!email || !password) {
        return res.status(400).json({ error: 'Email and password are required' });
      }
      
      const result = await authService.login(email, password);
      
      if (!result.success || !result.data) {
        await auditService.logLogin('', false, req, result.error);
        return res.status(401).json({ error: result.error });
      }
      
      await auditService.logLogin(result.data.user.id, true, req);
      
      res.json({
        access_token: result.data.accessToken,
        refresh_token: result.data.refreshToken,
        token_type: result.data.tokenType,
        user: {
          id: result.data.user.id,
          email: result.data.user.email,
          full_name: result.data.user.fullName,
          role: result.data.user.role,
        }
      });
    } catch (error: any) {
      console.error('V1 Login error:', error);
      res.status(500).json({ error: 'Login failed' });
    }
  });

  // V1 Auth - Refresh Token
  app.post('/v1/auth/refresh', async (req, res) => {
    try {
      const { refresh_token } = req.body;
      
      if (!refresh_token) {
        return res.status(400).json({ error: 'refresh_token is required' });
      }
      
      const result = await authService.refreshTokens(refresh_token);
      
      if (!result.success || !result.data) {
        return res.status(401).json({ error: result.error || 'Invalid or expired refresh token' });
      }
      
      res.json({
        access_token: result.data.accessToken,
        refresh_token: result.data.refreshToken,
        token_type: result.data.tokenType,
        user: {
          id: result.data.user.id,
          email: result.data.user.email,
          full_name: result.data.user.fullName,
          role: result.data.user.role,
        }
      });
    } catch (error: any) {
      console.error('V1 Token refresh error:', error);
      res.status(500).json({ error: 'Token refresh failed' });
    }
  });

  // V1 Ports - List with search and filtering
  app.get('/v1/ports', optionalAuth, async (req, res) => {
    try {
      const { q, country_code, limit = '50' } = req.query;
      const ports = await storage.getPorts();
      
      let filtered = ports;
      
      if (q) {
        const search = String(q).toLowerCase();
        filtered = filtered.filter(p => 
          p.name.toLowerCase().includes(search) || 
          p.code.toLowerCase().includes(search) ||
          (p.unlocode && p.unlocode.toLowerCase().includes(search))
        );
      }
      
      if (country_code) {
        const cc = String(country_code).toUpperCase();
        filtered = filtered.filter(p => p.countryCode === cc || p.country.toUpperCase().includes(cc));
      }
      
      const limitNum = Math.min(parseInt(String(limit)) || 50, 500);
      filtered = filtered.slice(0, limitNum);
      
      res.json({
        items: filtered.map(p => ({
          id: p.id,
          name: p.name,
          unlocode: p.unlocode || p.code,
          country_code: p.countryCode || p.country?.substring(0, 2).toUpperCase(),
          latitude: parseFloat(String(p.latitude)),
          longitude: parseFloat(String(p.longitude)),
          timezone: p.timezone || 'UTC',
        })),
        total: filtered.length,
      });
    } catch (error) {
      console.error('V1 ports list error:', error);
      res.status(500).json({ error: 'Failed to fetch ports' });
    }
  });

  // V1 Ports - Get by ID with 7-day KPIs (optimized SQL)
  app.get('/v1/ports/:port_id', optionalAuth, async (req, res) => {
    try {
      const { port_id } = req.params;
      const ports = await storage.getPorts();
      const port = ports.find(p => p.id === port_id);
      
      if (!port) {
        return res.status(404).json({ error: 'Port not found' });
      }
      
      const { getPortMetrics7d } = await import('./services/portStatisticsService');
      const metrics = await getPortMetrics7d(port_id);
      
      res.json({
        id: port.id,
        name: port.name,
        unlocode: port.unlocode || port.code,
        country_code: port.countryCode || port.country?.substring(0, 2).toUpperCase(),
        latitude: parseFloat(String(port.latitude)),
        longitude: parseFloat(String(port.longitude)),
        timezone: port.timezone || 'UTC',
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
      console.error('V1 port detail error:', error);
      res.status(500).json({ error: 'Failed to fetch port' });
    }
  });

  // V1 Ports - Get port calls
  app.get('/v1/ports/:port_id/calls', optionalAuth, async (req, res) => {
    try {
      const { port_id } = req.params;
      const { start_time, end_time, limit = '100' } = req.query;
      
      const startDate = start_time ? new Date(String(start_time)) : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
      const endDate = end_time ? new Date(String(end_time)) : new Date();
      
      const portCalls = await storage.getPortCallsByPort(port_id, startDate, endDate);
      const limitNum = Math.min(parseInt(String(limit)) || 100, 500);
      
      const items = await Promise.all(
        portCalls.slice(0, limitNum).map(async (call) => {
          const vessel = await storage.getVessel(call.vesselId);
          
          let dwellHours = null;
          if (call.departureTime && call.arrivalTime) {
            const arrival = new Date(call.arrivalTime);
            const departure = new Date(call.departureTime);
            dwellHours = Math.round((departure.getTime() - arrival.getTime()) / (1000 * 60 * 60) * 10) / 10;
          }
          
          return {
            id: call.id,
            vessel_id: call.vesselId,
            vessel_name: vessel?.name || 'Unknown',
            arrival_time_utc: call.arrivalTime,
            departure_time_utc: call.departureTime,
            dwell_hours: dwellHours,
          };
        })
      );
      
      res.json({ items });
    } catch (error) {
      console.error('V1 port calls error:', error);
      res.status(500).json({ error: 'Failed to fetch port calls' });
    }
  });

  // V1 Ports - Daily arrivals/departures time series (last 7 days)
  app.get('/v1/ports/:port_id/daily_stats', optionalAuth, async (req, res) => {
    try {
      const { port_id } = req.params;
      const { getDailyArrivalsTimeSeries } = await import('./services/portStatisticsService');
      const timeSeries = await getDailyArrivalsTimeSeries(port_id);
      res.json({ port_id, items: timeSeries });
    } catch (error) {
      console.error('V1 port daily stats error:', error);
      res.status(500).json({ error: 'Failed to fetch daily statistics' });
    }
  });

  // V1 Ports - Top busy ports (last 7 days)
  app.get('/v1/ports/stats/top_busy', optionalAuth, async (req, res) => {
    try {
      const { limit = '20' } = req.query;
      const { getTopBusyPorts } = await import('./services/portStatisticsService');
      const topPorts = await getTopBusyPorts(Math.min(parseInt(String(limit)) || 20, 100));
      res.json({ items: topPorts });
    } catch (error) {
      console.error('V1 top busy ports error:', error);
      res.status(500).json({ error: 'Failed to fetch top busy ports' });
    }
  });

  // V1 Signals - List with filters
  app.get('/v1/signals', optionalAuth, async (req, res) => {
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
        limit = '50',
        offset = '0',
      } = req.query;

      const dayExact = day ? parseSignalDay(String(day)) : null;
      let dayFrom = dayExact ?? (day_from ? parseSignalDay(String(day_from)) : null);
      let dayTo = dayExact ?? (day_to ? parseSignalDay(String(day_to)) : null);

      if ((day && !dayExact) || (day_from && !dayFrom) || (day_to && !dayTo)) {
        return res.status(400).json({ error: 'day/day_from/day_to must be YYYY-MM-DD' });
      }

      const limitNum = Math.min(parseInt(String(limit)) || 50, 500);
      const offsetNum = Math.max(parseInt(String(offset)) || 0, 0);
      const severityMin = severity_min ? String(severity_min).toUpperCase() : undefined;
      const clusteredParam = clustered ? String(clustered).toLowerCase() : undefined;
      const clusteredFlag = clusteredParam === undefined ? true : !['false', '0', 'no'].includes(clusteredParam);
      const includeEntity = String(include_entity ?? "false").toLowerCase() === "true";

      let resolvedPortId = port_id ? String(port_id) : undefined;
      const portQuery = port ? String(port).trim() : undefined;
      if (!resolvedPortId && portQuery) {
        const normalized = portQuery.toLowerCase();
        const exactMatches = await db
          .select()
          .from(ports)
          .where(or(
            sql`lower(${ports.id}) = ${normalized}`,
            sql`lower(${ports.code}) = ${normalized}`,
            sql`lower(${ports.unlocode}) = ${normalized}`,
            sql`lower(${ports.name}) = ${normalized}`,
          ))
          .limit(2);

        if (exactMatches.length === 1) {
          resolvedPortId = exactMatches[0].id;
        } else if (exactMatches.length > 1) {
          return res.status(400).json({ error: 'Ambiguous port query (exact match)' });
        } else {
          const partialMatches = await db
            .select()
            .from(ports)
            .where(sql`${ports.name} ILIKE ${`%${portQuery}%`}`)
            .limit(2);

          if (partialMatches.length === 0) {
            return res.status(404).json({ error: 'Port not found for port filter' });
          }
          if (partialMatches.length > 1) {
            return res.status(400).json({ error: 'Ambiguous port query (partial match)' });
          }
          resolvedPortId = partialMatches[0].id;
        }
      }

      if (!day && !day_from && !day_to) {
        const conditions = [] as any[];
        if (resolvedPortId) {
          conditions.push(eq(signals.entityType, 'port'));
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
                END >= ${rank}`,
            );
          }
        }

        const whereSql = conditions.length > 0 ? sql`${sql.join(conditions, sql` AND `)}` : sql`1=1`;
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

      let entityMap: Map<string, { id: string; name: string; code: string; unlocode: string }> | null = null;
      if (includeEntity) {
        const portIds = Array.from(new Set(items
          .filter((signal) => signal.entityType === 'port')
          .map((signal) => signal.entityId)));
        if (portIds.length > 0) {
          const portRows = await db
            .select({
              id: ports.id,
              name: ports.name,
              code: ports.code,
              unlocode: ports.unlocode,
            })
            .from(ports)
            .where(inArray(ports.id, portIds));
          entityMap = new Map(portRows.map((row) => [row.id, row]));
        } else {
          entityMap = new Map();
        }
      }

      const compat = String(req.query.compat ?? "false").toLowerCase() === "true";
      const mapped = items.map((signal) => buildSignalResponse(signal, {
        compat,
        includeEntity,
        entityMap: entityMap ?? new Map<string, SignalEntity>(),
      }));

      res.json({ items: mapped, total });
    } catch (error) {
      console.error('V1 signals list error:', error);
      res.status(500).json({ error: 'Failed to fetch signals' });
    }
  });

  // V1 Signals - Get by ID
  app.get('/v1/signals/:id', optionalAuth, async (req, res) => {
    try {
      const signal = await getSignalById(req.params.id);
      if (!signal) {
        return res.status(404).json({ error: 'Signal not found' });
      }

      const compat = String(req.query.compat ?? "false").toLowerCase() === "true";
      const includeEntity = String(req.query.include_entity ?? "false").toLowerCase() === "true";
      let entityMap: Map<string, SignalEntity> | undefined;
      if (includeEntity && signal.entityType === 'port') {
        const portRow = await db
          .select({
            id: ports.id,
            name: ports.name,
            code: ports.code,
            unlocode: ports.unlocode,
          })
          .from(ports)
          .where(eq(ports.id, signal.entityId))
          .limit(1);
        if (portRow.length > 0) {
          entityMap = new Map([[portRow[0].id, {
            id: portRow[0].id,
            type: "port",
            name: portRow[0].name,
            code: portRow[0].code,
            unlocode: portRow[0].unlocode,
          }]]);
        }
      }

      res.json(buildSignalResponse(signal, {
        compat,
        includeEntity,
        entityMap,
      }));
    } catch (error) {
      console.error('V1 signal detail error:', error);
      res.status(500).json({ error: 'Failed to fetch signal' });
    }
  });

  // V1 Alert Deliveries - List with filters (read-only)
    app.get('/v1/alert-deliveries', authenticateApiKey, async (req, res) => {
      try {
        const {
          day,
          days,
          port,
          subscription_id,
          run_id,
          status,
          destination,
          severity_min,
          is_test,
          include_entity,
          cursor,
          limit = '50',
        } = req.query;

        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });

      const dayExact = day ? parseSignalDay(String(day)) : null;
      if (day && !dayExact) {
        return res.status(400).json({ error: 'day must be YYYY-MM-DD' });
      }

      let resolvedPortId: string | undefined;
      const portQuery = port ? String(port).trim() : undefined;
      if (portQuery) {
        const normalized = portQuery.toLowerCase();
        const exactMatches = await db
          .select()
          .from(ports)
          .where(or(
            sql`lower(${ports.id}) = ${normalized}`,
            sql`lower(${ports.code}) = ${normalized}`,
            sql`lower(${ports.unlocode}) = ${normalized}`,
            sql`lower(${ports.name}) = ${normalized}`,
          ))
          .limit(2);
        if (exactMatches.length === 1) {
          resolvedPortId = exactMatches[0].id;
        } else if (exactMatches.length > 1) {
          return res.status(400).json({ error: 'Ambiguous port query (exact match)' });
        } else {
          const partialMatches = await db
            .select()
            .from(ports)
            .where(sql`${ports.name} ILIKE ${`%${portQuery}%`}`)
            .limit(2);
          if (partialMatches.length === 0) {
            return res.status(404).json({ error: 'Port not found for port filter' });
          }
          if (partialMatches.length > 1) {
            return res.status(400).json({ error: 'Ambiguous port query (partial match)' });
          }
          resolvedPortId = partialMatches[0].id;
        }
      }

        const limitNum = Math.min(parseInt(String(limit)) || 50, 200);
        const daysNum = days ? Math.min(Math.max(parseInt(String(days)) || 30, 1), 365) : undefined;
        const includeEntity = String(include_entity ?? "false").toLowerCase() === "true";

        let cursorCreatedAt: Date | null = null;
        let cursorId: string | null = null;
        if (cursor) {
          try {
            const decoded = Buffer.from(String(cursor), "base64").toString("utf8");
            const [createdAtIso, id] = decoded.split("|");
            const createdAt = new Date(createdAtIso);
            if (createdAtIso && id && !Number.isNaN(createdAt.getTime())) {
              cursorCreatedAt = createdAt;
              cursorId = id;
            } else {
              return res.status(400).json({ error: "Invalid cursor" });
            }
          } catch {
            return res.status(400).json({ error: "Invalid cursor" });
          }
        }

        const { items, total } = await listAlertDeliveries({
          days: daysNum,
          day: dayExact ?? null,
          tenantId,
          userId,
          entityId: resolvedPortId,
          subscriptionId: subscription_id ? String(subscription_id) : undefined,
          runId: run_id ? String(run_id) : undefined,
          status: status && String(status).toUpperCase() === "DLQ" ? undefined : status ? String(status) : undefined,
          destinationType: destination ? String(destination).toUpperCase() : undefined,
          isTest: typeof is_test === "string" ? String(is_test).toLowerCase() === "true" : undefined,
          cursorCreatedAt,
          cursorId,
          limit: limitNum,
        });

        const deliverySubscriptionIds = Array.from(new Set(items.map((row) => row.subscriptionId)));
        const subscriptionRows = deliverySubscriptionIds.length > 0
          ? await db.select({
            id: alertSubscriptions.id,
            scope: alertSubscriptions.scope,
            entityId: alertSubscriptions.entityId,
            entityType: alertSubscriptions.entityType,
          })
            .from(alertSubscriptions)
            .where(and(
              inArray(alertSubscriptions.id, deliverySubscriptionIds),
              eq(alertSubscriptions.tenantId, tenantId),
            ))
          : [];
        const subscriptionMap = new Map(subscriptionRows.map((row) => [row.id, row]));

      const deliveryIds = items.map((row) => row.id);
      const dlqRows = deliveryIds.length > 0
        ? await db.select({
          deliveryId: alertDlq.deliveryId,
          attemptCount: alertDlq.attemptCount,
          maxAttempts: alertDlq.maxAttempts,
          nextAttemptAt: alertDlq.nextAttemptAt,
        })
          .from(alertDlq)
          .where(and(
            inArray(alertDlq.deliveryId, deliveryIds),
            eq(alertDlq.tenantId, tenantId),
          ))
        : [];
      const dlqMap = new Map(dlqRows.map((row) => [row.deliveryId, row]));

      const clusterIds = Array.from(new Set(items.map((row) => row.clusterId).filter(Boolean))) as string[];
      const signalRows = clusterIds.length > 0
        ? await db
          .select({
            id: signals.id,
            clusterId: signals.clusterId,
            clusterType: signals.clusterType,
            clusterSeverity: signals.clusterSeverity,
            clusterSummary: signals.clusterSummary,
            confidenceScore: signals.confidenceScore,
            confidenceBand: signals.confidenceBand,
            method: signals.method,
            entityId: signals.entityId,
            day: signals.day,
            metadata: signals.metadata,
            createdAt: signals.createdAt,
          })
          .from(signals)
          .where(inArray(signals.clusterId, clusterIds))
        : [];

      const signalMap = new Map<string, typeof signalRows[number]>();
      const rankSignal = (row: typeof signalRows[number]) => {
        const severity = String(row.clusterSeverity ?? "LOW").toUpperCase();
        const rank = SEVERITY_RANK[severity as keyof typeof SEVERITY_RANK] ?? 0;
        const confidence = Number(row.confidenceScore ?? 0);
        return { rank, confidence };
      };
      for (const row of signalRows) {
        const key = `${row.clusterId}|${row.entityId}|${row.day instanceof Date ? formatSignalDay(row.day) : String(row.day)}`;
        const existing = signalMap.get(key);
        if (!existing) {
          signalMap.set(key, row);
          continue;
        }
        const currentRank = rankSignal(existing);
        const nextRank = rankSignal(row);
        if (nextRank.rank > currentRank.rank) {
          signalMap.set(key, row);
        } else if (nextRank.rank === currentRank.rank && nextRank.confidence > currentRank.confidence) {
          signalMap.set(key, row);
        }
      }

      let entityMap: Map<string, { id: string; name: string; code: string; unlocode: string }> | null = null;
      if (includeEntity) {
        const portIds = Array.from(new Set(items.map((row) => row.entityId)));
        if (portIds.length > 0) {
          const portRows = await db
            .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
            .from(ports)
            .where(inArray(ports.id, portIds));
          entityMap = new Map(portRows.map((row) => [row.id, row]));
        } else {
          entityMap = new Map();
        }
      }

        const mapped = items.map((row) => {
          const entityRow = includeEntity ? entityMap?.get(row.entityId) : undefined;
          const key = `${row.clusterId}|${row.entityId}|${row.day instanceof Date ? formatSignalDay(row.day) : String(row.day)}`;
          const signalRow = signalMap.get(key);
          const alertPayload = signalRow ? buildSignalClusterAlertPayload({
            day: signalRow.day,
            entityType: "port",
            entityId: signalRow.entityId,
            clusterId: signalRow.clusterId,
            clusterSeverity: signalRow.clusterSeverity,
            confidenceScore: signalRow.confidenceScore,
            confidenceBand: signalRow.confidenceBand,
            clusterSummary: signalRow.clusterSummary,
            metadata: signalRow.metadata ?? {},
          }) : null;
          const dlqRow = dlqMap.get(row.id);
          const dlqPending = Boolean(dlqRow);
          const dlqTerminal = dlqRow ? dlqRow.attemptCount >= dlqRow.maxAttempts : false;
          const subRow = subscriptionMap.get(row.subscriptionId);
          return {
            id: row.id,
            run_id: row.runId,
            subscription_id: row.subscriptionId,
            scope: subRow?.scope ?? "PORT",
            cluster_id: row.clusterId,
            cluster_type: signalRow?.clusterType ?? null,
            cluster_summary: signalRow?.clusterSummary ?? null,
            cluster_severity: signalRow?.clusterSeverity ?? null,
            confidence_score: signalRow?.confidenceScore ?? null,
            confidence_band: signalRow?.confidenceBand ?? null,
            method: signalRow?.method ?? null,
            entity_type: row.entityType,
            entity_id: row.entityId,
          day: row.day instanceof Date ? formatSignalDay(row.day) : String(row.day),
          destination_type: row.destinationType,
          endpoint: row.endpoint,
          status: row.status,
          is_test: row.isTest,
          dlq_pending: dlqPending,
          dlq_terminal: dlqTerminal,
          dlq_attempts: dlqRow?.attemptCount ?? null,
          dlq_next_attempt_at: dlqRow?.nextAttemptAt ?? null,
          alert_payload: alertPayload,
          attempts: row.attempts,
          last_http_status: row.lastHttpStatus,
          latency_ms: row.latencyMs,
          error: row.error,
          sent_at: row.sentAt,
          created_at: row.createdAt,
          ...(entityRow ? {
            entity: {
              id: entityRow.id,
              type: "port",
              name: entityRow.name,
              code: entityRow.code,
              unlocode: entityRow.unlocode,
            },
          } : {}),
        };
      });

      let filtered = mapped;
      const statusParam = status ? String(status).toUpperCase() : undefined;
      if (statusParam === "DLQ") {
        filtered = filtered.filter((row) => row.dlq_pending);
      } else if (statusParam === "SKIPPED") {
        filtered = filtered.filter((row) => String(row.status).startsWith("SKIPPED"));
      }
      if (severity_min) {
        const required = SEVERITY_RANK[String(severity_min).toUpperCase() as keyof typeof SEVERITY_RANK] ?? 0;
        filtered = filtered.filter((row) => {
          const severity = String(row.cluster_severity ?? "LOW").toUpperCase();
          const rank = SEVERITY_RANK[severity as keyof typeof SEVERITY_RANK] ?? 0;
          return rank >= required;
        });
      }

      const last = items.length > 0 ? items[items.length - 1] : null;
      const nextCursor = items.length === limitNum && last?.createdAt
        ? Buffer.from(`${new Date(last.createdAt as any).toISOString()}|${last.id}`).toString("base64")
        : null;

      res.json({
        version: "1",
        items: filtered,
        total,
        next_cursor: nextCursor,
      });
    } catch (error) {
      console.error('V1 alert deliveries error:', error);
      res.status(500).json({ error: 'Failed to fetch alert deliveries' });
      }
    });

    app.get('/v1/alert-deliveries/:id', authenticateApiKey, async (req, res) => {
      try {
        const id = req.params.id;
        const includeEntity = String(req.query.include_entity ?? "false").toLowerCase() === "true";
        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
        const [delivery] = await db
          .select()
          .from(alertDeliveries)
          .innerJoin(alertSubscriptions, eq(alertDeliveries.subscriptionId, alertSubscriptions.id))
          .where(and(
            eq(alertDeliveries.id, id),
            eq(alertSubscriptions.userId, userId),
            eq(alertSubscriptions.tenantId, tenantId),
          ))
          .limit(1);
        if (!delivery) return res.status(404).json({ error: "Delivery not found" });

        const deliveryRow = (delivery as any).alert_deliveries ?? delivery;
        const subscription = (delivery as any).alert_subscriptions;

        const [dlqRow] = await db
          .select({
            deliveryId: alertDlq.deliveryId,
            attemptCount: alertDlq.attemptCount,
            maxAttempts: alertDlq.maxAttempts,
            nextAttemptAt: alertDlq.nextAttemptAt,
            lastError: alertDlq.lastError,
          })
            .from(alertDlq)
            .where(and(
              eq(alertDlq.deliveryId, deliveryRow.id),
              eq(alertDlq.tenantId, tenantId),
            ))
          .limit(1);

        const attempts = await db
          .select()
            .from(alertDeliveryAttempts)
            .where(and(
              eq(alertDeliveryAttempts.deliveryId, deliveryRow.id),
              eq(alertDeliveryAttempts.tenantId, tenantId),
            ))
          .orderBy(alertDeliveryAttempts.createdAt);

        const [signalRow] = await db
          .select({
            id: signals.id,
            clusterId: signals.clusterId,
            clusterType: signals.clusterType,
            clusterSeverity: signals.clusterSeverity,
            clusterSummary: signals.clusterSummary,
            confidenceScore: signals.confidenceScore,
            confidenceBand: signals.confidenceBand,
            method: signals.method,
            entityId: signals.entityId,
            day: signals.day,
            metadata: signals.metadata,
            createdAt: signals.createdAt,
          })
            .from(signals)
            .where(eq(signals.clusterId, deliveryRow.clusterId))
          .limit(1);

        const alertPayload = signalRow ? buildSignalClusterAlertPayload({
          day: signalRow.day,
          entityType: "port",
          entityId: signalRow.entityId,
          clusterId: signalRow.clusterId,
          clusterSeverity: signalRow.clusterSeverity,
          confidenceScore: signalRow.confidenceScore,
          confidenceBand: signalRow.confidenceBand,
          clusterSummary: signalRow.clusterSummary,
          metadata: signalRow.metadata ?? {},
        }) : null;

        let entity = null as any;
        if (includeEntity) {
            if (subscription?.scope === "GLOBAL" || deliveryRow.entityId === GLOBAL_SCOPE_ENTITY_ID) {
            entity = { id: GLOBAL_SCOPE_ENTITY_ID, type: "port", name: "All ports", code: "ALL", unlocode: "ALL" };
          } else {
            const [portRow] = await db
              .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
              .from(ports)
                .where(eq(ports.id, deliveryRow.entityId))
              .limit(1);
            if (portRow) {
              entity = { id: portRow.id, type: "port", name: portRow.name, code: portRow.code, unlocode: portRow.unlocode };
            }
          }
        }

        const response = {
            id: deliveryRow.id,
            run_id: deliveryRow.runId,
            subscription_id: deliveryRow.subscriptionId,
          scope: subscription?.scope ?? "PORT",
            cluster_id: deliveryRow.clusterId,
          cluster_type: signalRow?.clusterType ?? null,
          cluster_summary: signalRow?.clusterSummary ?? null,
          cluster_severity: signalRow?.clusterSeverity ?? null,
          confidence_score: signalRow?.confidenceScore ?? null,
          confidence_band: signalRow?.confidenceBand ?? null,
          method: signalRow?.method ?? null,
            entity_type: deliveryRow.entityType,
            entity_id: deliveryRow.entityId,
            day: deliveryRow.day instanceof Date ? formatSignalDay(deliveryRow.day) : String(deliveryRow.day),
            destination_type: deliveryRow.destinationType,
            endpoint: deliveryRow.endpoint,
            status: deliveryRow.status,
            is_test: deliveryRow.isTest,
          dlq_pending: Boolean(dlqRow),
          dlq_terminal: dlqRow ? dlqRow.attemptCount >= dlqRow.maxAttempts : false,
          dlq_attempts: dlqRow?.attemptCount ?? null,
          dlq_next_attempt_at: dlqRow?.nextAttemptAt ?? null,
          alert_payload: alertPayload,
            attempts: deliveryRow.attempts,
            last_http_status: deliveryRow.lastHttpStatus,
            latency_ms: deliveryRow.latencyMs,
            error: deliveryRow.error,
            sent_at: deliveryRow.sentAt,
            created_at: deliveryRow.createdAt,
          attempt_history: attempts.map((row) => ({
            attempt_no: row.attemptNo,
            status: row.status,
            latency_ms: row.latencyMs,
            http_status: row.httpStatus,
            error: row.error,
            sent_at: row.sentAt,
            created_at: row.createdAt,
          })),
          ...(entity ? { entity } : {}),
        };

        res.json({ version: "1", item: response });
      } catch (error: any) {
        res.status(500).json({ error: error.message || "Failed to load delivery" });
      }
    });

  // V1 Alert Subscriptions
    app.get('/v1/alert-subscriptions', authenticateApiKey, async (req, res) => {
      try {
        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
        const includeEntity = String(req.query?.include_entity ?? "false") === "true";
        const limitNum = Math.min(parseInt(String(req.query?.limit ?? "50")) || 50, 200);
        const cursor = req.query?.cursor;
        let cursorCreatedAt: string | null = null;
        let cursorId: string | null = null;
        if (cursor) {
          try {
            const decoded = Buffer.from(String(cursor), "base64").toString("utf8");
            const [createdAtIso, id] = decoded.split("|");
            const createdAtMs = Date.parse(createdAtIso);
            if (createdAtIso && id && !Number.isNaN(createdAtMs)) {
              cursorCreatedAt = createdAtIso;
              cursorId = id;
            } else {
              return res.status(400).json({ error: "Invalid cursor" });
            }
          } catch {
            return res.status(400).json({ error: "Invalid cursor" });
          }
        }

        const conditions = [
          eq(alertSubscriptions.tenantId, tenantId),
          eq(alertSubscriptions.userId, userId),
        ];
        if (cursorCreatedAt && cursorId) {
          conditions.push(
            sql`(${alertSubscriptions.createdAt} < ${cursorCreatedAt}::timestamptz OR (${alertSubscriptions.createdAt} = ${cursorCreatedAt}::timestamptz AND ${alertSubscriptions.id} < ${cursorId}))`,
          );
        }

        const rows = await db
          .select({
            id: alertSubscriptions.id,
            tenantId: alertSubscriptions.tenantId,
            userId: alertSubscriptions.userId,
            scope: alertSubscriptions.scope,
            entityType: alertSubscriptions.entityType,
            entityId: alertSubscriptions.entityId,
            severityMin: alertSubscriptions.severityMin,
            confidenceMin: alertSubscriptions.confidenceMin,
            channel: alertSubscriptions.channel,
            endpoint: alertSubscriptions.endpoint,
            secret: alertSubscriptions.secret,
            signatureVersion: alertSubscriptions.signatureVersion,
            isEnabled: alertSubscriptions.isEnabled,
            lastTestAt: alertSubscriptions.lastTestAt,
            lastTestStatus: alertSubscriptions.lastTestStatus,
            lastTestError: alertSubscriptions.lastTestError,
            createdAt: alertSubscriptions.createdAt,
            updatedAt: alertSubscriptions.updatedAt,
            createdAtRaw: sql<string>`to_char(${alertSubscriptions.createdAt}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')`,
          })
          .from(alertSubscriptions)
          .where(and(...conditions))
          .orderBy(desc(alertSubscriptions.createdAt), desc(alertSubscriptions.id))
          .limit(limitNum);

        let entityMap = new Map<string, { id: string; type: "port"; name: string; code: string; unlocode: string }>();
        if (includeEntity) {
          const ids = rows
            .map((row) => row.entityId)
            .filter((id) => id && id !== GLOBAL_SCOPE_ENTITY_ID);
          if (ids.length) {
            const portRows = await db
              .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
              .from(ports)
              .where(inArray(ports.id, ids));
            for (const port of portRows) {
              entityMap.set(port.id, {
                id: port.id,
                type: "port",
                name: port.name,
                code: port.code ?? port.unlocode ?? "",
                unlocode: port.unlocode ?? port.code ?? "",
              });
            }
          }
        }

        const items = rows.map((row) => ({
          id: row.id,
          user_id: row.userId,
          scope: row.scope ?? "PORT",
          destination_type: row.channel,
          destination: row.endpoint,
          entity_type: row.entityType,
          entity_id: row.entityId,
          ...(includeEntity
            ? {
                entity:
                  row.scope === "GLOBAL" || row.entityId === GLOBAL_SCOPE_ENTITY_ID
                    ? { id: GLOBAL_SCOPE_ENTITY_ID, type: "port", name: "All ports", code: "ALL", unlocode: "ALL" }
                    : entityMap.get(row.entityId) ?? null,
              }
            : {}),
          severity_min: row.severityMin,
          enabled: row.isEnabled,
          signature_version: row.signatureVersion,
          has_secret: Boolean(row.secret),
          created_at: row.createdAt?.toISOString?.() ?? row.createdAt,
        updated_at: row.updatedAt?.toISOString?.() ?? row.updatedAt,
        last_test_at: row.lastTestAt?.toISOString?.() ?? null,
        last_test_status: row.lastTestStatus ?? null,
        last_test_error: row.lastTestError ?? null,
      }));

      const last = rows.length > 0 ? rows[rows.length - 1] : null;
      const nextCursor = rows.length === limitNum && last?.createdAtRaw
        ? Buffer.from(`${last.createdAtRaw}|${last.id}`).toString("base64")
        : null;

      res.json({ version: "1", items, next_cursor: nextCursor });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to list subscriptions" });
    }
  });

    app.post('/v1/alert-subscriptions', authenticateApiKey, async (req, res) => {
      try {
      const allowHttp = process.env.NODE_ENV !== "production";
      const userId = req.auth?.userId;
      const tenantId = req.auth?.tenantId;
      if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
        const destinationType = String(req.body?.destination_type ?? req.body?.channel ?? "WEBHOOK").toUpperCase();
        const destination = String(req.body?.destination ?? req.body?.endpoint ?? "").trim();
        const severityMin = String(req.body?.severity_min ?? "HIGH").toUpperCase();
        const enabled = req.body?.enabled !== false;
      const signatureVersion = String(req.body?.signature_version ?? "v1");
      const providedSecret = req.body?.secret ? String(req.body?.secret) : null;
      const scope = normalizeScope(req.body?.scope);
      let entityId = req.body?.entity_id ? String(req.body?.entity_id) : null;

        if (!destination) {
          return res.status(400).json({ error: "destination is required" });
        }
        if (!["WEBHOOK", "EMAIL"].includes(destinationType)) {
          return res.status(400).json({ error: "destination_type must be WEBHOOK or EMAIL" });
      }
      if (!["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityMin)) {
        return res.status(400).json({ error: "invalid severity_min" });
      }

      if (destinationType === "WEBHOOK") {
        if (!isValidWebhookUrl(destination, allowHttp)) {
          return res.status(400).json({ error: "invalid webhook url" });
        }
      } else {
        const email = normalizeEmail(destination);
        if (!isValidEmail(email)) {
          return res.status(400).json({ error: "invalid email" });
        }
      }

      const secret = destinationType === "WEBHOOK"
        ? (providedSecret ?? generateSecret())
        : null;

      if (scope === "GLOBAL") {
        entityId = GLOBAL_SCOPE_ENTITY_ID;
      } else if (!entityId) {
        return res.status(400).json({ error: "entity_id is required when scope=PORT" });
      }

        const [created] = await db
          .insert(alertSubscriptions)
          .values({
            tenantId,
            userId,
            scope,
            entityType: "port",
            entityId,
            severityMin,
            channel: destinationType,
            endpoint: destinationType === "EMAIL" ? normalizeEmail(destination) : destination,
            secret,
            signatureVersion,
            isEnabled: enabled,
            updatedAt: new Date(),
          })
        .returning();

      res.status(201).json({
        version: "1",
        id: created.id,
        user_id: created.userId,
        scope: created.scope ?? "PORT",
        destination_type: created.channel,
        destination: created.endpoint,
        entity_type: created.entityType,
        entity_id: created.entityId,
        severity_min: created.severityMin,
        enabled: created.isEnabled,
        signature_version: created.signatureVersion,
        has_secret: Boolean(created.secret),
        created_at: created.createdAt?.toISOString?.(),
        updated_at: created.updatedAt?.toISOString?.(),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to create subscription" });
    }
  });

    app.patch('/v1/alert-subscriptions/:id', authenticateApiKey, async (req, res) => {
      try {
        const id = req.params.id;
        const allowHttp = process.env.NODE_ENV !== "production";
        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
        const [existing] = await db
          .select()
          .from(alertSubscriptions)
          .where(and(
            eq(alertSubscriptions.id, id),
            eq(alertSubscriptions.userId, userId),
            eq(alertSubscriptions.tenantId, tenantId),
          ))
          .limit(1);
        if (!existing) return res.status(404).json({ error: "subscription not found" });

        const updates: any = { updatedAt: new Date() };

        if (req.body?.scope) {
          const scope = normalizeScope(req.body.scope);
          updates.scope = scope;
          if (scope === "GLOBAL") {
            updates.entityId = GLOBAL_SCOPE_ENTITY_ID;
          }
          if (scope === "PORT" && !req.body?.entity_id) {
            return res.status(400).json({ error: "entity_id is required when scope=PORT" });
          }
        }

        if (req.body?.entity_id) {
          updates.entityId = String(req.body.entity_id);
        }

        if (req.body?.severity_min) {
          const severityMin = String(req.body.severity_min).toUpperCase();
          if (!["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(severityMin)) {
            return res.status(400).json({ error: "invalid severity_min" });
          }
        updates.severityMin = severityMin;
      }

      if (req.body?.enabled !== undefined) {
        updates.isEnabled = Boolean(req.body.enabled);
      }

        if (req.body?.destination) {
          const destination = String(req.body.destination).trim();
          if (existing.channel === "WEBHOOK") {
            if (!isValidWebhookUrl(destination, allowHttp)) {
              return res.status(400).json({ error: "invalid webhook url" });
            }
            updates.endpoint = destination;
          } else {
          const email = normalizeEmail(destination);
          if (!isValidEmail(email)) return res.status(400).json({ error: "invalid email" });
          updates.endpoint = email;
        }
      }

        const [updated] = await db
          .update(alertSubscriptions)
          .set(updates)
          .where(and(
            eq(alertSubscriptions.id, id),
            eq(alertSubscriptions.userId, userId),
            eq(alertSubscriptions.tenantId, tenantId),
          ))
          .returning();

      if (!updated) return res.status(404).json({ error: "subscription not found" });

      res.json({
        version: "1",
        id: updated.id,
        user_id: updated.userId,
        destination_type: updated.channel,
        destination: updated.endpoint,
        severity_min: updated.severityMin,
        enabled: updated.isEnabled,
        signature_version: updated.signatureVersion,
        has_secret: Boolean(updated.secret),
        created_at: updated.createdAt?.toISOString?.(),
        updated_at: updated.updatedAt?.toISOString?.(),
        last_test_at: updated.lastTestAt?.toISOString?.() ?? null,
        last_test_status: updated.lastTestStatus ?? null,
        last_test_error: updated.lastTestError ?? null,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to update subscription" });
    }
  });

    app.post('/v1/alert-subscriptions/:id/rotate-secret', authenticateApiKey, async (req, res) => {
      try {
        const id = req.params.id;
        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });
        const newSecret = generateSecret();
        const [updated] = await db
          .update(alertSubscriptions)
          .set({ secret: newSecret, updatedAt: new Date() })
          .where(and(
            eq(alertSubscriptions.id, id),
            eq(alertSubscriptions.userId, userId),
            eq(alertSubscriptions.tenantId, tenantId),
          ))
          .returning();

        if (!updated) return res.status(404).json({ error: "subscription not found" });

        res.json({ version: "1", rotated: true, secret: newSecret });
      } catch (error: any) {
        res.status(500).json({ error: error.message || "Failed to rotate secret" });
      }
    });

    app.post('/v1/alert-subscriptions/:id/test', authenticateApiKey, async (req, res) => {
      try {
        const id = req.params.id;
        const mode = String(req.body?.mode ?? "synthetic");
        const severity = String(req.body?.severity ?? "HIGH").toUpperCase();
        const now = new Date();
        const userId = req.auth?.userId;
        const tenantId = req.auth?.tenantId;
        if (!userId || !tenantId) return res.status(401).json({ error: "API key required" });

        const [subscription] = await db
          .select()
          .from(alertSubscriptions)
          .where(and(
            eq(alertSubscriptions.id, id),
            eq(alertSubscriptions.userId, userId),
            eq(alertSubscriptions.tenantId, tenantId),
          ))
          .limit(1);

        if (!subscription) {
          return res.status(404).json({ error: "subscription not found" });
        }

      const rateKey = subscription.id;
      const windowMs = 60_000;
      const timestamps = (subscriptionTestRateLimit.get(rateKey) ?? []).filter((ts) => now.getTime() - ts < windowMs);
      if (timestamps.length >= 5) {
        return res.status(429).json({ error: "rate limit exceeded" });
      }
      timestamps.push(now.getTime());
      subscriptionTestRateLimit.set(rateKey, timestamps);

      const payload = {
        event_type: "TEST_ALERT",
        sent_at: now.toISOString(),
        subscription_id: subscription.id,
        severity,
        mode,
        sample: req.body?.include_sample_signal ? { note: "Sample signal included" } : undefined,
      };

      let status: "SENT" | "FAILED" = "SENT";
      let latencyMs: number | null = null;
      let httpStatus: number | null = null;
      let errorMessage: string | null = null;

      if (subscription.channel === "WEBHOOK") {
        const { body, headers } = buildWebhookRequest({
          payload,
          secret: subscription.secret ?? null,
          subscriptionId: subscription.id,
          clusterId: `TEST:${subscription.id}`,
          day: now.toISOString().slice(0, 10),
          now,
        });
        try {
          const result = await sendWebhook({ endpoint: subscription.endpoint, body, headers });
          const attemptLogs = (result as any)?.attemptLogs ?? [];
          const last = attemptLogs.length ? attemptLogs[attemptLogs.length - 1] : null;
          latencyMs = last?.latency_ms ?? null;
          httpStatus = last?.http_status ?? (result as any)?.status ?? null;
        } catch (error: any) {
          status = "FAILED";
          errorMessage = error?.message ?? "Test delivery failed";
        }
      } else {
        try {
          await sendEmail({
            to: subscription.endpoint,
            subject: "[Veriscope] TEST ALERT",
            text: "Test alert delivery.",
          });
        } catch (error: any) {
          status = "FAILED";
          errorMessage = error?.message ?? "Test email failed";
        }
      }

      const [testRun] = await db.insert(alertRuns).values({
        tenantId,
        status: "TEST",
        startedAt: now,
        finishedAt: now,
        summary: { mode: "test" },
      }).returning();

        const [delivery] = await db.insert(alertDeliveries).values({
          runId: testRun.id,
          tenantId,
          userId: subscription.userId,
          subscriptionId: subscription.id,
          clusterId: `TEST:${subscription.id}`,
          entityType: subscription.entityType,
          entityId: subscription.entityId,
          day: now,
          destinationType: subscription.channel,
          endpoint: subscription.endpoint,
          status,
          attempts: 1,
          lastHttpStatus: httpStatus,
          latencyMs: latencyMs,
          error: errorMessage,
          sentAt: status === "SENT" ? now : null,
          isTest: true,
          createdAt: now,
        }).returning();

        await db.insert(alertDeliveryAttempts).values({
          tenantId,
          deliveryId: delivery.id,
          attemptNo: 1,
          status,
          latencyMs: latencyMs,
          httpStatus: httpStatus,
          error: errorMessage,
          sentAt: status === "SENT" ? now : null,
          createdAt: now,
        });

      await db.update(alertSubscriptions)
        .set({
          lastTestAt: now,
          lastTestStatus: status,
          lastTestError: errorMessage,
          updatedAt: now,
        })
        .where(and(eq(alertSubscriptions.id, subscription.id), eq(alertSubscriptions.tenantId, tenantId)));

      res.json({
        version: "1",
        status,
        latency_ms: latencyMs,
        http_status: httpStatus,
        error: errorMessage,
        delivery_id: delivery?.id,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message || "Failed to send test" });
    }
  });

  // V1 Vessels - List with filters
  app.get('/v1/vessels', optionalAuth, async (req, res) => {
    try {
      const { mmsi, imo, name } = req.query;
      const vessels = await storage.getVessels();
      
      let filtered = vessels;
      
      if (mmsi) {
        filtered = filtered.filter(v => v.mmsi === String(mmsi));
      } else if (imo) {
        filtered = filtered.filter(v => v.imo === String(imo));
      } else if (name) {
        const search = String(name).toLowerCase();
        filtered = filtered.filter(v => v.name.toLowerCase().includes(search));
      }
      
      res.json({
        items: filtered.map(v => ({
          id: v.id,
          mmsi: v.mmsi,
          imo: v.imo,
          name: v.name,
          vessel_type: v.vesselType,
          flag: v.flag,
        })),
      });
    } catch (error) {
      console.error('V1 vessels list error:', error);
      res.status(500).json({ error: 'Failed to fetch vessels' });
    }
  });

  // V1 Vessels - Get latest position
  app.get('/v1/vessels/:vessel_id/latest_position', optionalAuth, async (req, res) => {
    try {
      const { vessel_id } = req.params;
      const vessel = await storage.getVessel(vessel_id);
      
      if (!vessel) {
        return res.status(404).json({ error: 'Vessel not found' });
      }
      
      const positions = await storage.getVesselPositions(vessel_id);
      const latest = positions.sort((a, b) => 
        new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime()
      )[0];
      
      if (!latest) {
        return res.status(404).json({ error: 'No position data available' });
      }
      
      res.json({
        vessel_id: vessel.id,
        mmsi: vessel.mmsi,
        timestamp_utc: latest.timestampUtc || latest.timestamp,
        latitude: parseFloat(String(latest.latitude)),
        longitude: parseFloat(String(latest.longitude)),
        sog_knots: latest.sogKnots ? parseFloat(String(latest.sogKnots)) : (latest.speed ? parseFloat(String(latest.speed)) : null),
        cog_deg: latest.cogDeg ? parseFloat(String(latest.cogDeg)) : (latest.course ? parseFloat(String(latest.course)) : null),
      });
    } catch (error) {
      console.error('V1 vessel latest position error:', error);
      res.status(500).json({ error: 'Failed to fetch vessel position' });
    }
  });

  // V1 Vessels - Get positions (GeoJSON)
  app.get('/v1/vessels/positions', optionalAuth, async (req, res) => {
    try {
      const { bbox, since_minutes = '60', limit = '2000' } = req.query;
      
      if (!bbox) {
        return res.status(400).json({ error: 'bbox parameter is required (minLon,minLat,maxLon,maxLat)' });
      }
      
      const [minLon, minLat, maxLon, maxLat] = String(bbox).split(',').map(parseFloat);
      const sinceMinutes = parseInt(String(since_minutes)) || 60;
      const limitNum = Math.min(parseInt(String(limit)) || 2000, 5000);
      
      const sinceTime = new Date(Date.now() - sinceMinutes * 60 * 1000);
      
      const vessels = await storage.getVessels();
      const features: any[] = [];
      
      for (const vessel of vessels.slice(0, 100)) {
        const positions = await storage.getVesselPositions(vessel.id);
        const recentPositions = positions.filter(p => {
          const posTime = new Date(p.timestampUtc || p.timestamp || 0);
          const lat = parseFloat(String(p.latitude));
          const lon = parseFloat(String(p.longitude));
          return posTime >= sinceTime && 
                 lat >= minLat && lat <= maxLat &&
                 lon >= minLon && lon <= maxLon;
        });
        
        const latest = recentPositions.sort((a, b) => 
          new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime()
        )[0];
        
        if (latest) {
          features.push({
            type: 'Feature',
            geometry: {
              type: 'Point',
              coordinates: [parseFloat(String(latest.longitude)), parseFloat(String(latest.latitude))],
            },
            properties: {
              vessel_id: vessel.id,
              mmsi: vessel.mmsi,
              name: vessel.name,
              sog_knots: latest.sogKnots ? parseFloat(String(latest.sogKnots)) : (latest.speed ? parseFloat(String(latest.speed)) : null),
              cog_deg: latest.cogDeg ? parseFloat(String(latest.cogDeg)) : (latest.course ? parseFloat(String(latest.course)) : null),
              timestamp_utc: latest.timestampUtc || latest.timestamp,
            },
          });
        }
        
        if (features.length >= limitNum) break;
      }
      
      res.json({
        type: 'FeatureCollection',
        features,
      });
    } catch (error) {
      console.error('V1 vessels positions error:', error);
      res.status(500).json({ error: 'Failed to fetch vessel positions' });
    }
  });

  // GraphQL-style API endpoints (simplified REST for now)
  
  // Cache stats endpoint
  app.get('/api/cache/stats', authenticate, requireRole('admin'), (req, res) => {
    res.json({
      ...cacheService.getStats(),
      hitRate: `${(cacheService.getHitRate() * 100).toFixed(2)}%`
    });
  });

  // Vessels endpoint with caching, optional pagination, and geo-filtering
  app.get('/api/vessels', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      const geoParams = parseGeoQueryParams(req);
      
      const vessels = await cacheService.getOrSet(
        CACHE_KEYS.VESSELS,
        () => storage.getVessels(),
        CACHE_TTL.MEDIUM
      );
      
      // Get latest positions for all vessels
      const latestPositions = await storage.getLatestVesselPositions();
      
      // Map positions by vesselId for quick lookup
      const positionMap = new Map<string, any>();
      for (const pos of latestPositions) {
        if (!positionMap.has(pos.vesselId) || new Date(pos.timestamp || 0) > new Date(positionMap.get(pos.vesselId).timestamp || 0)) {
          positionMap.set(pos.vesselId, pos);
        }
      }
      
      // Attach latest position to each vessel
      let result = vessels.map((vessel: any) => {
        const position = positionMap.get(vessel.id);
        return {
          ...vessel,
          position: position ? {
            latitude: position.latitude,
            longitude: position.longitude,
            speedOverGround: position.speedOverGround || 0,
            navigationStatus: position.navigationStatus || 'unknown',
            timestamp: position.timestamp,
          } : null,
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
      res.status(500).json({ error: 'Failed to fetch vessels' });
    }
  });

  // Ports endpoint with caching, optional pagination, and geo-filtering (public access)
  app.get('/api/ports', optionalAuth, async (req, res) => {
    try {
      const usePagination = req.query.paginate === 'true';
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
      res.status(500).json({ error: 'Failed to fetch ports' });
    }
  });

  // Port statistics endpoint with caching
  app.get('/api/ports/:portId/stats', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId } = req.params;
      const stats = await cacheService.getOrSet(
        CACHE_KEYS.PORT_STATS(portId),
        () => storage.getLatestPortStats(portId),
        CACHE_TTL.MEDIUM
      );
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch port statistics' });
    }
  });

  // Storage sites endpoint with caching and optional pagination (public access)
  app.get('/api/storage/sites', optionalAuth, async (req, res) => {
    try {
      const { portId } = req.query;
      const usePagination = req.query.paginate === 'true';
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
      res.status(500).json({ error: 'Failed to fetch storage sites' });
    }
  });

  // Floating storage endpoint (public access)
  app.get('/api/storage/floating', optionalAuth, async (req, res) => {
    try {
      const { getFloatingStorageStats } = await import('./services/storageDataService');
      const data = await getFloatingStorageStats();
      res.json(data);
    } catch (error) {
      console.error('Error fetching floating storage:', error);
      res.status(500).json({ error: 'Failed to fetch floating storage data' });
    }
  });

  // SPR reserves endpoint (public access)
  app.get('/api/storage/spr', optionalAuth, async (req, res) => {
    try {
      const { getSprStats } = await import('./services/storageDataService');
      const data = await getSprStats();
      res.json(data);
    } catch (error) {
      console.error('Error fetching SPR data:', error);
      res.status(500).json({ error: 'Failed to fetch SPR reserves data' });
    }
  });

  // Storage time series endpoint (public access)
  app.get('/api/storage/timeseries', optionalAuth, async (req, res) => {
    try {
      const { metricType, region, storageType, weeks } = req.query;
      const { getStorageTimeSeriesData } = await import('./services/storageDataService');
      const data = await getStorageTimeSeriesData({
        metricType: metricType as string,
        region: region as string,
        storageType: storageType as string,
        weeks: weeks ? parseInt(weeks as string) : 52,
      });
      res.json(data);
    } catch (error) {
      console.error('Error fetching storage time series:', error);
      res.status(500).json({ error: 'Failed to fetch storage time series data' });
    }
  });

  // Signals endpoint with caching and optional pagination
  app.get('/api/signals', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 50 });
      
      const signals = await cacheService.getOrSet(
        CACHE_KEYS.ACTIVE_SIGNALS,
        () => storage.getActiveSignals(),
        CACHE_TTL.SHORT
      );
      
      if (usePagination) {
        res.json(paginateArray(signals, pagination));
      } else {
        res.json(signals);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch signals' });
    }
  });

  // Predictions endpoint with caching and optional pagination
  app.get('/api/predictions', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { target } = req.query;
      const usePagination = req.query.paginate === 'true';
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
      res.status(500).json({ error: 'Failed to fetch predictions' });
    }
  });

  // Port delay events endpoint
  app.get('/api/ports/:portId/delays', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId } = req.params;
      const { limit } = req.query;
      const delays = await storage.getPortDelayEvents(portId, limit ? parseInt(limit as string) : 50);
      res.json(delays);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch port delay events' });
    }
  });

  // Vessel delay snapshots endpoint
  app.get('/api/vessels/delays', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const { vesselId, portId, limit } = req.query;
      const snapshots = await storage.getVesselDelaySnapshots(
        vesselId as string | undefined,
        portId as string | undefined,
        limit ? parseInt(limit as string) : 50
      );
      res.json(snapshots);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch vessel delay snapshots' });
    }
  });

  // Market delay impact endpoint
  app.get('/api/market/delays/impact', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { portId, commodityId, limit } = req.query;
      const impacts = await storage.getMarketDelayImpacts(
        portId as string | undefined,
        commodityId as string | undefined,
        limit ? parseInt(limit as string) : 20
      );
      res.json(impacts);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market delay impacts' });
    }
  });

  // Delay-adjusted predictions endpoint
  app.get('/api/predictions/delay-adjusted', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { portId, commodityCode } = req.query;
      
      // Get latest market delay impact
      const impacts = await storage.getMarketDelayImpacts(portId as string | undefined, undefined, 1);
      const latestImpact = impacts[0];
      
      // Get base prediction
      const predictions = await storage.getLatestPredictions(commodityCode as string);
      const basePrediction = predictions[0];
      
      if (!basePrediction || !latestImpact) {
        return res.json({ 
          delayAdjusted: false,
          prediction: basePrediction,
          message: 'No delay impact data available'
        });
      }
      
      // Adjust prediction based on delay impact
      const priceImpact = parseFloat(latestImpact.priceImpact || '0');
      const basePrice = parseFloat(basePrediction.predictedPrice);
      const adjustedPrice = basePrice + priceImpact;
      
      res.json({
        delayAdjusted: true,
        basePrediction: basePrediction,
        delayImpact: latestImpact,
        adjustedPrediction: {
          ...basePrediction,
          predictedPrice: adjustedPrice.toFixed(2),
          adjustmentReason: `Adjusted for ${latestImpact.vesselCount} delayed vessels carrying ${latestImpact.totalDelayedVolume} tons`
        }
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch delay-adjusted predictions' });
    }
  });

  // Rotterdam data endpoints
  app.get('/api/rotterdam-data', authenticate, requirePermission('read:ports'), async (req, res) => {
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
      res.status(500).json({ error: 'Failed to fetch Rotterdam data' });
    }
  });
  
  app.get('/api/rotterdam-data/months', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const months = rotterdamDataService.getAvailableMonths();
      res.json(months);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch available months' });
    }
  });
  
  app.get('/api/rotterdam-data/latest', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const latest = rotterdamDataService.getLatestData();
      res.json(latest);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch latest Rotterdam data' });
    }
  });

  // Real-time Port of Rotterdam arrivals/departures (simulated API)
  app.get('/api/rotterdam/arrivals', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const arrivals = await rotterdamDataService.getExpectedArrivals();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        count: arrivals.length,
        arrivals,
        lastUpdated: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam arrivals' });
    }
  });

  app.get('/api/rotterdam/departures', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const departures = await rotterdamDataService.getRecentDepartures();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        count: departures.length,
        departures,
        lastUpdated: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam departures' });
    }
  });

  app.get('/api/rotterdam/vessels-at-port', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const vessels = await rotterdamDataService.getVesselsAtPort();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        atBerth: vessels.atBerth,
        atAnchor: vessels.atAnchor,
        totalAtBerth: vessels.atBerth.length,
        totalAtAnchor: vessels.atAnchor.length,
        lastUpdated: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch vessels at Rotterdam' });
    }
  });

  app.get('/api/rotterdam/activity-summary', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const summary = await rotterdamDataService.getPortActivitySummary();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        ...summary
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam activity summary' });
    }
  });

  // ===== CARGO CHAINS & TRADE FLOWS =====
  
  // Get all trade flows with cargo chain details
  app.get('/api/trade-flows', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { limit } = req.query;
      const flows = await storage.getActiveTradeFlows();
      
      // Enrich with cargo legs, STS events, and splits
      const enrichedFlows = await Promise.all(
        flows.slice(0, limit ? parseInt(limit as string) : 50).map(async (flow) => {
          const [legs, stsEvents, splits] = await Promise.all([
            storage.getCargoLegsByTradeFlow(flow.id),
            storage.getSTSEventsByTradeFlow(flow.id),
            storage.getCargoSplitsByTradeFlow(flow.id)
          ]);
          
          return {
            ...flow,
            cargoChain: legs,
            stsEvents,
            splits
          };
        })
      );
      
      res.json(enrichedFlows);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch trade flows' });
    }
  });

  // Get single trade flow with complete cargo chain
  app.get('/api/trade-flows/:flowId', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { flowId } = req.params;
      const flows = await storage.getActiveTradeFlows();
      const flow = flows.find(f => f.id === flowId);
      
      if (!flow) {
        return res.status(404).json({ error: 'Trade flow not found' });
      }
      
      const [legs, stsEvents, splits, vessel, commodity] = await Promise.all([
        storage.getCargoLegsByTradeFlow(flowId),
        storage.getSTSEventsByTradeFlow(flowId),
        storage.getCargoSplitsByTradeFlow(flowId),
        storage.getVesselByMMSI(flow.vesselId),
        storage.getCommodities().then(c => c.find(com => com.id === flow.commodityId))
      ]);
      
      res.json({
        ...flow,
        vessel,
        commodity,
        cargoChain: legs,
        stsEvents,
        splits
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch trade flow details' });
    }
  });

  // STS Events endpoints
  app.get('/api/sts-events', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { vesselId, limit } = req.query;
      
      if (vesselId) {
        const events = await storage.getSTSEventsByVessel(vesselId as string);
        res.json(events);
      } else {
        const events = await storage.getSTSEvents(limit ? parseInt(limit as string) : 50);
        res.json(events);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch STS events' });
    }
  });

  // Flow Forecasts endpoints
  app.get('/api/flow-forecasts', authenticate, requirePermission('read:predictions'), async (req, res) => {
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
        res.json(forecasts.slice(0, limit ? parseInt(limit as string) : 20));
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch flow forecasts' });
    }
  });

  // Cargo splits endpoint
  app.get('/api/cargo-splits', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { tradeFlowId, limit } = req.query;
      
      if (tradeFlowId) {
        const splits = await storage.getCargoSplitsByTradeFlow(tradeFlowId as string);
        res.json(splits);
      } else {
        const splits = await storage.getCargoSplits(limit ? parseInt(limit as string) : 50);
        res.json(splits);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch cargo splits' });
    }
  });

  // ===== MARITIME INTELLIGENCE =====
  
  // Port Calls endpoints with optional pagination
  app.get('/api/port-calls', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId, vesselId } = req.query;
      const usePagination = req.query.paginate === 'true';
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
      res.status(500).json({ error: 'Failed to fetch port calls' });
    }
  });

  app.post('/api/port-calls', authenticate, requirePermission('write:ports'), async (req, res) => {
    try {
      const portCall = await storage.createPortCall(req.body);
      res.status(201).json(portCall);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create port call' });
    }
  });

  // Container Operations endpoints with optional pagination
  app.get('/api/container-operations', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId, vesselId } = req.query;
      const usePagination = req.query.paginate === 'true';
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
      res.status(500).json({ error: 'Failed to fetch container operations' });
    }
  });

  app.get('/api/container-operations/stats/:portId', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId } = req.params;
      const stats = await storage.getContainerStatsByPort(portId);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch container stats' });
    }
  });

  app.post('/api/container-operations', authenticate, requirePermission('write:ports'), async (req, res) => {
    try {
      const operation = await storage.createContainerOperation(req.body);
      res.status(201).json(operation);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create container operation' });
    }
  });

  // Bunkering Events endpoints with optional pagination
  app.get('/api/bunkering-events', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const { vesselId, portId } = req.query;
      const usePagination = req.query.paginate === 'true';
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
      res.status(500).json({ error: 'Failed to fetch bunkering events' });
    }
  });

  app.get('/api/bunkering-events/stats/:vesselId', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const { vesselId } = req.params;
      const stats = await storage.getBunkeringStatsByVessel(vesselId);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch bunkering stats' });
    }
  });

  app.post('/api/bunkering-events', authenticate, requirePermission('write:vessels'), async (req, res) => {
    try {
      const event = await storage.createBunkeringEvent(req.body);
      res.status(201).json(event);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create bunkering event' });
    }
  });

  // Communications/Inbox endpoints with optional pagination
  app.get('/api/communications', authenticate, requirePermission('read:alerts'), async (req, res) => {
    try {
      const { userId } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      
      const communications = await storage.getCommunications(
        userId as string | undefined,
        500
      );
      
      if (usePagination) {
        res.json(paginateArray(communications, pagination));
      } else {
        res.json(communications);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch communications' });
    }
  });

  app.get('/api/communications/unread', authenticate, requirePermission('read:alerts'), async (req, res) => {
    try {
      const { userId } = req.query;
      const unread = await storage.getUnreadCommunications(userId as string || 'default');
      res.json(unread);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch unread communications' });
    }
  });

  app.post('/api/communications', authenticate, requirePermission('write:alerts'), async (req, res) => {
    try {
      const communication = await storage.createCommunication(req.body);
      res.status(201).json(communication);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create communication' });
    }
  });

  app.patch('/api/communications/:id/read', authenticate, requirePermission('write:alerts'), async (req, res) => {
    try {
      const { id } = req.params;
      const communication = await storage.markCommunicationAsRead(id);
      res.json(communication);
    } catch (error) {
      res.status(500).json({ error: 'Failed to mark communication as read' });
    }
  });

  // ===== COMMODITY PACK ROUTES =====
  
  // Crude & Products Pack
  app.get('/api/crude-grades', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { category, limit } = req.query;
      const grades = await storage.getCrudeGrades(
        category as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(grades);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch crude grades' });
    }
  });

  app.post('/api/crude-grades', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const grade = await storage.createCrudeGrade(req.body);
      res.status(201).json(grade);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create crude grade' });
    }
  });

  // LNG/LPG Pack
  app.get('/api/lng-cargoes', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { cargoType, portId, limit } = req.query;
      const cargoes = await storage.getLngCargoes(
        cargoType as string | undefined,
        portId as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(cargoes);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch LNG cargoes' });
    }
  });

  app.get('/api/lng-cargoes/diversions', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { limit } = req.query;
      const cargoes = await storage.getDiversionCargoes(
        limit ? parseInt(limit as string) : 100
      );
      res.json(cargoes);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch diversion cargoes' });
    }
  });

  app.post('/api/lng-cargoes', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const cargo = await storage.createLngCargo(req.body);
      res.status(201).json(cargo);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create LNG cargo' });
    }
  });

  // Dry Bulk Pack
  app.get('/api/dry-bulk-fixtures', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodityType, vesselSize, limit } = req.query;
      const fixtures = await storage.getDryBulkFixtures(
        commodityType as string | undefined,
        vesselSize as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(fixtures);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch dry bulk fixtures' });
    }
  });

  app.post('/api/dry-bulk-fixtures', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const fixture = await storage.createDryBulkFixture(req.body);
      res.status(201).json(fixture);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create dry bulk fixture' });
    }
  });

  // Petrochem Pack
  app.get('/api/petrochem-products', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { category, region, limit } = req.query;
      const products = await storage.getPetrochemProducts(
        category as string | undefined,
        region as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(products);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch petrochem products' });
    }
  });

  app.post('/api/petrochem-products', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const product = await storage.createPetrochemProduct(req.body);
      res.status(201).json(product);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create petrochem product' });
    }
  });

  // Agri & Biofuel Pack
  app.get('/api/agri-biofuel-flows', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodityType, flowType, limit } = req.query;
      const flows = await storage.getAgriBiofuelFlows(
        commodityType as string | undefined,
        flowType as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(flows);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch agri/biofuel flows' });
    }
  });

  app.get('/api/agri-biofuel-flows/sustainable', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { limit } = req.query;
      const flows = await storage.getSustainableBiofuelFlows(
        limit ? parseInt(limit as string) : 100
      );
      res.json(flows);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch sustainable biofuel flows' });
    }
  });

  app.post('/api/agri-biofuel-flows', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const flow = await storage.createAgriBiofuelFlow(req.body);
      res.status(201).json(flow);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create agri/biofuel flow' });
    }
  });

  // ===== REFINERY/PLANT INTELLIGENCE ROUTES =====
  app.get('/api/refineries', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { region, maintenanceStatus, limit } = req.query;
      const refineries = await storage.getRefineries(
        region as string | undefined,
        maintenanceStatus as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(refineries);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refineries' });
    }
  });

  app.get('/api/refineries/:refineryCode', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const refinery = await storage.getRefineryByCode(req.params.refineryCode);
      if (!refinery) {
        return res.status(404).json({ error: 'Refinery not found' });
      }
      res.json(refinery);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refinery' });
    }
  });

  app.post('/api/refineries', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const refinery = await storage.createRefinery(req.body);
      res.status(201).json(refinery);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create refinery' });
    }
  });

  // ===== SUPPLY & DEMAND BALANCES ROUTES =====
  app.get('/api/supply-demand-balances', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodity, region, period, limit } = req.query;
      const balances = await storage.getSupplyDemandBalances(
        commodity as string | undefined,
        region as string | undefined,
        period as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(balances);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch supply & demand balances' });
    }
  });

  app.get('/api/supply-demand-balances/latest', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodity, region, limit } = req.query;
      const balances = await storage.getLatestBalances(
        commodity as string | undefined,
        region as string | undefined,
        limit ? parseInt(limit as string) : 10
      );
      res.json(balances);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch latest balances' });
    }
  });

  app.post('/api/supply-demand-balances', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const balance = await storage.createSupplyDemandBalance(req.body);
      res.status(201).json(balance);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create supply & demand balance' });
    }
  });

  // ===== RESEARCH & INSIGHT LAYER ROUTES =====
  app.get('/api/research-reports', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { category, subcategory, limit } = req.query;
      const reports = await storage.getResearchReports(
        category as string | undefined,
        subcategory as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(reports);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch research reports' });
    }
  });

  app.get('/api/research-reports/published', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { limit } = req.query;
      const reports = await storage.getPublishedReports(
        limit ? parseInt(limit as string) : 10
      );
      res.json(reports);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch published reports' });
    }
  });

  app.get('/api/research-reports/:reportId', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const report = await storage.getReportById(req.params.reportId);
      if (!report) {
        return res.status(404).json({ error: 'Report not found' });
      }
      res.json(report);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch report' });
    }
  });

  app.post('/api/research-reports', authenticate, requirePermission('write:signals'), async (req, res) => {
    try {
      const report = await storage.createResearchReport(req.body);
      res.status(201).json(report);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create research report' });
    }
  });

  // Initialize services and start mock data generation (no auth required for auto-start)
  app.post('/api/init', async (req, res) => {
    try {
      if (initCompleted) {
        return res.json({
          message: 'Veriscope services already initialized',
          portCount: initCompleted.portCount,
          initializedAt: initCompleted.completedAt,
          status: 'already_initialized'
        });
      }

      const waitingForInit = !!initInProgress;
      if (!initInProgress) {
        initInProgress = (async () => {
          const startedAt = new Date().toISOString();
          console.log('Initializing Veriscope services...');
          
          // Seed global ports database
          const { seedGlobalPorts, getPortCount, seedPortCalls, getPortCallCount } = await import('./services/portSeedService');
          await seedGlobalPorts();
          const portCount = await getPortCount();
          console.log(`Total ports in database: ${portCount}`);
          
          // Initialize mock data (creates vessels needed for port calls)
          await mockDataService.initializeBaseData();
          
          // Seed port call data for arrivals/departures/dwell time (after vessels exist)
          await seedPortCalls();
          const portCallCount = await getPortCallCount();
          console.log(`Total port calls in database: ${portCallCount}`);
          
          // Initialize refinery satellite data
          const { initializeRefineryAois, generateMockSatelliteData } = await import('./services/refinerySatelliteService');
          await initializeRefineryAois();
          await generateMockSatelliteData();
          
          // Initialize storage data (floating storage, SPR reserves, time series)
          const { initializeStorageData } = await import('./services/storageDataService');
          await initializeStorageData();
          
          // Start services
          aisService.startSimulation(wss);
          signalsService.startMonitoring(wss);
          predictionService.startPredictionService();
          delayService.start(wss);
          portCallService.start();
          startPortDailyBaselineScheduler();

          const result = {
            portCount,
            startedAt,
            completedAt: new Date().toISOString()
          };
          initCompleted = result;
          return result;
        })();

        initInProgress.finally(() => {
          initInProgress = null;
        });
      }

      const result = await initInProgress;
      res.json({
        message: 'Veriscope services initialized successfully',
        portCount: result.portCount,
        initializedAt: result.completedAt,
        status: waitingForInit ? 'initialized_after_wait' : 'initialized'
      });
    } catch (error) {
      console.error('Initialization error:', error);
      res.status(500).json({ error: 'Failed to initialize services' });
    }
  });

  // ===== CSV-BASED DATA ENDPOINTS =====
  
  // Refinery Units
  app.get('/api/refinery/units', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { plant } = req.query;
      const units = await storage.getRefineryUnits(plant as string);
      res.json(units);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refinery units' });
    }
  });

  // Refinery Utilization
  app.get('/api/refinery/utilization', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { startDate, endDate, plant } = req.query;
      const utilization = await storage.getRefineryUtilization(
        startDate as string,
        endDate as string,
        plant as string
      );
      res.json(utilization);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refinery utilization' });
    }
  });

  // Refinery Crack Spreads
  app.get('/api/refinery/crack-spreads', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { startDate, endDate } = req.query;
      const spreads = await storage.getRefineryCrackSpreads(
        startDate as string,
        endDate as string
      );
      res.json(spreads);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch crack spreads' });
    }
  });

  // Supply & Demand Models Daily
  app.get('/api/supply-demand/models-daily', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { startDate, endDate, region } = req.query;
      const models = await storage.getSdModelsDaily(
        startDate as string,
        endDate as string,
        region as string
      );
      res.json(models);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch S&D models' });
    }
  });

  // Supply & Demand Forecasts Weekly
  app.get('/api/supply-demand/forecasts-weekly', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { startDate, endDate, region } = req.query;
      const forecasts = await storage.getSdForecastsWeekly(
        startDate as string,
        endDate as string,
        region as string
      );
      res.json(forecasts);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch S&D forecasts' });
    }
  });

  // Research Insights Daily
  app.get('/api/research-insights/daily', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { startDate, endDate, limit } = req.query;
      const insights = await storage.getResearchInsightsDaily(
        startDate as string,
        endDate as string,
        limit ? parseInt(limit as string) : 100
      );
      res.json(insights);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch research insights' });
    }
  });

  // ===== ML PRICE PREDICTIONS =====
  
  // Get all ML predictions (optional filter by commodity type)
  app.get('/api/ml-predictions', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { commodityType, limit } = req.query;
      const predictions = await storage.getMlPredictions(
        commodityType as string,
        limit ? parseInt(limit as string) : 10
      );
      res.json(predictions);
    } catch (error) {
      console.error('Error fetching ML predictions:', error);
      res.status(500).json({ error: 'Failed to fetch ML predictions' });
    }
  });

  // Get latest ML prediction for a specific commodity
  app.get('/api/ml-predictions/latest/:commodityType', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { commodityType } = req.params;
      const prediction = await storage.getLatestMlPrediction(commodityType);
      
      if (!prediction) {
        return res.status(404).json({ error: 'No prediction found for this commodity' });
      }
      
      res.json(prediction);
    } catch (error) {
      console.error('Error fetching latest prediction:', error);
      res.status(500).json({ error: 'Failed to fetch latest prediction' });
    }
  });

  // Generate new ML prediction for a commodity
  app.post('/api/ml-predictions/generate', authenticate, requirePermission('write:predictions'), async (req, res) => {
    try {
      const { commodityType, currentPrice } = req.body;
      
      if (!commodityType) {
        return res.status(400).json({ error: 'commodityType is required' });
      }
      
      const { mlPredictionService } = await import('./services/mlPredictionService');
      const prediction = await mlPredictionService.generatePrediction(
        commodityType,
        currentPrice || 80
      );
      
      if (!prediction) {
        return res.status(500).json({ error: 'Failed to generate prediction' });
      }
      
      res.json(prediction);
    } catch (error) {
      console.error('Error generating prediction:', error);
      res.status(500).json({ error: 'Failed to generate prediction' });
    }
  });

  // ===== DATA QUALITY ENDPOINTS =====
  
  app.get('/api/data-quality/scores', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { entityId, limit } = req.query;
      const { dataQualityService } = await import('./services/dataQualityService');
      const scores = await dataQualityService.getLatestQualityScores(
        entityId as string | undefined,
        limit ? parseInt(limit as string) : 10
      );
      res.json(scores);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch data quality scores' });
    }
  });

  app.get('/api/data-quality/streams', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { dataQualityService } = await import('./services/dataQualityService');
      const streams = await dataQualityService.getAllStreamHealth();
      res.json(streams);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch stream health' });
    }
  });

  app.get('/api/data-quality/streams/:streamName', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { streamName } = req.params;
      const { dataQualityService } = await import('./services/dataQualityService');
      const health = await dataQualityService.getStreamHealth(streamName);
      res.json(health);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch stream health' });
    }
  });

  // Import CSV data endpoint for testing
  app.post('/api/import-csv', authenticate, requireRole('admin', 'operator'), async (req, res) => {
    try {
      const { importAllCSVData } = await import('./services/csvImportService');
      console.log('Starting CSV import...');
      await importAllCSVData();
      res.json({ message: 'CSV data imported successfully' });
    } catch (error: any) {
      console.error('CSV import error:', error);
      res.status(500).json({ error: 'Failed to import CSV data', details: error.message });
    }
  });

  // ===== MODEL REGISTRY & ML CREDIBILITY ENDPOINTS =====

  // List all models
  app.get('/api/models', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { status } = req.query;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const models = await modelRegistryService.listModels(status as string | undefined);
      res.json(models);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch models' });
    }
  });

  // Get single model
  app.get('/api/models/:modelId', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.getModel(modelId);
      if (!model) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch model' });
    }
  });

  // Create new model
  app.post('/api/models', authenticate, requirePermission('write:models'), async (req, res) => {
    try {
      const { modelName, version, modelType, features, hyperparameters, trainingMetrics, validationMetrics, status } = req.body;
      
      if (!modelName || typeof modelName !== 'string' || modelName.trim() === '') {
        return res.status(400).json({ error: 'modelName is required and must be a non-empty string' });
      }
      if (!version || typeof version !== 'string' || version.trim() === '') {
        return res.status(400).json({ error: 'version is required and must be a non-empty string' });
      }
      if (status && !['active', 'deprecated', 'archived'].includes(status)) {
        return res.status(400).json({ error: 'status must be one of: active, deprecated, archived' });
      }
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.createModel({
        modelName: modelName.trim(),
        version: version.trim(),
        modelType,
        features,
        hyperparameters,
        trainingMetrics,
        validationMetrics,
        status
      });
      if (!model) {
        return res.status(400).json({ error: 'Failed to create model' });
      }
      res.status(201).json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create model' });
    }
  });

  // Activate model (deprecates other active versions of same model)
  app.post('/api/models/:modelId/activate', authenticate, requirePermission('write:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.activateModel(modelId);
      if (!model) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to activate model' });
    }
  });

  // Deprecate model
  app.post('/api/models/:modelId/deprecate', authenticate, requirePermission('write:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.deprecateModel(modelId);
      if (!model) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to deprecate model' });
    }
  });

  // Get predictions for a model (with confidence intervals)
  app.get('/api/models/:modelId/predictions', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { limit } = req.query;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const predictions = await modelRegistryService.getPredictions(
        modelId,
        limit ? parseInt(limit as string) : 100
      );
      res.json(predictions);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch predictions' });
    }
  });

  // Create prediction with confidence interval
  app.post('/api/models/:modelId/predictions', authenticate, requirePermission('write:predictions'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { target, predictionDate, predictedValue, confidenceLevel, horizon, featuresUsed } = req.body;
      
      if (!target || typeof target !== 'string' || target.trim() === '') {
        return res.status(400).json({ error: 'target is required and must be a non-empty string' });
      }
      if (!predictionDate) {
        return res.status(400).json({ error: 'predictionDate is required' });
      }
      const parsedDate = new Date(predictionDate);
      if (isNaN(parsedDate.getTime())) {
        return res.status(400).json({ error: 'predictionDate must be a valid date' });
      }
      if (predictedValue === undefined || typeof predictedValue !== 'number' || isNaN(predictedValue)) {
        return res.status(400).json({ error: 'predictedValue is required and must be a number' });
      }
      if (confidenceLevel !== undefined && (typeof confidenceLevel !== 'number' || confidenceLevel <= 0 || confidenceLevel >= 1)) {
        return res.status(400).json({ error: 'confidenceLevel must be a number between 0 and 1' });
      }
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const prediction = await modelRegistryService.generatePredictionWithConfidence(
        modelId,
        target.trim(),
        parsedDate,
        predictedValue,
        confidenceLevel || 0.95,
        horizon,
        featuresUsed
      );
      
      if (!prediction) {
        return res.status(400).json({ error: 'Failed to create prediction. Model may not exist.' });
      }
      res.status(201).json(prediction);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create prediction' });
    }
  });

  // Record actual value for backtesting
  app.post('/api/predictions/:predictionId/actual', authenticate, requirePermission('write:predictions'), async (req, res) => {
    try {
      const { predictionId } = req.params;
      const { actualValue } = req.body;
      
      if (actualValue === undefined || typeof actualValue !== 'number' || isNaN(actualValue)) {
        return res.status(400).json({ error: 'actualValue is required and must be a number' });
      }
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const prediction = await modelRegistryService.recordActualValue(predictionId, actualValue);
      
      if (!prediction) {
        return res.status(404).json({ error: 'Prediction not found' });
      }
      res.json(prediction);
    } catch (error) {
      res.status(500).json({ error: 'Failed to record actual value' });
    }
  });

  // Get backtest results for a model
  app.get('/api/models/:modelId/backtest', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { startDate, endDate } = req.query;
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const results = await modelRegistryService.getBacktestResults(
        modelId,
        startDate ? new Date(startDate as string) : undefined,
        endDate ? new Date(endDate as string) : undefined
      );
      
      if (!results) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(results);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch backtest results' });
    }
  });

  // Get drift metrics for a model
  app.get('/api/models/:modelId/drift', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const metrics = await modelRegistryService.getDriftMetrics(modelId);
      
      if (!metrics) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(metrics);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch drift metrics' });
    }
  });

  // ===== WATCHLISTS API =====
  
  app.get('/api/watchlists', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const watchlists = await storage.getWatchlists(userId);
      res.json(watchlists);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch watchlists' });
    }
  });

  app.post('/api/watchlists', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const { name, type, items, alertSettings, isDefault } = req.body;
      
      if (!name || !type || !items) {
        return res.status(400).json({ error: 'Name, type, and items are required' });
      }
      
      const watchlist = await storage.createWatchlist({
        userId,
        name,
        type,
        items,
        alertSettings,
        isDefault: isDefault || false
      });
      res.status(201).json(watchlist);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create watchlist' });
    }
  });

  app.get('/api/watchlists/:id', optionalAuth, async (req, res) => {
    try {
      const watchlist = await storage.getWatchlistById(req.params.id);
      if (!watchlist) {
        return res.status(404).json({ error: 'Watchlist not found' });
      }
      res.json(watchlist);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch watchlist' });
    }
  });

  app.patch('/api/watchlists/:id', optionalAuth, async (req, res) => {
    try {
      const { name, items, alertSettings, isDefault } = req.body;
      const watchlist = await storage.updateWatchlist(req.params.id, {
        name,
        items,
        alertSettings,
        isDefault
      });
      res.json(watchlist);
    } catch (error) {
      res.status(500).json({ error: 'Failed to update watchlist' });
    }
  });

  app.delete('/api/watchlists/:id', optionalAuth, async (req, res) => {
    try {
      await storage.deleteWatchlist(req.params.id);
      res.json({ success: true });
    } catch (error) {
      res.status(500).json({ error: 'Failed to delete watchlist' });
    }
  });

  // ===== ALERT RULES API =====
  
  app.get('/api/alert-rules', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const rules = await storage.getAlertRules(userId);
      res.json(rules);
    } catch (error) {
      console.error('Error fetching alert rules:', error);
      res.status(500).json({ error: 'Failed to fetch alert rules' });
    }
  });

  app.post('/api/alert-rules', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const { name, type, conditions, channels, cooldownMinutes, watchlistId, isActive, severity, isMuted } = req.body;
      
      if (!name || !type || !conditions || !channels) {
        return res.status(400).json({ error: 'Name, type, conditions, and channels are required' });
      }
      
      const rule = await storage.createAlertRule({
        userId,
        name,
        type,
        conditions,
        channels,
        cooldownMinutes: cooldownMinutes || 60,
        watchlistId,
        isActive: isActive !== false,
        severity: severity || 'medium',
        isMuted: isMuted || false
      });
      res.status(201).json(rule);
    } catch (error) {
      console.error('Error creating alert rule:', error);
      res.status(500).json({ error: 'Failed to create alert rule' });
    }
  });

  app.get('/api/alert-rules/:id', optionalAuth, async (req, res) => {
    try {
      const rule = await storage.getAlertRuleById(req.params.id);
      if (!rule) {
        return res.status(404).json({ error: 'Alert rule not found' });
      }
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch alert rule' });
    }
  });

  app.patch('/api/alert-rules/:id', optionalAuth, async (req, res) => {
    try {
      const { name, conditions, channels, cooldownMinutes, isActive, watchlistId, severity, isMuted, snoozedUntil } = req.body;
      const rule = await storage.updateAlertRule(req.params.id, {
        name,
        conditions,
        channels,
        cooldownMinutes,
        isActive,
        watchlistId,
        severity,
        isMuted,
        snoozedUntil: snoozedUntil ? new Date(snoozedUntil) : undefined
      });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to update alert rule' });
    }
  });

  // Snooze an alert rule
  app.post('/api/alert-rules/:id/snooze', optionalAuth, async (req, res) => {
    try {
      const { hours } = req.body;
      if (!hours || hours < 1 || hours > 168) {
        return res.status(400).json({ error: 'Hours must be between 1 and 168 (7 days)' });
      }
      const snoozedUntil = new Date(Date.now() + hours * 60 * 60 * 1000);
      const rule = await storage.updateAlertRule(req.params.id, { snoozedUntil });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to snooze alert rule' });
    }
  });

  // Unsnooze an alert rule
  app.post('/api/alert-rules/:id/unsnooze', optionalAuth, async (req, res) => {
    try {
      const rule = await storage.updateAlertRule(req.params.id, { snoozedUntil: null as any });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to unsnooze alert rule' });
    }
  });

  // Mute/unmute an alert rule
  app.post('/api/alert-rules/:id/mute', optionalAuth, async (req, res) => {
    try {
      const { muted } = req.body;
      const rule = await storage.updateAlertRule(req.params.id, { isMuted: muted !== false });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to mute/unmute alert rule' });
    }
  });

  app.delete('/api/alert-rules/:id', optionalAuth, async (req, res) => {
    try {
      await storage.deleteAlertRule(req.params.id);
      res.json({ success: true });
    } catch (error) {
      res.status(500).json({ error: 'Failed to delete alert rule' });
    }
  });

  // ===== REFINERY SATELLITE MONITORING API =====
  
  app.get('/api/refinery/aois', optionalAuth, async (req, res) => {
    try {
      const { getAois } = await import('./services/refinerySatelliteService');
      const aois = await getAois();
      res.json(aois);
    } catch (error) {
      console.error('Error fetching AOIs:', error);
      res.status(500).json({ error: 'Failed to fetch AOIs' });
    }
  });

  app.get('/api/refinery/aois/:code', optionalAuth, async (req, res) => {
    try {
      const { getAoiByCode } = await import('./services/refinerySatelliteService');
      const aoi = await getAoiByCode(req.params.code);
      if (!aoi) {
        return res.status(404).json({ error: 'AOI not found' });
      }
      res.json(aoi);
    } catch (error) {
      console.error('Error fetching AOI:', error);
      res.status(500).json({ error: 'Failed to fetch AOI' });
    }
  });

  app.get('/api/refinery/activity/latest', optionalAuth, async (req, res) => {
    try {
      const { getLatestActivityIndex } = await import('./services/refinerySatelliteService');
      const aoiCode = (req.query.aoi as string) || 'rotterdam_full';
      const latest = await getLatestActivityIndex(aoiCode);
      if (!latest) {
        return res.status(404).json({ error: 'No activity data found' });
      }
      res.json(latest);
    } catch (error) {
      console.error('Error fetching latest activity:', error);
      res.status(500).json({ error: 'Failed to fetch latest activity' });
    }
  });

  app.get('/api/refinery/activity/timeline', optionalAuth, async (req, res) => {
    try {
      const { getActivityTimeline } = await import('./services/refinerySatelliteService');
      const aoiCode = (req.query.aoi as string) || 'rotterdam_full';
      const weeks = parseInt(req.query.weeks as string) || 12;
      const timeline = await getActivityTimeline(aoiCode, weeks);
      res.json(timeline);
    } catch (error) {
      console.error('Error fetching activity timeline:', error);
      res.status(500).json({ error: 'Failed to fetch activity timeline' });
    }
  });

  app.get('/api/refinery/observations', optionalAuth, async (req, res) => {
    try {
      const { getRecentObservations } = await import('./services/refinerySatelliteService');
      const aoiCode = (req.query.aoi as string) || 'rotterdam_full';
      const limit = parseInt(req.query.limit as string) || 10;
      const observations = await getRecentObservations(aoiCode, limit);
      res.json(observations);
    } catch (error) {
      console.error('Error fetching observations:', error);
      res.status(500).json({ error: 'Failed to fetch observations' });
    }
  });

  app.get('/api/refinery/summary', optionalAuth, async (req, res) => {
    try {
      const { getSummaryStats } = await import('./services/refinerySatelliteService');
      const summary = await getSummaryStats();
      res.json(summary);
    } catch (error) {
      console.error('Error fetching summary:', error);
      res.status(500).json({ error: 'Failed to fetch summary' });
    }
  });

  app.post('/api/refinery/refresh', optionalAuth, async (req, res) => {
    try {
      const { refreshSatelliteData } = await import('./services/refinerySatelliteService');
      const result = await refreshSatelliteData();
      res.json(result);
    } catch (error) {
      console.error('Error refreshing satellite data:', error);
      res.status(500).json({ error: 'Failed to refresh satellite data' });
    }
  });

  // ===== CSV EXPORT API =====
  
  app.get('/api/export/vessels', optionalAuth, async (req, res) => {
    try {
      const vessels = await storage.getVessels();
      const csv = generateCSV(vessels, ['id', 'mmsi', 'name', 'imo', 'vesselType', 'flag', 'owner', 'operator', 'buildYear', 'deadweight', 'length', 'beam', 'draft', 'capacity']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=vessels.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export vessels' });
    }
  });

  app.get('/api/export/ports', optionalAuth, async (req, res) => {
    try {
      const ports = await storage.getPorts();
      const csv = generateCSV(ports, ['id', 'name', 'code', 'country', 'region', 'latitude', 'longitude', 'type', 'capacity', 'depth', 'operationalStatus']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=ports.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export ports' });
    }
  });

  app.get('/api/export/signals', optionalAuth, async (req, res) => {
    try {
      const signals = await storage.getActiveSignals(500);
      const csv = generateCSV(signals, ['id', 'type', 'title', 'description', 'frequency', 'isActive', 'lastTriggered', 'createdAt']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=signals.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export signals' });
    }
  });

  app.get('/api/export/predictions', optionalAuth, async (req, res) => {
    try {
      const predictions = await storage.getPredictions();
      const csv = generateCSV(predictions, ['id', 'commodityId', 'marketId', 'timeframe', 'currentPrice', 'predictedPrice', 'confidence', 'direction', 'validUntil', 'createdAt']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=predictions.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export predictions' });
    }
  });

  return httpServer;
}

function generateCSV(data: any[], columns: string[]): string {
  if (!data || data.length === 0) {
    return columns.join(',') + '\n';
  }
  
  const header = columns.join(',');
  const rows = data.map(row => {
    return columns.map(col => {
      const value = row[col];
      if (value === null || value === undefined) return '';
      if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
        return `"${value.replace(/"/g, '""')}"`;
      }
      if (typeof value === 'object') {
        return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
      }
      return String(value);
    }).join(',');
  });
  
  return [header, ...rows].join('\n');
}
