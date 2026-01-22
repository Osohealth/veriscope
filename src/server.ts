import http, { IncomingMessage, ServerResponse } from "node:http";
import crypto from "node:crypto";
import { db } from "./db";
import { ports, portCalls, vessels } from "../drizzle/schema";
import { and, desc, eq, ilike, gte, lte } from "drizzle-orm";
import { getPortMetrics7d } from "./services/portStatisticsService";

const PORT = Number(process.env.PORT ?? 3000);
const JWT_SECRET = process.env.JWT_SECRET ?? "";

type JwtPayload = {
  sub?: string;
  exp?: number;
};

function base64UrlDecode(value: string) {
  const padded = value.padEnd(value.length + ((4 - (value.length % 4)) % 4), "=");
  return Buffer.from(padded.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}

function verifyJwt(token: string, secret: string): JwtPayload | null {
  if (!secret) {
    return null;
  }

  const parts = token.split(".");
  if (parts.length !== 3) {
    return null;
  }

  const [headerPart, payloadPart, signaturePart] = parts;
  let header: { alg?: string } = {};
  let payload: JwtPayload = {};

  try {
    header = JSON.parse(base64UrlDecode(headerPart));
    payload = JSON.parse(base64UrlDecode(payloadPart));
  } catch {
    return null;
  }

  if (header.alg !== "HS256") {
    return null;
  }

  const signingInput = `${headerPart}.${payloadPart}`;
  const expectedSignature = crypto
    .createHmac("sha256", secret)
    .update(signingInput)
    .digest("base64url");

  if (
    expectedSignature.length !== signaturePart.length ||
    !crypto.timingSafeEqual(Buffer.from(expectedSignature), Buffer.from(signaturePart))
  ) {
    return null;
  }

  if (typeof payload.exp === "number" && Date.now() / 1000 >= payload.exp) {
    return null;
  }

  return payload;
}

function requireAuth(req: IncomingMessage, res: ServerResponse) {
  const authHeader = req.headers.authorization ?? "";
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  const token = match?.[1];

  if (!token || !verifyJwt(token, JWT_SECRET)) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "unauthorized" }));
    return false;
  }

  return true;
}

function parseNumber(value: string | null, fallback: number) {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function buildOpenApiSpec() {
  return {
    openapi: "3.0.3",
    info: {
      title: "VeriScope API",
      version: "1.0.0",
    },
    components: {
      securitySchemes: {
        bearerAuth: {
          type: "http",
          scheme: "bearer",
          bearerFormat: "JWT",
        },
      },
    },
    security: [{ bearerAuth: [] }],
    paths: {
      "/v1/ports": {
        get: {
          summary: "List ports",
          parameters: [
            { name: "q", in: "query", schema: { type: "string" } },
            { name: "countryCode", in: "query", schema: { type: "string" } },
            { name: "limit", in: "query", schema: { type: "integer", default: 50 } },
            { name: "offset", in: "query", schema: { type: "integer", default: 0 } },
          ],
          responses: {
            "200": { description: "Ports list" },
            "401": { description: "Unauthorized" },
          },
        },
      },
      "/v1/ports/{id}": {
        get: {
          summary: "Get port detail and 7-day KPIs",
          parameters: [
            { name: "id", in: "path", required: true, schema: { type: "string" } },
          ],
          responses: {
            "200": { description: "Port detail with KPIs" },
            "401": { description: "Unauthorized" },
            "404": { description: "Not found" },
          },
        },
      },
      "/v1/ports/{id}/calls": {
        get: {
          summary: "List port calls with vessel name and dwell hours",
          parameters: [
            { name: "id", in: "path", required: true, schema: { type: "string" } },
            { name: "startTime", in: "query", schema: { type: "string", format: "date-time" } },
            { name: "endTime", in: "query", schema: { type: "string", format: "date-time" } },
            { name: "limit", in: "query", schema: { type: "integer", default: 50 } },
            { name: "offset", in: "query", schema: { type: "integer", default: 0 } },
          ],
          responses: {
            "200": { description: "Port calls list" },
            "401": { description: "Unauthorized" },
          },
        },
      },
    },
  };
}

async function handleRequest(req: IncomingMessage, res: ServerResponse) {
  const url = new URL(req.url ?? "", `http://${req.headers.host}`);

  if (req.method === "GET" && url.pathname === "/docs") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(buildOpenApiSpec()));
    return;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  const isPortsRoute = segments[0] === "v1" && segments[1] === "ports";

  if (req.method === "GET" && isPortsRoute) {
    if (!requireAuth(req, res)) {
      return;
    }

    if (segments.length === 2) {
      const limit = Math.min(parseNumber(url.searchParams.get("limit"), 50), 200);
      const offset = Math.max(parseNumber(url.searchParams.get("offset"), 0), 0);
      const q = url.searchParams.get("q");
      const countryCode = url.searchParams.get("countryCode");
      const conditions = [];

      if (q) {
        conditions.push(ilike(ports.name, `%${q}%`));
      }
      if (countryCode) {
        conditions.push(eq(ports.countryCode, countryCode));
      }

      const portsList = await db
        .select({
          id: ports.id,
          name: ports.name,
          countryCode: ports.countryCode,
          lat: ports.lat,
          lon: ports.lon,
          geofenceRadiusKm: ports.geofenceRadiusKm,
        })
        .from(ports)
        .where(conditions.length ? and(...conditions) : undefined)
        .orderBy(ports.name)
        .limit(limit)
        .offset(offset);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data: portsList, limit, offset }));
      return;
    }

    if (segments.length === 3) {
      const portId = segments[2];
      if (!portId) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "port id is required" }));
        return;
      }

      const [port] = await db
        .select({
          id: ports.id,
          name: ports.name,
          countryCode: ports.countryCode,
          lat: ports.lat,
          lon: ports.lon,
          geofenceRadiusKm: ports.geofenceRadiusKm,
        })
        .from(ports)
        .where(eq(ports.id, portId));

      if (!port) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "port not found" }));
        return;
      }

      const metrics_7d = await getPortMetrics7d(portId);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ...port, metrics_7d }));
      return;
    }

    if (segments.length === 4 && segments[3] === "calls") {
      const portId = segments[2];
      const startTime = url.searchParams.get("startTime");
      const endTime = url.searchParams.get("endTime");
      const limit = Math.min(parseNumber(url.searchParams.get("limit"), 50), 200);
      const offset = Math.max(parseNumber(url.searchParams.get("offset"), 0), 0);
      const conditions = [eq(portCalls.portId, portId)];

      if (startTime) {
        conditions.push(gte(portCalls.arrivalTimeUtc, new Date(startTime)));
      }
      if (endTime) {
        conditions.push(lte(portCalls.arrivalTimeUtc, new Date(endTime)));
      }

      const calls = await db
        .select({
          id: portCalls.id,
          vesselId: portCalls.vesselId,
          vesselName: vessels.name,
          arrivalTimeUtc: portCalls.arrivalTimeUtc,
          departureTimeUtc: portCalls.departureTimeUtc,
        })
        .from(portCalls)
        .innerJoin(vessels, eq(portCalls.vesselId, vessels.id))
        .where(and(...conditions))
        .orderBy(desc(portCalls.arrivalTimeUtc))
        .limit(limit)
        .offset(offset);

      const now = Date.now();
      const data = calls.map((call) => {
        const arrival = call.arrivalTimeUtc.getTime();
        const departure = call.departureTimeUtc?.getTime() ?? now;
        const dwellHours = Math.max(0, (departure - arrival) / 36e5);

        return {
          ...call,
          dwellHours,
        };
      });

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data, limit, offset }));
      return;
    }
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "not found" }));
}

http.createServer((req, res) => {
  handleRequest(req, res).catch((error) => {
    console.error("Request failed", error);
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "internal server error" }));
  });
}).listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});
