import http, { IncomingMessage, ServerResponse } from "node:http";
import crypto from "node:crypto";
import { db } from "./db";
import { aisPositions, ports, portCalls, vessels } from "../drizzle/schema";
import { and, desc, eq, gte, ilike, lte, sql } from "drizzle-orm";
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

function parseBbox(value: string | null) {
  if (!value) {
    return null;
  }
  const parts = value.split(",").map((part) => Number(part.trim()));
  if (parts.length !== 4 || parts.some((part) => !Number.isFinite(part))) {
    return null;
  }
  const [minLon, minLat, maxLon, maxLat] = parts;
  return { minLon, minLat, maxLon, maxLat };
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
      "/v1/vessels": {
        get: {
          summary: "Search vessels",
          parameters: [
            { name: "mmsi", in: "query", schema: { type: "string" } },
            { name: "imo", in: "query", schema: { type: "string" } },
            { name: "name", in: "query", schema: { type: "string" } },
          ],
          responses: {
            "200": { description: "Vessels list" },
            "401": { description: "Unauthorized" },
          },
        },
      },
      "/v1/vessels/{id}/latest-position": {
        get: {
          summary: "Get latest AIS position for a vessel",
          parameters: [
            { name: "id", in: "path", required: true, schema: { type: "string" } },
          ],
          responses: {
            "200": { description: "Latest position" },
            "401": { description: "Unauthorized" },
            "404": { description: "Not found" },
          },
        },
      },
      "/v1/vessels/positions": {
        get: {
          summary: "Get latest positions per vessel inside a bbox",
          parameters: [
            { name: "bbox", in: "query", schema: { type: "string", example: "minLon,minLat,maxLon,maxLat" } },
            { name: "sinceMinutes", in: "query", schema: { type: "integer", default: 60 } },
            { name: "limit", in: "query", schema: { type: "integer", default: 2000 } },
          ],
          responses: {
            "200": { description: "Latest positions per vessel" },
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
  const isVesselsRoute = segments[0] === "v1" && segments[1] === "vessels";

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

  if (req.method === "GET" && isVesselsRoute) {
    if (!requireAuth(req, res)) {
      return;
    }

    if (segments.length === 2) {
      const mmsi = url.searchParams.get("mmsi");
      const imo = url.searchParams.get("imo");
      const name = url.searchParams.get("name");
      const conditions = [];

      if (mmsi) {
        conditions.push(eq(vessels.mmsi, mmsi));
      }
      if (imo) {
        conditions.push(eq(vessels.imo, imo));
      }
      if (name) {
        conditions.push(ilike(vessels.name, `%${name}%`));
      }

      const vesselsList = await db
        .select({
          id: vessels.id,
          name: vessels.name,
          mmsi: vessels.mmsi,
          imo: vessels.imo,
        })
        .from(vessels)
        .where(conditions.length ? and(...conditions) : undefined)
        .orderBy(vessels.name);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data: vesselsList }));
      return;
    }

    if (segments.length === 3 && segments[2] === "positions") {
      const bbox = parseBbox(url.searchParams.get("bbox"));
      if (!bbox) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "bbox must be minLon,minLat,maxLon,maxLat" }));
        return;
      }

      const sinceMinutes = Math.max(parseNumber(url.searchParams.get("sinceMinutes"), 60), 1);
      const limit = Math.min(parseNumber(url.searchParams.get("limit"), 2000), 5000);
      const sinceDate = new Date(Date.now() - sinceMinutes * 60 * 1000);

      const results = await db.execute(sql`
        select distinct on (p.vessel_id)
          p.id,
          p.vessel_id as "vesselId",
          v.name as "vesselName",
          v.mmsi,
          v.imo,
          p.timestamp_utc as "timestampUtc",
          p.lat,
          p.lon,
          p.speed,
          p.course,
          p.heading,
          p.nav_status as "navStatus",
          p.source
        from ais_positions p
        inner join vessels v on v.id = p.vessel_id
        where p.timestamp_utc >= ${sinceDate}
          and p.lon between ${bbox.minLon} and ${bbox.maxLon}
          and p.lat between ${bbox.minLat} and ${bbox.maxLat}
        order by p.vessel_id, p.timestamp_utc desc
        limit ${limit}
      `);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data: results.rows, limit, sinceMinutes, bbox }));
      return;
    }

    if (segments.length === 4 && segments[3] === "latest-position") {
      const vesselId = segments[2];
      const [latest] = await db
        .select({
          id: aisPositions.id,
          vesselId: aisPositions.vesselId,
          timestampUtc: aisPositions.timestampUtc,
          lat: aisPositions.lat,
          lon: aisPositions.lon,
          speed: aisPositions.speed,
          course: aisPositions.course,
          heading: aisPositions.heading,
          navStatus: aisPositions.navStatus,
          source: aisPositions.source,
        })
        .from(aisPositions)
        .where(eq(aisPositions.vesselId, vesselId))
        .orderBy(desc(aisPositions.timestampUtc))
        .limit(1);

      if (!latest) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "latest position not found" }));
        return;
      }

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(latest));
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
