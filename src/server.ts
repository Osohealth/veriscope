import http, { IncomingMessage, ServerResponse } from "node:http";
import { db } from "./db";
import { ports } from "../drizzle/schema";
import { eq } from "drizzle-orm";
import { getPortMetrics7d } from "./services/portStatisticsService";

const PORT = Number(process.env.PORT ?? 3000);

async function handleRequest(req: IncomingMessage, res: ServerResponse) {
  const url = new URL(req.url ?? "", `http://${req.headers.host}`);

  if (req.method === "GET" && url.pathname.startsWith("/v1/ports/")) {
    const portId = url.pathname.replace("/v1/ports/", "").trim();
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
