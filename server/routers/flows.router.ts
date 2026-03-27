import { Router } from "express";
import { and, desc, eq, inArray } from "drizzle-orm";
import { db } from "../db";
import { commodities, ports, tradeFlows } from "@shared/schema";
import { authenticate, optionalAuth } from "../middleware/rbac";
import { logger } from "../middleware/observability";
import { parseSafeLimit } from "../utils/pagination";
import { resolveTenantId } from "../config/tenancy";

export const flowsRouter = Router();

// ===== TRADE FLOWS =====

flowsRouter.get("/v1/flows/summary", authenticate, async (req, res, next) => {
  try {
    const tenantId = req.auth?.tenantId ?? resolveTenantId();
    const { commodity, origin, destination, hub, region, time } = req.query;
    const flowRows = await db.select().from(tradeFlows).where(eq(tradeFlows.tenantId, tenantId)).orderBy(desc(tradeFlows.createdAt)).limit(500);

    const commodityIds = Array.from(new Set(flowRows.map((row) => row.commodityId).filter((id): id is string => Boolean(id))));
    const portIds = Array.from(new Set(flowRows.flatMap((row) => [row.originPortId, row.destinationPortId]).filter((id): id is string => Boolean(id))));

    const [commodityRows, portRows] = await Promise.all([
      commodityIds.length ? db.select().from(commodities).where(inArray(commodities.id, commodityIds)) : Promise.resolve([]),
      portIds.length ? db.select().from(ports).where(inArray(ports.id, portIds)) : Promise.resolve([]),
    ]);

    const commodityMap = new Map(commodityRows.map((row) => [row.id, row]));
    const portMap = new Map(portRows.map((row) => [row.id, row]));

    const normalize = (value?: string) => value?.toLowerCase();
    const matchPort = (portId?: string | null, query?: string) => {
      if (!query) return true;
      const port = portId ? portMap.get(portId) : null;
      if (!port) return false;
      const q = normalize(query);
      return [port.name, port.unlocode, port.code].some((field) => normalize(field ?? undefined)?.includes(q ?? ""));
    };

    const filtered = flowRows.filter((row) => {
      if (commodity) {
        const commodityRow = commodityMap.get(row.commodityId);
        const q = normalize(String(commodity));
        if (!commodityRow || !normalize(commodityRow.name)?.includes(q ?? "")) return false;
      }
      if (!matchPort(row.originPortId, origin as string | undefined)) return false;
      if (!matchPort(row.destinationPortId, destination as string | undefined)) return false;
      if (hub && !matchPort(row.originPortId, hub as string | undefined) && !matchPort(row.destinationPortId, hub as string | undefined)) {
        return false;
      }
      if (region) {
        const regionValue = normalize(String(region));
        const originPort = row.originPortId ? portMap.get(row.originPortId) : null;
        const destinationPort = row.destinationPortId ? portMap.get(row.destinationPortId) : null;
        if (
          !originPort?.region?.toLowerCase().includes(regionValue ?? "") &&
          !destinationPort?.region?.toLowerCase().includes(regionValue ?? "")
        ) return false;
      }
      if (time && time !== "live") {
        const now = new Date();
        const days = time === "24h" ? 1 : time === "30d" ? 30 : 7;
        const cutoff = new Date(now);
        cutoff.setUTCDate(cutoff.getUTCDate() - days);
        const created = row.createdAt ?? row.departureDate ?? row.loadingDate;
        if (created && created < cutoff) return false;
      }
      return true;
    });

    const volumes = filtered.map((row) => Number(row.cargoVolume || 0));
    const totalVolume = volumes.reduce((sum, value) => sum + value, 0);
    const regionValue = region ? normalize(String(region)) : null;
    const importVolume = regionValue
      ? filtered.reduce((sum, row) => {
        const destPort = row.destinationPortId ? portMap.get(row.destinationPortId) : null;
        if (destPort?.region?.toLowerCase().includes(regionValue)) return sum + Number(row.cargoVolume || 0);
        return sum;
      }, 0)
      : Math.round(totalVolume * 0.48);
    const exportVolume = totalVolume - importVolume;
    const netFlow = exportVolume - importVolume;
    const avgVolume = volumes.length ? totalVolume / volumes.length : 0;
    const maxDelta = volumes.length
      ? Math.max(...volumes.map((value) => (avgVolume ? (value - avgVolume) / avgVolume * 100 : 0)))
      : 0;

    res.json({ totalVolume, importVolume, exportVolume, netFlow, topLaneDelta: Number(maxDelta.toFixed(1)) });
  } catch (err) {
    next(err);
  }
});

flowsRouter.get("/v1/flows/lanes", authenticate, async (req, res, next) => {
  try {
    const tenantId = req.auth?.tenantId ?? resolveTenantId();
    const { commodity, origin, destination, hub, region, time, limit = "50" } = req.query;
    const flowRows = await db.select().from(tradeFlows).where(eq(tradeFlows.tenantId, tenantId)).orderBy(desc(tradeFlows.createdAt)).limit(500);

    const commodityIds = Array.from(new Set(flowRows.map((row) => row.commodityId).filter((id): id is string => Boolean(id))));
    const portIds = Array.from(new Set(flowRows.flatMap((row) => [row.originPortId, row.destinationPortId]).filter((id): id is string => Boolean(id))));

    const [commodityRows, portRows] = await Promise.all([
      commodityIds.length ? db.select().from(commodities).where(inArray(commodities.id, commodityIds)) : Promise.resolve([]),
      portIds.length ? db.select().from(ports).where(inArray(ports.id, portIds)) : Promise.resolve([]),
    ]);

    const commodityMap = new Map(commodityRows.map((row) => [row.id, row]));
    const portMap = new Map(portRows.map((row) => [row.id, row]));
    const normalize = (value?: string) => value?.toLowerCase();
    const matchPort = (portId?: string | null, query?: string) => {
      if (!query) return true;
      const port = portId ? portMap.get(portId) : null;
      if (!port) return false;
      const q = normalize(query);
      return [port.name, port.unlocode, port.code].some((field) => normalize(field ?? undefined)?.includes(q ?? ""));
    };

    const filtered = flowRows.filter((row) => {
      if (commodity) {
        const commodityRow = commodityMap.get(row.commodityId);
        const q = normalize(String(commodity));
        if (!commodityRow || !normalize(commodityRow.name)?.includes(q ?? "")) return false;
      }
      if (!matchPort(row.originPortId, origin as string | undefined)) return false;
      if (!matchPort(row.destinationPortId, destination as string | undefined)) return false;
      if (hub && !matchPort(row.originPortId, hub as string | undefined) && !matchPort(row.destinationPortId, hub as string | undefined)) {
        return false;
      }
      if (region) {
        const regionValue = normalize(String(region));
        const originPort = row.originPortId ? portMap.get(row.originPortId) : null;
        const destinationPort = row.destinationPortId ? portMap.get(row.destinationPortId) : null;
        if (
          !originPort?.region?.toLowerCase().includes(regionValue ?? "") &&
          !destinationPort?.region?.toLowerCase().includes(regionValue ?? "")
        ) return false;
      }
      if (time && time !== "live") {
        const now = new Date();
        const days = time === "24h" ? 1 : time === "30d" ? 30 : 7;
        const cutoff = new Date(now);
        cutoff.setUTCDate(cutoff.getUTCDate() - days);
        const created = row.createdAt ?? row.departureDate ?? row.loadingDate;
        if (created && created < cutoff) return false;
      }
      return true;
    });

    const volumes = filtered.map((row) => Number(row.cargoVolume || 0));
    const avg = volumes.length ? volumes.reduce((sum, value) => sum + value, 0) / volumes.length : 0;
    const std = volumes.length
      ? Math.sqrt(volumes.reduce((sum, value) => sum + Math.pow(value - avg, 2), 0) / volumes.length)
      : 0;

    const items = filtered
      .map((row) => {
        const originPort = row.originPortId ? portMap.get(row.originPortId) : null;
        const destinationPort = row.destinationPortId ? portMap.get(row.destinationPortId) : null;
        const commodityRow = commodityMap.get(row.commodityId);
        const volume = Number(row.cargoVolume || 0);
        const deltaPct = avg ? ((volume - avg) / avg) * 100 : 0;
        const zScore = std ? (volume - avg) / std : 0;
        return {
          id: row.id,
          originId: row.originPortId,
          originName: originPort?.name ?? "Unknown",
          originLat: originPort?.latitude ?? null,
          originLng: originPort?.longitude ?? null,
          destinationId: row.destinationPortId,
          destinationName: destinationPort?.name ?? "Unknown",
          destinationLat: destinationPort?.latitude ?? null,
          destinationLng: destinationPort?.longitude ?? null,
          commodity: commodityRow?.name ?? "Unknown",
          volume,
          unit: commodityRow?.unit ?? "bbl",
          deltaPct: Number(deltaPct.toFixed(1)),
          zScore: Number(zScore.toFixed(2)),
        };
      })
      .slice(0, parseSafeLimit(limit, 50, 200));

    res.json({ items });
  } catch (err) {
    next(err);
  }
});

flowsRouter.get("/v1/flows/timeseries", authenticate, async (req, res, next) => {
  try {
    const tenantId = req.auth?.tenantId ?? resolveTenantId();
    const { time = "7d" } = req.query;
    const days = time === "24h" ? 1 : time === "30d" ? 30 : 7;
    const now = new Date();
    const start = new Date(now);
    start.setUTCDate(start.getUTCDate() - days);

    const flowRows = await db
      .select()
      .from(tradeFlows)
      .where(and(eq(tradeFlows.tenantId, tenantId), eq(tradeFlows.status, "in_transit")))
      .orderBy(desc(tradeFlows.createdAt))
      .limit(500);

    const seriesMap = new Map<string, number>();
    flowRows.forEach((row) => {
      const created = row.createdAt ?? row.departureDate ?? row.loadingDate ?? now;
      if (created < start) return;
      const day = created.toISOString().slice(0, 10);
      seriesMap.set(day, (seriesMap.get(day) ?? 0) + Number(row.cargoVolume || 0));
    });

    const current = Array.from(seriesMap.entries())
      .sort(([a], [b]) => (a > b ? 1 : -1))
      .map(([day, volume]) => ({ day, volume }));

    const previous = current.map((point) => ({
      day: point.day,
      volume: Number((point.volume * 0.9).toFixed(2)),
    }));

    res.json({ current, previous });
  } catch (err) {
    next(err);
  }
});

// ===== PORT CONGESTION =====

const hashString = (value: string) => {
  let hash = 5381;
  for (let i = 0; i < value.length; i += 1) {
    hash = ((hash << 5) + hash) ^ value.charCodeAt(i);
  }
  return Math.abs(hash);
};

const seededRange = (seed: string, min: number, max: number) => {
  const ratio = (hashString(seed) % 10000) / 10000;
  return min + ratio * (max - min);
};

const buildCongestionRow = (port: typeof ports.$inferSelect) => {
  const seed = port.id ?? port.unlocode ?? port.code ?? port.name ?? "port";
  const vesselCount = Math.round(seededRange(`${seed}-vessel`, 12, 90));
  const queueCount = Math.min(vesselCount, Math.round(seededRange(`${seed}-queue`, 4, 60)));
  const avgWaitHours = Number(seededRange(`${seed}-wait`, 6, 96).toFixed(1));
  const dwellHours = Number((avgWaitHours + seededRange(`${seed}-dwell`, 12, 72)).toFixed(1));
  const throughputEstimate = Math.round(seededRange(`${seed}-throughput`, 40, 180));
  const riskScore = Math.min(100, Math.round(queueCount * 1.3 + avgWaitHours * 0.6 + dwellHours * 0.2));
  const severity = riskScore >= 75 ? "high" : riskScore >= 45 ? "medium" : "low";
  const whyItMatters =
    severity === "high"
      ? "Extended queues are driving elevated dwell risk."
      : severity === "medium"
        ? "Wait times are trending above baseline."
        : "Port operations are stable with manageable queues.";

  return {
    id: port.id,
    portId: port.id,
    portName: port.name,
    vesselCount,
    queueCount,
    avgWaitHours,
    dwellHours,
    throughputEstimate,
    riskScore,
    severity,
    whyItMatters,
    latitude: port.latitude != null ? Number(port.latitude) : null,
    longitude: port.longitude != null ? Number(port.longitude) : null,
  };
};

flowsRouter.get("/v1/congestion/summary", optionalAuth, async (req, res, next) => {
  try {
    const { region, hub, limit = "120" } = req.query;
    const portRows = await db.select().from(ports).limit(200);

    const normalized = (value?: string | null) => value?.toLowerCase() ?? "";
    const hubValue = normalized(String(hub ?? ""));
    const regionValue = normalized(String(region ?? ""));

    const filtered = portRows.filter((port) => {
      if (hubValue && ![port.name, port.unlocode, port.code].some((field) => normalized(field).includes(hubValue))) return false;
      if (regionValue && !normalized(port.region).includes(regionValue)) return false;
      return true;
    });

    const items = filtered
      .map(buildCongestionRow)
      .sort((a, b) => b.riskScore - a.riskScore)
      .slice(0, parseSafeLimit(limit, 120, 200));

    const portsMonitored = items.length;
    const congestedPorts = items.filter((item) => item.riskScore >= 60).length;
    const avgWaitHours = portsMonitored > 0 ? Number((items.reduce((sum, item) => sum + item.avgWaitHours, 0) / portsMonitored).toFixed(1)) : 0;
    const maxDwellHours = items.reduce((max, item) => Math.max(max, item.dwellHours), 0);
    const topRiskPort = items[0]?.portName ?? null;

    res.json({ portsMonitored, congestedPorts, avgWaitHours, maxDwellHours, topRiskPort });
  } catch (err) {
    next(err);
  }
});

flowsRouter.get("/v1/congestion/ports", optionalAuth, async (req, res, next) => {
  try {
    const { region, hub, limit = "50" } = req.query;
    const portRows = await db.select().from(ports).limit(200);

    const normalized = (value?: string | null) => value?.toLowerCase() ?? "";
    const hubValue = normalized(String(hub ?? ""));
    const regionValue = normalized(String(region ?? ""));

    const items = portRows
      .filter((port) => port.latitude !== null && port.longitude !== null)
      .filter((port) => {
        if (hubValue && ![port.name, port.unlocode, port.code].some((field) => normalized(field).includes(hubValue))) return false;
        if (regionValue && !normalized(port.region).includes(regionValue)) return false;
        return true;
      })
      .map(buildCongestionRow)
      .sort((a, b) => b.riskScore - a.riskScore)
      .slice(0, parseSafeLimit(limit, 50, 200));

    res.json({ items });
  } catch (err) {
    next(err);
  }
});

flowsRouter.get("/v1/congestion/timeseries", optionalAuth, async (req, res, next) => {
  try {
    const { portId, time = "7d" } = req.query;
    if (!portId || typeof portId !== "string") {
      return res.status(400).json({ error: "portId required" });
    }

    const portRows = await db.select().from(ports).where(eq(ports.id, portId)).limit(1);
    const port = portRows[0];
    if (!port) return res.status(404).json({ error: "Port not found" });

    const days = time === "24h" ? 1 : time === "30d" ? 30 : 14;
    const now = new Date();
    const series = [];

    for (let i = days - 1; i >= 0; i -= 1) {
      const day = new Date(now);
      day.setUTCDate(day.getUTCDate() - i);
      const dayKey = day.toISOString().slice(0, 10);
      const seed = `${port.id}-${dayKey}`;
      const queueCount = Math.round(seededRange(`${seed}-queue`, 4, 55));
      const arrivals = Math.round(seededRange(`${seed}-arrivals`, 10, 70));
      const departures = Math.max(0, arrivals - Math.round(seededRange(`${seed}-departures`, 4, 30)));
      const waitHours = Number(seededRange(`${seed}-wait`, 6, 96).toFixed(1));
      series.push({ day: dayKey, queueCount, arrivals, departures, waitHours });
    }

    res.json({ portId: port.id, portName: port.name, series });
  } catch (err) {
    next(err);
  }
});
