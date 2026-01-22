import { and, desc, eq, gte, isNull } from "drizzle-orm";
import { db } from "../db";
import { aisPositions, portCalls, ports } from "../../drizzle/schema";

export type PositionSample = {
  lat: number;
  lon: number;
  timestampUtc: Date;
};

export type PortGeofence = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  geofenceRadiusKm: number;
};

export type PortTransition = {
  arrivalAt?: Date;
  departureAt?: Date;
  currentlyInside: boolean;
};

const EARTH_RADIUS_KM = 6371;

export function haversineKm(a: PositionSample, b: { lat: number; lon: number }): number {
  const toRad = (value: number) => (value * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);

  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);

  const haversine =
    sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(haversine));
}

export function isInPort(position: PositionSample, port: PortGeofence): boolean {
  const distance = haversineKm(position, port);
  return distance <= port.geofenceRadiusKm;
}

export function detectPortTransitions(
  positions: PositionSample[],
  port: PortGeofence,
): PortTransition {
  if (positions.length === 0) {
    return { currentlyInside: false };
  }

  const sorted = [...positions].sort(
    (a, b) => a.timestampUtc.getTime() - b.timestampUtc.getTime(),
  );

  let lastInside = isInPort(sorted[0], port);
  let arrivalAt: Date | undefined;
  let departureAt: Date | undefined;

  for (let i = 1; i < sorted.length; i += 1) {
    const current = sorted[i];
    const inside = isInPort(current, port);

    if (!lastInside && inside) {
      arrivalAt = current.timestampUtc;
    } else if (lastInside && !inside) {
      departureAt = current.timestampUtc;
    }

    lastInside = inside;
  }

  return {
    arrivalAt,
    departureAt,
    currentlyInside: lastInside,
  };
}

async function loadRecentPositions(vesselId: string, lookbackHours: number) {
  const since = new Date(Date.now() - lookbackHours * 60 * 60 * 1000);
  return db
    .select({ lat: aisPositions.lat, lon: aisPositions.lon, timestampUtc: aisPositions.timestampUtc })
    .from(aisPositions)
    .where(and(eq(aisPositions.vesselId, vesselId), gte(aisPositions.timestampUtc, since)))
    .orderBy(aisPositions.timestampUtc);
}

async function loadPorts(): Promise<PortGeofence[]> {
  const rows = await db
    .select({
      id: ports.id,
      name: ports.name,
      lat: ports.lat,
      lon: ports.lon,
      geofenceRadiusKm: ports.geofenceRadiusKm,
    })
    .from(ports);
  return rows;
}

async function loadOpenCall(vesselId: string, portId: string) {
  const [openCall] = await db
    .select({ id: portCalls.id })
    .from(portCalls)
    .where(
      and(
        eq(portCalls.vesselId, vesselId),
        eq(portCalls.portId, portId),
        isNull(portCalls.departureTimeUtc),
      ),
    )
    .orderBy(desc(portCalls.arrivalTimeUtc))
    .limit(1);
  return openCall;
}

async function openCall(vesselId: string, portId: string, arrivalTimeUtc: Date) {
  await db.insert(portCalls).values({ vesselId, portId, arrivalTimeUtc });
}

async function closeCall(callId: string, departureTimeUtc: Date) {
  await db
    .update(portCalls)
    .set({ departureTimeUtc })
    .where(eq(portCalls.id, callId));
}

export async function processPortCallsForVessel(
  vesselId: string,
  lookbackHours = 6,
): Promise<void> {
  const positions = await loadRecentPositions(vesselId, lookbackHours);
  if (positions.length === 0) {
    return;
  }

  const allPorts = await loadPorts();

  for (const port of allPorts) {
    const transition = detectPortTransitions(positions, port);
    const openCallRecord = await loadOpenCall(vesselId, port.id);

    if (transition.arrivalAt && !openCallRecord) {
      await openCall(vesselId, port.id, transition.arrivalAt);
    }

    if (transition.departureAt && openCallRecord) {
      await closeCall(openCallRecord.id, transition.departureAt);
    }
  }
}
