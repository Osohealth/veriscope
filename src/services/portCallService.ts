import { and, desc, eq, gte, isNull } from "drizzle-orm";
import { db } from "../db";
import { aisPositions, portCalls, ports, vesselPortState } from "../../drizzle/schema";

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

type VesselState = {
  vesselId: string;
  inPort: boolean;
  currentPortId: string | null;
  currentPortCallId: string | null;
  lastPositionTimeUtc: Date | null;
};

type PortStateTransition = {
  action: "open" | "close" | "none";
  nextState: VesselState;
  portId: string | null;
};

type DbTx = typeof db;

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

export function derivePortStateTransition(
  state: VesselState,
  isInPortNow: boolean,
  detectedPortId: string | null,
  positionTime: Date,
): PortStateTransition {
  const nextState: VesselState = {
    ...state,
    lastPositionTimeUtc: positionTime,
    currentPortId: state.currentPortId,
    currentPortCallId: state.currentPortCallId,
  };

  if (!state.inPort && isInPortNow && detectedPortId) {
    nextState.inPort = true;
    nextState.currentPortId = detectedPortId;
    return { action: "open", nextState, portId: detectedPortId };
  }

  if (state.inPort && !isInPortNow) {
    nextState.inPort = false;
    nextState.currentPortId = null;
    nextState.currentPortCallId = null;
    return { action: "close", nextState, portId: state.currentPortId };
  }

  if (state.inPort && isInPortNow && detectedPortId === state.currentPortId) {
    return { action: "none", nextState, portId: state.currentPortId };
  }

  return { action: "none", nextState, portId: state.currentPortId };
}

async function loadLatestPosition(vesselId: string, lookbackHours: number) {
  const since = new Date(Date.now() - lookbackHours * 60 * 60 * 1000);
  return db
    .select({
      lat: aisPositions.lat,
      lon: aisPositions.lon,
      timestampUtc: aisPositions.timestampUtc,
    })
    .from(aisPositions)
    .where(and(eq(aisPositions.vesselId, vesselId), gte(aisPositions.timestampUtc, since)))
    .orderBy(desc(aisPositions.timestampUtc))
    .limit(1);
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

function findPortForPosition(position: PositionSample, allPorts: PortGeofence[]) {
  let best: { portId: string; distance: number } | null = null;
  for (const port of allPorts) {
    const distance = haversineKm(position, port);
    if (distance <= port.geofenceRadiusKm) {
      if (!best || distance < best.distance) {
        best = { portId: port.id, distance };
      }
    }
  }
  return best?.portId ?? null;
}

async function loadOrCreateState(tx: DbTx, vesselId: string): Promise<VesselState> {
  const [state] = await tx
    .select({
      vesselId: vesselPortState.vesselId,
      inPort: vesselPortState.inPort,
      currentPortId: vesselPortState.currentPortId,
      currentPortCallId: vesselPortState.currentPortCallId,
      lastPositionTimeUtc: vesselPortState.lastPositionTimeUtc,
    })
    .from(vesselPortState)
    .where(eq(vesselPortState.vesselId, vesselId));

  if (state) {
    return {
      vesselId: state.vesselId,
      inPort: state.inPort,
      currentPortId: state.currentPortId,
      currentPortCallId: state.currentPortCallId,
      lastPositionTimeUtc: state.lastPositionTimeUtc,
    };
  }

  await tx.insert(vesselPortState).values({ vesselId, inPort: false });
  return {
    vesselId,
    inPort: false,
    currentPortId: null,
    currentPortCallId: null,
    lastPositionTimeUtc: null,
  };
}

export async function processPortCallsForVessel(
  vesselId: string,
  lookbackHours = 6,
): Promise<void> {
  const positions = await loadLatestPosition(vesselId, lookbackHours);
  if (positions.length === 0) {
    return;
  }

  const [latestPosition] = positions;
  const allPorts = await loadPorts();
  const detectedPortId = findPortForPosition(latestPosition, allPorts);
  const isInPortNow = Boolean(detectedPortId);

  await db.transaction(async (tx) => {
    const state = await loadOrCreateState(tx, vesselId);
    const transition = derivePortStateTransition(
      state,
      isInPortNow,
      detectedPortId,
      latestPosition.timestampUtc,
    );

    if (transition.action === "open" && transition.portId) {
      const [newCall] = await tx
        .insert(portCalls)
        .values({
          vesselId,
          portId: transition.portId,
          arrivalTimeUtc: latestPosition.timestampUtc,
        })
        .returning({ id: portCalls.id });

      await tx
        .update(vesselPortState)
        .set({
          inPort: true,
          currentPortId: transition.portId,
          currentPortCallId: newCall?.id ?? null,
          lastPositionTimeUtc: transition.nextState.lastPositionTimeUtc,
          updatedAt: new Date(),
        })
        .where(eq(vesselPortState.vesselId, vesselId));
      return;
    }

    if (transition.action === "close") {
      if (state.currentPortCallId) {
        await tx
          .update(portCalls)
          .set({ departureTimeUtc: latestPosition.timestampUtc })
          .where(eq(portCalls.id, state.currentPortCallId));
      } else {
        const [openCall] = await tx
          .select({ id: portCalls.id })
          .from(portCalls)
          .where(and(eq(portCalls.vesselId, vesselId), isNull(portCalls.departureTimeUtc)))
          .orderBy(desc(portCalls.arrivalTimeUtc))
          .limit(1);

        if (openCall) {
          await tx
            .update(portCalls)
            .set({ departureTimeUtc: latestPosition.timestampUtc })
            .where(eq(portCalls.id, openCall.id));
        }
      }

      await tx
        .update(vesselPortState)
        .set({
          inPort: false,
          currentPortId: null,
          currentPortCallId: null,
          lastPositionTimeUtc: transition.nextState.lastPositionTimeUtc,
          updatedAt: new Date(),
        })
        .where(eq(vesselPortState.vesselId, vesselId));
      return;
    }

    await tx
      .update(vesselPortState)
      .set({
        inPort: transition.nextState.inPort,
        currentPortId: transition.nextState.currentPortId,
        currentPortCallId: transition.nextState.currentPortCallId,
        lastPositionTimeUtc: transition.nextState.lastPositionTimeUtc,
        updatedAt: new Date(),
      })
      .where(eq(vesselPortState.vesselId, vesselId));
  });
}
