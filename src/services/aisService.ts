import { db } from "../db";
import { aisPositions, vessels } from "../../drizzle/schema";

type AisSource = "mock" | "aisstream";

export type AisPosition = {
  mmsi?: string;
  imo?: string;
  timestampUtc: Date;
  lat: number;
  lon: number;
  sog?: number;
  cog?: number;
  heading?: number;
  navStatus?: string;
  source: AisSource;
};

const MOCK_VESSELS: Array<Pick<AisPosition, "mmsi" | "imo" | "lat" | "lon">> = [
  { mmsi: "367123456", imo: "9395044", lat: 37.7749, lon: -122.4194 },
  { mmsi: "235987654", imo: "9356789", lat: 47.6062, lon: -122.3321 },
  { mmsi: "338112233", imo: "9123456", lat: 40.7128, lon: -74.006 },
];

const AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream";

const AIS_MODE = (process.env.AIS_MODE ?? "").toLowerCase();
const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY ?? "";

function resolveMode(): AisSource {
  if (AIS_MODE === "aisstream") {
    return "aisstream";
  }
  if (AIS_MODE === "mock") {
    return "mock";
  }
  return AISSTREAM_API_KEY ? "aisstream" : "mock";
}

function buildMockPositions(count = 3): AisPosition[] {
  const now = new Date();
  return Array.from({ length: count }).map((_, index) => {
    const vessel = MOCK_VESSELS[index % MOCK_VESSELS.length];
    return {
      mmsi: vessel.mmsi,
      imo: vessel.imo,
      timestampUtc: now,
      lat: vessel.lat + (Math.random() - 0.5) * 0.1,
      lon: vessel.lon + (Math.random() - 0.5) * 0.1,
      sog: 8 + Math.random() * 12,
      cog: Math.random() * 360,
      heading: Math.random() * 360,
      navStatus: "under_way",
      source: "mock",
    };
  });
}

async function fetchAisStreamPositions(): Promise<AisPosition[]> {
  if (!AISSTREAM_API_KEY) {
    console.warn("AISSTREAM_API_KEY not set; falling back to mock positions.");
    return buildMockPositions();
  }

  if (typeof WebSocket === "undefined") {
    console.warn("WebSocket is not available; falling back to mock positions.");
    return buildMockPositions();
  }

  return new Promise((resolve) => {
    const positions: AisPosition[] = [];
    const socket = new WebSocket(AISSTREAM_URL);
    const timeout = setTimeout(() => {
      socket.close();
      resolve(positions.length ? positions : buildMockPositions());
    }, 5000);

    socket.addEventListener("open", () => {
      socket.send(
        JSON.stringify({
          APIKey: AISSTREAM_API_KEY,
          BoundingBoxes: [[[90, -180], [-90, 180]]],
        }),
      );
    });

    socket.addEventListener("message", (event) => {
      try {
        const payload = JSON.parse(String(event.data));
        const message = payload?.Message;
        const position = message?.PositionReport;
        if (!position) {
          return;
        }
        if (typeof position?.Latitude !== "number" || typeof position?.Longitude !== "number") {
          return;
        }
        const timestamp = position?.Timestamp ? new Date(position.Timestamp * 1000) : new Date();
        positions.push({
          mmsi: position?.MMSI ? String(position.MMSI) : undefined,
          timestampUtc: timestamp,
          lat: position.Latitude,
          lon: position.Longitude,
          sog: position?.Sog,
          cog: position?.Cog,
          heading: position?.Heading,
          navStatus:
            typeof position?.NavigationalStatus === "number"
              ? String(position.NavigationalStatus)
              : position?.NavigationalStatus,
          source: "aisstream",
        });
        if (positions.length >= 5) {
          clearTimeout(timeout);
          socket.close();
          resolve(positions);
        }
      } catch (error) {
        console.warn("Failed to parse AISStream message", error);
      }
    });

    socket.addEventListener("close", () => {
      clearTimeout(timeout);
      resolve(positions.length ? positions : buildMockPositions());
    });

    socket.addEventListener("error", () => {
      clearTimeout(timeout);
      socket.close();
      resolve(buildMockPositions());
    });
  });
}

function buildVesselName(position: AisPosition): string {
  return `Vessel ${position.mmsi ?? position.imo ?? "unknown"}`;
}

async function upsertVesselIdentity(position: AisPosition): Promise<string> {
  if (position.mmsi) {
    const inserted = await db
      .insert(vessels)
      .values({ mmsi: position.mmsi, name: buildVesselName(position), imo: position.imo })
      .onConflictDoUpdate({
        target: vessels.mmsi,
        set: { imo: position.imo ?? null },
      })
      .returning({ id: vessels.id });
    return inserted[0].id;
  }

  if (position.imo) {
    const inserted = await db
      .insert(vessels)
      .values({ imo: position.imo, name: buildVesselName(position), mmsi: position.mmsi })
      .onConflictDoUpdate({
        target: vessels.imo,
        set: { mmsi: position.mmsi ?? null },
      })
      .returning({ id: vessels.id });
    return inserted[0].id;
  }

  const inserted = await db
    .insert(vessels)
    .values({ name: buildVesselName(position) })
    .returning({ id: vessels.id });

  return inserted[0].id;
}

async function insertPositions(positions: AisPosition[]): Promise<void> {
  for (const position of positions) {
    if (!Number.isFinite(position.lat) || !Number.isFinite(position.lon)) {
      continue;
    }
    const vesselId = await upsertVesselIdentity(position);

    await db.insert(aisPositions).values({
      vesselId,
      timestampUtc: position.timestampUtc,
      lat: position.lat,
      lon: position.lon,
      speed: position.sog,
      course: position.cog,
      heading: position.heading,
      navStatus: position.navStatus,
      source: position.source,
    });
  }
}

export async function ingestPositions(): Promise<void> {
  const mode = resolveMode();
  const positions =
    mode === "aisstream" ? await fetchAisStreamPositions() : buildMockPositions();

  await insertPositions(positions);
}
