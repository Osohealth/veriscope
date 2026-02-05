import { strict as assert } from "node:assert";
import { randomUUID } from "node:crypto";
import { db } from "../server/db";
import { portCalls, portDailyBaselines, ports, vessels } from "@shared/schema";
import { backfillPortDailyBaselines } from "../server/services/portDailyBaselineService";
import { eq, sql } from "drizzle-orm";

async function main() {
  const portId = randomUUID();
  const vesselId = randomUUID();
  const portCode = `TST${portId.slice(0, 7)}`.toUpperCase();
  const mmsi = `${Math.floor(900000000 + Math.random() * 99999999)}`;

  const now = new Date();
  const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const daysToSeed = 35;

  try {
    await db.insert(ports).values({
      id: portId,
      name: "Test Port",
      code: portCode,
      unlocode: portCode,
      country: "Testland",
      countryCode: "TT",
      region: "Test",
      latitude: "0",
      longitude: "0",
      timezone: "UTC",
      type: "container_port",
    });

    await db.insert(vessels).values({
      id: vesselId,
      name: "Test Vessel",
      mmsi,
      vesselType: "vlcc",
      flag: "TT",
    });

    const calls = [];
    for (let i = 0; i < daysToSeed; i++) {
      const day = new Date(todayUtc);
      day.setUTCDate(todayUtc.getUTCDate() - i);
      const arrival = new Date(day.getTime() + 2 * 60 * 60 * 1000);
      const departure = new Date(day.getTime() + 8 * 60 * 60 * 1000);

      calls.push({
        vesselId,
        portId,
        callType: "arrival",
        status: "completed",
        arrivalTime: arrival,
        departureTime: departure,
      });
    }

    await db.insert(portCalls).values(calls);

    await backfillPortDailyBaselines({ days: 40 });

    const baselineRows = await db
      .select()
      .from(portDailyBaselines)
      .where(eq(portDailyBaselines.portId, portId));

    assert(
      baselineRows.length >= daysToSeed,
      "baseline rows created after backfill",
    );

    const targetDay = new Date(todayUtc);
    targetDay.setUTCDate(todayUtc.getUTCDate() - 30);
    const targetDayKey = targetDay.toISOString().slice(0, 10);
    const targetRow = baselineRows.find((row) => {
      const rowDay = row.day instanceof Date ? row.day.toISOString().slice(0, 10) : String(row.day);
      return rowDay === targetDayKey;
    });

    assert(targetRow, "baseline row exists for rolling window day");
    assert(
      targetRow?.arrivals30dAvg !== null && targetRow?.arrivals30dAvg !== undefined,
      "rolling metrics computed when enough history exists",
    );

    const beforeCountResult = await db
      .select({ count: sql<number>`count(*)` })
      .from(portDailyBaselines)
      .where(eq(portDailyBaselines.portId, portId));
    const beforeCount = Number(beforeCountResult[0]?.count || 0);

    await backfillPortDailyBaselines({ days: 40 });

    const afterCountResult = await db
      .select({ count: sql<number>`count(*)` })
      .from(portDailyBaselines)
      .where(eq(portDailyBaselines.portId, portId));
    const afterCount = Number(afterCountResult[0]?.count || 0);

    assert.strictEqual(
      afterCount,
      beforeCount,
      "backfill is idempotent (no duplicate rows)",
    );

    console.log("PASS: port daily baselines backfill tests");
  } finally {
    try {
      await db
        .delete(portDailyBaselines)
        .where(eq(portDailyBaselines.portId, portId));
      await db.delete(portCalls).where(eq(portCalls.portId, portId));
      await db.delete(vessels).where(eq(vessels.id, vesselId));
      await db.delete(ports).where(eq(ports.id, portId));
    } catch (cleanupError) {
      console.warn("Cleanup warning:", (cleanupError as Error).message);
    }
  }
}

main().catch((error) => {
  console.error("Baseline tests failed:", error);
  process.exit(1);
});
