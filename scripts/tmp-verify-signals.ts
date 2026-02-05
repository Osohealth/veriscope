import { randomUUID } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../server/db";
import { evaluatePortSignalsForDay, formatSignalDay, getYesterdayUtcDay } from "../server/services/signalEngine";
import { portDailyBaselines, ports, signals } from "@shared/schema";

async function main() {
  const columns = await db.execute(sql`
    SELECT column_name, is_nullable, data_type
    FROM information_schema.columns
    WHERE table_name = 'signals'
    ORDER BY ordinal_position
  `);
  console.log("signals_columns:");
  // @ts-ignore
  for (const row of columns.rows ?? columns) {
    console.log(`${row.column_name}\t${row.is_nullable}\t${row.data_type}`);
  }

  const nullCounts = await db.execute(sql`
    SELECT
      count(*) FILTER (WHERE confidence_score IS NULL) AS confidence_score_nulls,
      count(*) FILTER (WHERE confidence_band IS NULL) AS confidence_band_nulls,
      count(*) FILTER (WHERE method IS NULL) AS method_nulls,
      count(*) FILTER (WHERE cluster_id IS NULL) AS cluster_id_nulls,
      count(*) FILTER (WHERE cluster_type IS NULL) AS cluster_type_nulls,
      count(*) FILTER (WHERE cluster_summary IS NULL) AS cluster_summary_nulls
    FROM signals
  `);

  console.log("signals_new_field_null_counts:");
  // @ts-ignore
  console.log(JSON.stringify(nullCounts.rows ? nullCounts.rows[0] : nullCounts[0] ?? nullCounts, null, 2));

  const testDay = getYesterdayUtcDay();
  const portId = randomUUID();
  const code = `TST${portId.slice(0, 6).toUpperCase()}`;

  try {
    await db.insert(ports).values({
      id: portId,
      name: `Temp Port ${code}`,
      code,
      unlocode: code,
      country: "Testland",
      countryCode: "TT",
      region: "Test",
      latitude: "0",
      longitude: "0",
      timezone: "UTC",
      type: "container_port",
    });

    await db.insert(portDailyBaselines).values({
      portId,
      day: testDay,
      arrivals: 80,
      departures: 40,
      uniqueVessels: 30,
      avgDwellHours: 12,
      openCalls: 20,
      arrivals30dAvg: 50,
      arrivals30dStd: 5,
      dwell30dAvg: 6,
      dwell30dStd: 1,
      openCalls30dAvg: 10,
    });

    const result = await evaluatePortSignalsForDay(testDay, { portIds: [portId] });

    const created = await db
      .select()
      .from(signals)
      .where(and(eq(signals.entityId, portId), eq(signals.day, testDay)));

    console.log("min_history_guardrail:");
    console.log(JSON.stringify({
      day: formatSignalDay(testDay),
      portId,
      upserted: result.upserted,
      signalCount: created.length,
    }, null, 2));
  } finally {
    await db.delete(signals).where(eq(signals.entityId, portId));
    await db.delete(portDailyBaselines).where(eq(portDailyBaselines.portId, portId));
    await db.delete(ports).where(eq(ports.id, portId));
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
