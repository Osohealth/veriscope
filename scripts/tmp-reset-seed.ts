import { and, eq, gte, lte, sql } from "drizzle-orm";
import { db } from "../server/db";
import { portDailyBaselines, ports, signals } from "@shared/schema";
import { parseSignalDay } from "../server/services/signalEngine";

const dayArg = process.argv[2];
const portArg = process.argv[3];
const day = parseSignalDay(dayArg);
if (!day || !portArg) {
  console.error("usage: tsx scripts/tmp-reset-seed.ts YYYY-MM-DD PORT_QUERY");
  process.exit(1);
}

const normalized = portArg.toLowerCase();
const [port] = await db
  .select()
  .from(ports)
  .where(sql`lower(${ports.id}) = ${normalized} OR lower(${ports.code}) = ${normalized} OR lower(${ports.unlocode}) = ${normalized} OR lower(${ports.name}) = ${normalized}`)
  .limit(1);

if (!port) {
  console.error("port not found");
  process.exit(1);
}

const historyStart = new Date(day);
historyStart.setUTCDate(historyStart.getUTCDate() - 10);

await db.delete(signals).where(and(eq(signals.entityId, port.id), gte(signals.day, historyStart), lte(signals.day, day)));
await db.delete(portDailyBaselines).where(and(eq(portDailyBaselines.portId, port.id), gte(portDailyBaselines.day, historyStart), lte(portDailyBaselines.day, day)));

console.log(JSON.stringify({ portId: port.id, day: dayArg, historyStart: historyStart.toISOString().slice(0,10) }));
