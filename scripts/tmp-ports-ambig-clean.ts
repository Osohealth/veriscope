import { db } from "../server/db";
import { ports } from "@shared/schema";
import { inArray } from "drizzle-orm";

const ids = process.argv.slice(2);
if (ids.length === 0) {
  process.exit(0);
}
await db.delete(ports).where(inArray(ports.id, ids));
