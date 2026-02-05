import { randomUUID } from "node:crypto";
import { db } from "../server/db";
import { ports } from "@shared/schema";

const portA = randomUUID();
const portB = randomUUID();
const codeA = `AMB${portA.slice(0,4).toUpperCase()}`;
const codeB = `AMB${portB.slice(0,4).toUpperCase()}`;

await db.insert(ports).values([
  {
    id: portA,
    name: "Ambiguity Port Alpha",
    code: codeA,
    unlocode: codeA,
    country: "Testland",
    countryCode: "TT",
    region: "Test",
    latitude: "0",
    longitude: "0",
    timezone: "UTC",
    type: "container_port",
  },
  {
    id: portB,
    name: "Ambiguity Port Beta",
    code: codeB,
    unlocode: codeB,
    country: "Testland",
    countryCode: "TT",
    region: "Test",
    latitude: "0",
    longitude: "0",
    timezone: "UTC",
    type: "container_port",
  }
]);

console.log(JSON.stringify({ portA, portB }));
