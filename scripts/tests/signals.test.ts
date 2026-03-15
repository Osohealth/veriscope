import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import { createTestHarness } from "../test/harness";
import { db } from "../../server/db";
import { evaluatePortSignalsForDay, listSignals } from "../../server/services/signalEngine";
import { buildSignalClusterAlertPayload } from "../../server/services/signalAlertService";
import { buildSignalResponse } from "../../server/services/signalResponse";
import { METRIC_LABELS } from "../../shared/metrics";
import { portDailyBaselines, ports, signals } from "../../shared/schema";

const buildPort = (id: string, code: string) => ({
  id,
  name: `Test Port ${code}`,
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

test.describe("signals", () => {
  const harness = createTestHarness({ label: "signals", cleanup: "full" });

  test.before(async () => {
    await harness.beforeAll();
  });

  test.beforeEach(async (t) => {
    await harness.beforeEach(t);
  });

  test.afterEach(async () => {
    await harness.afterEach();
  });

  test.after(async () => {
    await harness.afterAll();
  });

  test("signals engine + response", async () => {
    const testDay = new Date(Date.UTC(2026, 0, 15));
    const portA = randomUUID();
    const portB = randomUUID();
    const portC = randomUUID();
    const portD = randomUUID();
    const portE = randomUUID();
    const portF = randomUUID();
    const missingPort = randomUUID();
    const portIds = [portA, portB, portC, portD, portE, portF];

    await db.insert(ports).values([
        buildPort(portA, `TSA${portA.slice(0, 6).toUpperCase()}`),
        buildPort(portB, `TSB${portB.slice(0, 6).toUpperCase()}`),
        buildPort(portC, `TSC${portC.slice(0, 6).toUpperCase()}`),
        buildPort(portD, `TSD${portD.slice(0, 6).toUpperCase()}`),
        buildPort(portE, `TSE${portE.slice(0, 6).toUpperCase()}`),
        buildPort(portF, `TSF${portF.slice(0, 6).toUpperCase()}`),
      ]);

      const historyDays = 10;
      const historyRows = portIds.flatMap((portId) => {
        const rows = [];
        for (let offset = 1; offset <= historyDays; offset += 1) {
          rows.push({
            portId,
            day: new Date(Date.UTC(2026, 0, 15 - offset)),
            arrivals: 5,
            departures: 5,
            uniqueVessels: 5,
            avgDwellHours: 8,
            openCalls: 5,
            arrivals30dAvg: 5,
            arrivals30dStd: 1,
            dwell30dAvg: 8,
            dwell30dStd: 1,
            openCalls30dAvg: 5,
          });
        }
        return rows;
      });

      await db.insert(portDailyBaselines).values([
        ...historyRows,
        {
          portId: portA,
          day: testDay,
          arrivals: 25,
          departures: 20,
          uniqueVessels: 22,
          avgDwellHours: 10,
          openCalls: 2,
          arrivals30dAvg: 15,
          arrivals30dStd: 5,
          dwell30dAvg: 10,
          dwell30dStd: 4,
          openCalls30dAvg: 2,
        },
        {
          portId: portB,
          day: testDay,
          arrivals: 6,
          departures: 5,
          uniqueVessels: 6,
          avgDwellHours: 20,
          openCalls: 3,
          arrivals30dAvg: 6,
          arrivals30dStd: 4,
          dwell30dAvg: 8,
          dwell30dStd: 4,
          openCalls30dAvg: 3,
        },
        {
          portId: portC,
          day: testDay,
          arrivals: 5,
          departures: 5,
          uniqueVessels: 5,
          avgDwellHours: 7,
          openCalls: 18,
          arrivals30dAvg: 5,
          arrivals30dStd: 2,
          dwell30dAvg: 7,
          dwell30dStd: 2,
          openCalls30dAvg: 8,
        },
        {
          portId: portD,
          day: testDay,
          arrivals: 5,
          departures: 5,
          uniqueVessels: 5,
          avgDwellHours: 7,
          openCalls: 10,
          arrivals30dAvg: 5,
          arrivals30dStd: 2,
          dwell30dAvg: 7,
          dwell30dStd: 2,
          openCalls30dAvg: 4,
        },
        {
          portId: portE,
          day: testDay,
          arrivals: 4,
          departures: 4,
          uniqueVessels: 4,
          avgDwellHours: 4,
          openCalls: 2,
          arrivals30dAvg: 4,
          arrivals30dStd: 2,
          dwell30dAvg: 10,
          dwell30dStd: 3,
          openCalls30dAvg: 2,
        },
        {
          portId: portF,
          day: testDay,
          arrivals: 130,
          departures: 100,
          uniqueVessels: 100,
          avgDwellHours: 6,
          openCalls: 6,
          arrivals30dAvg: 100,
          arrivals30dStd: 5,
          dwell30dAvg: 6,
          dwell30dStd: 1,
          openCalls30dAvg: 5,
        },
      ]);

      const firstRun = await evaluatePortSignalsForDay(testDay, { portIds });
      assert.strictEqual(firstRun.upserted, 4, "expected four signals to be upserted");

      const signalRows = await db
        .select()
        .from(signals)
        .where(and(eq(signals.day, testDay), inArray(signals.entityId, portIds)));

      assert.strictEqual(signalRows.length, 4, "signals created for anomaly, dwell, congestion, critical anomaly");

      const signalTypes = new Set(signalRows.map((row) => row.signalType));
      assert(signalTypes.has("PORT_ARRIVALS_ANOMALY"));
      assert(signalTypes.has("PORT_DWELL_SPIKE"));
      assert(signalTypes.has("PORT_CONGESTION_BUILDUP"));

      const arrivalsSignal = signalRows.find((row) => row.signalType === "PORT_ARRIVALS_ANOMALY" && row.entityId === portA);
      assert(arrivalsSignal?.severity === "MEDIUM", "arrival anomaly severity mapped to MEDIUM");
      assert(arrivalsSignal?.explanation, "arrival anomaly explanation populated");
      assert(arrivalsSignal?.confidenceBand, "arrival anomaly confidence band set");
      assert(arrivalsSignal?.confidenceScore !== null, "arrival anomaly confidence score set");
      assert(arrivalsSignal?.method, "arrival anomaly method set");
      assert(arrivalsSignal?.clusterId, "arrival anomaly cluster id set");
      assert(arrivalsSignal?.clusterKey, "arrival anomaly cluster key set");
      assert(arrivalsSignal?.clusterType === "PORT_DISRUPTION", "arrival anomaly cluster type set");
      assert(arrivalsSignal?.clusterSeverity, "arrival anomaly cluster severity set");
      assert(arrivalsSignal?.clusterSummary, "arrival anomaly cluster summary set");

      const dwellSignal = signalRows.find((row) => row.signalType === "PORT_DWELL_SPIKE");
      assert(dwellSignal?.severity === "HIGH", "dwell spike severity mapped to HIGH");
      assert(dwellSignal?.confidenceBand, "dwell spike confidence band set");
      assert(dwellSignal?.confidenceScore !== null, "dwell spike confidence score set");
      assert(dwellSignal?.method, "dwell spike method set");
      assert(dwellSignal?.clusterId, "dwell spike cluster id set");
      assert(dwellSignal?.clusterKey, "dwell spike cluster key set");
      assert(dwellSignal?.clusterType === "PORT_DISRUPTION", "dwell spike cluster type set");
      assert(dwellSignal?.clusterSeverity, "dwell spike cluster severity set");
      assert(dwellSignal?.clusterSummary, "dwell spike cluster summary set");

      const congestionSignal = signalRows.find((row) => row.signalType === "PORT_CONGESTION_BUILDUP");
      assert(congestionSignal?.severity === "HIGH", "congestion severity mapped to HIGH");
      assert(congestionSignal?.confidenceBand, "congestion confidence band set");
      assert(congestionSignal?.confidenceScore !== null, "congestion confidence score set");
      assert(congestionSignal?.method, "congestion method set");
      assert(congestionSignal?.clusterId, "congestion cluster id set");
      assert(congestionSignal?.clusterKey, "congestion cluster key set");
      assert(congestionSignal?.clusterType === "PORT_DISRUPTION", "congestion cluster type set");
      assert(congestionSignal?.clusterSeverity, "congestion cluster severity set");
      assert(congestionSignal?.clusterSummary, "congestion cluster summary set");

      const criticalSignal = signalRows.find((row) => row.entityId === portF && row.signalType === "PORT_ARRIVALS_ANOMALY");
      assert(criticalSignal?.severity === "CRITICAL", "critical anomaly severity mapped to CRITICAL");
      assert(criticalSignal?.confidenceBand, "critical anomaly confidence band set");
      assert(criticalSignal?.confidenceScore !== null, "critical anomaly confidence score set");
      assert(criticalSignal?.method, "critical anomaly method set");
      assert(criticalSignal?.clusterId, "critical anomaly cluster id set");

      const beforeCountResult = await db
        .select({ count: sql<number>`count(*)` })
        .from(signals)
        .where(and(eq(signals.day, testDay), inArray(signals.entityId, portIds)));
      const beforeCount = Number(beforeCountResult[0]?.count ?? 0);

      await evaluatePortSignalsForDay(testDay, { portIds });

      const afterCountResult = await db
        .select({ count: sql<number>`count(*)` })
        .from(signals)
        .where(and(eq(signals.day, testDay), inArray(signals.entityId, portIds)));
      const afterCount = Number(afterCountResult[0]?.count ?? 0);

      assert.strictEqual(afterCount, beforeCount, "signal engine is idempotent");

      const listByPort = await listSignals({ portId: portA, limit: 10, offset: 0 });
      assert.strictEqual(listByPort.total, 1, "filter by port returns one signal");
      assert.strictEqual(listByPort.items.length, 1);

      const listBySeverity = await listSignals({ severity: "HIGH", limit: 10, offset: 0 });
      assert(listBySeverity.items.length >= 2, "severity filter returns high signals");

      const alertPayload = buildSignalClusterAlertPayload({
        day: testDay,
        entityType: "port",
        entityId: portA,
        clusterId: arrivalsSignal?.clusterId ?? null,
        clusterSeverity: arrivalsSignal?.clusterSeverity ?? null,
        confidenceScore: arrivalsSignal?.confidenceScore ?? null,
        confidenceBand: arrivalsSignal?.confidenceBand ?? null,
        clusterSummary: arrivalsSignal?.clusterSummary ?? null,
        metadata: arrivalsSignal?.metadata ?? {},
      });

      assert(alertPayload.event_type === "VERISCOPE_SIGNAL_CLUSTER");
      assert(alertPayload.cluster_id !== undefined);
      assert(alertPayload.cluster_severity !== undefined);
      assert(Array.isArray(alertPayload.top_drivers));
      assert(alertPayload.top_drivers[0]?.metric, "alert payload includes top driver metric");
      assert(alertPayload.top_drivers[0]?.value !== undefined, "alert payload includes top driver value");

      assert(METRIC_LABELS.arrivals, "metric label map includes arrivals");
      assert(METRIC_LABELS.departures, "metric label map includes departures");
      assert(METRIC_LABELS.unique_vessels, "metric label map includes unique_vessels");
      assert(METRIC_LABELS.avg_dwell_hours, "metric label map includes avg_dwell_hours");
      assert(METRIC_LABELS.open_calls, "metric label map includes open_calls");

      await db.insert(signals).values({
        signalType: "PORT_ARRIVALS_ANOMALY",
        entityType: "port",
        entityId: missingPort,
        day: testDay,
        severity: "HIGH",
        value: 20,
        baseline: 10,
        stddev: 2,
        zscore: 5,
        deltaPct: 100,
        confidenceScore: 0.9,
        confidenceBand: "HIGH",
        method: "zscore_30d",
        clusterId: `PORT_DISRUPTION:${missingPort}:${testDay.toISOString().slice(0, 10)}`,
        clusterKey: `PORT_DISRUPTION|${missingPort}|${testDay.toISOString().slice(0, 10)}`,
        clusterType: "PORT_DISRUPTION",
        clusterSeverity: "HIGH",
        clusterSummary: "Arrivals +100.0%",
        explanation: "Synthetic missing port signal.",
        metadata: {
          day: testDay.toISOString().slice(0, 10),
          metric: "arrivals",
          baseline_window: "30d",
          min_history_days: 10,
          data_quality: {
            history_days_used: 10,
            completeness_pct: 33,
            missing_points: 20,
          },
        },
      });

      const portRows = await db
        .select({ id: ports.id, name: ports.name, code: ports.code, unlocode: ports.unlocode })
        .from(ports)
        .where(inArray(ports.id, [portA]));
      const entityMap = new Map(portRows.map((row) => [row.id, { ...row, type: "port" as const }]));

      const signalForPort = signalRows.find((row) => row.entityId === portA)!;
      const withEntity = buildSignalResponse(signalForPort, {
        compat: false,
        includeEntity: true,
        entityMap,
      });
      assert(withEntity.entity, "include_entity includes embedded entity");
      assert.strictEqual(withEntity.entity.id, portA, "embedded entity id matches");

      const withoutEntity = buildSignalResponse(signalForPort, {
        compat: false,
        includeEntity: false,
        entityMap,
      });
      assert(!("entity" in withoutEntity), "default response omits entity");

      const missingSignal = await db
        .select()
        .from(signals)
        .where(eq(signals.entityId, missingPort))
        .limit(1);
      const missingEntityResponse = buildSignalResponse(missingSignal[0], {
        compat: false,
        includeEntity: true,
        entityMap,
      });
      assert(!("entity" in missingEntityResponse), "missing entity omitted");
  });
});
