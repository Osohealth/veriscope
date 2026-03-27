import bcrypt from "bcryptjs";
import { db } from "../db";
import {
  users, ports, vessels, storageFacilities, commodities, markets,
  commodityPrices, tradeFlows, cargoLegs, stsEvents, cargoSplits, flowForecasts,
  marketAnalytics, alerts, notifications, portStats, portDailyBaselines,
  predictions, storageFillData, floatingStorage, sprReserves, storageTimeSeries,
  portDelayEvents, vesselDelaySnapshots, marketDelayImpacts,
  portCalls, containerOperations, bunkeringEvents, communications,
  crudeGrades, lngCargoes, dryBulkFixtures, petrochemProducts, agriBiofuelFlows,
  refineries, supplyDemandBalances, researchReports,
  refineryUnits, refineryUtilizationDaily, refineryCrackSpreadsDaily,
  sdModelsDaily, sdForecastsWeekly, researchInsightsDaily,
  watchlists, alertRules, modelRegistry, modelPredictions, dataQualityScores,
} from "@shared/schema";
import { eq } from "drizzle-orm";
import { logger } from "../middleware/observability";

const ADMIN_USER_ID = "admin-user-id";
const TENANT_ID = "00000000-0000-0000-0000-000000000001";

// ── helpers ──────────────────────────────────────────────────────────────────
const daysAgo = (n: number) => { const d = new Date(); d.setDate(d.getDate() - n); return d; };
const hoursAgo = (n: number) => new Date(Date.now() - n * 3600_000);
const daysFromNow = (n: number) => { const d = new Date(); d.setDate(d.getDate() + n); return d; };
const rand = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;
const pick = <T>(arr: T[]): T => arr[rand(0, arr.length - 1)];

class MockDataService {
  async initializeBaseData() {
    try {
      const existing = await db.select({ id: users.id }).from(users).where(eq(users.email, "admin@example.com")).limit(1);
      if (existing.length > 0) {
        logger.info("Mock data already seeded — skipping");
        return;
      }
      logger.info("Seeding mock data for MVP demo...");
      await this.createAdminUser();
      const portIds = await this.seedPorts();
      const vesselIds = await this.seedVessels();
      const commodityIds = await this.seedCommodities();
      const marketIds = await this.seedMarkets(commodityIds);
      await this.seedStorageFacilities(portIds);
      const tradeFlowIds = await this.seedTradeFlows(vesselIds, commodityIds, portIds);
      await this.seedCargoLegs(tradeFlowIds, portIds);
      await this.seedStsEvents(vesselIds, commodityIds, portIds, tradeFlowIds);
      await this.seedCargoSplits(tradeFlowIds, commodityIds, portIds, vesselIds);
      await this.seedFlowForecasts(portIds, commodityIds);
      await this.seedCommodityPrices(commodityIds, marketIds);
      await this.seedMarketAnalytics(commodityIds, marketIds);
      await this.seedPortStats(portIds);
      await this.seedPortDailyBaselines(portIds);
      await this.seedPredictions(commodityIds, marketIds);
      await this.seedStorageFillData(portIds);
      await this.seedFloatingStorage(vesselIds);
      await this.seedSprReserves();
      await this.seedStorageTimeSeries();
      await this.seedPortDelayEvents(portIds, vesselIds);
      await this.seedVesselDelaySnapshots(vesselIds, portIds, commodityIds);
      await this.seedMarketDelayImpacts(portIds, commodityIds, marketIds);
      await this.seedPortCalls(vesselIds, portIds);
      await this.seedContainerOperations(vesselIds, portIds);
      await this.seedBunkeringEvents(vesselIds, portIds);
      await this.seedCommunications();
      await this.seedCrudeGrades();
      await this.seedLngCargoes(vesselIds, portIds);
      await this.seedDryBulkFixtures(vesselIds, portIds);
      await this.seedPetrochemProducts();
      await this.seedAgriBiofuelFlows();
      await this.seedRefineries();
      await this.seedSupplyDemandBalances();
      await this.seedResearchReports();
      await this.seedRefineryUnits();
      await this.seedRefineryUtilizationDaily();
      await this.seedRefineryCrackSpreadsDaily();
      await this.seedSdModelsDaily();
      await this.seedSdForecastsWeekly();
      await this.seedResearchInsightsDaily();
      await this.seedWatchlists();
      await this.seedAlertRules();
      await this.seedAlerts();
      await this.seedNotifications();
      await this.seedModelRegistry();
      await this.seedDataQualityScores(portIds, vesselIds);
      logger.info("Mock data seeding complete");
    } catch (error: any) {
      logger.error("Mock data seeding failed", { error });
      throw error;
    }
  }

  // ── 1. Admin user ──────────────────────────────────────────────────────────
  private async createAdminUser() {
    const passwordHash = await bcrypt.hash("admin123", 10);
    await db.insert(users).values({
      id: ADMIN_USER_ID,
      email: "admin@example.com",
      fullName: "Admin User",
      passwordHash,
      role: "admin",
      isActive: true,
      createdAt: new Date(),
    });
    logger.info("Admin user created: admin@example.com / admin123");
  }

  // ── 2. Ports ───────────────────────────────────────────────────────────────
  private async seedPorts(): Promise<Record<string, string>> {
    const portDefs = [
      { code: "NLRTM", name: "Rotterdam", country: "Netherlands", countryCode: "NL", region: "Europe", lat: "51.9225000", lng: "4.4792000", tz: "Europe/Amsterdam", type: "oil_terminal", radius: "15.00" },
      { code: "SGSIN", name: "Singapore", country: "Singapore", countryCode: "SG", region: "Asia", lat: "1.2644000", lng: "103.8203000", tz: "Asia/Singapore", type: "container_port", radius: "12.00" },
      { code: "AEFJR", name: "Fujairah", country: "UAE", countryCode: "AE", region: "Middle East", lat: "25.1288000", lng: "56.3366000", tz: "Asia/Dubai", type: "oil_terminal", radius: "10.00" },
      { code: "USHOU", name: "Houston", country: "United States", countryCode: "US", region: "North America", lat: "29.7604000", lng: "-95.3698000", tz: "America/Chicago", type: "oil_terminal", radius: "12.00" },
      { code: "CNSHA", name: "Shanghai", country: "China", countryCode: "CN", region: "Asia", lat: "31.2304000", lng: "121.4737000", tz: "Asia/Shanghai", type: "container_port", radius: "8.00" },
      { code: "BEANR", name: "Antwerp", country: "Belgium", countryCode: "BE", region: "Europe", lat: "51.2194000", lng: "4.4025000", tz: "Europe/Brussels", type: "container_port", radius: "8.00" },
      { code: "SARTA", name: "Ras Tanura", country: "Saudi Arabia", countryCode: "SA", region: "Middle East", lat: "26.6385000", lng: "50.0508000", tz: "Asia/Riyadh", type: "oil_terminal", radius: "10.00" },
      { code: "QARAF", name: "Ras Laffan", country: "Qatar", countryCode: "QA", region: "Middle East", lat: "25.9300000", lng: "51.5300000", tz: "Asia/Qatar", type: "lng_terminal", radius: "8.00" },
      { code: "DEHAM", name: "Hamburg", country: "Germany", countryCode: "DE", region: "Europe", lat: "53.5511000", lng: "9.9937000", tz: "Europe/Berlin", type: "container_port", radius: "8.00" },
      { code: "USLAX", name: "Los Angeles", country: "United States", countryCode: "US", region: "North America", lat: "33.7361000", lng: "-118.2611000", tz: "America/Los_Angeles", type: "container_port", radius: "8.00" },
      { code: "JPTYO", name: "Tokyo", country: "Japan", countryCode: "JP", region: "Asia", lat: "35.6762000", lng: "139.6503000", tz: "Asia/Tokyo", type: "container_port", radius: "8.00" },
      { code: "GRPIR", name: "Piraeus", country: "Greece", countryCode: "GR", region: "Europe", lat: "37.9475000", lng: "23.6412000", tz: "Europe/Athens", type: "container_port", radius: "8.00" },
    ];
    const ids: Record<string, string> = {};
    for (const p of portDefs) {
      const existing = await db.select({ id: ports.id }).from(ports).where(eq(ports.code, p.code)).limit(1);
      if (existing.length > 0) {
        ids[p.code] = existing[0].id;
        continue;
      }
      const [row] = await db.insert(ports).values({
        name: p.name, code: p.code, unlocode: p.code,
        country: p.country, countryCode: p.countryCode, region: p.region,
        latitude: p.lat, longitude: p.lng, timezone: p.tz,
        type: p.type, geofenceRadiusKm: p.radius, operationalStatus: "active",
      }).returning({ id: ports.id });
      ids[p.code] = row.id;
    }
    logger.info(`Ports seeded: ${portDefs.length}`);
    return ids;
  }

  // ── 3. Vessels ─────────────────────────────────────────────────────────────
  private async seedVessels(): Promise<string[]> {
    const vesselDefs = [
      { mmsi: "256148000", imo: "9387421", name: "Seaways Pioneer", type: "vlcc", flag: "LR", operator: "Seaways Shipping", buildYear: 2018, dwt: 318000 },
      { mmsi: "235074166", imo: "9445231", name: "Nordic Thunder", type: "suezmax", flag: "GB", operator: "Nordic Maritime", buildYear: 2017, dwt: 159000 },
      { mmsi: "636092932", imo: "9456123", name: "Ocean Voyager", type: "aframax", flag: "LR", operator: "Ocean Lines", buildYear: 2019, dwt: 115000 },
      { mmsi: "538006575", imo: "9392847", name: "Maritime Express", type: "vlcc", flag: "MH", operator: "Express Maritime", buildYear: 2016, dwt: 298000 },
      { mmsi: "477995700", imo: "9428394", name: "Titan Carrier", type: "suezmax", flag: "HK", operator: "Titan Shipping", buildYear: 2020, dwt: 164000 },
      { mmsi: "244615000", imo: "9517293", name: "Euro Trader", type: "aframax", flag: "NL", operator: "Euro Marine", buildYear: 2018, dwt: 109000 },
      { mmsi: "311050500", imo: "9601842", name: "Pacific Glory", type: "vlcc", flag: "BS", operator: "Pacific Tankers", buildYear: 2015, dwt: 305000 },
      { mmsi: "209425000", imo: "9498372", name: "Argos Breeze", type: "suezmax", flag: "CY", operator: "Argos Marine", buildYear: 2019, dwt: 157000 },
      { mmsi: "636013559", imo: "9533018", name: "Sahara Wind", type: "aframax", flag: "LR", operator: "Sahara Tankers", buildYear: 2021, dwt: 112000 },
      { mmsi: "636014321", imo: "9571248", name: "LNG Horizon", type: "lng_carrier", flag: "MH", operator: "LNG Ventures", buildYear: 2020, dwt: 90000 },
      { mmsi: "352001832", imo: "9612983", name: "Container King", type: "container", flag: "PA", operator: "King Lines", buildYear: 2017, dwt: 145000 },
      { mmsi: "548420700", imo: "9483291", name: "Bulker Atlas", type: "capesize", flag: "PH", operator: "Atlas Bulk", buildYear: 2016, dwt: 180000 },
      { mmsi: "440136000", imo: "9554837", name: "Hana Maru", type: "vlcc", flag: "KR", operator: "Hana Shipping", buildYear: 2019, dwt: 310000 },
      { mmsi: "477182300", imo: "9499012", name: "Pearl Dragon", type: "aframax", flag: "HK", operator: "Dragon Tankers", buildYear: 2018, dwt: 108000 },
      { mmsi: "235108000", imo: "9482730", name: "Caspian Star", type: "suezmax", flag: "GB", operator: "Caspian Shipping", buildYear: 2017, dwt: 160000 },
    ];
    const ids: string[] = [];
    for (const v of vesselDefs) {
      const [row] = await db.insert(vessels).values({
        mmsi: v.mmsi, imo: v.imo, name: v.name, vesselType: v.type,
        flag: v.flag, operator: v.operator, buildYear: v.buildYear,
        deadweight: v.dwt, isActive: true,
      }).onConflictDoNothing().returning({ id: vessels.id });
      if (row) ids.push(row.id);
    }
    const all = await db.select({ id: vessels.id }).from(vessels);
    logger.info(`Vessels seeded: ${all.length}`);
    return all.map(r => r.id);
  }

  // ── 4. Commodities ─────────────────────────────────────────────────────────
  private async seedCommodities(): Promise<Record<string, string>> {
    const defs = [
      { name: "Brent Crude", code: "BRENT", category: "oil", subcategory: "crude_oil", unit: "barrel" },
      { name: "WTI Crude", code: "WTI", category: "oil", subcategory: "crude_oil", unit: "barrel" },
      { name: "Dubai Crude", code: "DUBAI", category: "oil", subcategory: "crude_oil", unit: "barrel" },
      { name: "LNG Spot", code: "LNG_SPOT", category: "lng", subcategory: "liquefied_natural_gas", unit: "mmbtu" },
      { name: "ULSD (Ultra Low Sulfur Diesel)", code: "ULSD", category: "refined_products", subcategory: "diesel", unit: "barrel" },
      { name: "Gasoline RBOB", code: "RBOB", category: "refined_products", subcategory: "gasoline", unit: "barrel" },
      { name: "Jet Fuel A1", code: "JET_A1", category: "refined_products", subcategory: "jet_fuel", unit: "barrel" },
      { name: "Iron Ore", code: "IRON_ORE", category: "dry_bulk", subcategory: "iron_ore", unit: "ton" },
      { name: "Coal (Thermal)", code: "COAL_THERM", category: "dry_bulk", subcategory: "thermal_coal", unit: "ton" },
      { name: "Ethylene", code: "ETHYLENE", category: "chemicals", subcategory: "olefins", unit: "ton" },
    ];
    const ids: Record<string, string> = {};
    for (const c of defs) {
      const [row] = await db.insert(commodities).values({ ...c, isActive: true })
        .onConflictDoNothing().returning({ id: commodities.id, code: commodities.code });
      if (row) ids[row.code] = row.id;
    }
    const all = await db.select({ id: commodities.id, code: commodities.code }).from(commodities);
    for (const r of all) ids[r.code] = r.id;
    logger.info(`Commodities seeded: ${all.length}`);
    return ids;
  }

  // ── 5. Markets ─────────────────────────────────────────────────────────────
  private async seedMarkets(cIds: Record<string, string>): Promise<Record<string, string>> {
    const defs = [
      { name: "ICE Brent", code: "ICE_BRENT", type: "physical", region: "global" },
      { name: "NYMEX WTI", code: "NYMEX_WTI", type: "financial", region: "americas" },
      { name: "Platts Singapore", code: "PLTS_SIN", type: "physical", region: "asia" },
      { name: "LNG JKM", code: "LNG_JKM", type: "physical", region: "asia" },
      { name: "Baltic Exchange", code: "BALTIC", type: "shipping", region: "global" },
      { name: "NYMEX Gasoline", code: "NYMEX_GAS", type: "financial", region: "americas" },
    ];
    const ids: Record<string, string> = {};
    for (const m of defs) {
      const [row] = await db.insert(markets).values({ ...m, currency: "USD", isActive: true })
        .onConflictDoNothing().returning({ id: markets.id, code: markets.code });
      if (row) ids[row.code] = row.id;
    }
    const all = await db.select({ id: markets.id, code: markets.code }).from(markets);
    for (const r of all) ids[r.code] = r.id;
    logger.info(`Markets seeded: ${all.length}`);
    return ids;
  }

  // ── 6. Storage Facilities ──────────────────────────────────────────────────
  private async seedStorageFacilities(portIds: Record<string, string>) {
    const defs = [
      { name: "Rotterdam Terminal T1", portCode: "NLRTM", type: "crude_oil", cap: 5000000, level: 3850000, op: "Vopak" },
      { name: "Rotterdam Terminal T2", portCode: "NLRTM", type: "refined_products", cap: 2500000, level: 1900000, op: "Vopak" },
      { name: "Singapore Jurong Island", portCode: "SGSIN", type: "crude_oil", cap: 4000000, level: 3200000, op: "Jurong" },
      { name: "Fujairah Tank Farm", portCode: "AEFJR", type: "crude_oil", cap: 8000000, level: 6400000, op: "Fujairah Oil" },
      { name: "Fujairah Products Terminal", portCode: "AEFJR", type: "refined_products", cap: 3200000, level: 2560000, op: "Fujairah Oil" },
      { name: "Houston Ship Channel", portCode: "USHOU", type: "crude_oil", cap: 6000000, level: 4500000, op: "Enterprise" },
      { name: "Ras Laffan LNG", portCode: "QARAF", type: "lng", cap: 1200000, level: 960000, op: "Qatar Energy" },
      { name: "Antwerp Tank Farm", portCode: "BEANR", type: "refined_products", cap: 2000000, level: 1400000, op: "Oiltanking" },
    ];
    for (const d of defs) {
      const portId = portIds[d.portCode];
      await db.insert(storageFacilities).values({
        name: d.name, portId, type: d.type,
        totalCapacity: d.cap, currentLevel: d.level,
        utilizationRate: ((d.level / d.cap) * 100).toFixed(2),
        operator: d.op, isActive: true,
      }).onConflictDoNothing();
    }
    logger.info(`Storage facilities seeded: ${defs.length}`);
  }

  // ── 7. Trade Flows ─────────────────────────────────────────────────────────
  private async seedTradeFlows(vIds: string[], cIds: Record<string, string>, pIds: Record<string, string>): Promise<string[]> {
    const portCodes = Object.keys(pIds);
    const statuses = ["loading", "in_transit", "discharging", "completed"];
    const grades = ["Brent", "WTI", "Dubai", "ESPO", "Urals", "Arab Light"];
    const charterers = ["Shell", "BP", "TotalEnergies", "ExxonMobil", "Vitol", "Trafigura", "Gunvor"];
    const inserted: string[] = [];
    const commodityIds = Object.values(cIds);

    const tradeFlowRows: any[] = [];
    for (let i = 0; i < 40; i++) {
      const originCode = pick(portCodes);
      let destCode = pick(portCodes);
      while (destCode === originCode) destCode = pick(portCodes);
      const loadDays = rand(1, 30);
      const transitDays = rand(5, 25);
      tradeFlowRows.push({
        tenantId: TENANT_ID,
        vesselId: pick(vIds),
        commodityId: pick(commodityIds),
        originPortId: pIds[originCode],
        destinationPortId: pIds[destCode],
        cargoVolume: rand(50000, 280000),
        cargoValue: (rand(30, 120) * 1000000).toFixed(2),
        loadingDate: daysAgo(loadDays + transitDays),
        departureDate: daysAgo(loadDays + transitDays - 1),
        expectedArrival: daysAgo(loadDays - transitDays),
        actualArrival: i < 25 ? daysAgo(rand(0, loadDays)) : null,
        status: i < 10 ? "loading" : i < 20 ? "in_transit" : i < 30 ? "discharging" : "completed",
        charterer: pick(charterers),
        trader: pick(["Archer", "Freepoint", "SOCAR", "PDV", "Saudi Aramco"]),
        freight: (rand(2, 8) * 100000).toFixed(2),
        grade: pick(grades),
        hasSTS: i % 7 === 0,
        isSplit: i % 11 === 0,
        metadata: { source: "mock", confidence: 0.95 },
      });
    }
    const rows = await db.insert(tradeFlows).values(tradeFlowRows).returning({ id: tradeFlows.id });
    inserted.push(...rows.map(r => r.id));
    logger.info(`Trade flows seeded: ${inserted.length}`);
    return inserted;
  }

  // ── 8. Cargo Legs ──────────────────────────────────────────────────────────
  private async seedCargoLegs(tfIds: string[], pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const legTypes = ["origin", "waypoint", "destination"];
    const activities = ["loading", "discharging", "bunkering"];
    const rows: any[] = [];
    for (const tfId of tfIds.slice(0, 20)) {
      for (let seq = 1; seq <= 3; seq++) {
        rows.push({
          tradeFlowId: tfId,
          sequence: seq,
          portId: pick(portIds),
          legType: seq === 1 ? "origin" : seq === 3 ? "destination" : "waypoint",
          arrivalDate: daysAgo(rand(1, 20)),
          departureDate: daysAgo(rand(0, 10)),
          volumeLoaded: rand(10000, 100000),
          volumeDischargedRounded: rand(10000, 100000),
          activity: seq === 1 ? "loading" : seq === 3 ? "discharging" : "bunkering",
          waitTimeHours: (rand(2, 48)).toFixed(2),
        });
      }
    }
    await db.insert(cargoLegs).values(rows).onConflictDoNothing();
    logger.info(`Cargo legs seeded: ${rows.length}`);
  }

  // ── 9. STS Events ──────────────────────────────────────────────────────────
  private async seedStsEvents(vIds: string[], cIds: Record<string, string>, pIds: Record<string, string>, tfIds: string[]) {
    const portIds = Object.values(pIds);
    const commodityIds = Object.values(cIds);
    const rows: any[] = [];
    for (let i = 0; i < 12; i++) {
      const motherIdx = rand(0, vIds.length - 1);
      let daughterIdx = rand(0, vIds.length - 1);
      while (daughterIdx === motherIdx) daughterIdx = rand(0, vIds.length - 1);
      const start = daysAgo(rand(1, 60));
      const end = new Date(start.getTime() + rand(6, 24) * 3600_000);
      rows.push({
        motherVesselId: vIds[motherIdx],
        daughterVesselId: vIds[daughterIdx],
        tradeFlowId: tfIds[i % tfIds.length],
        locationPortId: pick(portIds),
        latitude: (rand(-10, 60) + Math.random()).toFixed(7),
        longitude: (rand(-10, 130) + Math.random()).toFixed(7),
        commodityId: pick(commodityIds),
        volumeTransferred: rand(50000, 200000),
        grade: pick(["Brent", "WTI", "ESPO"]),
        startTime: start,
        endTime: end,
        status: "completed",
        reason: pick(["arbitrage", "blending", "storage_optimization"]),
      });
    }
    await db.insert(stsEvents).values(rows).onConflictDoNothing();
    logger.info(`STS events seeded: ${rows.length}`);
  }

  // ── 10. Cargo Splits ─────────────────────────────────────────────────────
  private async seedCargoSplits(tfIds: string[], cIds: Record<string, string>, pIds: Record<string, string>, vIds: string[]) {
    const portIds = Object.values(pIds);
    const commodityIds = Object.values(cIds);
    const rows: any[] = [];
    for (const tfId of tfIds.slice(0, 10)) {
      rows.push({
        tradeFlowId: tfId, splitSequence: 1, commodityId: pick(commodityIds),
        grade: "Brent", volume: rand(40000, 100000), percentage: "60.00",
        destinationPortId: pick(portIds), buyer: pick(["Repsol", "Eni", "OMV"]),
        price: (rand(70, 100) + Math.random()).toFixed(4),
      });
      rows.push({
        tradeFlowId: tfId, splitSequence: 2, commodityId: pick(commodityIds),
        grade: "WTI", volume: rand(20000, 60000), percentage: "40.00",
        destinationPortId: pick(portIds), buyer: pick(["BP", "Shell", "Equinor"]),
        price: (rand(70, 100) + Math.random()).toFixed(4),
      });
    }
    await db.insert(cargoSplits).values(rows).onConflictDoNothing();
    logger.info(`Cargo splits seeded: ${rows.length}`);
  }

  // ── 11. Flow Forecasts ────────────────────────────────────────────────────
  private async seedFlowForecasts(pIds: Record<string, string>, cIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const commodityIds = Object.values(cIds);
    const timeframes = ["7d", "14d", "30d"];
    const trends = ["increasing", "decreasing", "stable"];
    const rows: any[] = [];
    for (let i = 0; i < 24; i++) {
      const originId = pick(portIds);
      let destId = pick(portIds);
      while (destId === originId) destId = pick(portIds);
      rows.push({
        originPortId: originId, destinationPortId: destId,
        commodityId: pick(commodityIds), timeframe: pick(timeframes),
        forecastedVolume: rand(200000, 2000000),
        forecastedVesselCount: rand(3, 25),
        confidence: (0.7 + Math.random() * 0.25).toFixed(4),
        trend: pick(trends),
        historicalAverage: rand(150000, 1800000),
        factors: { weather: 0.2, seasonality: 0.4, price_spread: 0.3 },
        modelVersion: "v2.3",
        validFrom: daysAgo(1),
        validUntil: daysFromNow(rand(7, 30)),
      });
    }
    await db.insert(flowForecasts).values(rows).onConflictDoNothing();
    logger.info(`Flow forecasts seeded: ${rows.length}`);
  }

  // ── 12. Commodity Prices ──────────────────────────────────────────────────
  private async seedCommodityPrices(cIds: Record<string, string>, mIds: Record<string, string>) {
    const commodityIds = Object.values(cIds);
    const marketIds = Object.values(mIds);
    const rows: any[] = [];
    for (let day = 0; day < 90; day++) {
      for (const cId of commodityIds.slice(0, 5)) {
        const base = 75 + Math.random() * 30;
        rows.push({
          commodityId: cId,
          marketId: pick(marketIds),
          price: (base + (Math.random() - 0.5) * 5).toFixed(4),
          unit: "barrel",
          priceType: "spot",
          change: ((Math.random() - 0.5) * 3).toFixed(4),
          changePercent: ((Math.random() - 0.5) * 2).toFixed(3),
          timestamp: daysAgo(day),
        });
      }
    }
    await db.insert(commodityPrices).values(rows).onConflictDoNothing();
    logger.info(`Commodity prices seeded: ${rows.length}`);
  }

  // ── 13. Market Analytics ──────────────────────────────────────────────────
  private async seedMarketAnalytics(cIds: Record<string, string>, mIds: Record<string, string>) {
    const commodityIds = Object.values(cIds);
    const marketIds = Object.values(mIds);
    const regions = ["global", "europe", "asia", "americas", "middle_east"];
    const rows: any[] = [];
    for (let i = 0; i < 30; i++) {
      const ps = daysAgo(i + 7);
      const pe = daysAgo(i);
      rows.push({
        commodityId: pick(commodityIds), marketId: pick(marketIds),
        region: pick(regions), period: "weekly",
        supplyData: { production: rand(8000, 12000), imports: rand(2000, 5000) },
        demandData: { consumption: rand(9000, 13000) },
        inventoryData: { level: rand(400, 600), days_of_cover: rand(20, 35) },
        balanceData: { surplus: rand(-500, 500) },
        periodStart: ps, periodEnd: pe,
      });
    }
    await db.insert(marketAnalytics).values(rows).onConflictDoNothing();
    logger.info(`Market analytics seeded: ${rows.length}`);
  }

  // ── 14. Port Stats ────────────────────────────────────────────────────────
  private async seedPortStats(pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const rows: any[] = [];
    for (const portId of portIds) {
      for (let day = 0; day < 30; day++) {
        rows.push({
          portId, date: daysAgo(day),
          arrivals: rand(8, 35), departures: rand(8, 35),
          queueLength: rand(2, 15), averageWaitHours: (rand(8, 120)).toFixed(2),
          totalVessels: rand(20, 80), throughputMT: (rand(50, 500) * 1000).toFixed(2),
          byClass: { VLCC: rand(1, 8), Suezmax: rand(2, 10), Aframax: rand(3, 12) },
        });
      }
    }
    await db.insert(portStats).values(rows).onConflictDoNothing();
    logger.info(`Port stats seeded: ${rows.length}`);
  }

  // ── 15. Port Daily Baselines ──────────────────────────────────────────────
  private async seedPortDailyBaselines(pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const rows: any[] = [];
    for (const portId of portIds) {
      for (let day = 0; day < 60; day++) {
        const d = daysAgo(day);
        const dateStr = d.toISOString().split("T")[0];
        rows.push({
          portId, day: dateStr,
          arrivals: rand(5, 30), departures: rand(5, 30),
          uniqueVessels: rand(15, 60), avgDwellHours: rand(12, 96),
          openCalls: rand(2, 15),
          arrivals30dAvg: rand(10, 25), arrivals30dStd: rand(2, 8),
          dwell30dAvg: rand(24, 72), dwell30dStd: rand(4, 20),
          openCalls30dAvg: rand(5, 12),
        });
      }
    }
    await db.insert(portDailyBaselines).values(rows).onConflictDoNothing();
    logger.info(`Port daily baselines seeded: ${rows.length}`);
  }

  // ── 16. Predictions ───────────────────────────────────────────────────────
  private async seedPredictions(cIds: Record<string, string>, mIds: Record<string, string>) {
    const commodityIds = Object.values(cIds);
    const marketIds = Object.values(mIds);
    const timeframes = ["1D", "1W", "1M"];
    const directions = ["up", "down", "stable"];
    const rows: any[] = [];
    for (let i = 0; i < 30; i++) {
      const current = rand(65, 105);
      const predicted = current + (Math.random() - 0.4) * 10;
      rows.push({
        commodityId: pick(commodityIds), marketId: pick(marketIds),
        timeframe: pick(timeframes),
        currentPrice: current.toFixed(4),
        predictedPrice: predicted.toFixed(4),
        confidence: (0.65 + Math.random() * 0.3).toFixed(4),
        direction: predicted > current ? "up" : predicted < current ? "down" : "stable",
        features: { supply_deficit: 0.3, sentiment: 0.6, weather: 0.1 },
        metadata: { model: "gradient_boost_v3" },
        validUntil: daysFromNow(rand(1, 30)),
      });
    }
    await db.insert(predictions).values(rows).onConflictDoNothing();
    logger.info(`Predictions seeded: ${rows.length}`);
  }

  // ── 17. Storage Fill Data ─────────────────────────────────────────────────
  private async seedStorageFillData(pIds: Record<string, string>) {
    const facilities = await db.select({ id: storageFacilities.id }).from(storageFacilities);
    if (!facilities.length) return;
    const rows: any[] = [];
    for (const fac of facilities) {
      for (let day = 0; day < 30; day++) {
        rows.push({
          siteId: fac.id,
          timestamp: daysAgo(day),
          fillIndex: (0.5 + Math.random() * 0.45).toFixed(4),
          confidence: (0.8 + Math.random() * 0.18).toFixed(4),
          source: pick(["SAR", "optical"]),
          metadata: { satellite: "Sentinel-1", pass: "ascending" },
        });
      }
    }
    await db.insert(storageFillData).values(rows).onConflictDoNothing();
    logger.info(`Storage fill data seeded: ${rows.length}`);
  }

  // ── 18. Floating Storage ──────────────────────────────────────────────────
  private async seedFloatingStorage(vIds: string[]) {
    const regions = ["North Sea", "Persian Gulf", "Singapore Strait", "Gulf of Mexico", "South China Sea"];
    const cargoTypes = ["crude_oil", "refined_products", "lng"];
    const grades = ["Brent", "WTI", "Dubai", "Urals", "ESPO", "ULSD"];
    const rows: any[] = [];
    for (let i = 0; i < 15; i++) {
      rows.push({
        vesselId: pick(vIds), vesselName: `Vessel ${i + 1}`,
        vesselType: pick(["VLCC", "Suezmax", "Aframax"]),
        imo: `900${rand(1000, 9999)}`,
        cargoType: pick(cargoTypes), cargoGrade: pick(grades),
        cargoVolume: rand(50000, 280000), cargoUnit: "MT",
        locationLat: (rand(-30, 60) + Math.random()).toFixed(7),
        locationLng: (rand(-20, 140) + Math.random()).toFixed(7),
        region: pick(regions), durationDays: rand(7, 90),
        startDate: daysAgo(rand(7, 90)),
        estimatedValue: (rand(20, 300) * 1000000).toFixed(2),
        charterer: pick(["Vitol", "Trafigura", "Gunvor"]),
        status: pick(["active", "active", "active", "releasing"]),
      });
    }
    await db.insert(floatingStorage).values(rows).onConflictDoNothing();
    logger.info(`Floating storage seeded: ${rows.length}`);
  }

  // ── 19. SPR Reserves ──────────────────────────────────────────────────────
  private async seedSprReserves() {
    const rows = [
      { country: "United States", countryCode: "US", region: "Gulf Coast", gradeType: "sweet_crude", volumeBarrels: "350000000", percentOfTotal: "60.00", capacityBarrels: "713500000", utilizationRate: "49.05", daysOfCover: 35, source: "DOE" },
      { country: "China", countryCode: "CN", region: "Multiple", gradeType: "mixed_crude", volumeBarrels: "295000000", percentOfTotal: "100.00", capacityBarrels: "503000000", utilizationRate: "58.65", daysOfCover: 40, source: "national_agency" },
      { country: "Japan", countryCode: "JP", region: "National", gradeType: "mixed_crude", volumeBarrels: "141000000", percentOfTotal: "100.00", capacityBarrels: "176000000", utilizationRate: "80.11", daysOfCover: 90, source: "IEA" },
      { country: "Germany", countryCode: "DE", region: "Multiple", gradeType: "sweet_crude", volumeBarrels: "60000000", percentOfTotal: "100.00", capacityBarrels: "85000000", utilizationRate: "70.59", daysOfCover: 90, source: "IEA" },
      { country: "South Korea", countryCode: "KR", region: "National", gradeType: "mixed_crude", volumeBarrels: "96000000", percentOfTotal: "100.00", capacityBarrels: "146000000", utilizationRate: "65.75", daysOfCover: 90, source: "IEA" },
    ];
    for (const r of rows) {
      await db.insert(sprReserves).values({ ...r, reportDate: daysAgo(7) }).onConflictDoNothing();
    }
    logger.info(`SPR reserves seeded: ${rows.length}`);
  }

  // ── 20. Storage Time Series ───────────────────────────────────────────────
  private async seedStorageTimeSeries() {
    const metricTypes = ["tank_level", "floating_storage", "spr_total"];
    const regions = ["global", "north_america", "europe", "asia"];
    const storageTypes = ["crude_oil", "refined_products", "lng"];
    const rows: any[] = [];
    for (let day = 0; day < 365; day++) {
      rows.push({
        recordDate: daysAgo(day),
        metricType: pick(metricTypes), region: pick(regions),
        storageType: pick(storageTypes),
        totalCapacity: (rand(1000, 5000) * 1000000).toFixed(2),
        currentLevel: (rand(600, 4200) * 1000000).toFixed(2),
        utilizationRate: (rand(60, 85) + Math.random()).toFixed(2),
        weekOverWeekChange: ((Math.random() - 0.5) * 20000000).toFixed(2),
        yearOverYearChange: ((Math.random() - 0.5) * 80000000).toFixed(2),
        fiveYearAverage: (rand(700, 3800) * 1000000).toFixed(2),
        confidence: (0.85 + Math.random() * 0.12).toFixed(4),
        source: pick(["satellite", "eia", "iea"]),
      });
    }
    await db.insert(storageTimeSeries).values(rows).onConflictDoNothing();
    logger.info(`Storage time series seeded: ${rows.length}`);
  }

  // ── 21. Port Delay Events ─────────────────────────────────────────────────
  private async seedPortDelayEvents(pIds: Record<string, string>, vIds: string[]) {
    const portIds = Object.values(pIds);
    const reasons = ["congestion", "weather", "maintenance", "customs"];
    const statuses = ["pending", "in_queue", "berthing", "discharged"];
    const rows: any[] = [];
    for (let i = 0; i < 50; i++) {
      const expected = daysAgo(rand(1, 30));
      const actual = new Date(expected.getTime() + rand(6, 96) * 3600_000);
      rows.push({
        portId: pick(portIds), vesselId: pick(vIds),
        expectedArrival: expected, actualArrival: actual,
        delayHours: (rand(6, 96)).toFixed(2),
        delayReason: pick(reasons), cargoVolume: rand(50000, 200000),
        cargoType: pick(["crude_oil", "refined_products", "lng"]),
        queuePosition: rand(1, 20),
        status: pick(statuses),
        metadata: { severity: pick(["low", "medium", "high"]) },
      });
    }
    await db.insert(portDelayEvents).values(rows).onConflictDoNothing();
    logger.info(`Port delay events seeded: ${rows.length}`);
  }

  // ── 22. Vessel Delay Snapshots ────────────────────────────────────────────
  private async seedVesselDelaySnapshots(vIds: string[], pIds: Record<string, string>, cIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const commodityIds = Object.values(cIds);
    const rows: any[] = [];
    for (const vId of vIds) {
      const scheduled = daysFromNow(rand(1, 14));
      const current = new Date(scheduled.getTime() + rand(0, 72) * 3600_000);
      rows.push({
        vesselId: vId,
        currentPortId: pick(portIds), destinationPortId: pick(portIds),
        scheduledETA: scheduled, currentETA: current,
        delayHours: ((current.getTime() - scheduled.getTime()) / 3600_000).toFixed(2),
        cargoVolume: rand(50000, 280000),
        cargoValue: (rand(30, 120) * 1000000).toFixed(2),
        commodityId: pick(commodityIds),
        impactSeverity: pick(["low", "medium", "high"]),
      });
    }
    await db.insert(vesselDelaySnapshots).values(rows).onConflictDoNothing();
    logger.info(`Vessel delay snapshots seeded: ${rows.length}`);
  }

  // ── 23. Market Delay Impacts ──────────────────────────────────────────────
  private async seedMarketDelayImpacts(pIds: Record<string, string>, cIds: Record<string, string>, mIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const commodityIds = Object.values(cIds);
    const marketIds = Object.values(mIds);
    const rows: any[] = [];
    for (let i = 0; i < 20; i++) {
      rows.push({
        portId: pick(portIds), commodityId: pick(commodityIds),
        marketId: pick(marketIds), timeframe: pick(["24h", "48h", "7d"]),
        totalDelayedVolume: rand(100000, 2000000),
        totalDelayedValue: (rand(50, 500) * 1000000).toFixed(2),
        averageDelayHours: (rand(12, 96)).toFixed(2),
        vesselCount: rand(3, 25),
        supplyImpact: (Math.random() * 5).toFixed(2),
        priceImpact: ((Math.random() - 0.3) * 3).toFixed(4),
        confidence: (0.7 + Math.random() * 0.28).toFixed(4),
        validUntil: daysFromNow(rand(1, 7)),
      });
    }
    await db.insert(marketDelayImpacts).values(rows).onConflictDoNothing();
    logger.info(`Market delay impacts seeded: ${rows.length}`);
  }

  // ── 24. Port Calls ────────────────────────────────────────────────────────
  private async seedPortCalls(vIds: string[], pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const callTypes = ["arrival", "departure", "anchorage", "berth"];
    const statuses = ["completed", "in_progress", "scheduled"];
    const purposes = ["loading", "discharging", "bunkering", "crew_change"];
    const rows: any[] = [];
    for (let i = 0; i < 80; i++) {
      const arrival = daysAgo(rand(1, 60));
      const departure = new Date(arrival.getTime() + rand(12, 120) * 3600_000);
      rows.push({
        vesselId: pick(vIds), portId: pick(portIds),
        callType: pick(callTypes), status: pick(statuses),
        arrivalTime: arrival, departureTime: departure,
        berthNumber: `B${rand(1, 20)}`,
        purpose: pick(purposes),
        cargoOperation: { type: pick(["crude", "products"]), volume: rand(50000, 200000) },
        waitTimeHours: (rand(2, 48)).toFixed(2),
        berthTimeHours: (rand(12, 72)).toFixed(2),
      });
    }
    await db.insert(portCalls).values(rows).onConflictDoNothing();
    logger.info(`Port calls seeded: ${rows.length}`);
  }

  // ── 25. Container Operations ──────────────────────────────────────────────
  private async seedContainerOperations(vIds: string[], pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const rows: any[] = [];
    for (let i = 0; i < 30; i++) {
      rows.push({
        vesselId: pick(vIds), portId: pick(portIds),
        operationType: pick(["load", "discharge", "transshipment"]),
        containerType: pick(["20ft", "40ft", "40ft_hc", "reefer"]),
        teuCount: rand(100, 3000), feuCount: rand(50, 1500),
        commodityType: pick(["general_cargo", "refrigerated", "hazmat"]),
        origin: pick(["Rotterdam", "Shanghai", "Singapore"]),
        destination: pick(["Houston", "Hamburg", "Busan"]),
        shippingLine: pick(["Maersk", "MSC", "CMA CGM", "Evergreen"]),
        bookingReference: `BK${rand(100000, 999999)}`,
        operationDate: daysAgo(rand(1, 30)),
        handlingTime: (rand(4, 48)).toFixed(2),
      });
    }
    await db.insert(containerOperations).values(rows).onConflictDoNothing();
    logger.info(`Container operations seeded: ${rows.length}`);
  }

  // ── 26. Bunkering Events ──────────────────────────────────────────────────
  private async seedBunkeringEvents(vIds: string[], pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const fuelTypes = ["vlsfo", "hsfo", "mgo", "lng", "methanol"];
    const rows: any[] = [];
    for (let i = 0; i < 40; i++) {
      const start = daysAgo(rand(1, 90));
      const end = new Date(start.getTime() + rand(4, 18) * 3600_000);
      const vol = rand(200, 3000);
      const price = rand(400, 800);
      rows.push({
        vesselId: pick(vIds), portId: pick(portIds),
        eventType: pick(["port_bunkering", "sts_bunkering", "scheduled"]),
        fuelType: pick(fuelTypes),
        volumeMT: vol.toFixed(2), pricePerMT: price.toFixed(2),
        totalCost: (vol * price).toFixed(2),
        supplier: pick(["Shell Marine", "World Fuel", "Monjasa", "Peninsula"]),
        startTime: start, endTime: end,
        grade: pick(["0.5%S VLSFO", "0.1%S MGO", "3.5%S IFO"]),
        consumptionRate: (rand(30, 80)).toFixed(2),
      });
    }
    await db.insert(bunkeringEvents).values(rows).onConflictDoNothing();
    logger.info(`Bunkering events seeded: ${rows.length}`);
  }

  // ── 27. Communications ────────────────────────────────────────────────────
  private async seedCommunications() {
    const types = ["alert", "notification", "system"];
    const categories = ["vessel_update", "port_event", "price_alert", "delay_warning"];
    const priorities = ["normal", "high", "critical", "low"];
    const subjects = [
      "CRITICAL: Rotterdam congestion — 18 vessels queued (3× baseline)",
      "HIGH: Fujairah crude storage at 87% capacity",
      "ALERT: LNG Horizon – 48-hour weather delay at Ras Laffan",
      "NOTICE: Crude price spread WTI/Brent narrows to $1.20",
      "SIGNAL: Unusual departure cluster detected at Singapore",
      "UPDATE: Seaways Pioneer ETA revised — now T+6h",
      "WARNING: New sanctions affecting IRKHI shipments",
      "INFO: Monthly supply-demand balance published",
      "CRITICAL: STS transfer detected — Fujairah anchorage zone 4",
      "HIGH: Floating storage inventory up 12% week-on-week",
      "ALERT: Iron ore price spike — BCI index +3.8%",
      "NOTICE: Q1 refinery utilization report available",
    ];
    const rows: any[] = [];
    for (let i = 0; i < subjects.length; i++) {
      rows.push({
        userId: ADMIN_USER_ID,
        messageType: pick(types),
        category: pick(categories),
        subject: subjects[i],
        body: `Automated intelligence alert: ${subjects[i]}. Review dashboard for full analysis.`,
        priority: i < 3 ? "critical" : i < 6 ? "high" : pick(priorities),
        isRead: i > 6,
        isArchived: false,
        createdAt: hoursAgo(rand(1, 240)),
      });
    }
    await db.insert(communications).values(rows).onConflictDoNothing();
    logger.info(`Communications seeded: ${rows.length}`);
  }

  // ── 28. Crude Grades ──────────────────────────────────────────────────────
  private async seedCrudeGrades() {
    const defs = [
      { name: "Brent Blend", gradeCode: "BRENT", category: "crude", origin: "North Sea", api: "38.3", sulfur: "0.370", benchmark: "Brent", price: "83.45" },
      { name: "WTI", gradeCode: "WTI_C", category: "crude", origin: "United States", api: "39.6", sulfur: "0.240", benchmark: "WTI", price: "79.85" },
      { name: "Dubai Crude", gradeCode: "DUBAI_C", category: "crude", origin: "UAE", api: "31.0", sulfur: "2.040", benchmark: "Dubai", price: "81.20" },
      { name: "Arab Light", gradeCode: "ARAB_L", category: "crude", origin: "Saudi Arabia", api: "33.0", sulfur: "1.770", benchmark: "Dubai", price: "80.10" },
      { name: "Arab Heavy", gradeCode: "ARAB_H", category: "crude", origin: "Saudi Arabia", api: "27.7", sulfur: "2.800", benchmark: "Dubai", price: "77.50" },
      { name: "ESPO Blend", gradeCode: "ESPO", category: "crude", origin: "Russia", api: "34.8", sulfur: "0.560", benchmark: "Dubai", price: "78.30" },
      { name: "Urals Blend", gradeCode: "URALS", category: "crude", origin: "Russia", api: "31.5", sulfur: "1.350", benchmark: "Brent", price: "76.90" },
      { name: "Basra Light", gradeCode: "BASRA_L", category: "crude", origin: "Iraq", api: "29.8", sulfur: "2.960", benchmark: "Dubai", price: "78.80" },
      { name: "CPC Blend", gradeCode: "CPC", category: "crude", origin: "Kazakhstan", api: "44.2", sulfur: "0.540", benchmark: "Brent", price: "84.10" },
      { name: "Forties Blend", gradeCode: "FORTIES", category: "crude", origin: "UK", api: "40.7", sulfur: "0.330", benchmark: "Brent", price: "82.95" },
    ];
    for (const d of defs) {
      await db.insert(crudeGrades).values({
        name: d.name, gradeCode: d.gradeCode, category: d.category,
        origin: d.origin, apiGravity: d.api, sulfurContent: d.sulfur,
        priceBenchmark: d.benchmark, currentPrice: d.price, priceUnit: "USD/bbl",
      }).onConflictDoNothing();
    }
    logger.info(`Crude grades seeded: ${defs.length}`);
  }

  // ── 29. LNG Cargoes ───────────────────────────────────────────────────────
  private async seedLngCargoes(vIds: string[], pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const buyers = ["Tokyo Gas", "JERA", "Kogas", "Shell LNG", "TotalEnergies"];
    const sellers = ["Qatar Energy", "Shell", "Chevron", "Woodside"];
    const rows: any[] = [];
    for (let i = 0; i < 20; i++) {
      rows.push({
        cargoId: `LNG-${2024}-${String(i + 1).padStart(3, "0")}`,
        cargoType: pick(["lng", "lpg", "propane"]),
        vesselId: pick(vIds),
        loadPortId: pick(portIds), dischargePortId: pick(portIds),
        volume: (rand(60000, 175000)).toFixed(2),
        volumeUnit: "m3",
        isDiversion: i % 5 === 0,
        loadDate: daysAgo(rand(1, 60)),
        dischargeDate: daysFromNow(rand(1, 20)),
        price: (rand(10, 18) + Math.random()).toFixed(2), priceUnit: "USD/mmbtu",
        buyer: pick(buyers), seller: pick(sellers),
        contractType: pick(["spot", "term", "fob", "dap"]),
        terminalCapacity: (rand(5, 12) * 1000000).toFixed(2),
      });
    }
    await db.insert(lngCargoes).values(rows).onConflictDoNothing();
    logger.info(`LNG cargoes seeded: ${rows.length}`);
  }

  // ── 30. Dry Bulk Fixtures ─────────────────────────────────────────────────
  private async seedDryBulkFixtures(vIds: string[], pIds: Record<string, string>) {
    const portIds = Object.values(pIds);
    const rows: any[] = [];
    for (let i = 0; i < 25; i++) {
      const laycanStart = daysFromNow(rand(1, 14));
      const laycanEnd = new Date(laycanStart.getTime() + 3 * 86400_000);
      rows.push({
        fixtureId: `FIX-${2024}-${String(i + 1).padStart(4, "0")}`,
        commodityType: pick(["coal", "iron_ore", "grain", "bauxite"]),
        subtype: pick(["thermal_coal", "coking_coal", "wheat", "iron_ore_fines"]),
        vesselId: pick(vIds),
        vesselSize: pick(["capesize", "panamax", "handymax"]),
        loadPortId: pick(portIds), dischargePortId: pick(portIds),
        quantity: (rand(50000, 200000)).toFixed(2),
        freightRate: (rand(8, 35) + Math.random()).toFixed(2),
        charterer: pick(["Vale", "BHP", "Rio Tinto", "Glencore"]),
        shipper: pick(["Cargill", "ADM", "Louis Dreyfus"]),
        laycanStart, laycanEnd,
        loadDate: daysFromNow(rand(2, 20)),
        eta: daysFromNow(rand(20, 60)),
        fixtureDate: daysAgo(rand(1, 14)),
        marketIndex: pick(["BCI", "BPI", "BSI"]),
      });
    }
    await db.insert(dryBulkFixtures).values(rows).onConflictDoNothing();
    logger.info(`Dry bulk fixtures seeded: ${rows.length}`);
  }

  // ── 31. Petrochem Products ────────────────────────────────────────────────
  private async seedPetrochemProducts() {
    const defs = [
      { productName: "Ethylene", productCode: "ETHYLENE_EU", category: "olefins", subcategory: "ethylene", feedstock: "naphtha", yieldRate: "28.50", marginSpread: "250.00", currentPrice: "850.00", region: "Europe", capacity: "1200000.00", utilization: "88.50" },
      { productName: "Propylene", productCode: "PROPYLENE_AS", category: "olefins", subcategory: "propylene", feedstock: "naphtha", yieldRate: "14.20", marginSpread: "180.00", currentPrice: "720.00", region: "Asia", capacity: "950000.00", utilization: "85.30" },
      { productName: "Benzene", productCode: "BENZENE_GL", category: "aromatics", subcategory: "benzene", feedstock: "naphtha", yieldRate: "8.50", marginSpread: "320.00", currentPrice: "1100.00", region: "Global", capacity: "800000.00", utilization: "91.20" },
      { productName: "Paraxylene", productCode: "PX_AS", category: "aromatics", subcategory: "paraxylene", feedstock: "naphtha", yieldRate: "6.80", marginSpread: "280.00", currentPrice: "980.00", region: "Asia", capacity: "1500000.00", utilization: "94.00" },
      { productName: "Polyethylene", productCode: "PE_AS", category: "polymers", subcategory: "polyethylene", feedstock: "ethane", yieldRate: "95.00", marginSpread: "350.00", currentPrice: "1250.00", region: "Asia", capacity: "2000000.00", utilization: "87.60" },
    ];
    for (const d of defs) {
      await db.insert(petrochemProducts).values({
        productName: d.productName, productCode: d.productCode,
        category: d.category, subcategory: d.subcategory,
        feedstock: d.feedstock, yieldRate: d.yieldRate,
        marginSpread: d.marginSpread, currentPrice: d.currentPrice,
        priceUnit: "USD/ton", region: d.region,
        capacity: d.capacity, utilizationRate: d.utilization,
      }).onConflictDoNothing();
    }
    logger.info(`Petrochem products seeded: ${defs.length}`);
  }

  // ── 32. Agri/Biofuel Flows ────────────────────────────────────────────────
  private async seedAgriBiofuelFlows() {
    const rows: any[] = [];
    const entries = [
      { id: "AGR-001", type: "soybean", flow: "export", from: "Brazil", to: "China", vol: "1850000", biofuel: null, feedstock: null, cert: null, ci: null, price: "520.00" },
      { id: "AGR-002", type: "palm_oil", flow: "export", from: "Indonesia", to: "India", vol: "980000", biofuel: "biodiesel", feedstock: "palm_oil", cert: "RSPO", ci: "45.20", price: "890.00" },
      { id: "AGR-003", type: "ethanol", flow: "production", from: "United States", to: "United States", vol: "15800000", biofuel: "ethanol", feedstock: "corn", cert: "ISCC", ci: "28.50", price: "680.00" },
      { id: "AGR-004", type: "biodiesel", flow: "export", from: "Germany", to: "Netherlands", vol: "420000", biofuel: "biodiesel", feedstock: "used_cooking_oil", cert: "2BSvs", ci: "12.30", price: "1250.00" },
      { id: "AGR-005", type: "rapeseed", flow: "export", from: "France", to: "Germany", vol: "750000", biofuel: "biodiesel", feedstock: "rapeseed", cert: "RSB", ci: "38.40", price: "550.00" },
    ];
    for (const e of entries) {
      await db.insert(agriBiofuelFlows).values({
        flowId: e.id, commodityType: e.type, flowType: e.flow as any,
        originCountry: e.from, destinationCountry: e.to,
        volume: e.vol, volumeUnit: "MT",
        biofuelType: e.biofuel as any, feedstock: e.feedstock as any,
        sustainabilityCert: e.cert as any,
        carbonIntensity: e.ci as any, price: e.price, priceUnit: "USD/ton",
        flowDate: daysAgo(rand(1, 30)), trader: pick(["Cargill", "Bunge", "ADM", "Louis Dreyfus"]),
      }).onConflictDoNothing();
    }
    logger.info(`Agri/biofuel flows seeded: ${entries.length}`);
  }

  // ── 33. Refineries ────────────────────────────────────────────────────────
  private async seedRefineries() {
    const defs = [
      { name: "Pernis Refinery", code: "PERNIS", country: "Netherlands", region: "Europe", op: "Shell", cap: "400000", thru: "368000", util: "92.00", nelson: "12.5", mainStatus: "operational" },
      { name: "Ras Tanura Refinery", code: "RAST", country: "Saudi Arabia", region: "Middle East", op: "Saudi Aramco", cap: "550000", thru: "495000", util: "90.00", nelson: "8.2", mainStatus: "operational" },
      { name: "Jurong Island Refinery", code: "JURONG", country: "Singapore", region: "Asia", op: "ExxonMobil", cap: "592000", thru: "514000", util: "86.80", nelson: "11.8", mainStatus: "operational" },
      { name: "Baytown Refinery", code: "BAYTOWN", country: "United States", region: "North America", op: "ExxonMobil", cap: "560000", thru: "520000", util: "92.86", nelson: "15.1", mainStatus: "operational" },
      { name: "Motiva Port Arthur", code: "MOTIVA_PA", country: "United States", region: "North America", op: "Saudi Aramco", cap: "630000", thru: "598500", util: "95.00", nelson: "13.7", mainStatus: "operational" },
      { name: "Ulsan Refinery", code: "ULSAN", country: "South Korea", region: "Asia", op: "SK Energy", cap: "840000", thru: "756000", util: "90.00", nelson: "10.9", mainStatus: "operational" },
      { name: "Jamnagar Refinery", code: "JAMNAGAR", country: "India", region: "Asia", op: "Reliance", cap: "1240000", thru: "1116000", util: "90.00", nelson: "14.3", mainStatus: "operational" },
      { name: "Grangemouth Refinery", code: "GRANGEMTH", country: "United Kingdom", region: "Europe", op: "INEOS", cap: "210000", thru: "168000", util: "80.00", nelson: "9.1", mainStatus: "planned_maintenance" },
    ];
    for (const d of defs) {
      await db.insert(refineries).values({
        name: d.name, refineryCode: d.code, country: d.country, region: d.region,
        operator: d.op, capacity: d.cap, currentThroughput: d.thru,
        utilizationRate: d.util, complexityIndex: d.nelson,
        yieldGasoline: "25.50", yieldDiesel: "38.20", yieldJetFuel: "12.30", yieldOther: "24.00",
        maintenanceStatus: d.mainStatus,
        marginPerBarrel: (rand(4, 18) + Math.random()).toFixed(2),
      }).onConflictDoNothing();
    }
    logger.info(`Refineries seeded: ${defs.length}`);
  }

  // ── 34. Supply/Demand Balances ────────────────────────────────────────────
  private async seedSupplyDemandBalances() {
    const commodities = ["crude_oil", "gasoline", "diesel", "lng"];
    const regions = ["global", "north_america", "europe", "asia", "middle_east"];
    const rows: any[] = [];
    for (const commodity of commodities) {
      for (const region of regions) {
        for (let q = 0; q < 8; q++) {
          const year = q < 4 ? 2024 : 2025;
          const quarter = q % 4 + 1;
          rows.push({
            balanceId: `${commodity.toUpperCase()}-${region.toUpperCase()}-${year}Q${quarter}`,
            commodity, region,
            period: `${year}_Q${quarter}`,
            production: (rand(5000, 15000)).toFixed(2),
            consumption: (rand(4500, 14000)).toFixed(2),
            imports: (rand(500, 5000)).toFixed(2),
            exports: (rand(500, 5000)).toFixed(2),
            inventoryChange: ((Math.random() - 0.4) * 500).toFixed(2),
            closingInventory: (rand(1000, 8000)).toFixed(2),
            balanceValue: ((Math.random() - 0.4) * 1000).toFixed(2),
            unit: "kbd",
            forecastType: q < 6 ? "actual" : "forecast",
            dataSource: pick(["IEA", "EIA", "OPEC"]),
          });
        }
      }
    }
    await db.insert(supplyDemandBalances).values(rows).onConflictDoNothing();
    logger.info(`Supply/demand balances seeded: ${rows.length}`);
  }

  // ── 35. Research Reports ──────────────────────────────────────────────────
  private async seedResearchReports() {
    const defs = [
      { id: "RPT-2025-001", title: "Global Crude Outlook Q2 2025", category: "price_forecast", sub: "crude_oil", outlook: "bullish", analyst: "A. Davidson", conf: "high" },
      { id: "RPT-2025-002", title: "LNG Market Tightness — Asian Demand Surge", category: "market_analysis", sub: "lng", outlook: "bullish", analyst: "S. Nakamura", conf: "medium" },
      { id: "RPT-2025-003", title: "Refinery Margins Under Pressure", category: "supply_demand", sub: "refining", outlook: "bearish", analyst: "M. Hansen", conf: "high" },
      { id: "RPT-2025-004", title: "OPEC+ Strategy Review — Production Cuts Extended", category: "market_analysis", sub: "crude_oil", outlook: "bullish", analyst: "A. Davidson", conf: "high" },
      { id: "RPT-2025-005", title: "Shipping Rates: Baltic Index YTD Analysis", category: "trade_flow", sub: "shipping", outlook: "neutral", analyst: "P. Williams", conf: "medium" },
      { id: "RPT-2025-006", title: "Floating Storage Trends — North Sea Build-Up", category: "supply_demand", sub: "crude_oil", outlook: "bearish", analyst: "M. Hansen", conf: "medium" },
      { id: "RPT-2025-007", title: "Biofuel Mandate Impact on Crop Trade Flows", category: "trade_flow", sub: "biofuels", outlook: "bullish", analyst: "F. Dupont", conf: "low" },
      { id: "RPT-2025-008", title: "Petrochemical Margins Recovery — Asia Pacific", category: "market_analysis", sub: "refining", outlook: "bullish", analyst: "S. Nakamura", conf: "medium" },
    ];
    for (const d of defs) {
      await db.insert(researchReports).values({
        reportId: d.id, title: d.title, category: d.category,
        subcategory: d.sub, priceOutlook: d.outlook,
        summary: `Comprehensive analysis of ${d.title.toLowerCase()}. Key market drivers and outlook for the next 3-12 months.`,
        keyInsights: [{ insight: "Supply deficit emerging", confidence: 0.85 }, { insight: "Demand resilient", confidence: 0.78 }],
        shortTermForecast: "Prices expected to remain supported above $80/bbl on supply tightness.",
        mediumTermForecast: "Range-bound $75-$95/bbl through Q3 2025 on balanced fundamentals.",
        analyst: d.analyst, confidenceLevel: d.conf,
        tags: [d.sub, d.category, "2025"],
        publishDate: daysAgo(rand(1, 60)),
        isPublished: true,
      }).onConflictDoNothing();
    }
    logger.info(`Research reports seeded: ${defs.length}`);
  }

  // ── 36. Refinery Units ────────────────────────────────────────────────────
  private async seedRefineryUnits() {
    const units: any[] = [
      { plant: "Pernis", unit: "CDU", bpd: 400000 },
      { plant: "Pernis", unit: "VDU", bpd: 150000 },
      { plant: "Pernis", unit: "FCC", bpd: 110000 },
      { plant: "Baytown", unit: "CDU", bpd: 560000 },
      { plant: "Baytown", unit: "HCU", bpd: 200000 },
      { plant: "Jurong", unit: "CDU", bpd: 592000 },
      { plant: "Jurong", unit: "FCC", bpd: 180000 },
      { plant: "Jamnagar", unit: "CDU", bpd: 1240000 },
      { plant: "Jamnagar", unit: "VDU", bpd: 600000 },
      { plant: "Ulsan", unit: "CDU", bpd: 840000 },
    ];
    await db.insert(refineryUnits).values(units).onConflictDoNothing();
    logger.info(`Refinery units seeded: ${units.length}`);
  }

  // ── 37. Refinery Utilization Daily ───────────────────────────────────────
  private async seedRefineryUtilizationDaily() {
    const plants = ["Pernis", "Baytown", "Jurong", "Jamnagar", "Ulsan"];
    const rows: any[] = [];
    for (const plant of plants) {
      for (let day = 0; day < 90; day++) {
        rows.push({
          date: daysAgo(day).toISOString().split("T")[0],
          plant,
          utilizationPct: (80 + Math.random() * 15).toFixed(2),
        });
      }
    }
    await db.insert(refineryUtilizationDaily).values(rows).onConflictDoNothing();
    logger.info(`Refinery utilization daily seeded: ${rows.length}`);
  }

  // ── 38. Refinery Crack Spreads Daily ─────────────────────────────────────
  private async seedRefineryCrackSpreadsDaily() {
    const rows: any[] = [];
    for (let day = 0; day < 365; day++) {
      const crude = (70 + Math.random() * 30).toFixed(2);
      rows.push({
        date: daysAgo(day).toISOString().split("T")[0],
        spread321Usd: (rand(8, 25) + Math.random()).toFixed(2),
        gasolineUsd: (parseFloat(crude) + rand(10, 25)).toFixed(2),
        dieselUsd: (parseFloat(crude) + rand(15, 35)).toFixed(2),
        crudeUsd: crude,
      });
    }
    await db.insert(refineryCrackSpreadsDaily).values(rows).onConflictDoNothing();
    logger.info(`Crack spreads daily seeded: ${rows.length}`);
  }

  // ── 39. S&D Models Daily ──────────────────────────────────────────────────
  private async seedSdModelsDaily() {
    const regions = ["EU", "US", "ASIA"];
    const rows: any[] = [];
    for (const region of regions) {
      for (let day = 0; day < 180; day++) {
        const supply = rand(8000, 15000);
        const demand = rand(7500, 14500);
        rows.push({
          date: daysAgo(day).toISOString().split("T")[0],
          region,
          supplyMt: supply, demandMt: demand, balanceMt: supply - demand,
        });
      }
    }
    await db.insert(sdModelsDaily).values(rows).onConflictDoNothing();
    logger.info(`S&D models daily seeded: ${rows.length}`);
  }

  // ── 40. S&D Forecasts Weekly ──────────────────────────────────────────────
  private async seedSdForecastsWeekly() {
    const regions = ["EU", "US", "ASIA"];
    const rows: any[] = [];
    for (const region of regions) {
      for (let week = 0; week < 52; week++) {
        const d = new Date(); d.setDate(d.getDate() - week * 7);
        rows.push({
          weekEnd: d.toISOString().split("T")[0],
          region,
          balanceForecastMt: rand(-500, 800),
        });
      }
    }
    await db.insert(sdForecastsWeekly).values(rows).onConflictDoNothing();
    logger.info(`S&D forecasts weekly seeded: ${rows.length}`);
  }

  // ── 41. Research Insights Daily ───────────────────────────────────────────
  private async seedResearchInsightsDaily() {
    const titles = [
      "Crude backwardation deepens amid OPEC+ discipline",
      "LNG spot premiums spike on Asian cold snap",
      "Rotterdam congestion eases after port worker strike",
      "Brent/WTI spread narrows to 6-month low",
      "Floating storage build accelerates in Persian Gulf",
      "Shipping rates surge on Red Sea diversion premium",
      "Biofuel blending mandates boost rapeseed demand",
      "Petrochemical crackers hit 94% utilization in Asia",
      "Suez Canal transit volumes recovery slows",
      "Iron ore price supported by Chinese steel output data",
    ];
    const rows: any[] = [];
    for (let day = 0; day < titles.length; day++) {
      rows.push({
        date: daysAgo(day).toISOString().split("T")[0],
        title: titles[day],
        summary: `${titles[day]}. Analysis based on satellite, AIS, and pricing data compiled by the Veriscope intelligence engine.`,
        impactScore: (0.5 + Math.random() * 0.5).toFixed(2),
      });
    }
    await db.insert(researchInsightsDaily).values(rows).onConflictDoNothing();
    logger.info(`Research insights daily seeded: ${rows.length}`);
  }

  // ── 42. Watchlists ────────────────────────────────────────────────────────
  private async seedWatchlists() {
    const rows: any[] = [
      { userId: ADMIN_USER_ID, name: "Key Oil Terminals", type: "ports", isDefault: true, items: ["NLRTM", "AEFJR", "USHOU"] },
      { userId: ADMIN_USER_ID, name: "VLCC Fleet Watch", type: "vessels", isDefault: false, items: ["256148000", "538006575", "311050500"] },
      { userId: ADMIN_USER_ID, name: "Crude Benchmarks", type: "commodities", isDefault: false, items: ["BRENT", "WTI", "DUBAI"] },
      { userId: ADMIN_USER_ID, name: "Middle East Routes", type: "routes", isDefault: false, items: ["SARTA-NLRTM", "AEFJR-SGSIN", "QARAF-JPTYO"] },
    ];
    for (const r of rows) {
      await db.insert(watchlists).values({
        ...r, tenantId: TENANT_ID,
        alertSettings: { email: true, push: true, minSeverity: "medium" },
      }).onConflictDoNothing();
    }
    logger.info(`Watchlists seeded: ${rows.length}`);
  }

  // ── 43. Alert Rules ───────────────────────────────────────────────────────
  private async seedAlertRules() {
    const rows: any[] = [
      { name: "Brent > $95/bbl", type: "price_threshold", severity: "high", conditions: { commodity: "BRENT", operator: "gt", threshold: 95, unit: "USD/bbl" } },
      { name: "Rotterdam Congestion Alert", type: "congestion", severity: "critical", conditions: { port: "NLRTM", operator: "gt", threshold: 15, metric: "queued_vessels" } },
      { name: "Fujairah Storage > 90%", type: "storage_level", severity: "high", conditions: { port: "AEFJR", operator: "gt", threshold: 90, metric: "utilization_pct" } },
      { name: "VLCC Fleet Delay > 48h", type: "vessel_arrival", severity: "medium", conditions: { vessel_type: "vlcc", operator: "gt", threshold: 48, metric: "delay_hours" } },
      { name: "Singapore Port Queue > 20", type: "congestion", severity: "high", conditions: { port: "SGSIN", operator: "gt", threshold: 20, metric: "queued_vessels" } },
    ];
    for (const r of rows) {
      await db.insert(alertRules).values({
        ...r, userId: ADMIN_USER_ID, tenantId: TENANT_ID,
        isActive: true, isMuted: false, cooldownMinutes: 60,
        channels: { email: true, webhook: false, in_app: true },
      }).onConflictDoNothing();
    }
    logger.info(`Alert rules seeded: ${rows.length}`);
  }

  // ── 44. Alerts ────────────────────────────────────────────────────────────
  private async seedAlerts() {
    const rows: any[] = [
      { userId: ADMIN_USER_ID, type: "price_move", title: "Brent Crude +3.2% Spike", description: "Brent moved above $84/bbl following OPEC+ statement", frequency: "real_time", isActive: true },
      { userId: ADMIN_USER_ID, type: "vessel_arrival", title: "VLCC expected Rotterdam T+2h", description: "Seaways Pioneer ETA revised", frequency: "real_time", isActive: true },
      { userId: ADMIN_USER_ID, type: "trade_flow", title: "New STS Event Detected", description: "Fujairah anchorage Zone 4 — 2 VLCCs", frequency: "hourly", isActive: true },
      { userId: ADMIN_USER_ID, type: "storage_level", title: "Fujairah crude tanks at 87%", description: "Above 85% threshold — monitor for supply build", frequency: "daily", isActive: true },
    ];
    for (const r of rows) {
      await db.insert(alerts).values({
        ...r, conditions: {},
        lastTriggered: hoursAgo(rand(1, 48)),
      }).onConflictDoNothing();
    }
    logger.info(`Alerts seeded: ${rows.length}`);
  }

  // ── 45. Notifications ─────────────────────────────────────────────────────
  private async seedNotifications() {
    const rows: any[] = [
      { userId: ADMIN_USER_ID, type: "price_alert", title: "Brent > $84.00", message: "Brent Crude crossed $84.00/bbl threshold at 09:42 UTC", severity: "warning", isRead: false },
      { userId: ADMIN_USER_ID, type: "vessel_event", title: "Vessel Arrival — Rotterdam", message: "Seaways Pioneer arrived at Rotterdam (berth 12A)", severity: "info", isRead: false },
      { userId: ADMIN_USER_ID, type: "congestion", title: "Rotterdam queue: 18 vessels", message: "Port congestion elevated — 18 vessels in anchorage (3.2× 30d average)", severity: "critical", isRead: false },
      { userId: ADMIN_USER_ID, type: "storage_alert", title: "Fujairah tank utilisation 87%", message: "Storage approaching operational limit — review offtake schedule", severity: "warning", isRead: true },
      { userId: ADMIN_USER_ID, type: "trade_flow", title: "New cargo flow: SARTA → NLRTM", message: "250,000 MT Arab Light loading Ras Tanura — ETA Rotterdam +14 days", severity: "info", isRead: true },
    ];
    for (const r of rows) {
      await db.insert(notifications).values({ ...r, data: {} }).onConflictDoNothing();
    }
    logger.info(`Notifications seeded: ${rows.length}`);
  }

  // ── 46. Model Registry ────────────────────────────────────────────────────
  private async seedModelRegistry() {
    const models = [
      { name: "Price Forecast GBM", version: "3.2.1", type: "regression" },
      { name: "Congestion Classifier", version: "2.0.4", type: "classification" },
      { name: "Delay Predictor LSTM", version: "1.8.0", type: "regression" },
      { name: "Flow Volume Ensemble", version: "4.1.0", type: "ensemble" },
    ];
    const inserted: string[] = [];
    for (const m of models) {
      const [row] = await db.insert(modelRegistry).values({
        modelName: m.name, version: m.version, modelType: m.type,
        features: ["supply_deficit", "price_spread", "congestion_index"],
        status: "active", isActive: true,
        trainingMetrics: { rmse: 2.4, mae: 1.8, r2: 0.87 },
        validationMetrics: { rmse: 2.9, mae: 2.2, r2: 0.83 },
      }).onConflictDoNothing().returning({ id: modelRegistry.id });
      if (row) inserted.push(row.id);
    }
    // Seed model predictions
    const all = await db.select({ id: modelRegistry.id }).from(modelRegistry);
    const rows: any[] = [];
    for (const m of all) {
      for (let i = 0; i < 10; i++) {
        const predicted = rand(70, 105);
        rows.push({
          modelId: m.id, target: pick(["brent_price", "congestion_score", "delay_hours"]),
          predictionDate: daysAgo(i), horizon: pick(["1d", "7d", "30d"]),
          predictedValue: predicted.toFixed(4),
          confidenceLower: (predicted - rand(3, 8)).toFixed(4),
          confidenceUpper: (predicted + rand(3, 8)).toFixed(4),
          confidenceLevel: "0.9500",
          actualValue: i < 8 ? (predicted + (Math.random() - 0.5) * 5).toFixed(4) : null,
        });
      }
    }
    if (rows.length) await db.insert(modelPredictions).values(rows).onConflictDoNothing();
    logger.info(`Model registry seeded: ${all.length} models, ${rows.length} predictions`);
  }

  // ── 47. Data Quality Scores ───────────────────────────────────────────────
  private async seedDataQualityScores(pIds: Record<string, string>, vIds: string[]) {
    const portIds = Object.values(pIds);
    const metricTypes = ["congestion", "storage_fill", "vessel_count"];
    const rows: any[] = [];
    for (const portId of portIds) {
      for (const metric of metricTypes) {
        rows.push({
          metricType: metric, entityId: portId,
          value: (rand(5, 100) + Math.random()).toFixed(4),
          confidenceScore: (0.75 + Math.random() * 0.23).toFixed(4),
          dataCompleteness: (0.85 + Math.random() * 0.14).toFixed(4),
          dataFreshness: rand(60, 3600),
          outlierScore: (Math.random() * 0.3).toFixed(4),
          contributingSources: { ais_messages: rand(100, 2000), satellite_passes: rand(1, 4) },
          methodology: "z-score normalised against 30d rolling mean",
        });
      }
    }
    for (const vId of vIds.slice(0, 8)) {
      rows.push({
        metricType: "vessel_position", entityId: vId,
        value: (rand(0, 20) + Math.random()).toFixed(4),
        confidenceScore: (0.8 + Math.random() * 0.18).toFixed(4),
        dataCompleteness: (0.9 + Math.random() * 0.09).toFixed(4),
        dataFreshness: rand(30, 900),
        outlierScore: (Math.random() * 0.2).toFixed(4),
        contributingSources: { ais_feed: "aisstream.io" },
        methodology: "position quality from AIS signal strength",
      });
    }
    await db.insert(dataQualityScores).values(rows).onConflictDoNothing();
    logger.info(`Data quality scores seeded: ${rows.length}`);
  }
}

export const mockDataService = new MockDataService();

