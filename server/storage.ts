import { drizzle } from "drizzle-orm/neon-http";
import { 
  users, vessels, ports, vesselPositions, storageFacilities, commodities, markets, 
  commodityPrices, tradeFlows, marketAnalytics, alerts, notifications,
  portStats, predictions, storageFillData,
  portDelayEvents, vesselDelaySnapshots, marketDelayImpacts,
  cargoLegs, stsEvents, cargoSplits, flowForecasts,
  portCalls, containerOperations, bunkeringEvents, communications,
  crudeGrades, lngCargoes, dryBulkFixtures, petrochemProducts, agriBiofuelFlows,
  refineries, supplyDemandBalances, researchReports,
  refineryUnits, refineryUtilizationDaily, refineryCrackSpreadsDaily,
  sdModelsDaily, sdForecastsWeekly, researchInsightsDaily,
  watchlists, alertRules
} from "@shared/schema";
import { neon } from "@neondatabase/serverless";
import { eq, desc, and, gte, lte, or } from "drizzle-orm";
import type {
  User, InsertUser,
  Vessel, InsertVessel,
  Port, InsertPort,
  VesselPosition,
  StorageFacility,
  Commodity, InsertCommodity,
  Market, InsertMarket,
  CommodityPrice,
  TradeFlow, InsertTradeFlow,
  MarketAnalytics,
  Alert, InsertAlert,
  Notification,
  PortStats, InsertPortStats,
  Prediction, InsertPrediction,
  StorageFillData, InsertStorageFillData,
  PortDelayEvent, InsertPortDelayEvent,
  VesselDelaySnapshot, InsertVesselDelaySnapshot,
  MarketDelayImpact, InsertMarketDelayImpact,
  CargoLeg, InsertCargoLeg,
  STSEvent, InsertSTSEvent,
  CargoSplit, InsertCargoSplit,
  FlowForecast, InsertFlowForecast,
  PortCall, InsertPortCall,
  ContainerOperation, InsertContainerOperation,
  BunkeringEvent, InsertBunkeringEvent,
  Communication, InsertCommunication,
  CrudeGrade, InsertCrudeGrade,
  LngCargo, InsertLngCargo,
  DryBulkFixture, InsertDryBulkFixture,
  PetrochemProduct, InsertPetrochemProduct,
  AgriBiofuelFlow, InsertAgriBiofuelFlow,
  Refinery, InsertRefinery,
  SupplyDemandBalance, InsertSupplyDemandBalance,
  ResearchReport, InsertResearchReport,
  RefineryUnit, InsertRefineryUnit,
  RefineryUtilizationDaily, InsertRefineryUtilizationDaily,
  RefineryCrackSpreadsDaily, InsertRefineryCrackSpreadsDaily,
  SdModelsDaily, InsertSdModelsDaily,
  SdForecastsWeekly, InsertSdForecastsWeekly,
  ResearchInsightsDaily, InsertResearchInsightsDaily,
  Watchlist, InsertWatchlist,
  AlertRule, InsertAlertRule
} from "@shared/schema";

// Database connection
const sql = neon(process.env.DATABASE_URL!);
const db = drizzle(sql);

export interface IStorage {
  // ===== USER MANAGEMENT =====
  getUsers(): Promise<User[]>;
  createUser(user: InsertUser): Promise<User>;
  getUserByEmail(email: string): Promise<User | null>;
  getUserById(id: string): Promise<User | null>;
  updateUserLastLogin(userId: string): Promise<void>;

  // ===== COMMODITIES =====
  getCommodities(): Promise<Commodity[]>;
  createCommodity(commodity: InsertCommodity): Promise<Commodity>;
  getCommodityByCode(code: string): Promise<Commodity | null>;

  // ===== MARKETS =====
  getMarkets(): Promise<Market[]>;
  createMarket(market: InsertMarket): Promise<Market>;
  getMarketByCode(code: string): Promise<Market | null>;

  // ===== PORTS =====
  getPorts(): Promise<Port[]>;
  createPort(port: InsertPort): Promise<Port>;
  getPortByCode(code: string): Promise<Port | null>;

  // ===== VESSELS =====
  getVessels(): Promise<Vessel[]>;
  getVessel(id: string): Promise<Vessel | null>;
  createVessel(vessel: InsertVessel): Promise<Vessel>;
  getVesselByMMSI(mmsi: string): Promise<Vessel | null>;

  // ===== VESSEL POSITIONS =====
  getVesselPositions(vesselId?: string): Promise<VesselPosition[]>;
  getLatestVesselPositions(): Promise<VesselPosition[]>;
  createVesselPosition(position: Omit<VesselPosition, 'id'>): Promise<VesselPosition>;
  createAisPosition(position: Omit<VesselPosition, 'id'>): Promise<VesselPosition>;

  // ===== STORAGE FACILITIES =====
  getStorageFacilities(): Promise<StorageFacility[]>;
  getStorageFacilitiesByPort(portId: string): Promise<StorageFacility[]>;
  getStorageSites(portId?: string): Promise<StorageFacility[]>;

  // ===== MARKET DATA =====
  getCommodityPrices(): Promise<CommodityPrice[]>;
  getLatestCommodityPrices(): Promise<CommodityPrice[]>;
  createCommodityPrice(price: Omit<CommodityPrice, 'id'>): Promise<CommodityPrice>;

  // ===== TRADE FLOWS =====
  getTradeFlows(): Promise<TradeFlow[]>;
  getActiveTradeFlows(): Promise<TradeFlow[]>;
  createTradeFlow(tradeFlow: InsertTradeFlow): Promise<TradeFlow>;

  // ===== ANALYTICS =====
  getMarketAnalytics(): Promise<MarketAnalytics[]>;
  getMarketAnalyticsByRegion(region: string): Promise<MarketAnalytics[]>;

  // ===== ALERTS & NOTIFICATIONS =====
  getAlerts(): Promise<Alert[]>;
  createAlert(alert: InsertAlert): Promise<Alert>;
  createSignal(signal: InsertAlert): Promise<Alert>;
  getNotifications(): Promise<Notification[]>;
  createNotification(notification: Omit<Notification, 'id'>): Promise<Notification>;
  getActiveSignals(limit?: number): Promise<Alert[]>;

  // ===== PORT STATISTICS =====
  getPortStats(): Promise<PortStats[]>;
  getLatestPortStats(portId: string): Promise<PortStats | null>;
  createPortStats(stats: InsertPortStats): Promise<PortStats>;

  // ===== PREDICTIONS =====
  getPredictions(): Promise<Prediction[]>;
  getLatestPredictions(target?: string): Promise<Prediction[]>;
  createPrediction(prediction: InsertPrediction): Promise<Prediction>;

  // ===== STORAGE FILL DATA =====
  getStorageFillData(): Promise<StorageFillData[]>;
  getLatestStorageFillData(siteId: string): Promise<StorageFillData | null>;
  createStorageFillData(data: InsertStorageFillData): Promise<StorageFillData>;
  getLatestStorageFill(siteId?: string): Promise<StorageFillData[]>;

  // ===== LEGACY STORAGE METHODS =====
  createStorageSite(site: {
    portId: string;
    name: string;
    siteType: string;
    capacity: number;
    latitude: number;
    longitude: number;
  }): Promise<StorageFacility>;

  // ===== PORT DELAYS & MARKET IMPACT =====
  createPortDelayEvent(event: InsertPortDelayEvent): Promise<PortDelayEvent>;
  getPortDelayEvents(portId: string, limit?: number): Promise<PortDelayEvent[]>;
  createVesselDelaySnapshot(snapshot: InsertVesselDelaySnapshot): Promise<VesselDelaySnapshot>;
  getVesselDelaySnapshots(vesselId?: string, portId?: string, limit?: number): Promise<VesselDelaySnapshot[]>;
  createMarketDelayImpact(impact: InsertMarketDelayImpact): Promise<MarketDelayImpact>;
  getMarketDelayImpacts(portId?: string, commodityId?: string, limit?: number): Promise<MarketDelayImpact[]>;

  // ===== CARGO CHAINS & LEGS =====
  createCargoLeg(leg: InsertCargoLeg): Promise<CargoLeg>;
  getCargoLegsByTradeFlow(tradeFlowId: string): Promise<CargoLeg[]>;
  getCargoLegs(limit?: number): Promise<CargoLeg[]>;

  // ===== STS EVENTS =====
  createSTSEvent(event: InsertSTSEvent): Promise<STSEvent>;
  getSTSEvents(limit?: number): Promise<STSEvent[]>;
  getSTSEventsByVessel(vesselId: string): Promise<STSEvent[]>;
  getSTSEventsByTradeFlow(tradeFlowId: string): Promise<STSEvent[]>;

  // ===== CARGO SPLITS =====
  createCargoSplit(split: InsertCargoSplit): Promise<CargoSplit>;
  getCargoSplitsByTradeFlow(tradeFlowId: string): Promise<CargoSplit[]>;
  getCargoSplits(limit?: number): Promise<CargoSplit[]>;

  // ===== FLOW FORECASTS =====
  createFlowForecast(forecast: InsertFlowForecast): Promise<FlowForecast>;
  getFlowForecasts(limit?: number): Promise<FlowForecast[]>;
  getActiveFlowForecasts(): Promise<FlowForecast[]>;
  getFlowForecastsByRoute(originPortId: string, destinationPortId: string): Promise<FlowForecast[]>;

  // ===== PORT CALLS =====
  createPortCall(portCall: InsertPortCall): Promise<PortCall>;
  getPortCalls(portId?: string, vesselId?: string, limit?: number): Promise<PortCall[]>;
  getPortCallsByPort(portId: string, startDate: Date, endDate: Date): Promise<PortCall[]>;
  getLatestPortCalls(portId: string, limit?: number): Promise<PortCall[]>;
  updatePortCall(id: string, updates: Partial<InsertPortCall>): Promise<PortCall>;
  deletePortCall(id: string): Promise<void>;

  // ===== CONTAINER OPERATIONS =====
  createContainerOperation(operation: InsertContainerOperation): Promise<ContainerOperation>;
  getContainerOperations(portId?: string, vesselId?: string, limit?: number): Promise<ContainerOperation[]>;
  getContainerStatsByPort(portId: string): Promise<{
    totalOperations: number;
    totalTEU: number;
    totalFEU: number;
    loadOperations: number;
    dischargeOperations: number;
    transshipmentOperations: number;
  }>;
  updateContainerOperation(id: string, updates: Partial<InsertContainerOperation>): Promise<ContainerOperation>;
  deleteContainerOperation(id: string): Promise<void>;

  // ===== BUNKERING EVENTS =====
  createBunkeringEvent(event: InsertBunkeringEvent): Promise<BunkeringEvent>;
  getBunkeringEvents(vesselId?: string, portId?: string, limit?: number): Promise<BunkeringEvent[]>;
  getBunkeringStatsByVessel(vesselId: string): Promise<{
    totalEvents: number;
    totalVolumeMT: number;
    totalCost: number;
    avgPricePerMT: number;
    fuelTypes: string[];
  }>;
  updateBunkeringEvent(id: string, updates: Partial<InsertBunkeringEvent>): Promise<BunkeringEvent>;
  deleteBunkeringEvent(id: string): Promise<void>;

  // ===== COMMUNICATIONS =====
  createCommunication(communication: InsertCommunication): Promise<Communication>;
  getCommunications(userId?: string, limit?: number): Promise<Communication[]>;
  getUnreadCommunications(userId: string): Promise<Communication[]>;
  markCommunicationAsRead(id: string): Promise<Communication>;
  updateCommunication(id: string, updates: Partial<InsertCommunication>): Promise<Communication>;
  deleteCommunication(id: string): Promise<void>;

  // ===== CRUDE & PRODUCTS PACK =====
  createCrudeGrade(grade: InsertCrudeGrade): Promise<CrudeGrade>;
  getCrudeGrades(category?: string, limit?: number): Promise<CrudeGrade[]>;
  getCrudeGradeByCode(gradeCode: string): Promise<CrudeGrade | null>;
  updateCrudeGrade(id: string, updates: Partial<InsertCrudeGrade>): Promise<CrudeGrade>;
  deleteCrudeGrade(id: string): Promise<void>;

  // ===== LNG/LPG PACK =====
  createLngCargo(cargo: InsertLngCargo): Promise<LngCargo>;
  getLngCargoes(cargoType?: string, portId?: string, limit?: number): Promise<LngCargo[]>;
  getDiversionCargoes(limit?: number): Promise<LngCargo[]>;
  updateLngCargo(id: string, updates: Partial<InsertLngCargo>): Promise<LngCargo>;
  deleteLngCargo(id: string): Promise<void>;

  // ===== DRY BULK PACK =====
  createDryBulkFixture(fixture: InsertDryBulkFixture): Promise<DryBulkFixture>;
  getDryBulkFixtures(commodityType?: string, vesselSize?: string, limit?: number): Promise<DryBulkFixture[]>;
  getDryBulkFixturesByRoute(loadPortId: string, dischargePortId: string): Promise<DryBulkFixture[]>;
  updateDryBulkFixture(id: string, updates: Partial<InsertDryBulkFixture>): Promise<DryBulkFixture>;
  deleteDryBulkFixture(id: string): Promise<void>;

  // ===== PETROCHEM PACK =====
  createPetrochemProduct(product: InsertPetrochemProduct): Promise<PetrochemProduct>;
  getPetrochemProducts(category?: string, region?: string, limit?: number): Promise<PetrochemProduct[]>;
  getPetrochemProductByCode(productCode: string): Promise<PetrochemProduct | null>;
  updatePetrochemProduct(id: string, updates: Partial<InsertPetrochemProduct>): Promise<PetrochemProduct>;
  deletePetrochemProduct(id: string): Promise<void>;

  // ===== AGRI & BIOFUEL PACK =====
  createAgriBiofuelFlow(flow: InsertAgriBiofuelFlow): Promise<AgriBiofuelFlow>;
  getAgriBiofuelFlows(commodityType?: string, flowType?: string, limit?: number): Promise<AgriBiofuelFlow[]>;
  getSustainableBiofuelFlows(limit?: number): Promise<AgriBiofuelFlow[]>;
  updateAgriBiofuelFlow(id: string, updates: Partial<InsertAgriBiofuelFlow>): Promise<AgriBiofuelFlow>;
  deleteAgriBiofuelFlow(id: string): Promise<void>;

  // ===== REFINERY/PLANT INTELLIGENCE =====
  createRefinery(refinery: InsertRefinery): Promise<Refinery>;
  getRefineries(region?: string, maintenanceStatus?: string, limit?: number): Promise<Refinery[]>;
  getRefineryByCode(refineryCode: string): Promise<Refinery | null>;
  updateRefinery(id: string, updates: Partial<InsertRefinery>): Promise<Refinery>;
  deleteRefinery(id: string): Promise<void>;

  // ===== SUPPLY & DEMAND BALANCES =====
  createSupplyDemandBalance(balance: InsertSupplyDemandBalance): Promise<SupplyDemandBalance>;
  getSupplyDemandBalances(commodity?: string, region?: string, period?: string, limit?: number): Promise<SupplyDemandBalance[]>;
  getLatestBalances(commodity?: string, region?: string, limit?: number): Promise<SupplyDemandBalance[]>;
  updateSupplyDemandBalance(id: string, updates: Partial<InsertSupplyDemandBalance>): Promise<SupplyDemandBalance>;
  deleteSupplyDemandBalance(id: string): Promise<void>;

  // ===== RESEARCH & INSIGHT LAYER =====
  createResearchReport(report: InsertResearchReport): Promise<ResearchReport>;
  getResearchReports(category?: string, subcategory?: string, limit?: number): Promise<ResearchReport[]>;
  getPublishedReports(limit?: number): Promise<ResearchReport[]>;
  getReportById(reportId: string): Promise<ResearchReport | null>;
  updateResearchReport(id: string, updates: Partial<InsertResearchReport>): Promise<ResearchReport>;
  deleteResearchReport(id: string): Promise<void>;

  // ===== CSV-BASED DATA (REFINERY) =====
  getRefineryUnits(plant?: string): Promise<RefineryUnit[]>;
  getRefineryUtilization(startDate?: string, endDate?: string, plant?: string): Promise<RefineryUtilizationDaily[]>;
  getRefineryCrackSpreads(startDate?: string, endDate?: string): Promise<RefineryCrackSpreadsDaily[]>;

  // ===== CSV-BASED DATA (SUPPLY & DEMAND) =====
  getSdModelsDaily(startDate?: string, endDate?: string, region?: string): Promise<SdModelsDaily[]>;
  getSdForecastsWeekly(startDate?: string, endDate?: string, region?: string): Promise<SdForecastsWeekly[]>;

  // ===== CSV-BASED DATA (RESEARCH) =====
  getResearchInsightsDaily(startDate?: string, endDate?: string, limit?: number): Promise<ResearchInsightsDaily[]>;

  // ===== ML PRICE PREDICTIONS =====
  getMlPredictions(commodityType?: string, limit?: number): Promise<any[]>;
  getLatestMlPrediction(commodityType: string): Promise<any | null>;

  // ===== WATCHLISTS =====
  createWatchlist(watchlist: InsertWatchlist): Promise<Watchlist>;
  getWatchlists(userId: string): Promise<Watchlist[]>;
  getWatchlistById(id: string): Promise<Watchlist | null>;
  updateWatchlist(id: string, updates: Partial<InsertWatchlist>): Promise<Watchlist>;
  deleteWatchlist(id: string): Promise<void>;

  // ===== ALERT RULES =====
  createAlertRule(rule: InsertAlertRule): Promise<AlertRule>;
  getAlertRules(userId: string): Promise<AlertRule[]>;
  getAlertRuleById(id: string): Promise<AlertRule | null>;
  updateAlertRule(id: string, updates: Partial<InsertAlertRule>): Promise<AlertRule>;
  deleteAlertRule(id: string): Promise<void>;
  getActiveAlertRules(): Promise<AlertRule[]>;
}

export class DrizzleStorage implements IStorage {
  // ===== USER MANAGEMENT =====
  async getUsers(): Promise<User[]> {
    return await db.select().from(users);
  }

  async createUser(user: InsertUser): Promise<User> {
    const result = await db.insert(users).values(user).returning();
    return result[0];
  }

  async getUserByEmail(email: string): Promise<User | null> {
    const result = await db.select().from(users).where(eq(users.email, email)).limit(1);
    return result[0] || null;
  }

  async getUserById(id: string): Promise<User | null> {
    const result = await db.select().from(users).where(eq(users.id, id)).limit(1);
    return result[0] || null;
  }

  async updateUserLastLogin(userId: string): Promise<void> {
    await db.update(users).set({ lastLogin: new Date() }).where(eq(users.id, userId));
  }

  // ===== COMMODITIES =====
  async getCommodities(): Promise<Commodity[]> {
    return await db.select().from(commodities).where(eq(commodities.isActive, true));
  }

  async createCommodity(commodity: InsertCommodity): Promise<Commodity> {
    const result = await db.insert(commodities).values(commodity).returning();
    return result[0];
  }

  async getCommodityByCode(code: string): Promise<Commodity | null> {
    const result = await db.select().from(commodities).where(eq(commodities.code, code)).limit(1);
    return result[0] || null;
  }

  // ===== MARKETS =====
  async getMarkets(): Promise<Market[]> {
    return await db.select().from(markets).where(eq(markets.isActive, true));
  }

  async createMarket(market: InsertMarket): Promise<Market> {
    const result = await db.insert(markets).values(market).returning();
    return result[0];
  }

  async getMarketByCode(code: string): Promise<Market | null> {
    const result = await db.select().from(markets).where(eq(markets.code, code)).limit(1);
    return result[0] || null;
  }

  // ===== PORTS =====
  async getPorts(): Promise<Port[]> {
    return await db.select().from(ports);
  }

  async createPort(port: InsertPort): Promise<Port> {
    const result = await db.insert(ports).values(port).returning();
    return result[0];
  }

  async getPortByCode(code: string): Promise<Port | null> {
    const result = await db.select().from(ports).where(eq(ports.code, code)).limit(1);
    return result[0] || null;
  }

  // ===== VESSELS =====
  async getVessels(): Promise<Vessel[]> {
    return await db.select().from(vessels);
  }

  async createVessel(vessel: InsertVessel): Promise<Vessel> {
    const result = await db.insert(vessels).values(vessel).returning();
    return result[0];
  }

  async getVesselByMMSI(mmsi: string): Promise<Vessel | null> {
    const result = await db.select().from(vessels).where(eq(vessels.mmsi, mmsi)).limit(1);
    return result[0] || null;
  }

  async getVessel(id: string): Promise<Vessel | null> {
    const result = await db.select().from(vessels).where(eq(vessels.id, id)).limit(1);
    return result[0] || null;
  }

  // ===== VESSEL POSITIONS =====
  async getVesselPositions(vesselId?: string): Promise<VesselPosition[]> {
    if (vesselId) {
      return await db.select().from(vesselPositions)
        .where(eq(vesselPositions.vesselId, vesselId))
        .orderBy(desc(vesselPositions.timestamp))
        .limit(1000);
    }
    return await db.select().from(vesselPositions)
      .orderBy(desc(vesselPositions.timestamp))
      .limit(1000);
  }

  async getLatestVesselPositions(): Promise<VesselPosition[]> {
    // Get the latest position for each vessel
    return await db.select().from(vesselPositions)
      .orderBy(desc(vesselPositions.timestamp))
      .limit(100);
  }

  async createVesselPosition(position: Omit<VesselPosition, 'id'>): Promise<VesselPosition> {
    const result = await db.insert(vesselPositions).values(position).returning();
    return result[0];
  }

  async createAisPosition(position: Omit<VesselPosition, 'id'>): Promise<VesselPosition> {
    // AIS position updates use the same vessel positions table
    const result = await db.insert(vesselPositions).values(position).returning();
    return result[0];
  }

  // ===== STORAGE FACILITIES =====
  async getStorageFacilities(): Promise<StorageFacility[]> {
    return await db.select().from(storageFacilities).where(eq(storageFacilities.isActive, true));
  }

  async getStorageFacilitiesByPort(portId: string): Promise<StorageFacility[]> {
    return await db.select().from(storageFacilities)
      .where(and(
        eq(storageFacilities.portId, portId),
        eq(storageFacilities.isActive, true)
      ));
  }

  async getStorageSites(portId?: string): Promise<StorageFacility[]> {
    if (portId) {
      return await this.getStorageFacilitiesByPort(portId);
    }
    return await this.getStorageFacilities();
  }

  // ===== MARKET DATA =====
  async getCommodityPrices(): Promise<CommodityPrice[]> {
    return await db.select().from(commodityPrices)
      .orderBy(desc(commodityPrices.timestamp))
      .limit(1000);
  }

  async getLatestCommodityPrices(): Promise<CommodityPrice[]> {
    return await db.select().from(commodityPrices)
      .orderBy(desc(commodityPrices.timestamp))
      .limit(100);
  }

  async createCommodityPrice(price: Omit<CommodityPrice, 'id'>): Promise<CommodityPrice> {
    const result = await db.insert(commodityPrices).values(price).returning();
    return result[0];
  }

  // ===== TRADE FLOWS =====
  async getTradeFlows(): Promise<TradeFlow[]> {
    return await db.select().from(tradeFlows)
      .orderBy(desc(tradeFlows.createdAt))
      .limit(500);
  }

  async getActiveTradeFlows(): Promise<TradeFlow[]> {
    return await db.select().from(tradeFlows)
      .where(eq(tradeFlows.status, 'in_transit'))
      .orderBy(desc(tradeFlows.createdAt));
  }

  async createTradeFlow(tradeFlow: InsertTradeFlow): Promise<TradeFlow> {
    const result = await db.insert(tradeFlows).values(tradeFlow).returning();
    return result[0];
  }

  // ===== ANALYTICS =====
  async getMarketAnalytics(): Promise<MarketAnalytics[]> {
    return await db.select().from(marketAnalytics)
      .orderBy(desc(marketAnalytics.createdAt))
      .limit(100);
  }

  async getMarketAnalyticsByRegion(region: string): Promise<MarketAnalytics[]> {
    return await db.select().from(marketAnalytics)
      .where(eq(marketAnalytics.region, region))
      .orderBy(desc(marketAnalytics.createdAt))
      .limit(50);
  }

  // ===== ALERTS & NOTIFICATIONS =====
  async getAlerts(): Promise<Alert[]> {
    return await db.select().from(alerts).where(eq(alerts.isActive, true));
  }

  async createAlert(alert: InsertAlert): Promise<Alert> {
    const result = await db.insert(alerts).values(alert).returning();
    return result[0];
  }

  async createSignal(signal: InsertAlert): Promise<Alert> {
    // For now, signals are stored as alerts
    const result = await db.insert(alerts).values(signal).returning();
    return result[0];
  }

  async getNotifications(): Promise<Notification[]> {
    return await db.select().from(notifications)
      .orderBy(desc(notifications.timestamp))
      .limit(100);
  }

  async createNotification(notification: Omit<Notification, 'id'>): Promise<Notification> {
    const result = await db.insert(notifications).values(notification).returning();
    return result[0];
  }

  async getActiveSignals(limit: number = 10): Promise<Alert[]> {
    return await db.select().from(alerts)
      .where(eq(alerts.isActive, true))
      .orderBy(desc(alerts.createdAt))
      .limit(limit);
  }

  // ===== PORT STATISTICS =====
  async getPortStats(): Promise<PortStats[]> {
    return await db.select().from(portStats)
      .orderBy(desc(portStats.createdAt))
      .limit(100);
  }

  async getLatestPortStats(portId: string): Promise<PortStats | null> {
    const result = await db.select().from(portStats)
      .where(eq(portStats.portId, portId))
      .orderBy(desc(portStats.date))
      .limit(1);
    return result[0] || null;
  }

  async createPortStats(stats: InsertPortStats): Promise<PortStats> {
    const result = await db.insert(portStats).values(stats).returning();
    return result[0];
  }

  // ===== PREDICTIONS =====
  async getPredictions(): Promise<Prediction[]> {
    return await db.select().from(predictions)
      .orderBy(desc(predictions.createdAt))
      .limit(100);
  }

  async getLatestPredictions(commodityId?: string): Promise<Prediction[]> {
    if (commodityId) {
      return await db.select().from(predictions)
        .where(and(
          eq(predictions.commodityId, commodityId),
          gte(predictions.validUntil, new Date())
        ))
        .orderBy(desc(predictions.createdAt))
        .limit(50);
    }
    return await db.select().from(predictions)
      .where(gte(predictions.validUntil, new Date()))
      .orderBy(desc(predictions.createdAt))
      .limit(50);
  }

  async createPrediction(prediction: InsertPrediction): Promise<Prediction> {
    const result = await db.insert(predictions).values(prediction).returning();
    return result[0];
  }

  // ===== STORAGE FILL DATA =====
  async getStorageFillData(): Promise<StorageFillData[]> {
    return await db.select().from(storageFillData)
      .orderBy(desc(storageFillData.timestamp))
      .limit(500);
  }

  async getLatestStorageFillData(siteId: string): Promise<StorageFillData | null> {
    const result = await db.select().from(storageFillData)
      .where(eq(storageFillData.siteId, siteId))
      .orderBy(desc(storageFillData.timestamp))
      .limit(1);
    return result[0] || null;
  }

  async createStorageFillData(data: InsertStorageFillData): Promise<StorageFillData> {
    const result = await db.insert(storageFillData).values(data).returning();
    return result[0];
  }

  async getLatestStorageFill(siteId?: string): Promise<StorageFillData[]> {
    if (siteId) {
      return await db.select().from(storageFillData)
        .where(eq(storageFillData.siteId, siteId))
        .orderBy(desc(storageFillData.timestamp))
        .limit(50);
    }
    return await db.select().from(storageFillData)
      .orderBy(desc(storageFillData.timestamp))
      .limit(50);
  }

  // ===== LEGACY STORAGE METHODS =====
  async createStorageSite(site: {
    portId: string;
    name: string;
    siteType: string;
    capacity: number;
    latitude: number;
    longitude: number;
  }): Promise<StorageFacility> {
    // Map legacy createStorageSite to the new storage facilities table
    const facilityData = {
      name: site.name,
      portId: site.portId,
      type: site.siteType,
      totalCapacity: site.capacity,
      currentLevel: 0,
      utilizationRate: "0",
      specifications: {
        latitude: site.latitude,
        longitude: site.longitude
      }
    };
    const result = await db.insert(storageFacilities).values(facilityData).returning();
    return result[0];
  }

  // ===== PORT DELAYS & MARKET IMPACT =====
  async createPortDelayEvent(event: InsertPortDelayEvent): Promise<PortDelayEvent> {
    const result = await db.insert(portDelayEvents).values(event).returning();
    return result[0];
  }

  async getPortDelayEvents(portId: string, limit: number = 100): Promise<PortDelayEvent[]> {
    return await db.select().from(portDelayEvents)
      .where(eq(portDelayEvents.portId, portId))
      .orderBy(desc(portDelayEvents.createdAt))
      .limit(limit);
  }

  async createVesselDelaySnapshot(snapshot: InsertVesselDelaySnapshot): Promise<VesselDelaySnapshot> {
    const result = await db.insert(vesselDelaySnapshots).values(snapshot).returning();
    return result[0];
  }

  async getVesselDelaySnapshots(vesselId?: string, portId?: string, limit: number = 100): Promise<VesselDelaySnapshot[]> {
    if (vesselId && portId) {
      // Both filters provided
      return await db.select().from(vesselDelaySnapshots)
        .where(and(
          eq(vesselDelaySnapshots.vesselId, vesselId),
          eq(vesselDelaySnapshots.destinationPortId, portId)
        ))
        .orderBy(desc(vesselDelaySnapshots.lastUpdated))
        .limit(limit);
    } else if (vesselId) {
      // Only vessel filter
      return await db.select().from(vesselDelaySnapshots)
        .where(eq(vesselDelaySnapshots.vesselId, vesselId))
        .orderBy(desc(vesselDelaySnapshots.lastUpdated))
        .limit(limit);
    } else if (portId) {
      // Only port filter (filter by destination port)
      return await db.select().from(vesselDelaySnapshots)
        .where(eq(vesselDelaySnapshots.destinationPortId, portId))
        .orderBy(desc(vesselDelaySnapshots.lastUpdated))
        .limit(limit);
    } else {
      // No filters
      return await db.select().from(vesselDelaySnapshots)
        .orderBy(desc(vesselDelaySnapshots.lastUpdated))
        .limit(limit);
    }
  }

  async createMarketDelayImpact(impact: InsertMarketDelayImpact): Promise<MarketDelayImpact> {
    const result = await db.insert(marketDelayImpacts).values(impact).returning();
    return result[0];
  }

  async getMarketDelayImpacts(portId?: string, commodityId?: string, limit: number = 20): Promise<MarketDelayImpact[]> {
    if (portId && commodityId) {
      // Both filters provided
      return await db.select().from(marketDelayImpacts)
        .where(and(
          eq(marketDelayImpacts.portId, portId),
          eq(marketDelayImpacts.commodityId, commodityId)
        ))
        .orderBy(desc(marketDelayImpacts.createdAt))
        .limit(limit);
    } else if (portId) {
      // Only port filter
      return await db.select().from(marketDelayImpacts)
        .where(eq(marketDelayImpacts.portId, portId))
        .orderBy(desc(marketDelayImpacts.createdAt))
        .limit(limit);
    } else if (commodityId) {
      // Only commodity filter
      return await db.select().from(marketDelayImpacts)
        .where(eq(marketDelayImpacts.commodityId, commodityId))
        .orderBy(desc(marketDelayImpacts.createdAt))
        .limit(limit);
    } else {
      // No filters
      return await db.select().from(marketDelayImpacts)
        .orderBy(desc(marketDelayImpacts.createdAt))
        .limit(limit);
    }
  }

  // ===== CARGO CHAINS & LEGS =====
  async createCargoLeg(leg: InsertCargoLeg): Promise<CargoLeg> {
    const result = await db.insert(cargoLegs).values(leg).returning();
    return result[0];
  }

  async getCargoLegsByTradeFlow(tradeFlowId: string): Promise<CargoLeg[]> {
    return await db.select().from(cargoLegs)
      .where(eq(cargoLegs.tradeFlowId, tradeFlowId))
      .orderBy(cargoLegs.sequence);
  }

  async getCargoLegs(limit: number = 100): Promise<CargoLeg[]> {
    return await db.select().from(cargoLegs)
      .orderBy(desc(cargoLegs.createdAt))
      .limit(limit);
  }

  // ===== STS EVENTS =====
  async createSTSEvent(event: InsertSTSEvent): Promise<STSEvent> {
    const result = await db.insert(stsEvents).values(event).returning();
    return result[0];
  }

  async getSTSEvents(limit: number = 100): Promise<STSEvent[]> {
    return await db.select().from(stsEvents)
      .orderBy(desc(stsEvents.createdAt))
      .limit(limit);
  }

  async getSTSEventsByVessel(vesselId: string): Promise<STSEvent[]> {
    return await db.select().from(stsEvents)
      .where(
        or(
          eq(stsEvents.motherVesselId, vesselId),
          eq(stsEvents.daughterVesselId, vesselId)
        )
      )
      .orderBy(desc(stsEvents.startTime))
      .limit(50);
  }

  async getSTSEventsByTradeFlow(tradeFlowId: string): Promise<STSEvent[]> {
    return await db.select().from(stsEvents)
      .where(eq(stsEvents.tradeFlowId, tradeFlowId))
      .orderBy(desc(stsEvents.startTime));
  }

  // ===== CARGO SPLITS =====
  async createCargoSplit(split: InsertCargoSplit): Promise<CargoSplit> {
    const result = await db.insert(cargoSplits).values(split).returning();
    return result[0];
  }

  async getCargoSplitsByTradeFlow(tradeFlowId: string): Promise<CargoSplit[]> {
    return await db.select().from(cargoSplits)
      .where(eq(cargoSplits.tradeFlowId, tradeFlowId))
      .orderBy(cargoSplits.splitSequence);
  }

  async getCargoSplits(limit: number = 100): Promise<CargoSplit[]> {
    return await db.select().from(cargoSplits)
      .orderBy(desc(cargoSplits.createdAt))
      .limit(limit);
  }

  // ===== FLOW FORECASTS =====
  async createFlowForecast(forecast: InsertFlowForecast): Promise<FlowForecast> {
    const result = await db.insert(flowForecasts).values(forecast).returning();
    return result[0];
  }

  async getFlowForecasts(limit: number = 50): Promise<FlowForecast[]> {
    return await db.select().from(flowForecasts)
      .orderBy(desc(flowForecasts.createdAt))
      .limit(limit);
  }

  async getActiveFlowForecasts(): Promise<FlowForecast[]> {
    const now = new Date();
    return await db.select().from(flowForecasts)
      .where(gte(flowForecasts.validUntil, now))
      .orderBy(desc(flowForecasts.confidence))
      .limit(20);
  }

  async getFlowForecastsByRoute(originPortId: string, destinationPortId: string): Promise<FlowForecast[]> {
    const now = new Date();
    return await db.select().from(flowForecasts)
      .where(
        and(
          eq(flowForecasts.originPortId, originPortId),
          eq(flowForecasts.destinationPortId, destinationPortId),
          gte(flowForecasts.validUntil, now)
        )
      )
      .orderBy(desc(flowForecasts.validFrom));
  }

  // ===== PORT CALLS =====
  async createPortCall(portCall: InsertPortCall): Promise<PortCall> {
    const result = await db.insert(portCalls).values(portCall).returning();
    return result[0];
  }

  async getPortCalls(portId?: string, vesselId?: string, limit: number = 100): Promise<PortCall[]> {
    if (portId && vesselId) {
      return await db.select().from(portCalls)
        .where(and(eq(portCalls.portId, portId), eq(portCalls.vesselId, vesselId)))
        .orderBy(desc(portCalls.arrivalTime))
        .limit(limit);
    } else if (portId) {
      return await db.select().from(portCalls)
        .where(eq(portCalls.portId, portId))
        .orderBy(desc(portCalls.arrivalTime))
        .limit(limit);
    } else if (vesselId) {
      return await db.select().from(portCalls)
        .where(eq(portCalls.vesselId, vesselId))
        .orderBy(desc(portCalls.arrivalTime))
        .limit(limit);
    }
    
    return await db.select().from(portCalls)
      .orderBy(desc(portCalls.arrivalTime))
      .limit(limit);
  }

  async getLatestPortCalls(portId: string, limit: number = 50): Promise<PortCall[]> {
    return await db.select().from(portCalls)
      .where(eq(portCalls.portId, portId))
      .orderBy(desc(portCalls.arrivalTime))
      .limit(limit);
  }

  async getPortCallsByPort(portId: string, startDate: Date, endDate: Date): Promise<PortCall[]> {
    return await db.select().from(portCalls)
      .where(
        and(
          eq(portCalls.portId, portId),
          gte(portCalls.arrivalTime, startDate),
          lte(portCalls.arrivalTime, endDate)
        )
      )
      .orderBy(desc(portCalls.arrivalTime));
  }

  async updatePortCall(id: string, updates: Partial<InsertPortCall>): Promise<PortCall> {
    const result = await db.update(portCalls)
      .set(updates)
      .where(eq(portCalls.id, id))
      .returning();
    return result[0];
  }

  async deletePortCall(id: string): Promise<void> {
    await db.delete(portCalls).where(eq(portCalls.id, id));
  }

  // ===== CONTAINER OPERATIONS =====
  async createContainerOperation(operation: InsertContainerOperation): Promise<ContainerOperation> {
    const result = await db.insert(containerOperations).values(operation).returning();
    return result[0];
  }

  async getContainerOperations(portId?: string, vesselId?: string, limit: number = 100): Promise<ContainerOperation[]> {
    if (portId && vesselId) {
      return await db.select().from(containerOperations)
        .where(and(eq(containerOperations.portId, portId), eq(containerOperations.vesselId, vesselId)))
        .orderBy(desc(containerOperations.operationDate))
        .limit(limit);
    } else if (portId) {
      return await db.select().from(containerOperations)
        .where(eq(containerOperations.portId, portId))
        .orderBy(desc(containerOperations.operationDate))
        .limit(limit);
    } else if (vesselId) {
      return await db.select().from(containerOperations)
        .where(eq(containerOperations.vesselId, vesselId))
        .orderBy(desc(containerOperations.operationDate))
        .limit(limit);
    }
    
    return await db.select().from(containerOperations)
      .orderBy(desc(containerOperations.operationDate))
      .limit(limit);
  }

  async getContainerStatsByPort(portId: string): Promise<{
    totalOperations: number;
    totalTEU: number;
    totalFEU: number;
    loadOperations: number;
    dischargeOperations: number;
    transshipmentOperations: number;
  }> {
    const operations = await this.getContainerOperations(portId, undefined, 1000);
    const totalTEU = operations.reduce((sum, op) => sum + (op.teuCount || 0), 0);
    const totalFEU = operations.reduce((sum, op) => sum + (op.feuCount || 0), 0);
    
    return {
      totalOperations: operations.length,
      totalTEU,
      totalFEU,
      loadOperations: operations.filter(op => op.operationType === 'load').length,
      dischargeOperations: operations.filter(op => op.operationType === 'discharge').length,
      transshipmentOperations: operations.filter(op => op.operationType === 'transshipment').length
    };
  }

  async updateContainerOperation(id: string, updates: Partial<InsertContainerOperation>): Promise<ContainerOperation> {
    const result = await db.update(containerOperations)
      .set(updates)
      .where(eq(containerOperations.id, id))
      .returning();
    return result[0];
  }

  async deleteContainerOperation(id: string): Promise<void> {
    await db.delete(containerOperations).where(eq(containerOperations.id, id));
  }

  // ===== BUNKERING EVENTS =====
  async createBunkeringEvent(event: InsertBunkeringEvent): Promise<BunkeringEvent> {
    const result = await db.insert(bunkeringEvents).values(event).returning();
    return result[0];
  }

  async getBunkeringEvents(vesselId?: string, portId?: string, limit: number = 100): Promise<BunkeringEvent[]> {
    if (vesselId && portId) {
      return await db.select().from(bunkeringEvents)
        .where(and(eq(bunkeringEvents.vesselId, vesselId), eq(bunkeringEvents.portId, portId)))
        .orderBy(desc(bunkeringEvents.startTime))
        .limit(limit);
    } else if (vesselId) {
      return await db.select().from(bunkeringEvents)
        .where(eq(bunkeringEvents.vesselId, vesselId))
        .orderBy(desc(bunkeringEvents.startTime))
        .limit(limit);
    } else if (portId) {
      return await db.select().from(bunkeringEvents)
        .where(eq(bunkeringEvents.portId, portId))
        .orderBy(desc(bunkeringEvents.startTime))
        .limit(limit);
    }
    
    return await db.select().from(bunkeringEvents)
      .orderBy(desc(bunkeringEvents.startTime))
      .limit(limit);
  }

  async getBunkeringStatsByVessel(vesselId: string): Promise<{
    totalEvents: number;
    totalVolumeMT: number;
    totalCost: number;
    avgPricePerMT: number;
    fuelTypes: string[];
  }> {
    const events = await this.getBunkeringEvents(vesselId, undefined, 1000);
    const totalVolume = events.reduce((sum, ev) => sum + parseFloat(ev.volumeMT as string || '0'), 0);
    const totalCost = events.reduce((sum, ev) => sum + parseFloat(ev.totalCost as string || '0'), 0);
    
    return {
      totalEvents: events.length,
      totalVolumeMT: totalVolume,
      totalCost,
      avgPricePerMT: totalVolume > 0 ? totalCost / totalVolume : 0,
      fuelTypes: Array.from(new Set(events.map(e => e.fuelType)))
    };
  }

  async updateBunkeringEvent(id: string, updates: Partial<InsertBunkeringEvent>): Promise<BunkeringEvent> {
    const result = await db.update(bunkeringEvents)
      .set(updates)
      .where(eq(bunkeringEvents.id, id))
      .returning();
    return result[0];
  }

  async deleteBunkeringEvent(id: string): Promise<void> {
    await db.delete(bunkeringEvents).where(eq(bunkeringEvents.id, id));
  }

  // ===== COMMUNICATIONS =====
  async createCommunication(communication: InsertCommunication): Promise<Communication> {
    const result = await db.insert(communications).values(communication).returning();
    return result[0];
  }

  async getCommunications(userId?: string, limit: number = 100): Promise<Communication[]> {
    if (userId) {
      return await db.select().from(communications)
        .where(eq(communications.userId, userId))
        .orderBy(desc(communications.createdAt))
        .limit(limit);
    }
    return await db.select().from(communications)
      .orderBy(desc(communications.createdAt))
      .limit(limit);
  }

  async getUnreadCommunications(userId: string): Promise<Communication[]> {
    return await db.select().from(communications)
      .where(and(eq(communications.userId, userId), eq(communications.isRead, false)))
      .orderBy(desc(communications.createdAt));
  }

  async markCommunicationAsRead(id: string): Promise<Communication> {
    const result = await db.update(communications)
      .set({ isRead: true, readAt: new Date() })
      .where(eq(communications.id, id))
      .returning();
    return result[0];
  }

  async updateCommunication(id: string, updates: Partial<InsertCommunication>): Promise<Communication> {
    const result = await db.update(communications)
      .set(updates)
      .where(eq(communications.id, id))
      .returning();
    return result[0];
  }

  async deleteCommunication(id: string): Promise<void> {
    await db.delete(communications).where(eq(communications.id, id));
  }

  // ===== CRUDE & PRODUCTS PACK =====
  async createCrudeGrade(grade: InsertCrudeGrade): Promise<CrudeGrade> {
    const result = await db.insert(crudeGrades).values(grade).returning();
    return result[0];
  }

  async getCrudeGrades(category?: string, limit: number = 100): Promise<CrudeGrade[]> {
    if (category) {
      return await db.select().from(crudeGrades)
        .where(eq(crudeGrades.category, category))
        .limit(limit);
    }
    return await db.select().from(crudeGrades).limit(limit);
  }

  async getCrudeGradeByCode(gradeCode: string): Promise<CrudeGrade | null> {
    const result = await db.select().from(crudeGrades)
      .where(eq(crudeGrades.gradeCode, gradeCode))
      .limit(1);
    return result[0] || null;
  }

  async updateCrudeGrade(id: string, updates: Partial<InsertCrudeGrade>): Promise<CrudeGrade> {
    const result = await db.update(crudeGrades)
      .set(updates)
      .where(eq(crudeGrades.id, id))
      .returning();
    return result[0];
  }

  async deleteCrudeGrade(id: string): Promise<void> {
    await db.delete(crudeGrades).where(eq(crudeGrades.id, id));
  }

  // ===== LNG/LPG PACK =====
  async createLngCargo(cargo: InsertLngCargo): Promise<LngCargo> {
    const result = await db.insert(lngCargoes).values(cargo).returning();
    return result[0];
  }

  async getLngCargoes(cargoType?: string, portId?: string, limit: number = 100): Promise<LngCargo[]> {
    if (cargoType && portId) {
      return await db.select().from(lngCargoes)
        .where(and(
          eq(lngCargoes.cargoType, cargoType),
          or(eq(lngCargoes.loadPortId, portId), eq(lngCargoes.dischargePortId, portId))
        ))
        .orderBy(desc(lngCargoes.loadDate))
        .limit(limit);
    }
    if (cargoType) {
      return await db.select().from(lngCargoes)
        .where(eq(lngCargoes.cargoType, cargoType))
        .orderBy(desc(lngCargoes.loadDate))
        .limit(limit);
    }
    if (portId) {
      return await db.select().from(lngCargoes)
        .where(or(eq(lngCargoes.loadPortId, portId), eq(lngCargoes.dischargePortId, portId)))
        .orderBy(desc(lngCargoes.loadDate))
        .limit(limit);
    }
    return await db.select().from(lngCargoes)
      .orderBy(desc(lngCargoes.loadDate))
      .limit(limit);
  }

  async getDiversionCargoes(limit: number = 100): Promise<LngCargo[]> {
    return await db.select().from(lngCargoes)
      .where(eq(lngCargoes.isDiversion, true))
      .orderBy(desc(lngCargoes.loadDate))
      .limit(limit);
  }

  async updateLngCargo(id: string, updates: Partial<InsertLngCargo>): Promise<LngCargo> {
    const result = await db.update(lngCargoes)
      .set(updates)
      .where(eq(lngCargoes.id, id))
      .returning();
    return result[0];
  }

  async deleteLngCargo(id: string): Promise<void> {
    await db.delete(lngCargoes).where(eq(lngCargoes.id, id));
  }

  // ===== DRY BULK PACK =====
  async createDryBulkFixture(fixture: InsertDryBulkFixture): Promise<DryBulkFixture> {
    const result = await db.insert(dryBulkFixtures).values(fixture).returning();
    return result[0];
  }

  async getDryBulkFixtures(commodityType?: string, vesselSize?: string, limit: number = 100): Promise<DryBulkFixture[]> {
    if (commodityType && vesselSize) {
      return await db.select().from(dryBulkFixtures)
        .where(and(
          eq(dryBulkFixtures.commodityType, commodityType),
          eq(dryBulkFixtures.vesselSize, vesselSize)
        ))
        .orderBy(desc(dryBulkFixtures.fixtureDate))
        .limit(limit);
    }
    if (commodityType) {
      return await db.select().from(dryBulkFixtures)
        .where(eq(dryBulkFixtures.commodityType, commodityType))
        .orderBy(desc(dryBulkFixtures.fixtureDate))
        .limit(limit);
    }
    if (vesselSize) {
      return await db.select().from(dryBulkFixtures)
        .where(eq(dryBulkFixtures.vesselSize, vesselSize))
        .orderBy(desc(dryBulkFixtures.fixtureDate))
        .limit(limit);
    }
    return await db.select().from(dryBulkFixtures)
      .orderBy(desc(dryBulkFixtures.fixtureDate))
      .limit(limit);
  }

  async getDryBulkFixturesByRoute(loadPortId: string, dischargePortId: string): Promise<DryBulkFixture[]> {
    return await db.select().from(dryBulkFixtures)
      .where(and(
        eq(dryBulkFixtures.loadPortId, loadPortId),
        eq(dryBulkFixtures.dischargePortId, dischargePortId)
      ))
      .orderBy(desc(dryBulkFixtures.fixtureDate));
  }

  async updateDryBulkFixture(id: string, updates: Partial<InsertDryBulkFixture>): Promise<DryBulkFixture> {
    const result = await db.update(dryBulkFixtures)
      .set(updates)
      .where(eq(dryBulkFixtures.id, id))
      .returning();
    return result[0];
  }

  async deleteDryBulkFixture(id: string): Promise<void> {
    await db.delete(dryBulkFixtures).where(eq(dryBulkFixtures.id, id));
  }

  // ===== PETROCHEM PACK =====
  async createPetrochemProduct(product: InsertPetrochemProduct): Promise<PetrochemProduct> {
    const result = await db.insert(petrochemProducts).values(product).returning();
    return result[0];
  }

  async getPetrochemProducts(category?: string, region?: string, limit: number = 100): Promise<PetrochemProduct[]> {
    if (category && region) {
      return await db.select().from(petrochemProducts)
        .where(and(
          eq(petrochemProducts.category, category),
          eq(petrochemProducts.region, region)
        ))
        .limit(limit);
    }
    if (category) {
      return await db.select().from(petrochemProducts)
        .where(eq(petrochemProducts.category, category))
        .limit(limit);
    }
    if (region) {
      return await db.select().from(petrochemProducts)
        .where(eq(petrochemProducts.region, region))
        .limit(limit);
    }
    return await db.select().from(petrochemProducts).limit(limit);
  }

  async getPetrochemProductByCode(productCode: string): Promise<PetrochemProduct | null> {
    const result = await db.select().from(petrochemProducts)
      .where(eq(petrochemProducts.productCode, productCode))
      .limit(1);
    return result[0] || null;
  }

  async updatePetrochemProduct(id: string, updates: Partial<InsertPetrochemProduct>): Promise<PetrochemProduct> {
    const result = await db.update(petrochemProducts)
      .set(updates)
      .where(eq(petrochemProducts.id, id))
      .returning();
    return result[0];
  }

  async deletePetrochemProduct(id: string): Promise<void> {
    await db.delete(petrochemProducts).where(eq(petrochemProducts.id, id));
  }

  // ===== AGRI & BIOFUEL PACK =====
  async createAgriBiofuelFlow(flow: InsertAgriBiofuelFlow): Promise<AgriBiofuelFlow> {
    const result = await db.insert(agriBiofuelFlows).values(flow).returning();
    return result[0];
  }

  async getAgriBiofuelFlows(commodityType?: string, flowType?: string, limit: number = 100): Promise<AgriBiofuelFlow[]> {
    if (commodityType && flowType) {
      return await db.select().from(agriBiofuelFlows)
        .where(and(
          eq(agriBiofuelFlows.commodityType, commodityType),
          eq(agriBiofuelFlows.flowType, flowType)
        ))
        .orderBy(desc(agriBiofuelFlows.flowDate))
        .limit(limit);
    }
    if (commodityType) {
      return await db.select().from(agriBiofuelFlows)
        .where(eq(agriBiofuelFlows.commodityType, commodityType))
        .orderBy(desc(agriBiofuelFlows.flowDate))
        .limit(limit);
    }
    if (flowType) {
      return await db.select().from(agriBiofuelFlows)
        .where(eq(agriBiofuelFlows.flowType, flowType))
        .orderBy(desc(agriBiofuelFlows.flowDate))
        .limit(limit);
    }
    return await db.select().from(agriBiofuelFlows)
      .orderBy(desc(agriBiofuelFlows.flowDate))
      .limit(limit);
  }

  async getSustainableBiofuelFlows(limit: number = 100): Promise<AgriBiofuelFlow[]> {
    return await db.select().from(agriBiofuelFlows)
      .where(and(
        eq(agriBiofuelFlows.biofuelType, 'biodiesel'),
        gte(agriBiofuelFlows.carbonIntensity, '0')
      ))
      .orderBy(desc(agriBiofuelFlows.flowDate))
      .limit(limit);
  }

  async updateAgriBiofuelFlow(id: string, updates: Partial<InsertAgriBiofuelFlow>): Promise<AgriBiofuelFlow> {
    const result = await db.update(agriBiofuelFlows)
      .set(updates)
      .where(eq(agriBiofuelFlows.id, id))
      .returning();
    return result[0];
  }

  async deleteAgriBiofuelFlow(id: string): Promise<void> {
    await db.delete(agriBiofuelFlows).where(eq(agriBiofuelFlows.id, id));
  }

  // ===== REFINERY/PLANT INTELLIGENCE =====
  async createRefinery(refinery: InsertRefinery): Promise<Refinery> {
    const result = await db.insert(refineries).values(refinery).returning();
    return result[0];
  }

  async getRefineries(region?: string, maintenanceStatus?: string, limit: number = 100): Promise<Refinery[]> {
    if (region && maintenanceStatus) {
      return await db.select().from(refineries)
        .where(and(
          eq(refineries.region, region),
          eq(refineries.maintenanceStatus, maintenanceStatus)
        ))
        .limit(limit);
    }
    if (region) {
      return await db.select().from(refineries)
        .where(eq(refineries.region, region))
        .limit(limit);
    }
    if (maintenanceStatus) {
      return await db.select().from(refineries)
        .where(eq(refineries.maintenanceStatus, maintenanceStatus))
        .limit(limit);
    }
    return await db.select().from(refineries).limit(limit);
  }

  async getRefineryByCode(refineryCode: string): Promise<Refinery | null> {
    const result = await db.select().from(refineries)
      .where(eq(refineries.refineryCode, refineryCode))
      .limit(1);
    return result[0] || null;
  }

  async updateRefinery(id: string, updates: Partial<InsertRefinery>): Promise<Refinery> {
    const result = await db.update(refineries)
      .set(updates)
      .where(eq(refineries.id, id))
      .returning();
    return result[0];
  }

  async deleteRefinery(id: string): Promise<void> {
    await db.delete(refineries).where(eq(refineries.id, id));
  }

  // ===== SUPPLY & DEMAND BALANCES =====
  async createSupplyDemandBalance(balance: InsertSupplyDemandBalance): Promise<SupplyDemandBalance> {
    const result = await db.insert(supplyDemandBalances).values(balance).returning();
    return result[0];
  }

  async getSupplyDemandBalances(commodity?: string, region?: string, period?: string, limit: number = 100): Promise<SupplyDemandBalance[]> {
    if (commodity && region && period) {
      return await db.select().from(supplyDemandBalances)
        .where(and(
          eq(supplyDemandBalances.commodity, commodity),
          eq(supplyDemandBalances.region, region),
          eq(supplyDemandBalances.period, period)
        ))
        .limit(limit);
    }
    if (commodity && region) {
      return await db.select().from(supplyDemandBalances)
        .where(and(
          eq(supplyDemandBalances.commodity, commodity),
          eq(supplyDemandBalances.region, region)
        ))
        .limit(limit);
    }
    if (commodity) {
      return await db.select().from(supplyDemandBalances)
        .where(eq(supplyDemandBalances.commodity, commodity))
        .limit(limit);
    }
    if (region) {
      return await db.select().from(supplyDemandBalances)
        .where(eq(supplyDemandBalances.region, region))
        .limit(limit);
    }
    return await db.select().from(supplyDemandBalances).limit(limit);
  }

  async getLatestBalances(commodity?: string, region?: string, limit: number = 10): Promise<SupplyDemandBalance[]> {
    if (commodity && region) {
      return await db.select().from(supplyDemandBalances)
        .where(and(
          eq(supplyDemandBalances.commodity, commodity),
          eq(supplyDemandBalances.region, region)
        ))
        .orderBy(desc(supplyDemandBalances.period))
        .limit(limit);
    }
    if (commodity) {
      return await db.select().from(supplyDemandBalances)
        .where(eq(supplyDemandBalances.commodity, commodity))
        .orderBy(desc(supplyDemandBalances.period))
        .limit(limit);
    }
    if (region) {
      return await db.select().from(supplyDemandBalances)
        .where(eq(supplyDemandBalances.region, region))
        .orderBy(desc(supplyDemandBalances.period))
        .limit(limit);
    }
    return await db.select().from(supplyDemandBalances)
      .orderBy(desc(supplyDemandBalances.period))
      .limit(limit);
  }

  async updateSupplyDemandBalance(id: string, updates: Partial<InsertSupplyDemandBalance>): Promise<SupplyDemandBalance> {
    const result = await db.update(supplyDemandBalances)
      .set(updates)
      .where(eq(supplyDemandBalances.id, id))
      .returning();
    return result[0];
  }

  async deleteSupplyDemandBalance(id: string): Promise<void> {
    await db.delete(supplyDemandBalances).where(eq(supplyDemandBalances.id, id));
  }

  // ===== RESEARCH & INSIGHT LAYER =====
  async createResearchReport(report: InsertResearchReport): Promise<ResearchReport> {
    const result = await db.insert(researchReports).values(report).returning();
    return result[0];
  }

  async getResearchReports(category?: string, subcategory?: string, limit: number = 100): Promise<ResearchReport[]> {
    if (category && subcategory) {
      return await db.select().from(researchReports)
        .where(and(
          eq(researchReports.category, category),
          eq(researchReports.subcategory, subcategory),
          eq(researchReports.isPublished, true)
        ))
        .orderBy(desc(researchReports.publishDate))
        .limit(limit);
    }
    if (category) {
      return await db.select().from(researchReports)
        .where(and(
          eq(researchReports.category, category),
          eq(researchReports.isPublished, true)
        ))
        .orderBy(desc(researchReports.publishDate))
        .limit(limit);
    }
    return await db.select().from(researchReports)
      .where(eq(researchReports.isPublished, true))
      .orderBy(desc(researchReports.publishDate))
      .limit(limit);
  }

  async getPublishedReports(limit: number = 10): Promise<ResearchReport[]> {
    return await db.select().from(researchReports)
      .where(eq(researchReports.isPublished, true))
      .orderBy(desc(researchReports.publishDate))
      .limit(limit);
  }

  async getReportById(reportId: string): Promise<ResearchReport | null> {
    const result = await db.select().from(researchReports)
      .where(eq(researchReports.reportId, reportId))
      .limit(1);
    return result[0] || null;
  }

  async updateResearchReport(id: string, updates: Partial<InsertResearchReport>): Promise<ResearchReport> {
    const result = await db.update(researchReports)
      .set(updates)
      .where(eq(researchReports.id, id))
      .returning();
    return result[0];
  }

  async deleteResearchReport(id: string): Promise<void> {
    await db.delete(researchReports).where(eq(researchReports.id, id));
  }

  // ===== CSV-BASED DATA (REFINERY) =====
  async getRefineryUnits(plant?: string): Promise<RefineryUnit[]> {
    if (plant) {
      return await db.select().from(refineryUnits).where(eq(refineryUnits.plant, plant));
    }
    return await db.select().from(refineryUnits);
  }

  async getRefineryUtilization(startDate?: string, endDate?: string, plant?: string): Promise<RefineryUtilizationDaily[]> {
    let query = db.select().from(refineryUtilizationDaily);
    
    const conditions = [];
    if (startDate) conditions.push(gte(refineryUtilizationDaily.date, startDate));
    if (endDate) conditions.push(lte(refineryUtilizationDaily.date, endDate));
    if (plant) conditions.push(eq(refineryUtilizationDaily.plant, plant));
    
    if (conditions.length > 0) {
      query = query.where(and(...conditions)) as any;
    }
    
    return await query.orderBy(desc(refineryUtilizationDaily.date));
  }

  async getRefineryCrackSpreads(startDate?: string, endDate?: string): Promise<RefineryCrackSpreadsDaily[]> {
    let query = db.select().from(refineryCrackSpreadsDaily);
    
    const conditions = [];
    if (startDate) conditions.push(gte(refineryCrackSpreadsDaily.date, startDate));
    if (endDate) conditions.push(lte(refineryCrackSpreadsDaily.date, endDate));
    
    if (conditions.length > 0) {
      query = query.where(and(...conditions)) as any;
    }
    
    return await query.orderBy(desc(refineryCrackSpreadsDaily.date));
  }

  // ===== CSV-BASED DATA (SUPPLY & DEMAND) =====
  async getSdModelsDaily(startDate?: string, endDate?: string, region?: string): Promise<SdModelsDaily[]> {
    let query = db.select().from(sdModelsDaily);
    
    const conditions = [];
    if (startDate) conditions.push(gte(sdModelsDaily.date, startDate));
    if (endDate) conditions.push(lte(sdModelsDaily.date, endDate));
    if (region) conditions.push(eq(sdModelsDaily.region, region));
    
    if (conditions.length > 0) {
      query = query.where(and(...conditions)) as any;
    }
    
    return await query.orderBy(desc(sdModelsDaily.date));
  }

  async getSdForecastsWeekly(startDate?: string, endDate?: string, region?: string): Promise<SdForecastsWeekly[]> {
    let query = db.select().from(sdForecastsWeekly);
    
    const conditions = [];
    if (startDate) conditions.push(gte(sdForecastsWeekly.weekEnd, startDate));
    if (endDate) conditions.push(lte(sdForecastsWeekly.weekEnd, endDate));
    if (region) conditions.push(eq(sdForecastsWeekly.region, region));
    
    if (conditions.length > 0) {
      query = query.where(and(...conditions)) as any;
    }
    
    return await query.orderBy(desc(sdForecastsWeekly.weekEnd));
  }

  // ===== CSV-BASED DATA (RESEARCH) =====
  async getResearchInsightsDaily(startDate?: string, endDate?: string, limit: number = 100): Promise<ResearchInsightsDaily[]> {
    let query = db.select().from(researchInsightsDaily);
    
    const conditions = [];
    if (startDate) conditions.push(gte(researchInsightsDaily.date, startDate));
    if (endDate) conditions.push(lte(researchInsightsDaily.date, endDate));
    
    if (conditions.length > 0) {
      query = query.where(and(...conditions)) as any;
    }
    
    return await query.orderBy(desc(researchInsightsDaily.date)).limit(limit);
  }

  // ===== ML PRICE PREDICTIONS =====
  async getMlPredictions(commodityType?: string, limit: number = 10): Promise<any[]> {
    const { mlPricePredictions } = await import('@shared/schema');
    let query = db.select().from(mlPricePredictions);
    
    if (commodityType) {
      query = query.where(eq(mlPricePredictions.commodityType, commodityType)) as any;
    }
    
    return await query.orderBy(desc(mlPricePredictions.createdAt)).limit(limit);
  }

  async getLatestMlPrediction(commodityType: string): Promise<any | null> {
    const { mlPricePredictions } = await import('@shared/schema');
    const result = await db.select()
      .from(mlPricePredictions)
      .where(eq(mlPricePredictions.commodityType, commodityType))
      .orderBy(desc(mlPricePredictions.createdAt))
      .limit(1);
    
    return result[0] || null;
  }

  // ===== WATCHLISTS =====
  async createWatchlist(watchlist: InsertWatchlist): Promise<Watchlist> {
    const result = await db.insert(watchlists).values(watchlist).returning();
    return result[0];
  }

  async getWatchlists(userId: string): Promise<Watchlist[]> {
    return await db.select().from(watchlists)
      .where(eq(watchlists.userId, userId))
      .orderBy(desc(watchlists.createdAt));
  }

  async getWatchlistById(id: string): Promise<Watchlist | null> {
    const result = await db.select().from(watchlists).where(eq(watchlists.id, id)).limit(1);
    return result[0] || null;
  }

  async updateWatchlist(id: string, updates: Partial<InsertWatchlist>): Promise<Watchlist> {
    const result = await db.update(watchlists)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(watchlists.id, id))
      .returning();
    return result[0];
  }

  async deleteWatchlist(id: string): Promise<void> {
    await db.delete(watchlists).where(eq(watchlists.id, id));
  }

  // ===== ALERT RULES =====
  async createAlertRule(rule: InsertAlertRule): Promise<AlertRule> {
    const result = await db.insert(alertRules).values(rule).returning();
    return result[0];
  }

  async getAlertRules(userId: string): Promise<AlertRule[]> {
    return await db.select().from(alertRules)
      .where(eq(alertRules.userId, userId))
      .orderBy(desc(alertRules.createdAt));
  }

  async getAlertRuleById(id: string): Promise<AlertRule | null> {
    const result = await db.select().from(alertRules).where(eq(alertRules.id, id)).limit(1);
    return result[0] || null;
  }

  async updateAlertRule(id: string, updates: Partial<InsertAlertRule>): Promise<AlertRule> {
    const result = await db.update(alertRules)
      .set(updates)
      .where(eq(alertRules.id, id))
      .returning();
    return result[0];
  }

  async deleteAlertRule(id: string): Promise<void> {
    await db.delete(alertRules).where(eq(alertRules.id, id));
  }

  async getActiveAlertRules(): Promise<AlertRule[]> {
    return await db.select().from(alertRules)
      .where(eq(alertRules.isActive, true))
      .orderBy(desc(alertRules.createdAt));
  }
}

export const storage = new DrizzleStorage();