import { db } from "../db";
import { floatingStorage, sprReserves, storageTimeSeries, storageFillData, storageFacilities } from "@shared/schema";
import { eq, desc, sql } from "drizzle-orm";

function hashString(str: string): number {
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) ^ str.charCodeAt(i);
  }
  return Math.abs(hash);
}

function seededRandom(seed: string): number {
  const hash = hashString(seed);
  return (hash % 10000) / 10000;
}

function seededRandomRange(seed: string, min: number, max: number): number {
  return min + seededRandom(seed) * (max - min);
}

function seededPick<T>(seed: string, arr: T[]): T {
  const index = hashString(seed) % arr.length;
  return arr[index];
}

const VESSEL_NAMES = [
  "Front Ace", "Nordic Zenith", "Maran Castor", "Suezmax Phoenix", 
  "Gulf Titan", "Ocean Pioneer", "Atlantic Victory", "Pacific Star",
  "Emerald Sea", "Diamond Leader", "Ruby Express", "Sapphire Dawn",
  "Golden Dragon", "Silver Wave", "Bronze Eagle", "Platinum Sky"
];

const VESSEL_TYPES = ["VLCC", "Suezmax", "Aframax", "LNG"];
const CARGO_TYPES = ["crude_oil", "refined_products", "lng"];
const CARGO_GRADES = ["Brent", "WTI", "Oman", "Dubai", "ULSD", "Jet Fuel", "Gasoline", "LNG Spot"];
const REGIONS = ["North Sea", "Persian Gulf", "Singapore", "US Gulf", "West Africa", "Mediterranean"];
const CHARTERERS = ["Shell Trading", "BP Oil", "Vitol", "Trafigura", "Glencore", "Gunvor", "Mercuria", "Chevron"];

const SPR_COUNTRIES = [
  { country: "United States", countryCode: "USA", capacity: 714000000 },
  { country: "China", countryCode: "CHN", capacity: 550000000 },
  { country: "Japan", countryCode: "JPN", capacity: 324000000 },
  { country: "Germany", countryCode: "DEU", capacity: 196000000 },
  { country: "South Korea", countryCode: "KOR", capacity: 146000000 },
  { country: "France", countryCode: "FRA", capacity: 130000000 },
  { country: "India", countryCode: "IND", capacity: 87000000 },
  { country: "United Kingdom", countryCode: "GBR", capacity: 65000000 },
];

const GRADE_TYPES = ["sweet_crude", "sour_crude", "middle_east_blend", "west_african"];
const SPR_REGIONS = ["Gulf Coast", "Midwest", "Dalian", "Zhoushan", "Tomakomai", "Karlsruhe", "Yeosu"];

const METRIC_TYPES = ["tank_level", "floating_storage", "spr_total"];
const STORAGE_REGIONS = ["global", "north_america", "europe", "asia", "middle_east"];
const STORAGE_TYPES = ["crude_oil", "refined_products", "lng"];
const DATA_SOURCES = ["satellite", "eia", "iea", "industry", "customs"];

export async function generateFloatingStorageData(): Promise<void> {
  console.log("[StorageDataService] Generating floating storage data...");
  
  const existingCount = await db.select({ count: sql<number>`count(*)` }).from(floatingStorage);
  if (Number(existingCount[0]?.count) > 0) {
    console.log("[StorageDataService] Floating storage data already exists, skipping...");
    return;
  }

  const now = new Date();
  const records = [];
  
  for (let i = 0; i < 20; i++) {
    const seed = `floating-storage-${i}`;
    const vesselName = seededPick(`${seed}-name`, VESSEL_NAMES);
    const vesselType = seededPick(`${seed}-type`, VESSEL_TYPES);
    const cargoType = seededPick(`${seed}-cargo`, CARGO_TYPES);
    const region = seededPick(`${seed}-region`, REGIONS);
    
    const durationDays = Math.floor(seededRandomRange(`${seed}-duration`, 7, 120));
    const startDate = new Date(now.getTime() - durationDays * 24 * 60 * 60 * 1000);
    
    const baseVolume = vesselType === "VLCC" ? 2000000 : vesselType === "Suezmax" ? 1000000 : 600000;
    const cargoVolume = Math.floor(seededRandomRange(`${seed}-volume`, baseVolume * 0.85, baseVolume));
    
    const estimatedValue = cargoVolume * seededRandomRange(`${seed}-price`, 60, 85);
    
    records.push({
      vesselName,
      vesselType,
      imo: `IMO${9000000 + i * 1000}`,
      cargoType,
      cargoGrade: seededPick(`${seed}-grade`, CARGO_GRADES),
      cargoVolume,
      cargoUnit: "MT" as const,
      locationLat: String(seededRandomRange(`${seed}-lat`, -10, 55)),
      locationLng: String(seededRandomRange(`${seed}-lng`, -80, 120)),
      region,
      durationDays,
      startDate,
      estimatedValue: String(Math.round(estimatedValue)),
      charterer: seededPick(`${seed}-charterer`, CHARTERERS),
      status: seededRandom(`${seed}-status`) > 0.8 ? "releasing" : "active",
    });
  }
  
  await db.insert(floatingStorage).values(records);
  console.log(`[StorageDataService] Created ${records.length} floating storage records`);
}

export async function generateSprReservesData(): Promise<void> {
  console.log("[StorageDataService] Generating SPR reserves data...");
  
  const existingCount = await db.select({ count: sql<number>`count(*)` }).from(sprReserves);
  if (Number(existingCount[0]?.count) > 0) {
    console.log("[StorageDataService] SPR reserves data already exists, skipping...");
    return;
  }

  const now = new Date();
  const records = [];
  
  for (const countryData of SPR_COUNTRIES) {
    for (const gradeType of GRADE_TYPES) {
      const seed = `spr-${countryData.countryCode}-${gradeType}`;
      
      const gradePercent = seededRandomRange(`${seed}-percent`, 10, 40);
      const volumeBarrels = (countryData.capacity * gradePercent / 100) * seededRandomRange(`${seed}-fill`, 0.55, 0.85);
      const utilizationRate = (volumeBarrels / (countryData.capacity * gradePercent / 100)) * 100;
      
      const daysOfCover = Math.floor(seededRandomRange(`${seed}-cover`, 25, 90));
      
      records.push({
        country: countryData.country,
        countryCode: countryData.countryCode,
        region: seededPick(`${seed}-region`, SPR_REGIONS),
        gradeType,
        volumeBarrels: String(Math.round(volumeBarrels)),
        percentOfTotal: String(gradePercent.toFixed(2)),
        capacityBarrels: String(Math.round(countryData.capacity * gradePercent / 100)),
        utilizationRate: String(utilizationRate.toFixed(2)),
        daysOfCover,
        lastReleaseDate: seededRandom(`${seed}-release`) > 0.7 ? new Date(now.getTime() - seededRandomRange(`${seed}-release-days`, 30, 365) * 24 * 60 * 60 * 1000) : null,
        lastReleaseVolume: seededRandom(`${seed}-release`) > 0.7 ? String(Math.round(seededRandomRange(`${seed}-release-vol`, 1000000, 10000000))) : null,
        reportDate: now,
        source: seededPick(`${seed}-source`, ["DOE", "IEA", "national_agency"]),
        metadata: { reportMonth: now.toISOString().substring(0, 7) },
      });
    }
  }
  
  await db.insert(sprReserves).values(records);
  console.log(`[StorageDataService] Created ${records.length} SPR reserves records`);
}

export async function generateStorageTimeSeries(): Promise<void> {
  console.log("[StorageDataService] Generating storage time series data...");
  
  const existingCount = await db.select({ count: sql<number>`count(*)` }).from(storageTimeSeries);
  if (Number(existingCount[0]?.count) > 0) {
    console.log("[StorageDataService] Storage time series data already exists, skipping...");
    return;
  }

  const now = new Date();
  const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
  const records = [];
  
  const baseCapacities: Record<string, Record<string, number>> = {
    global: { crude_oil: 5500000000, refined_products: 2800000000, lng: 800000000 },
    north_america: { crude_oil: 1200000000, refined_products: 600000000, lng: 150000000 },
    europe: { crude_oil: 900000000, refined_products: 450000000, lng: 250000000 },
    asia: { crude_oil: 1800000000, refined_products: 950000000, lng: 300000000 },
    middle_east: { crude_oil: 800000000, refined_products: 400000000, lng: 50000000 },
  };
  
  let currentDate = new Date(oneYearAgo);
  let weekNumber = 0;
  
  while (currentDate <= now) {
    for (const metricType of METRIC_TYPES) {
      for (const region of STORAGE_REGIONS) {
        for (const storageType of STORAGE_TYPES) {
          const seed = `ts-${metricType}-${region}-${storageType}-${weekNumber}`;
          
          const capacity = baseCapacities[region]?.[storageType] || 100000000;
          
          const seasonalFactor = 1 + 0.08 * Math.sin((weekNumber / 52) * 2 * Math.PI);
          const trendFactor = 1 + (weekNumber / 52) * seededRandomRange(`${seed}-trend`, -0.03, 0.05);
          const noise = seededRandomRange(`${seed}-noise`, -0.02, 0.02);
          
          const baseUtilization = metricType === "spr_total" ? 0.72 : 0.68;
          const utilization = Math.min(0.95, Math.max(0.45, baseUtilization * seasonalFactor * trendFactor + noise));
          
          const currentLevel = capacity * utilization;
          const prevWeekLevel = capacity * (utilization - seededRandomRange(`${seed}-wow`, -0.02, 0.02));
          const weekOverWeekChange = currentLevel - prevWeekLevel;
          
          const yearAgoLevel = capacity * (utilization - seededRandomRange(`${seed}-yoy`, -0.08, 0.08));
          const yearOverYearChange = currentLevel - yearAgoLevel;
          
          const fiveYearAverage = capacity * seededRandomRange(`${seed}-5y`, 0.60, 0.75);
          
          records.push({
            recordDate: new Date(currentDate),
            metricType,
            region,
            storageType,
            totalCapacity: String(Math.round(capacity)),
            currentLevel: String(Math.round(currentLevel)),
            utilizationRate: String((utilization * 100).toFixed(2)),
            weekOverWeekChange: String(Math.round(weekOverWeekChange)),
            yearOverYearChange: String(Math.round(yearOverYearChange)),
            fiveYearAverage: String(Math.round(fiveYearAverage)),
            confidence: String((0.85 + seededRandom(`${seed}-conf`) * 0.13).toFixed(4)),
            source: seededPick(`${seed}-source`, DATA_SOURCES),
            metadata: { weekNumber, year: currentDate.getFullYear() },
          });
        }
      }
    }
    
    currentDate = new Date(currentDate.getTime() + 7 * 24 * 60 * 60 * 1000);
    weekNumber++;
  }
  
  const batchSize = 500;
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await db.insert(storageTimeSeries).values(batch);
  }
  
  console.log(`[StorageDataService] Created ${records.length} storage time series records (${weekNumber} weeks)`);
}

export async function generateStorageFillData(): Promise<void> {
  console.log("[StorageDataService] Generating storage fill data for facilities...");
  
  const existingCount = await db.select({ count: sql<number>`count(*)` }).from(storageFillData);
  if (Number(existingCount[0]?.count) > 0) {
    console.log("[StorageDataService] Storage fill data already exists, skipping...");
    return;
  }

  const facilities = await db.select().from(storageFacilities);
  if (facilities.length === 0) {
    console.log("[StorageDataService] No storage facilities found, skipping fill data generation");
    return;
  }

  const now = new Date();
  const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
  const records = [];
  
  for (const facility of facilities) {
    let currentDate = new Date(oneYearAgo);
    let weekNumber = 0;
    let previousFill = seededRandomRange(`fill-init-${facility.id}`, 0.55, 0.75);
    
    while (currentDate <= now) {
      const seed = `fill-${facility.id}-${weekNumber}`;
      
      const seasonalFactor = 0.05 * Math.sin((weekNumber / 52) * 2 * Math.PI);
      const randomWalk = seededRandomRange(`${seed}-walk`, -0.03, 0.03);
      
      let newFill = previousFill + seasonalFactor * 0.1 + randomWalk;
      newFill = Math.min(0.95, Math.max(0.35, newFill));
      
      const confidence = 0.82 + seededRandom(`${seed}-conf`) * 0.15;
      
      records.push({
        siteId: facility.id,
        timestamp: new Date(currentDate),
        fillIndex: String(newFill.toFixed(4)),
        confidence: String(confidence.toFixed(4)),
        source: seededPick(`${seed}-source`, ["SAR", "optical", "manual"]) as "SAR" | "optical" | "manual",
        metadata: { weekNumber, facilityName: facility.name },
      });
      
      previousFill = newFill;
      currentDate = new Date(currentDate.getTime() + 7 * 24 * 60 * 60 * 1000);
      weekNumber++;
    }
  }
  
  const batchSize = 500;
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await db.insert(storageFillData).values(batch);
  }
  
  console.log(`[StorageDataService] Created ${records.length} storage fill data records for ${facilities.length} facilities`);
}

export async function initializeStorageData(): Promise<void> {
  console.log("[StorageDataService] Initializing storage data...");
  
  try {
    await generateFloatingStorageData();
    await generateSprReservesData();
    await generateStorageTimeSeries();
    await generateStorageFillData();
    
    console.log("[StorageDataService] Storage data initialization complete");
  } catch (error) {
    console.error("[StorageDataService] Error initializing storage data:", error);
    throw error;
  }
}

export async function getFloatingStorageStats() {
  const data = await db.select().from(floatingStorage).where(eq(floatingStorage.status, "active"));
  
  const totalVolume = data.reduce((sum, r) => sum + r.cargoVolume, 0);
  const totalValue = data.reduce((sum, r) => sum + (parseFloat(r.estimatedValue || "0")), 0);
  
  const byRegion = data.reduce((acc, r) => {
    const region = r.region || "Unknown";
    if (!acc[region]) acc[region] = { count: 0, volume: 0 };
    acc[region].count++;
    acc[region].volume += r.cargoVolume;
    return acc;
  }, {} as Record<string, { count: number; volume: number }>);
  
  return {
    totalVessels: data.length,
    totalVolumeMT: totalVolume,
    totalValueUSD: totalValue,
    byRegion,
    vessels: data,
  };
}

export async function getSprStats() {
  const data = await db.select().from(sprReserves);
  
  const byCountry = data.reduce((acc, r) => {
    if (!acc[r.country]) {
      acc[r.country] = { 
        totalVolume: 0, 
        totalCapacity: 0,
        grades: {},
      };
    }
    acc[r.country].totalVolume += parseFloat(r.volumeBarrels);
    acc[r.country].totalCapacity += parseFloat(r.capacityBarrels || "0");
    acc[r.country].grades[r.gradeType] = {
      volume: parseFloat(r.volumeBarrels),
      percent: parseFloat(r.percentOfTotal),
      utilization: parseFloat(r.utilizationRate || "0"),
    };
    return acc;
  }, {} as Record<string, { totalVolume: number; totalCapacity: number; grades: Record<string, { volume: number; percent: number; utilization: number }> }>);
  
  const globalTotal = Object.values(byCountry).reduce((sum, c) => sum + c.totalVolume, 0);
  const globalCapacity = Object.values(byCountry).reduce((sum, c) => sum + c.totalCapacity, 0);
  
  return {
    globalTotalBarrels: globalTotal,
    globalCapacityBarrels: globalCapacity,
    globalUtilization: (globalTotal / globalCapacity) * 100,
    byCountry,
    allRecords: data,
  };
}

export async function getStorageTimeSeriesData(options: {
  metricType?: string;
  region?: string;
  storageType?: string;
  weeks?: number;
} = {}) {
  const { metricType, region, storageType, weeks = 52 } = options;
  
  let query = db.select().from(storageTimeSeries);
  
  const conditions = [];
  if (metricType) conditions.push(eq(storageTimeSeries.metricType, metricType));
  if (region) conditions.push(eq(storageTimeSeries.region, region));
  if (storageType) conditions.push(eq(storageTimeSeries.storageType, storageType));
  
  const cutoffDate = new Date(Date.now() - weeks * 7 * 24 * 60 * 60 * 1000);
  conditions.push(sql`${storageTimeSeries.recordDate} >= ${cutoffDate}`);
  
  const data = await db.select()
    .from(storageTimeSeries)
    .where(sql`${conditions.length > 0 ? sql.join(conditions, sql` AND `) : sql`1=1`}`)
    .orderBy(desc(storageTimeSeries.recordDate))
    .limit(weeks * 15 * 3 * 3);
  
  return data;
}
