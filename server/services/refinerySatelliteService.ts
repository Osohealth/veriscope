import { db } from "../db";
import { refineryAois, satelliteObservations, refineryActivityIndices } from "@shared/schema";
import { eq, desc, and, gte, lte } from "drizzle-orm";
import { 
  searchSentinel2Scenes, 
  selectBestWeeklyScene, 
  generateWeeks, 
  computeActivityIndex, 
  determineTrend 
} from "./planetaryComputerService";

const ROTTERDAM_AOIS = [
  {
    name: "Rotterdam Cluster",
    code: "rotterdam_full",
    region: "rotterdam_cluster",
    description: "Full Rotterdam industrial belt covering Pernis, Botlek, Europoort, and Maasvlakte",
    boundingBox: { minLat: 51.85, maxLat: 51.98, minLon: 4.00, maxLon: 4.25 },
    polygon: [
      [51.85, 4.00], [51.98, 4.00], [51.98, 4.25], [51.85, 4.25], [51.85, 4.00]
    ],
    facilities: [
      { name: "Shell Pernis", type: "refinery", capacity: 404000 },
      { name: "ExxonMobil Rotterdam", type: "refinery", capacity: 191000 },
      { name: "BP Rotterdam", type: "refinery", capacity: 377000 }
    ]
  },
  {
    name: "Pernis / Vondelingenplaat",
    code: "pernis",
    region: "rotterdam_cluster",
    description: "Shell Pernis refinery - largest in Europe",
    boundingBox: { minLat: 51.87, maxLat: 51.91, minLon: 4.35, maxLon: 4.42 },
    polygon: [
      [51.87, 4.35], [51.91, 4.35], [51.91, 4.42], [51.87, 4.42], [51.87, 4.35]
    ],
    facilities: [{ name: "Shell Pernis", type: "refinery", capacity: 404000 }]
  },
  {
    name: "Botlek / Vondelingenplaat",
    code: "botlek",
    region: "rotterdam_cluster",
    description: "ExxonMobil Rotterdam refinery and tank farms",
    boundingBox: { minLat: 51.87, maxLat: 51.90, minLon: 4.27, maxLon: 4.35 },
    polygon: [
      [51.87, 4.27], [51.90, 4.27], [51.90, 4.35], [51.87, 4.35], [51.87, 4.27]
    ],
    facilities: [{ name: "ExxonMobil Rotterdam", type: "refinery", capacity: 191000 }]
  },
  {
    name: "Europoort",
    code: "europoort",
    region: "rotterdam_cluster", 
    description: "BP Rotterdam refinery and surrounding terminals",
    boundingBox: { minLat: 51.93, maxLat: 51.97, minLon: 4.05, maxLon: 4.18 },
    polygon: [
      [51.93, 4.05], [51.97, 4.05], [51.97, 4.18], [51.93, 4.18], [51.93, 4.05]
    ],
    facilities: [{ name: "BP Rotterdam", type: "refinery", capacity: 377000 }]
  },
  {
    name: "Maasvlakte",
    code: "maasvlakte",
    region: "rotterdam_cluster",
    description: "Terminals and logistics continuation",
    boundingBox: { minLat: 51.95, maxLat: 52.01, minLon: 3.95, maxLon: 4.08 },
    polygon: [
      [51.95, 3.95], [52.01, 3.95], [52.01, 4.08], [51.95, 4.08], [51.95, 3.95]
    ],
    facilities: []
  }
];

function generateWeeklyData(weeksBack: number): { weekStart: Date; weekEnd: Date }[] {
  const weeks: { weekStart: Date; weekEnd: Date }[] = [];
  const now = new Date();
  
  for (let i = weeksBack; i >= 0; i--) {
    const weekStart = new Date(now);
    weekStart.setDate(now.getDate() - (i * 7) - now.getDay() + 1);
    weekStart.setHours(0, 0, 0, 0);
    
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + 6);
    weekEnd.setHours(23, 59, 59, 999);
    
    weeks.push({ weekStart, weekEnd });
  }
  
  return weeks;
}

function generateMockActivityIndex(baseValue: number, variance: number): number {
  const value = baseValue + (Math.random() - 0.5) * 2 * variance;
  return Math.max(0, Math.min(100, Math.round(value * 100) / 100));
}

function generateMockConfidence(cloudFree: number): number {
  const base = cloudFree * 0.8 + 20;
  return Math.max(0, Math.min(100, Math.round(base * 100) / 100));
}


export async function initializeRefineryAois(): Promise<void> {
  try {
    const existing = await db.select().from(refineryAois).limit(1);
    if (existing.length > 0) {
      console.log("[RefinerySatellite] AOIs already initialized");
      return;
    }

    for (const aoi of ROTTERDAM_AOIS) {
      await db.insert(refineryAois).values({
        name: aoi.name,
        code: aoi.code,
        region: aoi.region,
        description: aoi.description,
        boundingBox: aoi.boundingBox,
        polygon: aoi.polygon || null,
        facilities: aoi.facilities,
        isActive: true
      });
    }
    
    console.log("[RefinerySatellite] Initialized", ROTTERDAM_AOIS.length, "AOIs");
  } catch (error) {
    console.error("[RefinerySatellite] Error initializing AOIs:", error);
    throw error;
  }
}

export async function generateMockSatelliteData(): Promise<void> {
  try {
    const aois = await db.select().from(refineryAois);
    if (aois.length === 0) {
      await initializeRefineryAois();
      return generateMockSatelliteData();
    }

    const existingIndices = await db.select().from(refineryActivityIndices).limit(1);
    if (existingIndices.length > 0) {
      console.log("[RefinerySatellite] Activity indices already exist");
      return;
    }

    const weeks = generateWeeklyData(12);
    const mainAoi = aois.find(a => a.code === "rotterdam_full") || aois[0];
    
    let runningBaseline = 65;
    
    for (const week of weeks) {
      const cloudFree = 30 + Math.random() * 60;
      const isUsable = cloudFree > 40;
      
      const sceneId = `S2A_MSIL2A_${week.weekStart.toISOString().split('T')[0].replace(/-/g, '')}T103021_N0500_R108_T31UFT_${Date.now()}`;
      
      await db.insert(satelliteObservations).values({
        aoiId: mainAoi.id,
        sceneId,
        satellite: Math.random() > 0.5 ? "sentinel-2a" : "sentinel-2b",
        observationDate: new Date(week.weekStart.getTime() + Math.random() * 3 * 24 * 60 * 60 * 1000),
        cloudCoverPercent: (100 - cloudFree).toFixed(2),
        cloudFreeAoiPercent: cloudFree.toFixed(2),
        isUsable,
        processingLevel: "L2A",
        tileId: "31UFT",
        sunAzimuth: (150 + Math.random() * 60).toFixed(2),
        sunElevation: (25 + Math.random() * 40).toFixed(2),
        metadata: { source: "mock_stac" }
      });
      
      const activityIndex = generateMockActivityIndex(runningBaseline, 15);
      const swirAnomaly = generateMockActivityIndex(activityIndex * 0.9, 10);
      const plumeIndex = generateMockActivityIndex(activityIndex * 0.7, 12);
      const surfaceChange = generateMockActivityIndex(20, 15);
      
      const confidence = generateMockConfidence(cloudFree);
      const trend = determineTrend(activityIndex, runningBaseline);
      
      await db.insert(refineryActivityIndices).values({
        aoiId: mainAoi.id,
        weekStart: week.weekStart.toISOString().split('T')[0],
        weekEnd: week.weekEnd.toISOString().split('T')[0],
        sceneId: isUsable ? sceneId : null,
        observationDate: isUsable ? new Date(week.weekStart.getTime() + Math.random() * 3 * 24 * 60 * 60 * 1000) : null,
        activityIndex: activityIndex.toFixed(2),
        confidence: confidence.toFixed(2),
        swirAnomalyIndex: swirAnomaly.toFixed(2),
        plumeIndex: plumeIndex.toFixed(2),
        surfaceChangeIndex: surfaceChange.toFixed(2),
        cloudFreePercent: cloudFree.toFixed(2),
        baselineActivityIndex: runningBaseline.toFixed(2),
        activityTrend: trend,
        dataSource: "sentinel-2",
        methodology: "optical",
        metadata: { generated: true, mockData: true }
      });
      
      runningBaseline = runningBaseline * 0.8 + activityIndex * 0.2;
    }
    
    console.log("[RefinerySatellite] Generated", weeks.length, "weeks of activity data");
  } catch (error) {
    console.error("[RefinerySatellite] Error generating mock data:", error);
    throw error;
  }
}

export async function getAois(): Promise<any[]> {
  return db.select().from(refineryAois).where(eq(refineryAois.isActive, true));
}

export async function getAoiByCode(code: string): Promise<any> {
  const result = await db.select().from(refineryAois).where(eq(refineryAois.code, code)).limit(1);
  return result[0] || null;
}

export async function getLatestActivityIndex(aoiCode: string = "rotterdam_full"): Promise<any> {
  const aoi = await getAoiByCode(aoiCode);
  if (!aoi) return null;
  
  const result = await db.select()
    .from(refineryActivityIndices)
    .where(eq(refineryActivityIndices.aoiId, aoi.id))
    .orderBy(desc(refineryActivityIndices.weekStart))
    .limit(1);
  
  return result[0] ? { ...result[0], aoi } : null;
}

export async function getActivityTimeline(aoiCode: string = "rotterdam_full", weeks: number = 12): Promise<any[]> {
  const aoi = await getAoiByCode(aoiCode);
  if (!aoi) return [];
  
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - weeks * 7);
  
  return db.select()
    .from(refineryActivityIndices)
    .where(and(
      eq(refineryActivityIndices.aoiId, aoi.id),
      gte(refineryActivityIndices.weekStart, cutoffDate.toISOString().split('T')[0])
    ))
    .orderBy(desc(refineryActivityIndices.weekStart));
}

export async function getRecentObservations(aoiCode: string = "rotterdam_full", limit: number = 10): Promise<any[]> {
  const aoi = await getAoiByCode(aoiCode);
  if (!aoi) return [];
  
  return db.select()
    .from(satelliteObservations)
    .where(eq(satelliteObservations.aoiId, aoi.id))
    .orderBy(desc(satelliteObservations.observationDate))
    .limit(limit);
}

export async function getSummaryStats(): Promise<any> {
  const latest = await getLatestActivityIndex("rotterdam_full");
  const timeline = await getActivityTimeline("rotterdam_full", 4);
  
  if (!latest) {
    return {
      activityIndex: 0,
      confidence: 0,
      trend: "unknown",
      lastObservation: null,
      weeklyChange: 0,
      fourWeekAverage: 0
    };
  }
  
  const fourWeekAvg = timeline.length > 0
    ? timeline.reduce((sum, w) => sum + parseFloat(w.activityIndex), 0) / timeline.length
    : parseFloat(latest.activityIndex);
  
  const weeklyChange = timeline.length > 1
    ? parseFloat(timeline[0].activityIndex) - parseFloat(timeline[1].activityIndex)
    : 0;
  
  return {
    activityIndex: parseFloat(latest.activityIndex),
    confidence: parseFloat(latest.confidence),
    trend: latest.activityTrend,
    lastObservation: latest.observationDate,
    weeklyChange: Math.round(weeklyChange * 10) / 10,
    fourWeekAverage: Math.round(fourWeekAvg * 10) / 10,
    swirAnomaly: parseFloat(latest.swirAnomalyIndex),
    plumeIndex: parseFloat(latest.plumeIndex),
    surfaceChange: parseFloat(latest.surfaceChangeIndex),
    cloudFreePercent: parseFloat(latest.cloudFreePercent),
    aoi: latest.aoi
  };
}

export async function fetchRealSatelliteData(): Promise<{ success: boolean; message: string; weeksProcessed: number }> {
  try {
    const aois = await db.select().from(refineryAois);
    if (aois.length === 0) {
      await initializeRefineryAois();
      return fetchRealSatelliteData();
    }

    const mainAoi = aois.find(a => a.code === "rotterdam_full") || aois[0];
    const bbox = mainAoi.boundingBox as { minLat: number; maxLat: number; minLon: number; maxLon: number };
    
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 90);
    
    console.log("[RefinerySatellite] Fetching Sentinel-2 scenes from Planetary Computer...");
    console.log(`[RefinerySatellite] Date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);
    console.log(`[RefinerySatellite] Bounding box: ${JSON.stringify(bbox)}`);
    
    const scenes = await searchSentinel2Scenes(bbox, startDate, endDate, 70, 200);
    console.log(`[RefinerySatellite] Found ${scenes.length} Sentinel-2 scenes`);
    
    if (scenes.length === 0) {
      return { success: false, message: "No Sentinel-2 scenes found for Rotterdam AOI", weeksProcessed: 0 };
    }

    await db.delete(refineryActivityIndices).where(eq(refineryActivityIndices.aoiId, mainAoi.id));
    await db.delete(satelliteObservations).where(eq(satelliteObservations.aoiId, mainAoi.id));
    
    const weeks = generateWeeks(12);
    let runningBaseline = 65;
    let weeksProcessed = 0;
    
    for (const week of weeks) {
      const bestScene = await selectBestWeeklyScene(scenes, week.weekStart, week.weekEnd);
      
      if (bestScene) {
        const cloudFreePercent = 100 - bestScene.cloudCoverPercent;
        const isUsable = cloudFreePercent >= 30;
        
        await db.insert(satelliteObservations).values({
          aoiId: mainAoi.id,
          sceneId: bestScene.sceneId,
          satellite: bestScene.satellite,
          observationDate: bestScene.observationDate,
          cloudCoverPercent: bestScene.cloudCoverPercent.toFixed(2),
          cloudFreeAoiPercent: cloudFreePercent.toFixed(2),
          isUsable,
          processingLevel: bestScene.processingLevel,
          tileId: bestScene.tileId,
          sunAzimuth: bestScene.sunAzimuth?.toFixed(2) || null,
          sunElevation: bestScene.sunElevation?.toFixed(2) || null,
          metadata: { 
            source: "planetary_computer",
            assets: bestScene.assets
          }
        });
        
        const indices = computeActivityIndex(bestScene.cloudCoverPercent, isUsable, bestScene.sceneId);
        const trend = determineTrend(indices.activityIndex, runningBaseline);
        
        await db.insert(refineryActivityIndices).values({
          aoiId: mainAoi.id,
          weekStart: week.weekStart.toISOString().split('T')[0],
          weekEnd: week.weekEnd.toISOString().split('T')[0],
          sceneId: isUsable ? bestScene.sceneId : null,
          observationDate: isUsable ? bestScene.observationDate : null,
          activityIndex: indices.activityIndex.toFixed(2),
          confidence: indices.confidence.toFixed(2),
          swirAnomalyIndex: indices.swirAnomaly.toFixed(2),
          plumeIndex: indices.plumeIndex.toFixed(2),
          surfaceChangeIndex: indices.surfaceChange.toFixed(2),
          cloudFreePercent: cloudFreePercent.toFixed(2),
          baselineActivityIndex: runningBaseline.toFixed(2),
          activityTrend: trend,
          dataSource: "sentinel-2",
          methodology: "optical",
          metadata: { 
            generated: false, 
            source: "planetary_computer",
            sceneId: bestScene.sceneId
          }
        });
        
        if (isUsable) {
          runningBaseline = runningBaseline * 0.8 + indices.activityIndex * 0.2;
        }
        weeksProcessed++;
      } else {
        await db.insert(refineryActivityIndices).values({
          aoiId: mainAoi.id,
          weekStart: week.weekStart.toISOString().split('T')[0],
          weekEnd: week.weekEnd.toISOString().split('T')[0],
          sceneId: null,
          observationDate: null,
          activityIndex: "0",
          confidence: "0",
          swirAnomalyIndex: "0",
          plumeIndex: "0",
          surfaceChangeIndex: "0",
          cloudFreePercent: "0",
          baselineActivityIndex: runningBaseline.toFixed(2),
          activityTrend: "unknown",
          dataSource: "sentinel-2",
          methodology: "optical",
          metadata: { 
            generated: false, 
            source: "planetary_computer",
            noDataReason: "no_scene_available"
          }
        });
        weeksProcessed++;
      }
    }
    
    console.log(`[RefinerySatellite] Processed ${weeksProcessed} weeks of real satellite data`);
    return { 
      success: true, 
      message: `Successfully fetched and processed ${scenes.length} Sentinel-2 scenes across ${weeksProcessed} weeks`, 
      weeksProcessed 
    };
  } catch (error) {
    console.error("[RefinerySatellite] Error fetching real satellite data:", error);
    throw error;
  }
}

export async function refreshSatelliteData(): Promise<{ success: boolean; message: string }> {
  try {
    const result = await fetchRealSatelliteData();
    return { success: result.success, message: result.message };
  } catch (error) {
    console.error("[RefinerySatellite] Falling back to mock data:", error);
    await generateMockSatelliteData();
    return { success: true, message: "Used mock data due to API error" };
  }
}
