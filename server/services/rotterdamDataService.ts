import { readFileSync } from 'fs';
import { join } from 'path';
import { storage } from '../storage';

export interface RotterdamDataPoint {
  date: string;
  brentPrice: number;
  rotterdamPrice: number;
  localSpread: number;
  arrivalsCount: number;
  departuresCount: number;
  avgTankerDwt: number;
  receipts: number;
  exports: number;
  storageBbl: number;
  storageUtilization: number;
  berthedCount: number;
  anchoredCount: number;
  portWaitDays: number;
  congestionIndex: number;
  supplyPressureIndex: number;
  satelliteAnomalyFlag: number;
}

export interface VesselArrival {
  vesselId: string;
  vesselName: string;
  vesselType: string;
  mmsi: string;
  imo: string;
  flag: string;
  deadweight: number;
  eta: Date;
  destination: string;
  status: string;
  latitude: number;
  longitude: number;
  speed: number;
  lastUpdate: Date;
}

export interface VesselDeparture {
  vesselId: string;
  vesselName: string;
  vesselType: string;
  mmsi: string;
  imo: string;
  flag: string;
  deadweight: number;
  departureTime: Date;
  destination: string;
  status: string;
  latitude: number;
  longitude: number;
  speed: number;
  lastUpdate: Date;
}

export interface PortActivitySummary {
  expectedArrivals: number;
  recentDepartures: number;
  atBerth: number;
  atAnchor: number;
  underway: number;
  congestionLevel: 'low' | 'moderate' | 'high' | 'critical';
  averageWaitTime: number;
  lastUpdated: Date;
}

class RotterdamDataService {
  private data: RotterdamDataPoint[] = [];
  
  constructor() {
    this.loadData();
  }
  
  private loadData() {
    try {
      const csvPath = join(process.cwd(), 'attached_assets', 'rotterdam_crude_6m_synthetic_1759688909172.csv');
      const csvContent = readFileSync(csvPath, 'utf-8');
      const lines = csvContent.trim().split('\n');
      
      // Skip header
      for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',');
        if (values.length >= 18) {
          this.data.push({
            date: values[0],
            brentPrice: parseFloat(values[1]),
            rotterdamPrice: parseFloat(values[2]),
            localSpread: parseFloat(values[3]),
            arrivalsCount: parseInt(values[4]),
            departuresCount: parseInt(values[5]),
            avgTankerDwt: parseInt(values[6]),
            receipts: parseInt(values[7]),
            exports: parseInt(values[8]),
            storageBbl: parseInt(values[10]),
            storageUtilization: parseFloat(values[11]),
            berthedCount: parseInt(values[12]),
            anchoredCount: parseInt(values[13]),
            portWaitDays: parseFloat(values[14]),
            congestionIndex: parseFloat(values[15]),
            supplyPressureIndex: parseFloat(values[16]),
            satelliteAnomalyFlag: parseInt(values[17])
          });
        }
      }
      
      console.log(`Loaded ${this.data.length} Rotterdam data points`);
    } catch (error) {
      console.error('Error loading Rotterdam CSV data:', error);
    }
  }
  
  getAllData(): RotterdamDataPoint[] {
    return this.data;
  }
  
  getDataByMonth(month: string): RotterdamDataPoint[] {
    // month format: "2025-04" or "April 2025"
    return this.data.filter(point => {
      if (month.includes('-')) {
        // Format: "2025-04"
        return point.date.startsWith(month);
      } else {
        // Format: "April 2025" - convert to check
        const [monthName, year] = month.split(' ');
        const monthMap: Record<string, string> = {
          'April': '04', 'May': '05', 'June': '06',
          'July': '07', 'August': '08', 'September': '09', 'October': '10'
        };
        const monthNum = monthMap[monthName];
        return monthNum && point.date.startsWith(`${year}-${monthNum}`);
      }
    });
  }
  
  getLatestData(): RotterdamDataPoint | null {
    return this.data.length > 0 ? this.data[this.data.length - 1] : null;
  }
  
  getAvailableMonths(): string[] {
    const months = new Set<string>();
    this.data.forEach(point => {
      const [year, month] = point.date.split('-');
      months.add(`${year}-${month}`);
    });
    return Array.from(months).sort();
  }
  
  getAggregatedStats(month?: string): {
    avgBrentPrice: number;
    avgRotterdamPrice: number;
    avgSpread: number;
    totalArrivals: number;
    totalDepartures: number;
    avgStorageUtilization: number;
    avgCongestionIndex: number;
    totalReceipts: number;
    totalExports: number;
  } {
    const filteredData = month ? this.getDataByMonth(month) : this.data;
    
    if (filteredData.length === 0) {
      return {
        avgBrentPrice: 0,
        avgRotterdamPrice: 0,
        avgSpread: 0,
        totalArrivals: 0,
        totalDepartures: 0,
        avgStorageUtilization: 0,
        avgCongestionIndex: 0,
        totalReceipts: 0,
        totalExports: 0
      };
    }
    
    const sum = filteredData.reduce((acc, point) => ({
      brentPrice: acc.brentPrice + point.brentPrice,
      rotterdamPrice: acc.rotterdamPrice + point.rotterdamPrice,
      localSpread: acc.localSpread + point.localSpread,
      arrivalsCount: acc.arrivalsCount + point.arrivalsCount,
      departuresCount: acc.departuresCount + point.departuresCount,
      storageUtilization: acc.storageUtilization + point.storageUtilization,
      congestionIndex: acc.congestionIndex + point.congestionIndex,
      receipts: acc.receipts + point.receipts,
      exports: acc.exports + point.exports
    }), {
      brentPrice: 0, rotterdamPrice: 0, localSpread: 0,
      arrivalsCount: 0, departuresCount: 0, storageUtilization: 0,
      congestionIndex: 0, receipts: 0, exports: 0
    });
    
    const count = filteredData.length;
    
    return {
      avgBrentPrice: sum.brentPrice / count,
      avgRotterdamPrice: sum.rotterdamPrice / count,
      avgSpread: sum.localSpread / count,
      totalArrivals: sum.arrivalsCount,
      totalDepartures: sum.departuresCount,
      avgStorageUtilization: sum.storageUtilization / count,
      avgCongestionIndex: sum.congestionIndex / count,
      totalReceipts: sum.receipts,
      totalExports: sum.exports
    };
  }
  
  // Rotterdam port bounding box (approximate)
  private readonly ROTTERDAM_BOUNDS = {
    minLat: 51.85,
    maxLat: 52.05,
    minLon: 3.90,
    maxLon: 4.60
  };
  
  private isInRotterdamArea(lat: number, lon: number): boolean {
    return lat >= this.ROTTERDAM_BOUNDS.minLat && 
           lat <= this.ROTTERDAM_BOUNDS.maxLat &&
           lon >= this.ROTTERDAM_BOUNDS.minLon && 
           lon <= this.ROTTERDAM_BOUNDS.maxLon;
  }
  
  async getExpectedArrivals(): Promise<VesselArrival[]> {
    const vessels = await storage.getVessels();
    const positions = await storage.getLatestVesselPositions();
    
    const positionMap = new Map<string, any>();
    for (const pos of positions) {
      positionMap.set(pos.vesselId, pos);
    }
    
    const arrivals: VesselArrival[] = [];
    const now = new Date();
    const next24h = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    
    for (const vessel of vessels) {
      const pos = positionMap.get(vessel.id);
      if (!pos) continue;
      
      const lat = parseFloat(pos.latitude);
      const lon = parseFloat(pos.longitude);
      const speed = parseFloat(pos.speed || '0');
      
      // Check if vessel is approaching Rotterdam (outside but heading toward)
      const isApproaching = !this.isInRotterdamArea(lat, lon) && 
                           pos.destination?.toLowerCase().includes('rotterdam');
      
      // Or if vessel is in Rotterdam area with an ETA
      const isExpected = pos.eta && new Date(pos.eta) <= next24h;
      
      if (isApproaching || isExpected) {
        arrivals.push({
          vesselId: vessel.id,
          vesselName: vessel.name,
          vesselType: vessel.vesselType || 'unknown',
          mmsi: vessel.mmsi,
          imo: vessel.imo || '',
          flag: vessel.flag || '',
          deadweight: vessel.deadweight || 0,
          eta: pos.eta ? new Date(pos.eta) : new Date(now.getTime() + Math.random() * 24 * 60 * 60 * 1000),
          destination: pos.destination || 'Rotterdam',
          status: pos.status || 'underway',
          latitude: lat,
          longitude: lon,
          speed: speed,
          lastUpdate: pos.timestamp ? new Date(pos.timestamp) : now
        });
      }
    }
    
    // Sort by ETA
    return arrivals.sort((a, b) => a.eta.getTime() - b.eta.getTime());
  }
  
  async getRecentDepartures(): Promise<VesselDeparture[]> {
    const vessels = await storage.getVessels();
    const positions = await storage.getLatestVesselPositions();
    
    const positionMap = new Map<string, any>();
    for (const pos of positions) {
      positionMap.set(pos.vesselId, pos);
    }
    
    const departures: VesselDeparture[] = [];
    const now = new Date();
    
    for (const vessel of vessels) {
      const pos = positionMap.get(vessel.id);
      if (!pos) continue;
      
      const lat = parseFloat(pos.latitude);
      const lon = parseFloat(pos.longitude);
      const speed = parseFloat(pos.speed || '0');
      
      // Vessel is in Rotterdam area, underway, and destination is NOT Rotterdam
      const isDeparting = this.isInRotterdamArea(lat, lon) && 
                         pos.status === 'underway' && 
                         speed > 2 &&
                         !pos.destination?.toLowerCase().includes('rotterdam');
      
      if (isDeparting) {
        departures.push({
          vesselId: vessel.id,
          vesselName: vessel.name,
          vesselType: vessel.vesselType || 'unknown',
          mmsi: vessel.mmsi,
          imo: vessel.imo || '',
          flag: vessel.flag || '',
          deadweight: vessel.deadweight || 0,
          departureTime: pos.timestamp ? new Date(pos.timestamp) : now,
          destination: pos.destination || 'Unknown',
          status: pos.status || 'underway',
          latitude: lat,
          longitude: lon,
          speed: speed,
          lastUpdate: pos.timestamp ? new Date(pos.timestamp) : now
        });
      }
    }
    
    // Sort by departure time (most recent first)
    return departures.sort((a, b) => b.departureTime.getTime() - a.departureTime.getTime());
  }
  
  async getVesselsAtPort(): Promise<{ atBerth: VesselArrival[], atAnchor: VesselArrival[] }> {
    const vessels = await storage.getVessels();
    const positions = await storage.getLatestVesselPositions();
    
    const positionMap = new Map<string, any>();
    for (const pos of positions) {
      positionMap.set(pos.vesselId, pos);
    }
    
    const atBerth: VesselArrival[] = [];
    const atAnchor: VesselArrival[] = [];
    const now = new Date();
    
    for (const vessel of vessels) {
      const pos = positionMap.get(vessel.id);
      if (!pos) continue;
      
      const lat = parseFloat(pos.latitude);
      const lon = parseFloat(pos.longitude);
      const speed = parseFloat(pos.speed || '0');
      
      // Only consider vessels in Rotterdam area
      if (!this.isInRotterdamArea(lat, lon)) continue;
      
      const vesselData: VesselArrival = {
        vesselId: vessel.id,
        vesselName: vessel.name,
        vesselType: vessel.vesselType || 'unknown',
        mmsi: vessel.mmsi,
        imo: vessel.imo || '',
        flag: vessel.flag || '',
        deadweight: vessel.deadweight || 0,
        eta: now,
        destination: pos.destination || 'Rotterdam',
        status: pos.status || 'unknown',
        latitude: lat,
        longitude: lon,
        speed: speed,
        lastUpdate: pos.timestamp ? new Date(pos.timestamp) : now
      };
      
      if (pos.status === 'moored' || (speed < 0.5 && pos.status !== 'anchored')) {
        atBerth.push(vesselData);
      } else if (pos.status === 'anchored' || speed < 1) {
        atAnchor.push(vesselData);
      }
    }
    
    return { atBerth, atAnchor };
  }
  
  async getPortActivitySummary(): Promise<PortActivitySummary> {
    const [arrivals, departures, vesselsAtPort] = await Promise.all([
      this.getExpectedArrivals(),
      this.getRecentDepartures(),
      this.getVesselsAtPort()
    ]);
    
    const vessels = await storage.getVessels();
    const positions = await storage.getLatestVesselPositions();
    
    let underwayCount = 0;
    for (const pos of positions) {
      const lat = parseFloat(pos.latitude);
      const lon = parseFloat(pos.longitude);
      const speed = parseFloat(pos.speed || '0');
      
      if (this.isInRotterdamArea(lat, lon) && speed > 2) {
        underwayCount++;
      }
    }
    
    const totalAtPort = vesselsAtPort.atBerth.length + vesselsAtPort.atAnchor.length + underwayCount;
    
    // Determine congestion level
    let congestionLevel: 'low' | 'moderate' | 'high' | 'critical' = 'low';
    if (vesselsAtPort.atAnchor.length > 10) {
      congestionLevel = 'critical';
    } else if (vesselsAtPort.atAnchor.length > 5) {
      congestionLevel = 'high';
    } else if (vesselsAtPort.atAnchor.length > 2) {
      congestionLevel = 'moderate';
    }
    
    // Calculate average wait time based on latest CSV data
    const latestData = this.getLatestData();
    const averageWaitTime = latestData?.portWaitDays || 1.5;
    
    return {
      expectedArrivals: arrivals.length,
      recentDepartures: departures.length,
      atBerth: vesselsAtPort.atBerth.length,
      atAnchor: vesselsAtPort.atAnchor.length,
      underway: underwayCount,
      congestionLevel,
      averageWaitTime,
      lastUpdated: new Date()
    };
  }
}

export const rotterdamDataService = new RotterdamDataService();
