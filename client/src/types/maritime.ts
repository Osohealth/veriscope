// Maritime domain types

export interface Vessel {
  id: string;
  mmsi: string;
  imo?: string;
  name: string;
  callsign?: string;
  vesselType: string;
  vesselClass: 'VLCC' | 'Suezmax' | 'Aframax' | 'Other';
  dwt?: number;
  flag?: string;
  operator?: string;
  position?: AISPosition;
}

export interface AISPosition {
  id: string;
  mmsi: string;
  timestamp: string;
  latitude: string;
  longitude: string;
  speedOverGround?: number;
  courseOverGround?: number;
  heading?: number;
  navigationStatus: 'AT_ANCHOR' | 'UNDERWAY' | 'AT_BERTH' | 'IN_CHANNEL';
  draught?: number;
  destination?: string;
  eta?: string;
  provider: string;
}

export interface Port {
  id: string;
  name: string;
  code: string;
  country?: string;
  latitude: string;
  longitude: string;
  timezone?: string;
  isActive: boolean;
}

export interface PortStatistics {
  id: string;
  portId: string;
  date: string;
  arrivals: number;
  departures: number;
  queueLength: number;
  averageWaitHours: number;
  totalVessels: number;
  throughputMT: number;
  byClass: {
    VLCC: number;
    Suezmax: number;
    Aframax: number;
  };
}

export interface StorageSite {
  id: string;
  portId: string;
  name: string;
  siteType: 'tank_farm' | 'floating_storage';
  capacity?: number;
  latitude: string;
  longitude: string;
  isActive: boolean;
  fillData?: StorageFillData;
}

export interface StorageFillData {
  id: string;
  siteId: string;
  timestamp: string;
  fillIndex: number; // 0-1 scale
  confidence: number;
  source: 'SAR' | 'manual' | 'estimated';
  metadata?: any;
}

export interface Signal {
  id: string;
  timestamp: string;
  entityType: 'port' | 'vessel' | 'storage';
  entityId: string;
  signalType: string;
  severity: 1 | 2 | 3 | 4 | 5;
  title: string;
  description?: string;
  metadata?: any;
  isActive: boolean;
}

export interface Prediction {
  id: string;
  timestamp: string;
  target: 'Brent' | 'WTI' | 'TD3C';
  horizon: '1D' | '1W';
  predictionClass: 'UP' | 'DOWN' | 'FLAT';
  probability: number;
  confidence?: number;
  modelVersion?: string;
  features?: any;
}

export interface MaritimeAlert {
  id: string;
  type: 'CONGESTION' | 'STORAGE_FILL' | 'VESSEL_ANOMALY' | 'WEATHER';
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  title: string;
  message: string;
  timestamp: string;
  entityId: string;
  entityType: string;
  isRead: boolean;
}

export interface VesselFilter {
  vesselClass?: string[];
  navigationStatus?: string[];
  portArea?: string;
  dateRange?: {
    start: string;
    end: string;
  };
}

export interface MapBounds {
  north: number;
  south: number;
  east: number;
  west: number;
}

export interface MapLayer {
  id: string;
  name: string;
  type: 'vessels' | 'ports' | 'storage' | 'lanes';
  visible: boolean;
  opacity?: number;
}

export interface WebSocketMessage {
  type: 'ais_update' | 'new_signal' | 'port_stats_update';
  data: any;
  timestamp: string;
}
