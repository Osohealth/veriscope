import { useQuery } from "@tanstack/react-query";

export interface PortDelayEvent {
  id: string;
  portId: string;
  vesselId: string;
  expectedArrival: string;
  actualArrival: string | null;
  delayHours: string;
  delayReason: string | null;
  cargoVolume: number | null;
  cargoType: string | null;
  queuePosition: number | null;
  status: string | null;
  metadata: any;
  createdAt: string | null;
}

export interface MarketDelayImpact {
  id: string;
  portId: string;
  commodityId: string;
  marketId: string | null;
  timeframe: string;
  totalDelayedVolume: number;
  totalDelayedValue: string | null;
  averageDelayHours: string;
  vesselCount: number;
  supplyImpact: string | null;
  priceImpact: string | null;
  confidence: string;
  metadata: any;
  validUntil: string;
  createdAt: string | null;
}

export function usePortDelays(portId: string, limit: number = 50) {
  return useQuery<PortDelayEvent[]>({
    queryKey: ['/api/ports', portId, 'delays', limit],
    enabled: !!portId,
    refetchInterval: 60000, // Refetch every minute
  });
}

export function useMarketDelayImpact(portId?: string, commodityId?: string, limit: number = 1) {
  return useQuery<MarketDelayImpact[]>({
    queryKey: ['/api/market/delays/impact', { portId, commodityId, limit }],
    enabled: !!portId || !!commodityId,
    refetchInterval: 120000, // Refetch every 2 minutes
  });
}
