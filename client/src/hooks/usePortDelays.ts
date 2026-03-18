import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders, getAuthToken } from "@/lib/queryClient";

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
  const token = getAuthToken();
  const isAuthed = !!token;

  return useQuery<PortDelayEvent[]>({
    queryKey: ['/api/ports', portId, 'delays', limit],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (limit) params.set('limit', String(limit));
      const url = params.toString()
        ? `/api/ports/${portId}/delays?${params.toString()}`
        : `/api/ports/${portId}/delays`;
      const response = await fetch(url, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch port delays');
      return response.json();
    },
    enabled: isAuthed && !!portId,
    refetchInterval: 60000, // Refetch every minute
  });
}

export function useMarketDelayImpact(portId?: string, commodityId?: string, limit: number = 1) {
  const token = getAuthToken();
  const isAuthed = !!token;

  return useQuery<MarketDelayImpact[]>({
    queryKey: ['/api/market/delays/impact', portId ?? null, commodityId ?? null, limit],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (portId) params.set('portId', portId);
      if (commodityId) params.set('commodityId', commodityId);
      if (limit) params.set('limit', String(limit));
      const url = params.toString()
        ? `/api/market/delays/impact?${params.toString()}`
        : '/api/market/delays/impact';
      const response = await fetch(url, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch market delay impact');
      return response.json();
    },
    enabled: isAuthed && (!!portId || !!commodityId),
    refetchInterval: 120000, // Refetch every 2 minutes
  });
}
