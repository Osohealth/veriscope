import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders, getAuthToken } from "@/lib/queryClient";

export interface DelayAdjustedPrediction {
  delayAdjusted: boolean;
  basePrediction: {
    id: string;
    commodityId: string;
    marketId: string;
    timeframe: string;
    currentPrice: string;
    predictedPrice: string;
    confidence: string;
    direction: string;
    validUntil: string;
  } | null;
  delayImpact: {
    vesselCount: number;
    totalDelayedVolume: number;
    averageDelayHours: string;
    priceImpact: string;
  } | null;
  adjustedPrediction?: {
    predictedPrice: string;
    adjustmentReason: string;
  };
  message?: string;
}

export function useDelayAdjustedPredictions(portId?: string, commodityCode?: string) {
  const token = getAuthToken();
  const isAuthed = !!token;

  return useQuery<DelayAdjustedPrediction>({
    queryKey: ['/api/predictions/delay-adjusted', portId ?? null, commodityCode ?? null],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (portId) params.set('portId', portId);
      if (commodityCode) params.set('commodityCode', commodityCode);
      const url = params.toString()
        ? `/api/predictions/delay-adjusted?${params.toString()}`
        : '/api/predictions/delay-adjusted';
      const response = await fetch(url, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch delay-adjusted predictions');
      return response.json();
    },
    enabled: isAuthed && (!!portId || !!commodityCode),
    refetchInterval: 300000, // Refetch every 5 minutes
  });
}
