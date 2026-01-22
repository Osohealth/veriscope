import { useQuery } from "@tanstack/react-query";

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
  return useQuery<DelayAdjustedPrediction>({
    queryKey: ['/api/predictions/delay-adjusted', { portId, commodityCode }],
    enabled: !!portId || !!commodityCode,
    refetchInterval: 300000, // Refetch every 5 minutes
  });
}
