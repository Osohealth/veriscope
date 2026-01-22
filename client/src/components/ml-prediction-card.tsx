import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { TrendingUp, TrendingDown, Loader2, Brain, Ship, Clock, Activity } from "lucide-react";
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";

interface MLPredictionCardProps {
  commodityType: string;
  commodityLabel: string;
  currentPrice?: number;
}

interface MLPrediction {
  id: string;
  commodityType: string;
  predictionDate: string;
  targetDate: string;
  predictedPrice: number;
  priceChange: number;
  priceChangePercent: number;
  confidence: number;
  vesselArrivals: number;
  avgWaitTimeHours: number;
  bulkCarrierCount: number;
  oilCarrierCount: number;
  lngCarrierCount: number;
  portCongestionIndex: number;
  features: any;
}

export function MLPredictionCard({ commodityType, commodityLabel, currentPrice = 80 }: MLPredictionCardProps) {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  // Fetch latest ML prediction
  const { data: prediction, isLoading: isLoadingPrediction } = useQuery<MLPrediction>({
    queryKey: [`/api/ml-predictions/latest/${commodityType}`],
    retry: false,
  });

  // Generate new prediction mutation
  const generateMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', `/api/ml-predictions/generate`, {
        commodityType,
        currentPrice
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/ml-predictions/latest/${commodityType}`] });
      toast({
        title: "Prediction Generated",
        description: `ML model analyzed vessel movements and generated new price prediction for ${commodityLabel}`,
      });
    },
    onError: () => {
      toast({
        title: "Prediction Failed",
        description: "Could not generate prediction. Please try again.",
        variant: "destructive",
      });
    }
  });

  const priceDirection = prediction?.priceChange ? (prediction.priceChange > 0 ? 'up' : 'down') : 'neutral';
  const priceColor = priceDirection === 'up' ? 'text-green-400' : priceDirection === 'down' ? 'text-red-400' : 'text-yellow-400';
  const bgColor = priceDirection === 'up' ? 'bg-green-500/10 border-green-500/30' : priceDirection === 'down' ? 'bg-red-500/10 border-red-500/30' : 'bg-yellow-500/10 border-yellow-500/30';

  return (
    <Card className={`bg-slate-900/50 border ${bgColor}`} data-testid="card-ml-prediction">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <Brain className="h-5 w-5 text-purple-400" />
            ML Price Prediction - Next Day
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() => generateMutation.mutate()}
            disabled={generateMutation.isPending}
            className="text-xs bg-purple-500/20 border-purple-500/30 hover:bg-purple-500/30"
            data-testid="button-generate-prediction"
          >
            {generateMutation.isPending ? (
              <>
                <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                Generating...
              </>
            ) : (
              <>
                <Brain className="h-3 w-3 mr-1" />
                Generate New
              </>
            )}
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {isLoadingPrediction ? (
          <div className="flex items-center justify-center py-8 text-slate-400">
            <Loader2 className="h-6 w-6 animate-spin mr-2" />
            Loading ML prediction...
          </div>
        ) : !prediction ? (
          <div className="text-center py-8">
            <p className="text-slate-400 mb-4">No prediction available yet</p>
            <Button
              onClick={() => generateMutation.mutate()}
              disabled={generateMutation.isPending}
              className="bg-purple-600 hover:bg-purple-700"
              data-testid="button-first-prediction"
            >
              <Brain className="h-4 w-4 mr-2" />
              Generate First Prediction
            </Button>
          </div>
        ) : (
          <div className="space-y-4">
            {/* Price Prediction */}
            <div className="flex items-center justify-between p-4 bg-slate-800/50 rounded-lg">
              <div>
                <div className="text-sm text-slate-400">Predicted Price (Tomorrow)</div>
                <div className={`text-3xl font-bold ${priceColor}`} data-testid="text-predicted-price">
                  ${parseFloat(prediction.predictedPrice).toFixed(2)}
                </div>
              </div>
              <div className="text-right">
                <div className="text-sm text-slate-400">Price Change</div>
                <div className={`text-xl font-semibold flex items-center ${priceColor}`} data-testid="text-price-change">
                  {priceDirection === 'up' ? <TrendingUp className="h-5 w-5 mr-1" /> : <TrendingDown className="h-5 w-5 mr-1" />}
                  {priceDirection === 'up' ? '+' : ''}{parseFloat(prediction.priceChangePercent).toFixed(2)}%
                </div>
              </div>
            </div>

            {/* Confidence */}
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-400">Model Confidence</span>
              <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30" data-testid="badge-confidence">
                {(parseFloat(prediction.confidence) * 100).toFixed(0)}%
              </Badge>
            </div>

            {/* ML Features */}
            <div className="grid grid-cols-2 gap-3">
              <div className="p-3 bg-slate-800/30 rounded-lg">
                <div className="flex items-center gap-2 mb-1">
                  <Ship className="h-4 w-4 text-cyan-400" />
                  <span className="text-xs text-slate-400">Vessel Arrivals</span>
                </div>
                <div className="text-lg font-semibold text-white" data-testid="text-vessel-arrivals">{prediction.vesselArrivals}</div>
              </div>

              <div className="p-3 bg-slate-800/30 rounded-lg">
                <div className="flex items-center gap-2 mb-1">
                  <Clock className="h-4 w-4 text-orange-400" />
                  <span className="text-xs text-slate-400">Avg Wait Time</span>
                </div>
                <div className="text-lg font-semibold text-white" data-testid="text-wait-time">{parseFloat(prediction.avgWaitTimeHours).toFixed(1)}h</div>
              </div>

              <div className="p-3 bg-slate-800/30 rounded-lg">
                <div className="flex items-center gap-2 mb-1">
                  <Activity className="h-4 w-4 text-green-400" />
                  <span className="text-xs text-slate-400">Port Congestion</span>
                </div>
                <div className="text-lg font-semibold text-white" data-testid="text-congestion">{parseFloat(prediction.portCongestionIndex).toFixed(0)}/100</div>
              </div>

              <div className="p-3 bg-slate-800/30 rounded-lg">
                <div className="flex items-center gap-2 mb-1">
                  <Ship className="h-4 w-4 text-purple-400" />
                  <span className="text-xs text-slate-400">Carrier Mix</span>
                </div>
                <div className="text-sm font-semibold text-white" data-testid="text-carrier-mix">
                  {prediction.oilCarrierCount}O/{prediction.lngCarrierCount}L/{prediction.bulkCarrierCount}B
                </div>
              </div>
            </div>

            {/* Prediction Info */}
            <div className="text-xs text-slate-500 text-center pt-2 border-t border-slate-800">
              Prediction generated on {new Date(prediction.predictionDate).toLocaleDateString()} 
              {' • '}Target: {new Date(prediction.targetDate).toLocaleDateString()}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
