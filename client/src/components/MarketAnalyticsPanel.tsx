import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { BarChart3, TrendingUp, TrendingDown, Activity, DollarSign, AlertTriangle, Clock } from "lucide-react";
import { useDelayAdjustedPredictions } from "@/hooks/use-delay-adjusted-predictions";
import { useRotterdamData } from "@/hooks/use-rotterdam-data";

interface MarketAnalytics {
  id: string;
  commodityId: string;
  region: string;
  supplyData: any;
  demandData: any;
  inventoryData: any;
  balanceData: any;
}

interface MarketAnalyticsPanelProps {
  dashboardType: string;
  region?: string;
  selectedPort?: string;
  selectedMonth?: string;
}

export default function MarketAnalyticsPanel({ dashboardType, region = "global", selectedPort, selectedMonth }: MarketAnalyticsPanelProps) {
  // Fetch 24h delay-adjusted predictions for crude oil
  const { data: predictionData } = useDelayAdjustedPredictions(
    selectedPort === 'rotterdam' ? 'rotterdam' : undefined,
    dashboardType === 'crude-oil' ? 'BRENT' : undefined
  );
  const { data: analytics, isLoading } = useQuery<MarketAnalytics[]>({
    queryKey: ['/api/market-analytics', dashboardType, region],
    queryFn: async () => {
      const response = await fetch(`/api/market-analytics?type=${dashboardType}&region=${region}`);
      if (!response.ok) throw new Error('Failed to fetch market analytics');
      return response.json();
    },
    refetchInterval: 300000, // Refetch every 5 minutes
  });
  
  // Fetch Rotterdam data when Rotterdam is selected (with month filtering)
  const { data: rotterdamData } = useRotterdamData(selectedMonth, selectedPort === 'rotterdam');

  const getMarketData = () => {
    switch (dashboardType) {
      case 'crude-oil':
        // Use Rotterdam data if available and Rotterdam is selected
        if (selectedPort === 'rotterdam' && rotterdamData?.stats) {
          const stats = rotterdamData.stats;
          const avgReceipts = stats.totalReceipts / 1000000; // Convert to millions
          const avgExports = stats.totalExports / 1000000;
          const balance = avgReceipts - avgExports;
          const storageUtil = stats.avgStorageUtilization;
          const storageBbl = avgReceipts * 30; // Rough estimate for capacity
          const capacity = storageBbl / (storageUtil / 100);
          
          return {
            supplyDemand: { 
              supply: avgReceipts,
              demand: avgExports, 
              balance: balance
            },
            inventory: { 
              current: storageBbl, 
              capacity: capacity, 
              utilization: storageUtil 
            },
            indicators: [
              { name: 'Avg Congestion', value: stats.avgCongestionIndex.toFixed(1), trend: stats.avgCongestionIndex > 25 ? 'up' : 'stable', description: 'Congestion index' },
              { name: 'Storage Fill', value: `${storageUtil.toFixed(1)}%`, trend: 'stable', description: 'Rotterdam capacity' },
              { name: 'Total Arrivals', value: stats.totalArrivals.toString(), trend: 'stable', description: 'Vessel count' },
            ]
          };
        }
        
        return {
          supplyDemand: { supply: 102.4, demand: 101.8, balance: 0.6 },
          inventory: { current: 68.5, capacity: 85.2, utilization: 80.4 },
          indicators: [
            { name: 'Backwardation', value: '$2.45/bbl', trend: 'up', description: '1M-12M spread' },
            { name: 'Storage Fill', value: '80.4%', trend: 'stable', description: 'Global capacity' },
            { name: 'Refinery Runs', value: '89.2%', trend: 'up', description: 'Utilization rate' },
          ]
        };
      case 'refined-products':
        return {
          supplyDemand: { supply: 45.8, demand: 46.2, balance: -0.4 },
          inventory: { current: 142.3, capacity: 180.5, utilization: 78.8 },
          indicators: [
            { name: 'Crack Spreads', value: '$18.45/bbl', trend: 'up', description: 'Gasoline vs Crude' },
            { name: 'Product Stocks', value: '78.8%', trend: 'down', description: 'Days of supply' },
            { name: 'Demand Growth', value: '3.2%', trend: 'up', description: 'YoY change' },
          ]
        };
      case 'lng':
        return {
          supplyDemand: { supply: 395.2, demand: 398.1, balance: -2.9 },
          inventory: { current: 45.8, capacity: 62.1, utilization: 73.7 },
          indicators: [
            { name: 'JKM Premium', value: '$1.23/MMBtu', trend: 'up', description: 'vs TTF' },
            { name: 'Storage Fill', value: '73.7%', trend: 'stable', description: 'Import terminals' },
            { name: 'Spot Volumes', value: '28.4%', trend: 'up', description: 'vs Contract' },
          ]
        };
      default:
        return {
          supplyDemand: { supply: 102.4, demand: 101.8, balance: 0.6 },
          inventory: { current: 68.5, capacity: 85.2, utilization: 80.4 },
          indicators: [
            { name: 'Price Momentum', value: '64.2', trend: 'up', description: 'RSI indicator' },
            { name: 'Volume Index', value: '1.34', trend: 'stable', description: 'vs 30-day avg' },
          ]
        };
    }
  };

  const marketData = getMarketData();

  const getTrendIcon = (trend: string) => {
    switch (trend) {
      case 'up': return <TrendingUp className="w-4 h-4 text-green-500" />;
      case 'down': return <TrendingDown className="w-4 h-4 text-red-500" />;
      default: return <Activity className="w-4 h-4 text-muted-foreground" />;
    }
  };

  const getTrendColor = (trend: string) => {
    switch (trend) {
      case 'up': return 'text-green-500';
      case 'down': return 'text-red-500';
      default: return 'text-muted-foreground';
    }
  };

  const getDashboardTitle = () => {
    switch (dashboardType) {
      case 'crude-oil': return 'Crude Oil Market Analytics';
      case 'refined-products': return 'Products Market Analytics';
      case 'lng': return 'LNG Market Analytics';
      case 'maritime': return 'Shipping Market Analytics';
      case 'market-analytics': return 'Global Market Analytics';
      default: return 'Market Analytics';
    }
  };

  const getUnit = () => {
    switch (dashboardType) {
      case 'crude-oil': return 'M bbl/d';
      case 'refined-products': return 'M bbl/d';
      case 'lng': return 'bcm/y';
      default: return 'M MT';
    }
  };

  if (isLoading) {
    return (
      <div className="space-y-4">
        {[1, 2, 3].map((i) => (
          <Card key={i}>
            <CardHeader>
              <div className="h-5 bg-muted rounded w-32 animate-pulse" />
            </CardHeader>
            <CardContent>
              <div className="h-20 bg-muted rounded animate-pulse" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* 24-Hour Price Prediction */}
      {predictionData && predictionData.basePrediction && (
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <Clock className="w-4 h-4" />
                24-Hour Price Forecast
              </CardTitle>
              {predictionData.delayAdjusted && (
                <Badge variant="outline" className="text-xs bg-amber-500/10 text-amber-500 border-amber-500/30">
                  <AlertTriangle className="w-3 h-3 mr-1" />
                  Delay Adjusted
                </Badge>
              )}
            </div>
            <Badge variant="outline" className="w-fit text-xs">
              Based on vessel delays & cargo volumes
            </Badge>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {/* Current vs Predicted Price */}
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <div className="text-xs text-muted-foreground">Current Price</div>
                  <div className="text-2xl font-bold text-foreground" data-testid="current-price">
                    ${parseFloat(predictionData.basePrediction.currentPrice).toFixed(2)}
                  </div>
                  <div className="text-xs text-muted-foreground">$/bbl</div>
                </div>
                <div className="space-y-2">
                  <div className="text-xs text-muted-foreground">24h Forecast</div>
                  <div className="text-2xl font-bold text-green-500" data-testid="predicted-price">
                    ${parseFloat(
                      predictionData.adjustedPrediction?.predictedPrice || 
                      predictionData.basePrediction.predictedPrice
                    ).toFixed(2)}
                  </div>
                  <div className="text-xs text-muted-foreground">$/bbl</div>
                </div>
              </div>
              
              {/* Price Change & Confidence */}
              <div className="pt-3 border-t border-border space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-xs text-muted-foreground">Expected Change</span>
                  <span className="text-sm font-mono font-medium text-green-500">
                    +{(
                      parseFloat(predictionData.adjustedPrediction?.predictedPrice || predictionData.basePrediction.predictedPrice) -
                      parseFloat(predictionData.basePrediction.currentPrice)
                    ).toFixed(2)} 
                    ({((
                      (parseFloat(predictionData.adjustedPrediction?.predictedPrice || predictionData.basePrediction.predictedPrice) -
                      parseFloat(predictionData.basePrediction.currentPrice)) /
                      parseFloat(predictionData.basePrediction.currentPrice)
                    ) * 100).toFixed(1)}%)
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-xs text-muted-foreground">Confidence</span>
                  <div className="flex items-center gap-2">
                    <Progress 
                      value={parseFloat(predictionData.basePrediction.confidence) * 100} 
                      className="h-2 w-20" 
                    />
                    <span className="text-sm font-mono font-medium">
                      {(parseFloat(predictionData.basePrediction.confidence) * 100).toFixed(0)}%
                    </span>
                  </div>
                </div>
              </div>
              
              {/* Delay Impact Details */}
              {predictionData.delayAdjusted && predictionData.delayImpact && (
                <div className="pt-3 border-t border-border">
                  <div className="text-xs font-medium text-amber-500 mb-2">Delay Impact Factors:</div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Delayed Vessels:</span>
                      <span className="font-medium">{predictionData.delayImpact.vesselCount}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Avg Delay:</span>
                      <span className="font-medium text-amber-500">
                        {parseFloat(predictionData.delayImpact.averageDelayHours).toFixed(1)}h
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Cargo Volume:</span>
                      <span className="font-medium">
                        {(predictionData.delayImpact.totalDelayedVolume / 1000).toFixed(1)}K tons
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Price Impact:</span>
                      <span className="font-medium text-green-500">
                        +${parseFloat(predictionData.delayImpact.priceImpact).toFixed(2)}/bbl
                      </span>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}
      
      {/* Supply & Demand Balance */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            <BarChart3 className="w-4 h-4" />
            Supply & Demand Balance
          </CardTitle>
          <Badge variant="outline" className="w-fit text-xs">
            {region.charAt(0).toUpperCase() + region.slice(1)} - {getUnit()}
          </Badge>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-3 gap-4">
            <div className="text-center space-y-2">
              <div className="text-xs text-muted-foreground">Supply</div>
              <div className="text-lg font-bold text-blue-500" data-testid="supply-value">
                {marketData.supplyDemand.supply.toFixed(1)}
              </div>
              <div className="text-xs text-muted-foreground">{getUnit()}</div>
            </div>
            <div className="text-center space-y-2">
              <div className="text-xs text-muted-foreground">Demand</div>
              <div className="text-lg font-bold text-orange-500" data-testid="demand-value">
                {marketData.supplyDemand.demand.toFixed(1)}
              </div>
              <div className="text-xs text-muted-foreground">{getUnit()}</div>
            </div>
            <div className="text-center space-y-2">
              <div className="text-xs text-muted-foreground">Balance</div>
              <div className={`text-lg font-bold ${
                marketData.supplyDemand.balance > 0 ? 'text-green-500' : 'text-red-500'
              }`} data-testid="balance-value">
                {marketData.supplyDemand.balance > 0 ? '+' : ''}{marketData.supplyDemand.balance.toFixed(1)}
              </div>
              <div className="text-xs text-muted-foreground">{getUnit()}</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Inventory Levels */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium">Storage & Inventory</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <span className="text-sm text-muted-foreground">Current Levels</span>
              <span className="text-sm font-mono font-medium">
                {marketData.inventory.current.toFixed(1)} / {marketData.inventory.capacity.toFixed(1)} M bbl
              </span>
            </div>
            <div className="space-y-2">
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Utilization</span>
                <span className="font-mono font-medium">{marketData.inventory.utilization.toFixed(1)}%</span>
              </div>
              <Progress value={marketData.inventory.utilization} className="h-3" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Market Indicators */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium" data-testid="text-market-indicators">
            Market Indicators
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {marketData.indicators.map((indicator, index) => (
            <div 
              key={indicator.name}
              className="flex justify-between items-center py-2 border-b border-border last:border-0"
              data-testid={`market-indicator-${index}`}
            >
              <div className="space-y-1">
                <div className="text-sm font-medium">{indicator.name}</div>
                <div className="text-xs text-muted-foreground">
                  {indicator.description}
                </div>
              </div>
              <div className="text-right space-y-1">
                <div className="text-sm font-mono font-medium">
                  {indicator.value}
                </div>
                <div className={`flex items-center gap-1 justify-end ${getTrendColor(indicator.trend)}`}>
                  {getTrendIcon(indicator.trend)}
                  <span className="text-xs">{indicator.trend}</span>
                </div>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}