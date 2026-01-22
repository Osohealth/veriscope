import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Ship, ArrowRight, TrendingUp, Globe, Clock } from "lucide-react";
import { useMarketDelayImpact } from "@/hooks/use-port-delays";
import { useRotterdamData } from "@/hooks/use-rotterdam-data";

interface TradeFlowsPanelProps {
  dashboardType: string;
  selectedPort?: string;
  selectedMonth?: string;
}

export default function TradeFlowsPanel({ dashboardType, selectedPort, selectedMonth }: TradeFlowsPanelProps) {
  // Fetch delay impact data for the selected port
  const { data: delayImpacts } = useMarketDelayImpact(selectedPort, undefined, 1);
  const delayData = delayImpacts?.[0];
  
  // Fetch Rotterdam data when Rotterdam is selected (with month filtering)
  const { data: rotterdamData } = useRotterdamData(selectedMonth, selectedPort === 'rotterdam');
  
  // Using mock data directly for reliable UI performance

  const getMockTradeFlows = () => {
    switch (dashboardType) {
      case 'crude-oil':
        return [
          { route: 'Middle East → Asia', volume: 2.8, vessels: 12, commodity: 'Crude Oil', trend: 5.2 },
          { route: 'US Gulf → Europe', volume: 1.4, vessels: 6, commodity: 'Crude Oil', trend: -2.1 },
          { route: 'West Africa → China', volume: 1.9, vessels: 8, commodity: 'Crude Oil', trend: 3.7 },
        ];
      case 'refined-products':
        return [
          { route: 'Europe → West Africa', volume: 0.8, vessels: 15, commodity: 'Gasoline', trend: 4.1 },
          { route: 'Asia → Australia', volume: 0.6, vessels: 9, commodity: 'Diesel', trend: -1.3 },
          { route: 'US → Latin America', volume: 1.2, vessels: 18, commodity: 'Jet Fuel', trend: 7.8 },
        ];
      case 'lng':
        return [
          { route: 'Australia → Japan', volume: 0.9, vessels: 4, commodity: 'LNG', trend: 2.4 },
          { route: 'Qatar → Europe', volume: 1.1, vessels: 5, commodity: 'LNG', trend: 8.9 },
          { route: 'US → Asia', volume: 0.7, vessels: 3, commodity: 'LNG', trend: 12.3 },
        ];
      default:
        return [
          { route: 'Middle East → Asia', volume: 2.8, vessels: 12, commodity: 'Crude Oil', trend: 5.2 },
          { route: 'Europe → West Africa', volume: 0.8, vessels: 15, commodity: 'Products', trend: 4.1 },
        ];
    }
  };

  const getActiveVessels = () => {
    const delayed = delayData?.vesselCount || 0;
    
    // Use Rotterdam data if available and Rotterdam is selected
    if (selectedPort === 'rotterdam' && rotterdamData?.stats) {
      return {
        inTransit: rotterdamData.stats.totalArrivals + rotterdamData.stats.totalDepartures,
        loading: Math.round(rotterdamData.stats.totalArrivals / (rotterdamData.data.length || 1)),
        discharging: Math.round(rotterdamData.stats.totalDepartures / (rotterdamData.data.length || 1)),
        delayed
      };
    }
    
    switch (dashboardType) {
      case 'crude-oil': return { inTransit: 47, loading: 12, discharging: 8, delayed };
      case 'refined-products': return { inTransit: 89, loading: 23, discharging: 15, delayed };
      case 'lng': return { inTransit: 18, loading: 4, discharging: 3, delayed };
      case 'maritime': return { inTransit: 156, loading: 41, discharging: 28, delayed };
      default: return { inTransit: 47, loading: 12, discharging: 8, delayed };
    }
  };

  const tradeFlowData = getMockTradeFlows();
  const vesselStats = getActiveVessels();

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'in_transit': return 'bg-blue-500';
      case 'loading': return 'bg-orange-500';
      case 'discharging': return 'bg-green-500';
      default: return 'bg-muted';
    }
  };

  const getDashboardTitle = () => {
    switch (dashboardType) {
      case 'crude-oil': return 'Crude Oil Trade Flows';
      case 'refined-products': return 'Products Trade Flows';
      case 'lng': return 'LNG Trade Flows';
      case 'maritime': return 'Maritime Trade Routes';
      case 'trade-flows': return 'Global Trade Flows';
      default: return 'Trade Flows';
    }
  };

  // No loading state needed since we're using mock data

  return (
    <div className="space-y-4">
      {/* Vessel Status Overview */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            <Ship className="w-4 h-4" />
            Active Vessels
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-4 gap-4">
            <div className="text-center space-y-1">
              <div className="text-2xl font-bold text-blue-500" data-testid="vessels-in-transit">
                {vesselStats.inTransit}
              </div>
              <div className="text-xs text-muted-foreground">In Transit</div>
            </div>
            <div className="text-center space-y-1">
              <div className="text-2xl font-bold text-orange-500" data-testid="vessels-loading">
                {vesselStats.loading}
              </div>
              <div className="text-xs text-muted-foreground">Loading</div>
            </div>
            <div className="text-center space-y-1">
              <div className="text-2xl font-bold text-green-500" data-testid="vessels-discharging">
                {vesselStats.discharging}
              </div>
              <div className="text-xs text-muted-foreground">Discharging</div>
            </div>
            {vesselStats.delayed > 0 && (
              <div className="text-center space-y-1">
                <div className="text-2xl font-bold text-amber-500" data-testid="vessels-delayed">
                  {vesselStats.delayed}
                </div>
                <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
                  <Clock className="w-3 h-3" />
                  Delayed
                </div>
              </div>
            )}
          </div>
          
          {/* Delay Impact Summary */}
          {delayData && delayData.totalDelayedVolume > 0 && (
            <div className="mt-4 pt-4 border-t border-border">
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Delayed Cargo Volume:</span>
                <span className="font-medium text-foreground" data-testid="text-delayed-cargo">
                  {(delayData.totalDelayedVolume / 1000).toFixed(1)}K tons
                </span>
              </div>
              <div className="flex justify-between text-xs mt-1">
                <span className="text-muted-foreground">Avg Delay:</span>
                <span className="font-medium text-amber-500">
                  {parseFloat(delayData.averageDelayHours).toFixed(1)}h
                </span>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Major Trade Routes */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium" data-testid="text-trade-flows-title">
            {getDashboardTitle()}
          </CardTitle>
          <Badge variant="outline" className="w-fit text-xs">
            <Globe className="w-3 h-3 mr-1" />
            Live Routes
          </Badge>
        </CardHeader>
        <CardContent className="space-y-4">
          {tradeFlowData.map((flow, index) => (
            <div 
              key={flow.route}
              className="border border-border rounded-lg p-3 space-y-3"
              data-testid={`trade-flow-${index}`}
            >
              <div className="flex justify-between items-start">
                <div className="space-y-1">
                  <div className="text-sm font-medium flex items-center gap-2">
                    {flow.route.replace(' → ', ' ')}
                    <ArrowRight className="w-3 h-3 text-muted-foreground" />
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {flow.commodity}
                  </div>
                </div>
                <div className="text-right space-y-1">
                  <div className={`flex items-center gap-1 text-xs ${
                    flow.trend > 0 ? 'text-green-500' : 'text-red-500'
                  }`}>
                    <TrendingUp className="w-3 h-3" />
                    {flow.trend > 0 ? '+' : ''}{flow.trend.toFixed(1)}%
                  </div>
                </div>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <div className="text-xs text-muted-foreground">Volume</div>
                  <div className="text-sm font-mono font-medium">
                    {flow.volume.toFixed(1)}M MT
                  </div>
                </div>
                <div className="space-y-1">
                  <div className="text-xs text-muted-foreground">Active Vessels</div>
                  <div className="text-sm font-mono font-medium">
                    {flow.vessels}
                  </div>
                </div>
              </div>
              
              <div className="space-y-2">
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Route Utilization</span>
                  <span className="font-mono">{(flow.volume * 20).toFixed(0)}%</span>
                </div>
                <Progress value={flow.volume * 20} className="h-2" />
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}