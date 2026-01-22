import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { cn } from "@/lib/utils";
import KpiCard from "./KpiCard";
import StorageWidget from "./StorageWidget";
import CommodityPriceChart from "./CommodityPriceChart";
import TradeFlowsPanel from "./TradeFlowsPanel";
import MarketAnalyticsPanel from "./MarketAnalyticsPanel";
import { usePortStats } from "@/hooks/use-port-stats";
import { useSignals } from "@/hooks/use-signals";
import { usePredictions } from "@/hooks/use-predictions";
import { useVessels } from "@/hooks/use-vessels";
import { useMarketDelayImpact } from "@/hooks/use-port-delays";
import { AlertTriangle, Clock, Ship, TrendingUp } from "lucide-react";

interface RightPanelProps {
  selectedPort: string;
  activeTab: string;
  onTabChange: (tab: string) => void;
  onPortChange: (port: string) => void;
  scope: string;
  selectedMonth?: string;
}

export default function RightPanel({ selectedPort, activeTab, onTabChange, onPortChange, scope, selectedMonth }: RightPanelProps) {
  const { data: portStats } = usePortStats(selectedPort);
  const { data: signals } = useSignals();
  const { data: predictions } = usePredictions();
  const { data: vessels } = useVessels();
  
  // Fetch Rotterdam delay impact data
  const { data: delayImpacts } = useMarketDelayImpact(
    selectedPort === 'rotterdam' ? selectedPort : undefined,
    undefined,
    1
  );
  const rotterdamDelay = delayImpacts?.[0];

  const getTabsForDashboard = (dashboardType: string) => {
    switch (dashboardType) {
      case 'crude-oil':
        return [
          { id: "stats", label: "Crude Oil Market" },
          { id: "flows", label: "Trade Flows" },
          { id: "analytics", label: "Market Analytics" },
        ];
      case 'refined-products':
        return [
          { id: "stats", label: "Products Market" },
          { id: "flows", label: "Trade Flows" },
          { id: "analytics", label: "Market Analytics" },
        ];
      case 'lng':
        return [
          { id: "stats", label: "LNG Market" },
          { id: "flows", label: "Trade Flows" },
          { id: "analytics", label: "Market Analytics" },
        ];
      case 'maritime':
        return [
          { id: "stats", label: "Port Stats" },
          { id: "flows", label: "Trade Flows" },
          { id: "analytics", label: "Market Analytics" },
          { id: "signals", label: "Signals" },
        ];
      case 'trade-flows':
        return [
          { id: "flows", label: "Global Flows" },
          { id: "analytics", label: "Flow Analytics" },
          { id: "stats", label: "Port Stats" },
        ];
      case 'market-analytics':
        return [
          { id: "analytics", label: "Market Data" },
          { id: "stats", label: "Prices" },
          { id: "flows", label: "Trade Flows" },
        ];
      case 'flightscope':
        return [
          { id: "stats", label: "Airport Stats" },
          { id: "signals", label: "Flight Signals" },
          { id: "predict", label: "Aviation Predict" },
        ];
      default: // tankscope
        return [
          { id: "stats", label: "Port Stats" },
          { id: "signals", label: "Signals" },
          { id: "predict", label: "Predict" },
        ];
    }
  };

  const tabs = getTabsForDashboard(scope);

  // Mock recent arrivals based on vessels data
  const recentArrivals = vessels?.slice(0, 3).map(vessel => ({
    name: vessel.name,
    type: vessel.vesselClass || 'Unknown',
    time: '18:34 UTC',
    status: vessel.position?.navigationStatus === 'AT_ANCHOR' ? 'Laden' : 'Ballast'
  })) || [];

  // Mock queue data
  const queueVessels = vessels?.filter(v => v.position?.navigationStatus === 'AT_ANCHOR').slice(0, 3).map((vessel, index) => ({
    name: vessel.name,
    type: vessel.vesselClass || 'Unknown',
    waitTime: `${(14 - index * 3).toFixed(1)}h`,
    position: index + 1
  })) || [];

  return (
    <div className="w-96 bg-card border-l border-border flex flex-col">
      {/* Panel Tabs */}
      <div className="border-b border-border">
        <nav className="flex">
          {tabs.map((tab) => (
            <Button
              key={tab.id}
              variant="ghost"
              className={cn(
                "flex-1 px-4 py-3 text-sm font-medium rounded-none border-b-2",
                activeTab === tab.id
                  ? "text-primary border-primary"
                  : "text-muted-foreground border-transparent hover:text-foreground"
              )}
              onClick={() => onTabChange(tab.id)}
              data-testid={`tab-${tab.id}`}
            >
              {tab.label}
            </Button>
          ))}
        </nav>
      </div>
      
      {/* Panel Content */}
      <div className="flex-1 overflow-auto scrollbar-thin">
        {/* Rotterdam Port Delay Alert Banner */}
        {rotterdamDelay && parseFloat(rotterdamDelay.averageDelayHours) > 6 && (
          <div className="m-4 p-4 bg-amber-500/10 border border-amber-500/30 rounded-lg space-y-3" data-testid="alert-rotterdam-delay">
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-amber-500" />
                <div>
                  <h4 className="text-sm font-semibold text-amber-500">Rotterdam Port Delay Alert</h4>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    Significant delays detected at Europe's largest port
                  </p>
                </div>
              </div>
            </div>
            
            <div className="grid grid-cols-4 gap-3">
              <div className="flex flex-col">
                <div className="flex items-center gap-1.5 mb-1">
                  <Clock className="w-3.5 h-3.5 text-amber-500" />
                  <span className="text-xs text-muted-foreground">Avg Delay</span>
                </div>
                <span className="text-lg font-bold text-foreground" data-testid="text-avg-delay">
                  {parseFloat(rotterdamDelay.averageDelayHours).toFixed(1)}h
                </span>
              </div>
              
              <div className="flex flex-col">
                <div className="flex items-center gap-1.5 mb-1">
                  <Ship className="w-3.5 h-3.5 text-amber-500" />
                  <span className="text-xs text-muted-foreground">Vessels</span>
                </div>
                <span className="text-lg font-bold text-foreground" data-testid="text-delayed-vessels">
                  {rotterdamDelay.vesselCount}
                </span>
              </div>
              
              <div className="flex flex-col">
                <div className="flex items-center gap-1.5 mb-1">
                  <TrendingUp className="w-3.5 h-3.5 text-amber-500" />
                  <span className="text-xs text-muted-foreground">Volume</span>
                </div>
                <span className="text-lg font-bold text-foreground" data-testid="text-delayed-volume">
                  {(rotterdamDelay.totalDelayedVolume / 1000).toFixed(1)}K
                </span>
                <span className="text-xs text-muted-foreground">tons</span>
              </div>
              
              <div className="flex flex-col">
                <div className="flex items-center gap-1.5 mb-1">
                  <TrendingUp className="w-3.5 h-3.5 text-amber-500" />
                  <span className="text-xs text-muted-foreground">Impact</span>
                </div>
                <span className="text-lg font-bold text-foreground" data-testid="text-price-impact">
                  ${parseFloat(rotterdamDelay.priceImpact || '0').toFixed(2)}
                </span>
                <span className="text-xs text-muted-foreground">/bbl</span>
              </div>
            </div>
          </div>
        )}
        
        {/* Specialized content for commodity and maritime dashboards */}
        {(activeTab === "stats" && ['crude-oil', 'refined-products', 'lng', 'market-analytics'].includes(scope)) && (
          <div className="p-4 space-y-4">
            <CommodityPriceChart dashboardType={scope} selectedPort={selectedPort} selectedMonth={selectedMonth} />
          </div>
        )}

        {activeTab === "flows" && (
          <div className="p-4">
            <TradeFlowsPanel dashboardType={scope} selectedPort={selectedPort} selectedMonth={selectedMonth} />
          </div>
        )}

        {activeTab === "analytics" && (
          <div className="p-4">
            <MarketAnalyticsPanel dashboardType={scope} selectedPort={selectedPort} selectedMonth={selectedMonth} />
          </div>
        )}

        {/* Traditional port/airport stats for legacy dashboards */}
        {(activeTab === "stats" && !['crude-oil', 'refined-products', 'lng', 'market-analytics'].includes(scope)) && (
          <>
            {/* Port Selection */}
            <div className="p-4 border-b border-border">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-semibold text-foreground" data-testid="text-port-overview">
                  {scope === 'flightscope' ? 'Airport Overview' : 'Port Overview'}
                </h3>
                <Select value={selectedPort} onValueChange={onPortChange} data-testid="select-port">
                  <SelectTrigger className="w-32">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {scope === 'flightscope' ? (
                      <>
                        <SelectItem value="dxb">Dubai (DXB)</SelectItem>
                        <SelectItem value="ams">Amsterdam (AMS)</SelectItem>
                      </>
                    ) : (
                      <>
                        <SelectItem value="fujairah">Fujairah</SelectItem>
                        <SelectItem value="rotterdam">Rotterdam</SelectItem>
                      </>
                    )}
                  </SelectContent>
                </Select>
              </div>
              
              {/* Key Metrics Cards */}
              <div className="grid grid-cols-2 gap-3">
                {scope === 'flightscope' ? (
                  <>
                    <KpiCard
                      value="247"
                      label="Flights"
                      change="+15 today"
                      changeType="positive"
                      testId="kpi-flights"
                    />
                    <KpiCard
                      value="8"
                      label="Delayed"
                      change="-2 vs avg"
                      changeType="positive"
                      testId="kpi-delayed"
                    />
                    <KpiCard
                      value="12.3m"
                      label="Avg Delay"
                      change="-3.2m"
                      changeType="positive"
                      testId="kpi-delay"
                    />
                    <KpiCard
                      value="45.2K"
                      label="Passengers"
                      change="+8%"
                      changeType="positive"
                      testId="kpi-passengers"
                    />
                  </>
                ) : (
                  <>
                    <KpiCard
                      value={portStats?.totalVessels?.toString() || "47"}
                      label="Vessels"
                      change="+3 today"
                      changeType="positive"
                      testId="kpi-vessels"
                    />
                    <KpiCard
                      value={portStats?.queueLength?.toString() || "12"}
                      label="In Queue"
                      change="+2 vs avg"
                      changeType="warning"
                      testId="kpi-queue"
                    />
                    <KpiCard
                      value={portStats?.averageWaitHours?.toFixed(1) + "h" || "18.5h"}
                      label="Avg Wait"
                      change="+2.3h"
                      changeType="negative"
                      testId="kpi-wait"
                    />
                    <KpiCard
                      value={portStats?.throughputMT?.toFixed(1) + "M" || "2.1M"}
                      label="MT/day"
                      change="+5%"
                      changeType="positive"
                      testId="kpi-throughput"
                    />
                  </>
                )}
              </div>
            </div>
            
            {/* Recent Arrivals */}
            <div className="p-4 border-b border-border">
              <h4 className="text-sm font-semibold text-foreground mb-3" data-testid="text-recent-arrivals">
                Recent Arrivals
              </h4>
              <div className="space-y-3">
                {scope === 'flightscope' ? (
                  // Mock flight arrivals
                  [
                    { flight: 'EK215', aircraft: 'A380', time: '14:25 UTC', status: 'On Time' },
                    { flight: 'KL891', aircraft: 'B777', time: '14:40 UTC', status: 'Delayed' },
                    { flight: 'BA107', aircraft: 'A350', time: '15:15 UTC', status: 'Boarding' }
                  ].map((arrival, index) => (
                    <div key={index} className="flex items-center space-x-3 p-2 bg-muted rounded-lg"
                         data-testid={`arrival-${index}`}>
                      <div className={cn(
                        "w-2 h-2 rounded-full",
                        arrival.status === 'On Time' ? "bg-emerald-400" : 
                        arrival.status === 'Delayed' ? "bg-destructive" : "bg-blue-400"
                      )}></div>
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium text-foreground truncate">
                          {arrival.flight}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          {arrival.aircraft} • {arrival.time}
                        </div>
                      </div>
                      <div className="text-xs text-muted-foreground">{arrival.status}</div>
                    </div>
                  ))
                ) : (
                  recentArrivals.map((arrival, index) => (
                    <div key={index} className="flex items-center space-x-3 p-2 bg-muted rounded-lg"
                         data-testid={`arrival-${index}`}>
                      <div className={cn(
                        "w-2 h-2 rounded-full",
                        arrival.status === 'Laden' ? "bg-emerald-400" : "bg-blue-400"
                      )}></div>
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium text-foreground truncate">
                          {arrival.name}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          {arrival.type} • {arrival.time}
                        </div>
                      </div>
                      <div className="text-xs text-muted-foreground">{arrival.status}</div>
                    </div>
                  ))
                )}
                {scope !== 'flightscope' && recentArrivals.length === 0 && (
                  <div className="text-sm text-muted-foreground text-center py-4">
                    No recent arrivals data available
                  </div>
                )}
              </div>
            </div>
            
            {/* Queue Status */}
            <div className="p-4 border-b border-border">
              <h4 className="text-sm font-semibold text-foreground mb-3" data-testid="text-anchorage-queue">
                {scope === 'flightscope' ? 'Departure Queue' : 'Anchorage Queue'}
              </h4>
              <div className="space-y-2">
                {/* Queue Chart Placeholder */}
                <div className="h-24 bg-muted rounded-lg flex items-center justify-center">
                  <span className="text-xs text-muted-foreground">
                    {scope === 'flightscope' ? 'Departure Delays (24h)' : 'Queue Length Trend (24h)'}
                  </span>
                </div>
                
                {/* Current Queue */}
                <div className="space-y-2">
                  {queueVessels.map((vessel, index) => (
                    <div key={index} className="flex items-center justify-between p-2 bg-muted rounded"
                         data-testid={`queue-vessel-${index}`}>
                      <div>
                        <div className="text-sm font-medium text-foreground">{vessel.name}</div>
                        <div className="text-xs text-muted-foreground">
                          {vessel.type} • Waiting {vessel.waitTime}
                        </div>
                      </div>
                      <div className="text-xs text-amber-400">#{vessel.position}</div>
                    </div>
                  ))}
                  {queueVessels.length === 0 && (
                    <div className="text-sm text-muted-foreground text-center py-4">
                      No vessels in queue
                    </div>
                  )}
                </div>
              </div>
            </div>
            
            {/* Storage Tank Overview */}
            <div className="p-4">
              <h4 className="text-sm font-semibold text-foreground mb-3" data-testid="text-storage-proxy">
                {scope === 'flightscope' ? 'Fuel Capacity' : 'Storage Fill Proxy'}
              </h4>
              {scope === 'flightscope' ? (
                <div className="space-y-3">
                  <div className="flex justify-between items-center p-2 bg-muted rounded">
                    <span className="text-sm text-foreground">Jet A-1</span>
                    <span className="text-sm text-emerald-400">87%</span>
                  </div>
                  <div className="flex justify-between items-center p-2 bg-muted rounded">
                    <span className="text-sm text-foreground">Avgas</span>
                    <span className="text-sm text-amber-400">52%</span>
                  </div>
                  <div className="flex justify-between items-center p-2 bg-muted rounded">
                    <span className="text-sm text-foreground">Total Capacity</span>
                    <span className="text-sm text-foreground">124.5k gallons</span>
                  </div>
                </div>
              ) : (
                <StorageWidget portId={selectedPort} />
              )}
            </div>
          </>
        )}

        {activeTab === "signals" && (
          <div className="p-4">
            <h3 className="text-sm font-semibold text-foreground mb-4" data-testid="text-active-signals">
              {scope === 'flightscope' ? 'Active Flight Signals' : 'Active Signals'}
            </h3>
            <div className="space-y-3">
              {scope === 'flightscope' ? (
                // Mock flight signals
                [
                  { id: '1', title: 'Weather Alert: Thunderstorms', description: 'Severe weather affecting departures Terminal 3', severity: 4, timestamp: new Date() },
                  { id: '2', title: 'Runway Maintenance', description: 'Runway 09L/27R closed for maintenance until 18:00 UTC', severity: 3, timestamp: new Date() },
                  { id: '3', title: 'High Traffic Volume', description: 'Departure delays expected due to increased traffic', severity: 2, timestamp: new Date() }
                ].map((signal, index) => (
                  <div key={signal.id} className="p-3 bg-muted rounded-lg border-l-4 border-destructive"
                       data-testid={`signal-${index}`}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-foreground">{signal.title}</span>
                      <span className={cn(
                        "text-xs px-2 py-1 rounded",
                        signal.severity >= 4 ? "bg-destructive text-destructive-foreground" :
                        signal.severity >= 3 ? "bg-amber-500 text-white" : "bg-primary text-primary-foreground"
                      )}>
                        {signal.severity >= 4 ? 'Critical' : signal.severity >= 3 ? 'High' : 'Medium'}
                      </span>
                    </div>
                    <p className="text-xs text-muted-foreground mb-2">{signal.description}</p>
                    <div className="text-xs text-muted-foreground">
                      {signal.timestamp.toLocaleString()}
                    </div>
                  </div>
                ))
              ) : (
                signals?.slice(0, 10).map((signal, index) => (
                  <div key={signal.id} className="p-3 bg-muted rounded-lg border-l-4 border-destructive"
                       data-testid={`signal-${index}`}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-foreground">{signal.title}</span>
                      <span className={cn(
                        "text-xs px-2 py-1 rounded",
                        signal.severity >= 4 ? "bg-destructive text-destructive-foreground" :
                        signal.severity >= 3 ? "bg-amber-500 text-white" : "bg-primary text-primary-foreground"
                      )}>
                        High
                      </span>
                    </div>
                    <p className="text-xs text-muted-foreground mb-2">{signal.description}</p>
                    <div className="text-xs text-muted-foreground">
                      {new Date(signal.timestamp!).toLocaleString()}
                    </div>
                  </div>
                )) || (
                  <div className="text-sm text-muted-foreground text-center py-8">
                    No active signals
                  </div>
                )
              )}
            </div>
          </div>
        )}

        {activeTab === "predict" && (
          <div className="p-4">
            <h3 className="text-sm font-semibold text-foreground mb-4" data-testid="text-predictions">
              {scope === 'flightscope' ? 'Aviation Predictions' : 'Price Predictions'}
            </h3>
            <div className="space-y-4">
              {scope === 'flightscope' ? (
                // Mock aviation predictions
                [
                  { id: '1', target: 'Jet Fuel', horizon: '1D', predictionClass: 'UP', probability: 0.74, modelVersion: 'Aviation-ML-v2.1' },
                  { id: '2', target: 'Passenger Traffic', horizon: '1W', predictionClass: 'UP', probability: 0.82, modelVersion: 'Aviation-ML-v2.1' },
                  { id: '3', target: 'Flight Delays', horizon: '1D', predictionClass: 'DOWN', probability: 0.65, modelVersion: 'Aviation-ML-v2.1' },
                  { id: '4', target: 'Cargo Volume', horizon: '1W', predictionClass: 'UP', probability: 0.71, modelVersion: 'Aviation-ML-v2.1' }
                ].map((prediction, index) => (
                  <div key={prediction.id} className="p-3 bg-muted rounded-lg"
                       data-testid={`prediction-${index}`}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-foreground">
                        {prediction.target} {prediction.horizon}
                      </span>
                      <span className={cn(
                        "text-xs px-2 py-1 rounded font-medium",
                        prediction.predictionClass === 'UP' ? "bg-emerald-500 text-white" :
                        prediction.predictionClass === 'DOWN' ? "bg-destructive text-destructive-foreground" :
                        "bg-muted-foreground text-white"
                      )}>
                        {prediction.predictionClass}
                      </span>
                    </div>
                    <div className="flex items-center space-x-2 mb-2">
                      <div className="flex-1 bg-accent rounded-full h-2">
                        <div 
                          className="bg-primary h-2 rounded-full transition-all" 
                          style={{ width: `${prediction.probability * 100}%` }}
                        ></div>
                      </div>
                      <span className="text-xs text-muted-foreground">
                        {(prediction.probability * 100).toFixed(1)}%
                      </span>
                    </div>
                    <div className="text-xs text-muted-foreground">
                      Model: {prediction.modelVersion}
                    </div>
                  </div>
                ))
              ) : (
                predictions?.slice(0, 6).map((prediction, index) => (
                  <div key={prediction.id} className="p-3 bg-muted rounded-lg"
                       data-testid={`prediction-${index}`}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-foreground">
                        {prediction.target} {prediction.horizon}
                      </span>
                      <span className={cn(
                        "text-xs px-2 py-1 rounded font-medium",
                        prediction.predictionClass === 'UP' ? "bg-emerald-500 text-white" :
                        prediction.predictionClass === 'DOWN' ? "bg-destructive text-destructive-foreground" :
                        "bg-muted-foreground text-white"
                      )}>
                        {prediction.predictionClass}
                      </span>
                    </div>
                    <div className="flex items-center space-x-2 mb-2">
                      <div className="flex-1 bg-accent rounded-full h-2">
                        <div 
                          className="bg-primary h-2 rounded-full transition-all" 
                          style={{ width: `${(prediction.probability || 0) * 100}%` }}
                        ></div>
                      </div>
                      <span className="text-xs text-muted-foreground">
                        {((prediction.probability || 0) * 100).toFixed(1)}%
                      </span>
                    </div>
                    <div className="text-xs text-muted-foreground">
                      Model: {prediction.modelVersion}
                    </div>
                  </div>
                )) || (
                  <div className="text-sm text-muted-foreground text-center py-8">
                    Loading predictions...
                  </div>
                )
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
