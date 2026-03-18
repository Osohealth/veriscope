import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "wouter";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Progress } from "@/components/ui/progress";
import { 
  ArrowLeft, Ship, TrendingUp, TrendingDown, Activity,
  Navigation, Anchor, BarChart3, AlertCircle, DollarSign
} from "lucide-react";

interface Vessel {
  id: string;
  name: string;
  vesselType: string;
  deadweight: number;
  status?: string;
}

export default function FreightAnalyticsPage() {
  const [timeRange, setTimeRange] = useState<string>("7d");
  const [vesselType, setVesselType] = useState<string>("all");

  const { data: vessels, isLoading } = useQuery<Vessel[]>({
    queryKey: ['/api/vessels'],
  });

  // Filter vessels by type
  const filteredVessels = vessels?.filter(v => 
    vesselType === "all" || v.vesselType === vesselType
  ) || [];

  // Time range multipliers (simulating different time periods)
  const timeMultipliers: Record<string, { factor: number, ladenPct: number }> = {
    '7d': { factor: 0.2, ladenPct: 0.68 },
    '30d': { factor: 0.85, ladenPct: 0.65 },
    '90d': { factor: 2.5, ladenPct: 0.62 },
    '1y': { factor: 10, ladenPct: 0.64 },
  };
  
  const timeConfig = timeMultipliers[timeRange];
  
  // Calculate metrics (using real vessel data where available)
  const totalVessels = filteredVessels.length;
  const ladenVessels = Math.floor(totalVessels * timeConfig.ladenPct);
  const ballastVessels = totalVessels - ladenVessels;
  
  // Ton-miles and ton-days calculations adjusted by time range
  const avgDWT = filteredVessels.reduce((sum, v) => sum + (v.deadweight || 50000), 0) / (totalVessels || 1);
  const tonMiles = totalVessels * avgDWT * 3500 * timeConfig.factor;
  const tonDays = totalVessels * avgDWT * 25 * timeConfig.factor;

  // Freight rates by vessel type - adjusted by time range
  const rateAdjustment = timeRange === '7d' ? 1.15 : timeRange === '30d' ? 1.0 : timeRange === '90d' ? 0.92 : 0.88;
  const freightRates = [
    { type: "VLCC", rate: Math.round(24500 * rateAdjustment), change: 2.3, route: "AG-China", utilization: 78 },
    { type: "Suezmax", rate: Math.round(18200 * rateAdjustment), change: -1.5, route: "WAF-USG", utilization: 82 },
    { type: "Aframax", rate: Math.round(15800 * rateAdjustment), change: 4.1, route: "Med-NWE", utilization: 85 },
    { type: "Panamax", rate: Math.round(12300 * rateAdjustment), change: 1.8, route: "USG-Far East", utilization: 71 },
  ];

  // Congestion data - varies by time range
  const congestionMultiplier = timeRange === '7d' ? 1.0 : timeRange === '30d' ? 1.2 : timeRange === '90d' ? 0.85 : 0.7;
  const congestionData = [
    { port: "Singapore", vessels: Math.round(45 * congestionMultiplier), avgDelay: 3.2, impact: "high" as const },
    { port: "Rotterdam", vessels: Math.round(38 * congestionMultiplier), avgDelay: 2.8, impact: "medium" as const },
    { port: "Fujairah", vessels: Math.round(52 * congestionMultiplier), avgDelay: 4.1, impact: "high" as const },
    { port: "Houston", vessels: Math.round(29 * congestionMultiplier), avgDelay: 1.9, impact: "low" as const },
  ];

  // Route efficiency metrics - adjusted by time range
  const routeMultiplier = timeRange === '7d' ? 0.3 : timeRange === '30d' ? 1.0 : timeRange === '90d' ? 2.8 : 11;
  const routeMetrics = [
    { route: "AG → China", distance: 6500, avgSpeed: 13.5, efficiency: 92, vessels: Math.round(24 * routeMultiplier) },
    { route: "WAF → USG", distance: 5200, avgSpeed: 12.8, efficiency: 88, vessels: Math.round(18 * routeMultiplier) },
    { route: "Med → NWE", distance: 2800, avgSpeed: 14.2, efficiency: 95, vessels: Math.round(31 * routeMultiplier) },
    { route: "USG → Far East", distance: 8900, avgSpeed: 13.1, efficiency: 87, vessels: Math.round(15 * routeMultiplier) },
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-blue-950 to-slate-900">
      <div className="container mx-auto px-6 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-4">
            <Link href="/commodities">
              <Button variant="ghost" size="sm" data-testid="button-back-to-commodities">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back to Commodities
              </Button>
            </Link>
            <div>
              <h1 className="text-3xl font-bold text-white" data-testid="text-page-title">
                Freight Analytics
              </h1>
              <p className="text-slate-400" data-testid="text-page-description">
                Ton-miles metrics, laden vs ballast, and congestion impacts
              </p>
            </div>
          </div>
        </div>

        {/* Key Metrics */}
        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
            {[1, 2, 3, 4].map(i => (
              <Card key={i} className="bg-slate-900/50 border-slate-800">
                <CardContent className="p-6">
                  <div className="animate-pulse">
                    <div className="h-4 bg-slate-700 rounded w-20 mb-2"></div>
                    <div className="h-8 bg-slate-700 rounded w-24"></div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-400" data-testid="text-ton-miles-label">Ton-Miles</p>
                    <p className="text-2xl font-bold text-white mt-1" data-testid="text-ton-miles">
                      {(tonMiles / 1000000000).toFixed(1)}B
                    </p>
                    <p className="text-xs text-green-400 flex items-center gap-1 mt-1">
                      <TrendingUp className="h-3 w-3" />
                      +5.2% vs last period
                    </p>
                  </div>
                  <Activity className="h-8 w-8 text-blue-400" />
                </div>
              </CardContent>
            </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-ton-days-label">Ton-Days</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-ton-days">
                    {(tonDays / 1000000).toFixed(1)}M
                  </p>
                  <p className="text-xs text-red-400 flex items-center gap-1 mt-1">
                    <TrendingDown className="h-3 w-3" />
                    -1.8% vs last period
                  </p>
                </div>
                <BarChart3 className="h-8 w-8 text-green-400" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-laden-label">Laden Vessels</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-laden-count">
                    {ladenVessels}
                  </p>
                  <p className="text-xs text-slate-500">
                    {totalVessels > 0 ? ((ladenVessels / totalVessels) * 100).toFixed(0) : 0}% of fleet
                  </p>
                </div>
                <Ship className="h-8 w-8 text-orange-400" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-ballast-label">Ballast Vessels</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-ballast-count">
                    {ballastVessels}
                  </p>
                  <p className="text-xs text-slate-500">
                    {totalVessels > 0 ? ((ballastVessels / totalVessels) * 100).toFixed(0) : 0}% of fleet
                  </p>
                </div>
                <Anchor className="h-8 w-8 text-purple-400" />
              </div>
            </CardContent>
          </Card>
          </div>
        )}

        {/* Filters */}
        <div className="flex gap-4 mb-6">
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-400">Time Range:</span>
            <select
              value={timeRange}
              onChange={(e) => setTimeRange(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-white text-sm"
              data-testid="select-time-range"
            >
              <option value="7d">Last 7 Days</option>
              <option value="30d">Last 30 Days</option>
              <option value="90d">Last 90 Days</option>
              <option value="1y">Last Year</option>
            </select>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-400">Vessel Type:</span>
            <select
              value={vesselType}
              onChange={(e) => setVesselType(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-white text-sm"
              data-testid="select-vessel-type"
            >
              <option value="all">All Types</option>
              <option value="vlcc">VLCC</option>
              <option value="suezmax">Suezmax</option>
              <option value="aframax">Aframax</option>
              <option value="panamax">Panamax</option>
            </select>
          </div>
        </div>

        <Tabs defaultValue="ton-metrics" className="space-y-6">
          <TabsList className="bg-slate-900/50 border border-slate-800" data-testid="tabs-freight">
            <TabsTrigger value="ton-metrics" data-testid="tab-ton-metrics">
              <Activity className="h-4 w-4 mr-2" />
              Ton Metrics
            </TabsTrigger>
            <TabsTrigger value="laden-ballast" data-testid="tab-laden-ballast">
              <Ship className="h-4 w-4 mr-2" />
              Laden vs Ballast
            </TabsTrigger>
            <TabsTrigger value="congestion" data-testid="tab-congestion">
              <AlertCircle className="h-4 w-4 mr-2" />
              Congestion Impact
            </TabsTrigger>
            <TabsTrigger value="rates" data-testid="tab-rates">
              <DollarSign className="h-4 w-4 mr-2" />
              Freight Rates
            </TabsTrigger>
          </TabsList>

          {/* Ton Metrics Tab */}
          <TabsContent value="ton-metrics" className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2" data-testid="text-ton-miles-title">
                    <Activity className="h-5 w-5 text-blue-400" />
                    Ton-Miles Analysis
                  </CardTitle>
                  <CardDescription>Total cargo-weighted distance metrics</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="text-sm text-slate-400">Total Ton-Miles</div>
                      <div className="text-3xl font-bold text-white mt-1" data-testid="text-total-ton-miles">
                        {(tonMiles / 1000000000).toFixed(2)}B
                      </div>
                    </div>
                    <Badge className="bg-green-500/20 text-green-400" data-testid="badge-ton-miles-change">
                      +5.2%
                    </Badge>
                  </div>
                  <div className="grid grid-cols-2 gap-4 pt-4 border-t border-slate-700">
                    <div>
                      <div className="text-sm text-slate-400">By Crude Oil</div>
                      <div className="text-lg font-medium text-white mt-1" data-testid="text-crude-ton-miles">
                        {(tonMiles * 0.68 / 1000000000).toFixed(2)}B
                      </div>
                      <div className="text-xs text-slate-500">68% of total</div>
                    </div>
                    <div>
                      <div className="text-sm text-slate-400">By Refined Products</div>
                      <div className="text-lg font-medium text-white mt-1" data-testid="text-refined-ton-miles">
                        {(tonMiles * 0.32 / 1000000000).toFixed(2)}B
                      </div>
                      <div className="text-xs text-slate-500">32% of total</div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2" data-testid="text-ton-days-title">
                    <BarChart3 className="h-5 w-5 text-green-400" />
                    Ton-Days Analysis
                  </CardTitle>
                  <CardDescription>Cargo storage at sea metrics</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="text-sm text-slate-400">Total Ton-Days</div>
                      <div className="text-3xl font-bold text-white mt-1" data-testid="text-total-ton-days">
                        {(tonDays / 1000000).toFixed(2)}M
                      </div>
                    </div>
                    <Badge className="bg-red-500/20 text-red-400" data-testid="badge-ton-days-change">
                      -1.8%
                    </Badge>
                  </div>
                  <div className="grid grid-cols-2 gap-4 pt-4 border-t border-slate-700">
                    <div>
                      <div className="text-sm text-slate-400">Avg Days at Sea</div>
                      <div className="text-lg font-medium text-white mt-1" data-testid="text-avg-days">
                        25 days
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-slate-400">Avg Cargo Weight</div>
                      <div className="text-lg font-medium text-white mt-1" data-testid="text-avg-weight">
                        {(avgDWT / 1000).toFixed(0)}K MT
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white" data-testid="text-route-efficiency-title">Route Efficiency Metrics</CardTitle>
                <CardDescription>Performance analysis by major trade routes</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {routeMetrics.map((route, i) => (
                    <div key={i} className="p-4 bg-slate-800/30 rounded-lg" data-testid={`card-route-${i}`}>
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-3">
                          <Navigation className="h-5 w-5 text-blue-400" />
                          <div>
                            <div className="font-medium text-white" data-testid={`text-route-name-${i}`}>
                              {route.route}
                            </div>
                            <div className="text-sm text-slate-400">
                              {route.distance.toLocaleString()} nm
                            </div>
                          </div>
                        </div>
                        <div className="text-right">
                          <Badge data-testid={`badge-efficiency-${i}`}>
                            {route.efficiency}% Efficiency
                          </Badge>
                          <div className="text-sm text-slate-400 mt-1">
                            {route.vessels} vessels
                          </div>
                        </div>
                      </div>
                      <div className="grid grid-cols-3 gap-4 text-sm">
                        <div>
                          <div className="text-slate-400">Avg Speed</div>
                          <div className="text-white font-medium" data-testid={`text-speed-${i}`}>
                            {route.avgSpeed} knots
                          </div>
                        </div>
                        <div>
                          <div className="text-slate-400">Transit Time</div>
                          <div className="text-white font-medium" data-testid={`text-transit-${i}`}>
                            {Math.round(route.distance / (route.avgSpeed * 24))} days
                          </div>
                        </div>
                        <div>
                          <div className="text-slate-400">Ton-Miles</div>
                          <div className="text-white font-medium" data-testid={`text-route-ton-miles-${i}`}>
                            {((route.vessels * avgDWT * route.distance) / 1000000).toFixed(1)}M
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Laden vs Ballast Tab */}
          <TabsContent value="laden-ballast">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-laden-ballast-title">
                  <Ship className="h-5 w-5" />
                  Laden vs Ballast Fleet Analysis
                </CardTitle>
                <CardDescription>Current fleet utilization and positioning</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div className="p-6 bg-green-900/20 border border-green-500/30 rounded-lg">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-lg font-medium text-green-400" data-testid="text-laden-title">
                        Laden Vessels
                      </h3>
                      <Ship className="h-6 w-6 text-green-400" />
                    </div>
                    <div className="text-4xl font-bold text-white mb-2" data-testid="text-laden-fleet-count">
                      {ladenVessels}
                    </div>
                    <Progress value={(ladenVessels / totalVessels) * 100} className="h-3 mb-3" data-testid="progress-laden" />
                    <div className="space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-slate-400">Percentage of Fleet</span>
                        <span className="text-white font-medium" data-testid="text-laden-percentage">
                          {totalVessels > 0 ? ((ladenVessels / totalVessels) * 100).toFixed(1) : 0}%
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Total Cargo</span>
                        <span className="text-white font-medium" data-testid="text-laden-cargo">
                          {(ladenVessels * avgDWT * 0.95 / 1000000).toFixed(1)}M MT
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Avg Load Factor</span>
                        <span className="text-white font-medium" data-testid="text-load-factor">95%</span>
                      </div>
                    </div>
                  </div>

                  <div className="p-6 bg-orange-900/20 border border-orange-500/30 rounded-lg">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-lg font-medium text-orange-400" data-testid="text-ballast-title">
                        Ballast Vessels
                      </h3>
                      <Anchor className="h-6 w-6 text-orange-400" />
                    </div>
                    <div className="text-4xl font-bold text-white mb-2" data-testid="text-ballast-fleet-count">
                      {ballastVessels}
                    </div>
                    <Progress value={(ballastVessels / totalVessels) * 100} className="h-3 mb-3" data-testid="progress-ballast" />
                    <div className="space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-slate-400">Percentage of Fleet</span>
                        <span className="text-white font-medium" data-testid="text-ballast-percentage">
                          {totalVessels > 0 ? ((ballastVessels / totalVessels) * 100).toFixed(1) : 0}%
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Repositioning</span>
                        <span className="text-white font-medium" data-testid="text-repositioning">
                          {Math.floor(ballastVessels * 0.7)} vessels
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Avg Ballast Speed</span>
                        <span className="text-white font-medium" data-testid="text-ballast-speed">11.5 knots</span>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="h-64 bg-slate-800/30 rounded-lg flex items-center justify-center" data-testid="chart-laden-ballast">
                  <div className="text-center text-slate-400">
                    <BarChart3 className="h-12 w-12 mx-auto mb-3 opacity-50" />
                    <p>Laden vs Ballast trend chart</p>
                    <p className="text-sm text-slate-500 mt-1">Historical utilization patterns</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Congestion Impact Tab */}
          <TabsContent value="congestion">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-congestion-title">
                  <AlertCircle className="h-5 w-5" />
                  Port Congestion Impact Analysis
                </CardTitle>
                <CardDescription>Delay patterns and freight cost implications</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {congestionData.map((port, i) => (
                    <div key={i} className="p-4 bg-slate-800/30 rounded-lg" data-testid={`card-congestion-${i}`}>
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-3">
                          <div className={`p-2 rounded-lg ${
                            port.impact === 'high' ? 'bg-red-500/20' : 
                            port.impact === 'medium' ? 'bg-yellow-500/20' : 'bg-green-500/20'
                          }`}>
                            <AlertCircle className={`h-5 w-5 ${
                              port.impact === 'high' ? 'text-red-400' : 
                              port.impact === 'medium' ? 'text-yellow-400' : 'text-green-400'
                            }`} />
                          </div>
                          <div>
                            <div className="font-medium text-white" data-testid={`text-congestion-port-${i}`}>
                              {port.port}
                            </div>
                            <div className="text-sm text-slate-400">
                              {port.vessels} vessels waiting
                            </div>
                          </div>
                        </div>
                        <Badge 
                          variant={port.impact === 'high' ? 'destructive' : 'default'}
                          data-testid={`badge-impact-${i}`}
                        >
                          {port.impact.toUpperCase()} Impact
                        </Badge>
                      </div>
                      <div className="grid grid-cols-4 gap-4 text-sm">
                        <div>
                          <div className="text-slate-400">Avg Delay</div>
                          <div className="text-white font-medium" data-testid={`text-delay-${i}`}>
                            {port.avgDelay} days
                          </div>
                        </div>
                        <div>
                          <div className="text-slate-400">Cost Impact</div>
                          <div className="text-white font-medium" data-testid={`text-cost-${i}`}>
                            ${(port.avgDelay * 15000).toLocaleString()}/day
                          </div>
                        </div>
                        <div>
                          <div className="text-slate-400">Total Ton-Days</div>
                          <div className="text-white font-medium" data-testid={`text-ton-days-impact-${i}`}>
                            {(port.vessels * avgDWT * port.avgDelay / 1000).toFixed(0)}K
                          </div>
                        </div>
                        <div>
                          <div className="text-slate-400">Freight Premium</div>
                          <div className="text-white font-medium" data-testid={`text-premium-${i}`}>
                            +{(port.avgDelay * 3).toFixed(1)}%
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>

                <div className="mt-6 p-4 bg-amber-900/20 border border-amber-500/30 rounded-lg">
                  <div className="flex items-start gap-3">
                    <AlertCircle className="h-5 w-5 text-amber-400 mt-0.5" />
                    <div>
                      <div className="font-medium text-amber-100 mb-1" data-testid="text-congestion-alert-title">
                        Congestion Alert
                      </div>
                      <div className="text-sm text-amber-200/80" data-testid="text-congestion-alert-desc">
                        Fujairah port congestion increased 15% this week. Average waiting time now 4.1 days, adding ~$60K per vessel in demurrage costs.
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Freight Rates Tab */}
          <TabsContent value="rates">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-rates-title">
                  <DollarSign className="h-5 w-5" />
                  Freight Rate Analysis
                </CardTitle>
                <CardDescription>Current rates and trends by vessel class</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {freightRates.map((rate, i) => (
                    <Card key={i} className="bg-slate-800/30 border-slate-700" data-testid={`card-rate-${i}`}>
                      <CardContent className="p-6">
                        <div className="flex items-center justify-between mb-4">
                          <div>
                            <div className="text-lg font-medium text-white" data-testid={`text-rate-type-${i}`}>
                              {rate.type}
                            </div>
                            <div className="text-sm text-slate-400">{rate.route}</div>
                          </div>
                          <Badge 
                            className={rate.change > 0 ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}
                            data-testid={`badge-rate-change-${i}`}
                          >
                            {rate.change > 0 ? '+' : ''}{rate.change}%
                          </Badge>
                        </div>
                        <div className="space-y-3">
                          <div>
                            <div className="text-sm text-slate-400">Daily Rate</div>
                            <div className="text-3xl font-bold text-white" data-testid={`text-daily-rate-${i}`}>
                              ${rate.rate.toLocaleString()}
                            </div>
                          </div>
                          <div className="pt-3 border-t border-slate-700">
                            <div className="flex justify-between text-sm mb-2">
                              <span className="text-slate-400">Fleet Utilization</span>
                              <span className="text-white font-medium" data-testid={`text-utilization-${i}`}>
                                {rate.utilization}%
                              </span>
                            </div>
                            <Progress value={rate.utilization} className="h-2" data-testid={`progress-utilization-${i}`} />
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>

                <div className="mt-6 h-64 bg-slate-800/30 rounded-lg flex items-center justify-center" data-testid="chart-rates">
                  <div className="text-center text-slate-400">
                    <TrendingUp className="h-12 w-12 mx-auto mb-3 opacity-50" />
                    <p>Freight rate trends chart</p>
                    <p className="text-sm text-slate-500 mt-1">Historical rate movements by vessel class</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
