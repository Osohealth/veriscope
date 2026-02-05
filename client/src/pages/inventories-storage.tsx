import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "wouter";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { 
  ArrowLeft, Database, Droplet, TrendingUp, TrendingDown,
  Ship, Gauge, BarChart3, Activity, AlertTriangle, Globe, Calendar
} from "lucide-react";
import { 
  MethodologyBadge, 
  ConfidenceBadge, 
  DataSourceLegend,
  parseMethodology
} from "@/components/credibility-indicator";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { getAuthHeaders } from "@/lib/queryClient";

interface StorageSite {
  id: string;
  name: string;
  portId: string;
  type: string;
  totalCapacity: number;
  currentLevel: number;
  utilizationRate: string;
  operator: string | null;
  fillData?: StorageFillData;
}

interface StorageFillData {
  id: string;
  siteId: string;
  timestamp: string;
  fillIndex: string;
  confidence: string;
  source: string;
}

interface Port {
  id: string;
  name: string;
  code: string;
  country: string;
}

interface FloatingStorageVessel {
  id: string;
  vesselName: string;
  vesselType: string;
  imo: string | null;
  cargoType: string;
  cargoGrade: string | null;
  cargoVolume: number;
  cargoUnit: string;
  locationLat: string | null;
  locationLng: string | null;
  region: string | null;
  durationDays: number;
  startDate: string;
  estimatedValue: string | null;
  charterer: string | null;
  status: string;
  lastUpdated: string | null;
}

interface FloatingStorageResponse {
  totalVessels: number;
  totalVolumeMT: number;
  byVesselType: Record<string, { count: number; volume: number }>;
  byRegion: Record<string, { count: number; volume: number }>;
  vessels: FloatingStorageVessel[];
}

interface SprReserve {
  id: string;
  country: string;
  countryCode: string;
  region: string | null;
  gradeType: string;
  volumeBarrels: string;
  percentOfTotal: string;
  capacityBarrels: string | null;
  utilizationRate: string | null;
  daysOfCover: number | null;
  lastReleaseDate: string | null;
  lastReleaseVolume: string | null;
  reportDate: string;
  source: string | null;
}

interface SprResponse {
  globalTotalBarrels: number;
  globalCapacityBarrels: number;
  globalUtilization: number;
  byCountry: Record<string, { totalVolume: number; totalCapacity: number; grades: Record<string, number> }>;
  allRecords: SprReserve[];
}

interface StorageTimeSeries {
  id: string;
  recordDate: string;
  metricType: string;
  region: string | null;
  storageType: string | null;
  totalCapacity: string | null;
  currentLevel: string;
  utilizationRate: string | null;
  weekOverWeekChange: string | null;
  yearOverYearChange: string | null;
  fiveYearAverage: string | null;
  confidence: string | null;
  source: string | null;
}

export default function InventoriesStoragePage() {
  const [selectedPort, setSelectedPort] = useState<string>("all");
  const [selectedType, setSelectedType] = useState<string>("all");
  const [timeSeriesRegion, setTimeSeriesRegion] = useState<string>("global");
  const [timeSeriesStorageType, setTimeSeriesStorageType] = useState<string>("crude_oil");

  const { data: ports, isLoading: portsLoading } = useQuery<Port[]>({
    queryKey: ['/api/ports'],
  });

  const { data: storageSites, isLoading: sitesLoading } = useQuery<StorageSite[]>({
    queryKey: ['/api/storage/sites'],
  });

  const { data: floatingStorageData, isLoading: floatingLoading } = useQuery<FloatingStorageResponse>({
    queryKey: ['/api/storage/floating'],
  });
  
  // Extract vessels array from response
  const floatingStorage = floatingStorageData?.vessels || [];

  const { data: sprData, isLoading: sprLoading } = useQuery<SprResponse>({
    queryKey: ['/api/storage/spr'],
  });
  
  // Extract reserves array from response
  const sprReserves = sprData?.allRecords || [];

  const { data: timeSeries, isLoading: timeSeriesLoading } = useQuery<StorageTimeSeries[]>({
    queryKey: ['/api/storage/time-series', timeSeriesRegion, timeSeriesStorageType],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (timeSeriesRegion) params.set("region", timeSeriesRegion);
      if (timeSeriesStorageType) params.set("storageType", timeSeriesStorageType);
      const url = `/api/storage/time-series?${params.toString()}`;
      const response = await fetch(url, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error("Failed to fetch storage time series");
      return response.json();
    }
  });

  // Filter storage sites
  const filteredSites = storageSites?.filter(site => {
    const portMatch = selectedPort === "all" || site.portId === selectedPort;
    const typeMatch = selectedType === "all" || site.type === selectedType;
    return portMatch && typeMatch;
  }) || [];

  // Calculate aggregate statistics
  const totalCapacity = filteredSites.reduce((sum, site) => sum + site.totalCapacity, 0);
  const totalUsed = filteredSites.reduce((sum, site) => sum + site.currentLevel, 0);
  const avgUtilization = filteredSites.length > 0 
    ? filteredSites.reduce((sum, site) => sum + parseFloat(site.utilizationRate), 0) / filteredSites.length 
    : 0;

  // Group by type
  const crudeStorage = filteredSites.filter(s => s.type === 'crude_oil');
  const refinedStorage = filteredSites.filter(s => s.type === 'refined_products');
  const lngStorage = filteredSites.filter(s => s.type === 'lng');

  // Floating storage statistics
  const floatingStorageCount = floatingStorage?.length || 0;
  const floatingVolume = floatingStorage?.reduce((sum, fs) => sum + (fs.cargoVolume || 0), 0) || 0;

  // Group SPR by country
  const sprByCountry = sprReserves?.reduce((acc, spr) => {
    if (!acc[spr.country]) acc[spr.country] = [];
    acc[spr.country].push(spr);
    return acc;
  }, {} as Record<string, SprReserve[]>) || {};

  // Prepare time series chart data - guard against undefined
  const timeSeriesData = timeSeries ?? [];
  const chartData = timeSeriesData.map(ts => ({
    date: new Date(ts.recordDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    level: parseFloat(ts.currentLevel) / 1000000000, // Convert to billions
    utilization: ts.utilizationRate ? parseFloat(ts.utilizationRate) : 0,
    fiveYearAvg: ts.fiveYearAverage ? parseFloat(ts.fiveYearAverage) / 1000000000 : null,
  })).slice(-52); // Last 52 weeks

  // Calculate time series metrics
  const latestTimeSeries = timeSeriesData.length > 0 ? timeSeriesData[timeSeriesData.length - 1] : null;
  const weekAgoSeries = timeSeriesData.length > 1 ? timeSeriesData[timeSeriesData.length - 2] : null;
  const monthAgoSeries = timeSeriesData.length > 4 ? timeSeriesData[timeSeriesData.length - 5] : null;
  const yearAgoSeries = timeSeriesData.find(ts => {
    const tsDate = new Date(ts.recordDate);
    const yearAgo = new Date();
    yearAgo.setFullYear(yearAgo.getFullYear() - 1);
    return Math.abs(tsDate.getTime() - yearAgo.getTime()) < 7 * 24 * 60 * 60 * 1000;
  }) ?? null;

  const weekChange = latestTimeSeries && weekAgoSeries
    ? ((parseFloat(latestTimeSeries.currentLevel) - parseFloat(weekAgoSeries.currentLevel)) / parseFloat(weekAgoSeries.currentLevel) * 100)
    : 0;
  const monthChange = latestTimeSeries && monthAgoSeries
    ? ((parseFloat(latestTimeSeries.currentLevel) - parseFloat(monthAgoSeries.currentLevel)) / parseFloat(monthAgoSeries.currentLevel) * 100)
    : 0;
  const yoyChange = latestTimeSeries && yearAgoSeries
    ? ((parseFloat(latestTimeSeries.currentLevel) - parseFloat(yearAgoSeries.currentLevel)) / parseFloat(yearAgoSeries.currentLevel) * 100)
    : (latestTimeSeries?.yearOverYearChange ? parseFloat(latestTimeSeries.yearOverYearChange) / parseFloat(latestTimeSeries.currentLevel) * 100 : 0);

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
                Inventories & Storage
              </h1>
              <p className="text-slate-400" data-testid="text-page-description">
                Tank levels, floating storage, SPR splits, and historical time series
              </p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <span className="text-sm text-slate-400">Data Sources:</span>
            <DataSourceLegend />
          </div>
        </div>

        {/* Key Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-total-capacity-label">Total Capacity</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-total-capacity">
                    {(totalCapacity / 1000000).toFixed(1)}M
                  </p>
                  <p className="text-xs text-slate-500">barrels</p>
                </div>
                <Database className="h-8 w-8 text-blue-400" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-current-level-label">Current Level</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-current-level">
                    {(totalUsed / 1000000).toFixed(1)}M
                  </p>
                  <p className="text-xs text-slate-500">barrels</p>
                </div>
                <Droplet className="h-8 w-8 text-green-400" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-utilization-label">Avg Utilization</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-utilization">
                    {avgUtilization.toFixed(1)}%
                  </p>
                  <Progress value={avgUtilization} className="mt-2 h-1" data-testid="progress-utilization" />
                </div>
                <Gauge className="h-8 w-8 text-purple-400" />
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-slate-400" data-testid="text-floating-label">Floating Storage</p>
                  <p className="text-2xl font-bold text-white mt-1" data-testid="text-floating-count">
                    {floatingStorageCount}
                  </p>
                  <p className="text-xs text-slate-500">{(floatingVolume / 1000000).toFixed(1)}M bbl</p>
                </div>
                <Ship className="h-8 w-8 text-orange-400" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Filters */}
        <div className="flex gap-4 mb-6">
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-400">Port:</span>
            <select
              value={selectedPort}
              onChange={(e) => setSelectedPort(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-white text-sm"
              data-testid="select-port"
            >
              <option value="all">All Ports</option>
              {ports?.map(port => (
                <option key={port.id} value={port.id} data-testid={`option-port-${port.id}`}>
                  {port.name} ({port.code})
                </option>
              ))}
            </select>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-400">Type:</span>
            <select
              value={selectedType}
              onChange={(e) => setSelectedType(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-white text-sm"
              data-testid="select-type"
            >
              <option value="all">All Types</option>
              <option value="crude_oil">Crude Oil</option>
              <option value="refined_products">Refined Products</option>
              <option value="lng">LNG</option>
              <option value="chemicals">Chemicals</option>
            </select>
          </div>
        </div>

        <Tabs defaultValue="tank-levels" className="space-y-6">
          <TabsList className="bg-slate-900/50 border border-slate-800" data-testid="tabs-inventories">
            <TabsTrigger value="tank-levels" data-testid="tab-tank-levels">
              <Database className="h-4 w-4 mr-2" />
              Tank Levels
            </TabsTrigger>
            <TabsTrigger value="floating-storage" data-testid="tab-floating">
              <Ship className="h-4 w-4 mr-2" />
              Floating Storage
            </TabsTrigger>
            <TabsTrigger value="spr-splits" data-testid="tab-spr">
              <BarChart3 className="h-4 w-4 mr-2" />
              SPR Splits
            </TabsTrigger>
            <TabsTrigger value="time-series" data-testid="tab-time-series">
              <Activity className="h-4 w-4 mr-2" />
              Time Series
            </TabsTrigger>
          </TabsList>

          {/* Tank Levels Tab */}
          <TabsContent value="tank-levels" className="space-y-6">
            <div className="grid grid-cols-1 gap-6">
              {/* Crude Oil Tanks */}
              {crudeStorage.length > 0 && (
                <Card className="bg-slate-900/50 border-slate-800">
                  <CardHeader>
                    <CardTitle className="text-white flex items-center gap-2" data-testid="text-crude-title">
                      <Droplet className="h-5 w-5 text-amber-400" />
                      Crude Oil Storage
                    </CardTitle>
                    <CardDescription>Land-based crude oil tank farms</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {crudeStorage.map(site => (
                        <div key={site.id} className="p-4 bg-slate-800/30 rounded-lg" data-testid={`card-crude-${site.id}`}>
                          <div className="flex items-center justify-between mb-3">
                            <div>
                              <h4 className="font-medium text-white" data-testid={`text-site-name-${site.id}`}>
                                {site.name}
                              </h4>
                              <p className="text-sm text-slate-400" data-testid={`text-site-operator-${site.id}`}>
                                {site.operator || 'Independent Operator'}
                              </p>
                            </div>
                            <Badge 
                              variant={parseFloat(site.utilizationRate) > 85 ? 'destructive' : 'default'}
                              data-testid={`badge-util-${site.id}`}
                            >
                              {parseFloat(site.utilizationRate).toFixed(1)}% Full
                            </Badge>
                          </div>
                          <div className="space-y-2">
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-400">Capacity</span>
                              <span className="text-white font-medium" data-testid={`text-capacity-${site.id}`}>
                                {site.totalCapacity.toLocaleString()} bbl
                              </span>
                            </div>
                            <Progress 
                              value={parseFloat(site.utilizationRate)} 
                              className="h-3"
                              data-testid={`progress-tank-${site.id}`}
                            />
                            <div className="flex justify-between text-sm">
                              <span className="text-green-400" data-testid={`text-current-${site.id}`}>
                                Current: {site.currentLevel.toLocaleString()} bbl
                              </span>
                              <span className="text-slate-400" data-testid={`text-available-${site.id}`}>
                                Available: {(site.totalCapacity - site.currentLevel).toLocaleString()} bbl
                              </span>
                            </div>
                            {site.fillData && (
                              <div className="mt-2 pt-2 border-t border-slate-700 flex items-center gap-4 text-xs">
                                <span className="text-slate-500">
                                  Last Update: {new Date(site.fillData.timestamp).toLocaleDateString()}
                                </span>
                                <MethodologyBadge 
                                  methodology={parseMethodology(site.fillData.source)} 
                                  size="sm" 
                                />
                                <ConfidenceBadge 
                                  confidence={parseFloat(site.fillData.confidence)} 
                                  size="sm" 
                                />
                              </div>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Refined Products Tanks */}
              {refinedStorage.length > 0 && (
                <Card className="bg-slate-900/50 border-slate-800">
                  <CardHeader>
                    <CardTitle className="text-white flex items-center gap-2" data-testid="text-refined-title">
                      <Database className="h-5 w-5 text-blue-400" />
                      Refined Products Storage
                    </CardTitle>
                    <CardDescription>Gasoline, diesel, jet fuel, and other refined products</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {refinedStorage.map(site => (
                        <div key={site.id} className="p-4 bg-slate-800/30 rounded-lg" data-testid={`card-refined-${site.id}`}>
                          <div className="flex items-center justify-between mb-3">
                            <div>
                              <h4 className="font-medium text-white" data-testid={`text-refined-name-${site.id}`}>
                                {site.name}
                              </h4>
                              <p className="text-sm text-slate-400">{site.operator || 'Independent Operator'}</p>
                            </div>
                            <Badge 
                              variant={parseFloat(site.utilizationRate) > 85 ? 'destructive' : 'default'}
                              data-testid={`badge-refined-util-${site.id}`}
                            >
                              {parseFloat(site.utilizationRate).toFixed(1)}% Full
                            </Badge>
                          </div>
                          <Progress 
                            value={parseFloat(site.utilizationRate)} 
                            className="h-3"
                            data-testid={`progress-refined-${site.id}`}
                          />
                          <div className="flex justify-between text-sm mt-2">
                            <span className="text-slate-400">
                              {site.currentLevel.toLocaleString()} / {site.totalCapacity.toLocaleString()} bbl
                            </span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* LNG Storage */}
              {lngStorage.length > 0 && (
                <Card className="bg-slate-900/50 border-slate-800">
                  <CardHeader>
                    <CardTitle className="text-white flex items-center gap-2" data-testid="text-lng-title">
                      <Database className="h-5 w-5 text-green-400" />
                      LNG Storage
                    </CardTitle>
                    <CardDescription>Liquefied natural gas storage terminals</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {lngStorage.map(site => (
                        <div key={site.id} className="p-4 bg-slate-800/30 rounded-lg" data-testid={`card-lng-${site.id}`}>
                          <div className="flex items-center justify-between mb-3">
                            <h4 className="font-medium text-white" data-testid={`text-lng-name-${site.id}`}>
                              {site.name}
                            </h4>
                            <Badge data-testid={`badge-lng-util-${site.id}`}>
                              {parseFloat(site.utilizationRate).toFixed(1)}% Full
                            </Badge>
                          </div>
                          <Progress 
                            value={parseFloat(site.utilizationRate)} 
                            className="h-3"
                            data-testid={`progress-lng-${site.id}`}
                          />
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {filteredSites.length === 0 && !sitesLoading && (
                <Card className="bg-slate-900/50 border-slate-800">
                  <CardContent className="p-12 text-center">
                    <Database className="h-12 w-12 text-slate-600 mx-auto mb-4" />
                    <p className="text-slate-400" data-testid="text-no-sites">No storage sites available</p>
                  </CardContent>
                </Card>
              )}
            </div>
          </TabsContent>

          {/* Floating Storage Tab */}
          <TabsContent value="floating-storage">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-floating-title">
                  <Ship className="h-5 w-5" />
                  Floating Storage Inventory
                </CardTitle>
                <CardDescription>Vessels acting as floating storage facilities - {floatingStorageCount} vessels tracking {(floatingVolume / 1000000).toFixed(1)}M MT</CardDescription>
              </CardHeader>
              <CardContent>
                {floatingLoading ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {[1, 2, 3, 4, 5, 6].map(i => (
                      <Skeleton key={i} className="h-48 bg-slate-800" />
                    ))}
                  </div>
                ) : floatingStorage && floatingStorage.length > 0 ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {floatingStorage.map((fs) => (
                      <Card key={fs.id} className="bg-slate-800/30 border-slate-700" data-testid={`card-floating-${fs.id}`}>
                        <CardContent className="p-4">
                          <div className="flex items-center justify-between mb-3">
                            <Ship className="h-5 w-5 text-orange-400" />
                            <div className="flex gap-2">
                              <Badge variant={fs.status === 'active' ? 'default' : 'secondary'} data-testid={`badge-floating-status-${fs.id}`}>
                                {fs.status}
                              </Badge>
                              <Badge variant="outline" data-testid={`badge-floating-type-${fs.id}`}>
                                {fs.vesselType.toUpperCase()}
                              </Badge>
                            </div>
                          </div>
                          <div className="space-y-2">
                            <div>
                              <div className="text-sm text-slate-400">Vessel</div>
                              <div className="font-medium text-white" data-testid={`text-floating-vessel-${fs.id}`}>
                                {fs.vesselName}
                              </div>
                              <div className="text-xs text-slate-500">IMO: {fs.imo || 'N/A'}</div>
                            </div>
                            <div className="grid grid-cols-2 gap-2">
                              <div>
                                <div className="text-sm text-slate-400">Cargo</div>
                                <div className="text-white" data-testid={`text-floating-cargo-${fs.id}`}>
                                  {fs.cargoVolume.toLocaleString()} {fs.cargoUnit}
                                </div>
                                <div className="text-xs text-slate-500">{fs.cargoGrade || fs.cargoType}</div>
                              </div>
                              <div>
                                <div className="text-sm text-slate-400">Duration</div>
                                <div className="text-white" data-testid={`text-floating-duration-${fs.id}`}>
                                  {fs.durationDays} days
                                </div>
                              </div>
                            </div>
                            <div>
                              <div className="text-sm text-slate-400">Region</div>
                              <div className="text-white text-sm" data-testid={`text-floating-location-${fs.id}`}>
                                {fs.region || 'Unknown'}
                              </div>
                              <div className="text-xs text-slate-500">Charterer: {fs.charterer || 'N/A'}</div>
                            </div>
                            <div className="flex justify-between text-xs pt-2 border-t border-slate-700">
                              <span className="text-slate-500">Started: {new Date(fs.startDate).toLocaleDateString()}</span>
                              <span className="text-slate-500">Value: ${fs.estimatedValue ? parseFloat(fs.estimatedValue).toLocaleString() : 'N/A'}</span>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-12">
                    <Ship className="h-12 w-12 mx-auto text-slate-600 mb-4" />
                    <p className="text-slate-400">No floating storage vessels currently tracked</p>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* SPR Splits Tab */}
          <TabsContent value="spr-splits">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-spr-title">
                  <BarChart3 className="h-5 w-5" />
                  Strategic Petroleum Reserve (SPR) Splits
                </CardTitle>
                <CardDescription>Government reserve storage breakdown by location and grade - {sprReserves?.length || 0} facilities tracked</CardDescription>
              </CardHeader>
              <CardContent>
                {sprLoading ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {[1, 2, 3, 4].map(i => (
                      <Skeleton key={i} className="h-64 bg-slate-800" />
                    ))}
                  </div>
                ) : (
                  <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      {Object.entries(sprByCountry).map(([country, reserves]) => {
                        const capacitySum = reserves.reduce((sum, r) => sum + (r.capacityBarrels ? parseFloat(r.capacityBarrels) : 0), 0);
                        const totalCapacity = capacitySum / 1000000; // Convert to millions
                        const totalLevel = reserves.reduce((sum, r) => sum + parseFloat(r.volumeBarrels || '0'), 0) / 1000000; // Convert to millions
                        const sweetReserves = reserves.filter(r => r.gradeType === 'sweet_crude');
                        const sourReserves = reserves.filter(r => r.gradeType === 'sour_crude');
                        const totalSweet = sweetReserves.reduce((sum, r) => sum + parseFloat(r.volumeBarrels || '0'), 0) / 1000000;
                        const totalSour = sourReserves.reduce((sum, r) => sum + parseFloat(r.volumeBarrels || '0'), 0) / 1000000;
                        const utilRates = reserves.filter(r => r.utilizationRate != null);
                        const avgUtil = utilRates.length > 0 ? utilRates.reduce((sum, r) => sum + parseFloat(r.utilizationRate || '0'), 0) / utilRates.length : 0;
                        const daysReserves = reserves.filter(r => r.daysOfCover != null);
                        const avgDaysSupply = daysReserves.length > 0 ? daysReserves.reduce((sum, r) => sum + (r.daysOfCover || 0), 0) / daysReserves.length : 0;

                        return (
                          <Card key={country} className="bg-slate-800/30 border-slate-700" data-testid={`card-spr-${country.toLowerCase().replace(/\s+/g, '-')}`}>
                            <CardHeader>
                              <CardTitle className="text-white text-lg flex items-center gap-2" data-testid={`text-spr-${country.toLowerCase()}-title`}>
                                <Globe className="h-4 w-4 text-blue-400" />
                                {country} SPR
                              </CardTitle>
                              <CardDescription>{reserves.length} grade types - {avgDaysSupply.toFixed(0)} days cover</CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-4">
                              {totalSweet > 0 && (
                                <div>
                                  <div className="flex justify-between mb-2">
                                    <span className="text-slate-400">Sweet Crude</span>
                                    <span className="text-white font-medium" data-testid={`text-spr-${country.toLowerCase()}-sweet`}>
                                      {totalSweet.toFixed(0)}M bbl ({totalLevel > 0 ? ((totalSweet / totalLevel) * 100).toFixed(0) : 0}%)
                                    </span>
                                  </div>
                                  <Progress value={totalLevel > 0 ? (totalSweet / totalLevel) * 100 : 0} className="h-2" />
                                </div>
                              )}
                              {totalSour > 0 && (
                                <div>
                                  <div className="flex justify-between mb-2">
                                    <span className="text-slate-400">Sour Crude</span>
                                    <span className="text-white font-medium" data-testid={`text-spr-${country.toLowerCase()}-sour`}>
                                      {totalSour.toFixed(0)}M bbl ({totalLevel > 0 ? ((totalSour / totalLevel) * 100).toFixed(0) : 0}%)
                                    </span>
                                  </div>
                                  <Progress value={totalLevel > 0 ? (totalSour / totalLevel) * 100 : 0} className="h-2" />
                                </div>
                              )}
                              <div className="pt-2 border-t border-slate-700">
                                <div className="flex justify-between">
                                  <span className="text-slate-300 font-medium">Total Level</span>
                                  <span className="text-white font-bold" data-testid={`text-spr-${country.toLowerCase()}-total`}>
                                    {totalLevel.toFixed(0)}M bbl
                                  </span>
                                </div>
                                <div className="flex justify-between mt-1">
                                  <span className="text-slate-400 text-sm">Capacity</span>
                                  <span className="text-slate-300 text-sm">{totalCapacity.toFixed(0)}M bbl ({avgUtil.toFixed(1)}% utilized)</span>
                                </div>
                              </div>
                              <div className="text-xs text-slate-500 pt-2">
                                <div className="flex flex-wrap gap-1">
                                  {reserves.map(r => (
                                    <Badge key={r.id} variant="outline" className="text-xs">
                                      {r.gradeType.replace('_', ' ')}: {(parseFloat(r.volumeBarrels) / 1000000).toFixed(0)}M
                                    </Badge>
                                  ))}
                                </div>
                              </div>
                            </CardContent>
                          </Card>
                        );
                      })}
                    </div>

                    {sprReserves && sprReserves.some(r => r.lastReleaseDate) && (
                      <Card className="bg-amber-900/20 border-amber-500/30">
                        <CardContent className="p-4 flex items-start gap-3">
                          <AlertTriangle className="h-5 w-5 text-amber-400 mt-0.5" />
                          <div>
                            <div className="font-medium text-amber-100 mb-1" data-testid="text-spr-alert-title">
                              Recent SPR Activity
                            </div>
                            <div className="text-sm text-amber-200/80" data-testid="text-spr-alert-desc">
                              {sprReserves.filter(r => r.lastReleaseDate).slice(0, 2).map(r => (
                                <div key={r.id}>
                                  {r.country}: {(parseFloat(r.lastReleaseVolume || '0') / 1000000).toFixed(1)}M bbl release on {r.lastReleaseDate ? new Date(r.lastReleaseDate).toLocaleDateString() : 'N/A'}
                                </div>
                              ))}
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Time Series Tab */}
          <TabsContent value="time-series">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="text-white flex items-center gap-2" data-testid="text-time-series-title">
                      <Activity className="h-5 w-5" />
                      Historical Time Series
                    </CardTitle>
                    <CardDescription>Storage level trends and seasonal patterns - 53 weeks of data</CardDescription>
                  </div>
                  <div className="flex gap-4">
                    <div className="flex items-center gap-2">
                      <Globe className="h-4 w-4 text-slate-400" />
                      <select
                        value={timeSeriesRegion}
                        onChange={(e) => setTimeSeriesRegion(e.target.value)}
                        className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-white text-sm"
                        data-testid="select-ts-region"
                      >
                        <option value="global">Global</option>
                        <option value="north_america">North America</option>
                        <option value="europe">Europe</option>
                        <option value="asia">Asia</option>
                        <option value="middle_east">Middle East</option>
                      </select>
                    </div>
                    <div className="flex items-center gap-2">
                      <Database className="h-4 w-4 text-slate-400" />
                      <select
                        value={timeSeriesStorageType}
                        onChange={(e) => setTimeSeriesStorageType(e.target.value)}
                        className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-white text-sm"
                        data-testid="select-ts-type"
                      >
                        <option value="crude_oil">Crude Oil</option>
                        <option value="refined_products">Refined Products</option>
                        <option value="lng">LNG</option>
                      </select>
                    </div>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {timeSeriesLoading ? (
                  <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      {[1, 2, 3].map(i => <Skeleton key={i} className="h-24 bg-slate-800" />)}
                    </div>
                    <Skeleton className="h-64 bg-slate-800" />
                  </div>
                ) : (
                  <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <Card className="bg-slate-800/30 border-slate-700">
                        <CardContent className="p-4">
                          <div className="flex items-center gap-2 mb-2">
                            {weekChange >= 0 ? (
                              <TrendingUp className="h-4 w-4 text-green-400" />
                            ) : (
                              <TrendingDown className="h-4 w-4 text-red-400" />
                            )}
                            <span className="text-sm text-slate-400">Week-over-Week</span>
                          </div>
                          <div className={`text-2xl font-bold ${weekChange >= 0 ? 'text-green-400' : 'text-red-400'}`} data-testid="text-7d-change">
                            {weekChange >= 0 ? '+' : ''}{weekChange.toFixed(2)}%
                          </div>
                          <div className="text-xs text-slate-500 mt-1">
                            {latestTimeSeries?.weekOverWeekChange 
                              ? `${parseFloat(latestTimeSeries.weekOverWeekChange) >= 0 ? '+' : ''}${(parseFloat(latestTimeSeries.weekOverWeekChange) / 1000000).toFixed(1)}M bbl`
                              : 'N/A'}
                          </div>
                        </CardContent>
                      </Card>

                      <Card className="bg-slate-800/30 border-slate-700">
                        <CardContent className="p-4">
                          <div className="flex items-center gap-2 mb-2">
                            {monthChange >= 0 ? (
                              <TrendingUp className="h-4 w-4 text-green-400" />
                            ) : (
                              <TrendingDown className="h-4 w-4 text-red-400" />
                            )}
                            <span className="text-sm text-slate-400">Month-over-Month</span>
                          </div>
                          <div className={`text-2xl font-bold ${monthChange >= 0 ? 'text-green-400' : 'text-red-400'}`} data-testid="text-30d-change">
                            {monthChange >= 0 ? '+' : ''}{monthChange.toFixed(2)}%
                          </div>
                          <div className="text-xs text-slate-500 mt-1">~4 week trend</div>
                        </CardContent>
                      </Card>

                      <Card className="bg-slate-800/30 border-slate-700">
                        <CardContent className="p-4">
                          <div className="flex items-center gap-2 mb-2">
                            <Activity className="h-4 w-4 text-blue-400" />
                            <span className="text-sm text-slate-400">Year-over-Year</span>
                          </div>
                          <div className={`text-2xl font-bold ${yoyChange >= 0 ? 'text-blue-400' : 'text-amber-400'}`} data-testid="text-yoy-change">
                            {yoyChange >= 0 ? '+' : ''}{yoyChange.toFixed(2)}%
                          </div>
                          <div className="text-xs text-slate-500 mt-1">
                            {latestTimeSeries?.yearOverYearChange
                              ? `${parseFloat(latestTimeSeries.yearOverYearChange) >= 0 ? '+' : ''}${(parseFloat(latestTimeSeries.yearOverYearChange) / 1000000000).toFixed(2)}B bbl`
                              : 'N/A'}
                          </div>
                        </CardContent>
                      </Card>
                    </div>

                    {chartData.length > 0 ? (
                      <div className="h-80 bg-slate-800/30 rounded-lg p-4" data-testid="chart-time-series">
                        <ResponsiveContainer width="100%" height="100%">
                          <LineChart data={chartData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                            <XAxis 
                              dataKey="date" 
                              stroke="#94a3b8" 
                              tick={{ fill: '#94a3b8', fontSize: 11 }}
                              interval={Math.floor(chartData.length / 8)}
                            />
                            <YAxis 
                              stroke="#94a3b8" 
                              tick={{ fill: '#94a3b8', fontSize: 11 }}
                              tickFormatter={(v) => `${v.toFixed(1)}B`}
                              domain={['dataMin - 0.1', 'dataMax + 0.1']}
                            />
                            <Tooltip 
                              contentStyle={{ 
                                backgroundColor: '#1e293b', 
                                border: '1px solid #475569',
                                borderRadius: '8px'
                              }}
                              labelStyle={{ color: '#f1f5f9' }}
                              formatter={(value: number, name: string) => [
                                `${value.toFixed(2)}B bbl`, 
                                name === 'level' ? 'Current Level' : name === 'fiveYearAvg' ? '5-Year Avg' : name
                              ]}
                            />
                            <Legend />
                            <Line 
                              type="monotone" 
                              dataKey="level" 
                              stroke="#3b82f6" 
                              strokeWidth={2}
                              dot={false}
                              name="Storage Level"
                            />
                            {chartData.some(d => d.fiveYearAvg !== null) && (
                              <Line 
                                type="monotone" 
                                dataKey="fiveYearAvg" 
                                stroke="#f59e0b" 
                                strokeWidth={1}
                                strokeDasharray="5 5"
                                dot={false}
                                name="5-Year Average"
                              />
                            )}
                          </LineChart>
                        </ResponsiveContainer>
                      </div>
                    ) : (
                      <div className="h-64 bg-slate-800/30 rounded-lg flex items-center justify-center">
                        <div className="text-center text-slate-400">
                          <Activity className="h-12 w-12 mx-auto mb-3 opacity-50" />
                          <p>No time series data available</p>
                          <p className="text-sm text-slate-500 mt-1">Select a different region or storage type</p>
                        </div>
                      </div>
                    )}

                    {latestTimeSeries && (
                      <div className="flex items-center justify-between text-sm text-slate-400 pt-4 border-t border-slate-700">
                        <div className="flex items-center gap-2">
                          <Calendar className="h-4 w-4" />
                          <span>Latest data: {new Date(latestTimeSeries.recordDate).toLocaleDateString()}</span>
                        </div>
                        <div className="flex items-center gap-4">
                          <span>Current: {(parseFloat(latestTimeSeries.currentLevel) / 1000000000).toFixed(2)}B bbl</span>
                          <span>Utilization: {latestTimeSeries.utilizationRate ? parseFloat(latestTimeSeries.utilizationRate).toFixed(1) : 'N/A'}%</span>
                          <MethodologyBadge methodology={parseMethodology(latestTimeSeries.source)} size="sm" />
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
