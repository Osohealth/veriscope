import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { DataFreshnessIndicator } from "@/components/data-freshness-indicator";
import { useQuery } from "@tanstack/react-query";
import { 
  Satellite, 
  Activity, 
  Cloud, 
  Flame, 
  Wind, 
  Layers, 
  TrendingUp, 
  TrendingDown, 
  Minus,
  MapPin,
  Calendar,
  AlertTriangle,
  Info,
  ChevronLeft
} from "lucide-react";
import { Link } from "wouter";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Area, AreaChart, ReferenceLine } from "recharts";

interface SummaryStats {
  activityIndex: number;
  confidence: number;
  trend: string;
  lastObservation: string | null;
  weeklyChange: number;
  fourWeekAverage: number;
  swirAnomaly: number;
  plumeIndex: number;
  surfaceChange: number;
  cloudFreePercent: number;
  aoi: {
    name: string;
    code: string;
    facilities: Array<{ name: string; type: string; capacity: number }>;
  };
}

interface TimelineEntry {
  id: string;
  weekStart: string;
  weekEnd: string;
  activityIndex: string;
  confidence: string;
  swirAnomalyIndex: string;
  plumeIndex: string;
  surfaceChangeIndex: string;
  cloudFreePercent: string;
  activityTrend: string;
}

function getTrendIcon(trend: string) {
  switch (trend) {
    case 'increasing':
      return <TrendingUp className="w-4 h-4 text-green-400" />;
    case 'decreasing':
      return <TrendingDown className="w-4 h-4 text-red-400" />;
    case 'anomaly':
      return <AlertTriangle className="w-4 h-4 text-yellow-400" />;
    default:
      return <Minus className="w-4 h-4 text-gray-400" />;
  }
}

function getConfidenceColor(confidence: number): string {
  if (confidence >= 80) return 'text-green-400';
  if (confidence >= 60) return 'text-yellow-400';
  if (confidence >= 40) return 'text-orange-400';
  return 'text-red-400';
}

function getActivityColor(activity: number): string {
  if (activity >= 70) return 'bg-green-500';
  if (activity >= 50) return 'bg-yellow-500';
  if (activity >= 30) return 'bg-orange-500';
  return 'bg-red-500';
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return 'N/A';
  return new Date(dateStr).toLocaleDateString('en-US', { 
    month: 'short', 
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function formatWeek(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('en-US', { 
    month: 'short', 
    day: 'numeric'
  });
}

export default function RefinerySatellite() {
  const { data: summary, dataUpdatedAt, isLoading: summaryLoading } = useQuery<SummaryStats>({
    queryKey: ['/api/refinery/summary'],
    refetchInterval: 60000,
  });

  const { data: timeline = [], isLoading: timelineLoading } = useQuery<TimelineEntry[]>({
    queryKey: ['/api/refinery/activity/timeline'],
    refetchInterval: 60000,
  });

  const chartData = [...timeline].reverse().map(entry => ({
    week: formatWeek(entry.weekStart),
    activityIndex: parseFloat(entry.activityIndex),
    confidence: parseFloat(entry.confidence),
    swirAnomaly: parseFloat(entry.swirAnomalyIndex),
    plumeIndex: parseFloat(entry.plumeIndex),
    cloudFree: parseFloat(entry.cloudFreePercent),
  }));

  const isLoading = summaryLoading || timelineLoading;

  return (
    <div className="min-h-screen bg-[#0A0B1E] text-white">
      <header className="flex items-center justify-between p-6 border-b border-gray-800">
        <div className="flex items-center gap-4">
          <Link href="/dashboard">
            <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-back">
              <ChevronLeft className="w-5 h-5" />
            </Button>
          </Link>
          <div className="flex items-center gap-3">
            <Satellite className="w-6 h-6 text-blue-400" />
            <div>
              <h1 className="text-2xl font-bold">Rotterdam Refinery Satellite Monitoring</h1>
              <p className="text-sm text-gray-400">Sentinel-2 Based Activity Intelligence</p>
            </div>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <Badge variant="outline" className="border-blue-500 text-blue-400">
            <Satellite className="w-3 h-3 mr-1" /> Sentinel-2 L2A
          </Badge>
          <DataFreshnessIndicator 
            lastUpdate={dataUpdatedAt ? new Date(dataUpdatedAt) : undefined}
            streamName="Satellite Data"
            showLabel={true}
          />
        </div>
      </header>

      <div className="p-6 space-y-6">
        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
          </div>
        ) : (
          <>
            <div className="grid grid-cols-12 gap-6">
              <Card className="col-span-4 bg-gray-900/50 border-gray-700">
                <CardHeader className="pb-2">
                  <CardTitle className="text-white flex items-center gap-2">
                    <Activity className="w-5 h-5 text-blue-400" />
                    Refinery Activity Index
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger>
                          <Info className="w-4 h-4 text-gray-500" />
                        </TooltipTrigger>
                        <TooltipContent className="max-w-xs">
                          <p>Composite index (0-100) derived from SWIR flaring anomalies, plume visibility, and surface changes. Higher values indicate more industrial activity.</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </CardTitle>
                  <CardDescription className="text-gray-400">
                    {summary?.aoi?.name || 'Rotterdam Cluster'}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex items-center gap-6">
                    <div className="relative w-32 h-32">
                      <svg viewBox="0 0 100 100" className="w-full h-full transform -rotate-90">
                        <circle cx="50" cy="50" r="45" fill="none" stroke="#374151" strokeWidth="10" />
                        <circle 
                          cx="50" cy="50" r="45" fill="none" 
                          stroke="#3B82F6" strokeWidth="10"
                          strokeDasharray={`${(summary?.activityIndex || 0) * 2.83} 283`}
                          strokeLinecap="round"
                          data-testid="activity-ring"
                        />
                      </svg>
                      <div className="absolute inset-0 flex flex-col items-center justify-center">
                        <span className="text-3xl font-bold" data-testid="activity-value">
                          {summary?.activityIndex?.toFixed(0) || 0}
                        </span>
                        <span className="text-xs text-gray-400">/ 100</span>
                      </div>
                    </div>
                    <div className="space-y-3 flex-1">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-gray-400">Confidence</span>
                        <span className={`text-sm font-medium ${getConfidenceColor(summary?.confidence || 0)}`} data-testid="confidence-value">
                          {summary?.confidence?.toFixed(0) || 0}%
                        </span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-gray-400">Trend</span>
                        <div className="flex items-center gap-1" data-testid="trend-indicator">
                          {getTrendIcon(summary?.trend || 'stable')}
                          <span className="text-sm capitalize">{summary?.trend || 'stable'}</span>
                        </div>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-gray-400">Weekly Change</span>
                        <span className={`text-sm font-medium ${summary?.weeklyChange && summary.weeklyChange > 0 ? 'text-green-400' : summary?.weeklyChange && summary.weeklyChange < 0 ? 'text-red-400' : 'text-gray-400'}`}>
                          {summary?.weeklyChange ? (summary.weeklyChange > 0 ? '+' : '') + summary.weeklyChange.toFixed(1) : '0'}
                        </span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-gray-400">4-Week Avg</span>
                        <span className="text-sm text-white">{summary?.fourWeekAverage?.toFixed(1) || 'N/A'}</span>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="col-span-4 bg-gray-900/50 border-gray-700">
                <CardHeader className="pb-2">
                  <CardTitle className="text-white flex items-center gap-2">
                    <Flame className="w-5 h-5 text-orange-400" />
                    Signal Breakdown
                  </CardTitle>
                  <CardDescription className="text-gray-400">Individual proxy metrics</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Flame className="w-4 h-4 text-orange-400" />
                        <span className="text-sm text-gray-300">SWIR Anomaly (Flaring)</span>
                      </div>
                      <span className="text-sm font-medium text-white" data-testid="swir-value">
                        {summary?.swirAnomaly?.toFixed(1) || 'N/A'}
                      </span>
                    </div>
                    <div className="h-2 bg-gray-700 rounded-full">
                      <div 
                        className="h-full bg-orange-500 rounded-full"
                        style={{ width: `${summary?.swirAnomaly || 0}%` }}
                      />
                    </div>
                  </div>
                  
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Wind className="w-4 h-4 text-blue-300" />
                        <span className="text-sm text-gray-300">Plume Index (Steam)</span>
                      </div>
                      <span className="text-sm font-medium text-white" data-testid="plume-value">
                        {summary?.plumeIndex?.toFixed(1) || 'N/A'}
                      </span>
                    </div>
                    <div className="h-2 bg-gray-700 rounded-full">
                      <div 
                        className="h-full bg-blue-400 rounded-full"
                        style={{ width: `${summary?.plumeIndex || 0}%` }}
                      />
                    </div>
                  </div>
                  
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Layers className="w-4 h-4 text-purple-400" />
                        <span className="text-sm text-gray-300">Surface Change</span>
                      </div>
                      <span className="text-sm font-medium text-white" data-testid="surface-value">
                        {summary?.surfaceChange?.toFixed(1) || 'N/A'}
                      </span>
                    </div>
                    <div className="h-2 bg-gray-700 rounded-full">
                      <div 
                        className="h-full bg-purple-500 rounded-full"
                        style={{ width: `${summary?.surfaceChange || 0}%` }}
                      />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="col-span-4 bg-gray-900/50 border-gray-700">
                <CardHeader className="pb-2">
                  <CardTitle className="text-white flex items-center gap-2">
                    <Cloud className="w-5 h-5 text-gray-300" />
                    Observation Status
                  </CardTitle>
                  <CardDescription className="text-gray-400">Scene quality and coverage</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
                    <div className="flex items-center gap-2">
                      <Calendar className="w-4 h-4 text-blue-400" />
                      <span className="text-sm text-gray-300">Last Clear Observation</span>
                    </div>
                    <span className="text-sm font-medium text-white" data-testid="last-observation">
                      {formatDate(summary?.lastObservation || null)}
                    </span>
                  </div>
                  
                  <div className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
                    <div className="flex items-center gap-2">
                      <Cloud className="w-4 h-4 text-gray-400" />
                      <span className="text-sm text-gray-300">Cloud-Free Coverage</span>
                    </div>
                    <span className={`text-sm font-medium ${summary?.cloudFreePercent && summary.cloudFreePercent > 60 ? 'text-green-400' : 'text-yellow-400'}`}>
                      {summary?.cloudFreePercent?.toFixed(0) || 'N/A'}%
                    </span>
                  </div>
                  
                  <div className="p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <MapPin className="w-4 h-4 text-blue-400" />
                      <span className="text-sm font-medium text-blue-400">Key Facilities</span>
                    </div>
                    <div className="space-y-1">
                      {summary?.aoi?.facilities?.slice(0, 3).map((facility, i) => (
                        <div key={i} className="text-xs text-gray-300 flex justify-between">
                          <span>{facility.name}</span>
                          <span className="text-gray-500">{(facility.capacity / 1000).toFixed(0)}k bpd</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card className="bg-gray-900/50 border-gray-700">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Activity className="w-5 h-5 text-blue-400" />
                  Activity Timeline (12 Weeks)
                </CardTitle>
                <CardDescription className="text-gray-400">
                  Weekly activity index with confidence bands
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                      <defs>
                        <linearGradient id="activityGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.3}/>
                          <stop offset="95%" stopColor="#3B82F6" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis 
                        dataKey="week" 
                        stroke="#9CA3AF" 
                        tick={{ fill: '#9CA3AF', fontSize: 12 }}
                      />
                      <YAxis 
                        domain={[0, 100]} 
                        stroke="#9CA3AF" 
                        tick={{ fill: '#9CA3AF', fontSize: 12 }}
                      />
                      <ReferenceLine 
                        y={summary?.fourWeekAverage || 65} 
                        stroke="#10B981" 
                        strokeDasharray="5 5" 
                        label={{ value: '4-wk avg', fill: '#10B981', fontSize: 10 }}
                      />
                      <Area 
                        type="monotone" 
                        dataKey="activityIndex" 
                        stroke="#3B82F6" 
                        fillOpacity={1} 
                        fill="url(#activityGradient)"
                        strokeWidth={2}
                        name="Activity Index"
                        data-testid="timeline-chart"
                      />
                      <Line 
                        type="monotone" 
                        dataKey="confidence" 
                        stroke="#6B7280" 
                        strokeDasharray="3 3"
                        strokeWidth={1}
                        dot={false}
                        name="Confidence"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
                <div className="flex items-center justify-center gap-6 mt-4 text-sm">
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 bg-blue-500 rounded-full" />
                    <span className="text-gray-400">Activity Index</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-0.5 bg-gray-500" style={{ borderTop: '2px dashed #6B7280' }} />
                    <span className="text-gray-400">Confidence</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-0.5 bg-green-500" style={{ borderTop: '2px dashed #10B981' }} />
                    <span className="text-gray-400">4-Week Avg</span>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="grid grid-cols-12 gap-6">
              <Card className="col-span-8 bg-gray-900/50 border-gray-700">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2">
                    <MapPin className="w-5 h-5 text-green-400" />
                    Rotterdam Cluster AOI
                  </CardTitle>
                  <CardDescription className="text-gray-400">
                    Industrial belt: Pernis → Botlek → Europoort → Maasvlakte
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64 bg-gray-800 rounded-lg flex items-center justify-center relative overflow-hidden" data-testid="aoi-map">
                    <div className="absolute inset-0 bg-gradient-to-br from-blue-900/30 to-green-900/30" />
                    <div className="absolute inset-4 border-2 border-dashed border-blue-500/50 rounded-lg" />
                    <div className="absolute top-8 left-8 flex flex-col gap-1">
                      <Badge className="bg-red-500/80 text-white text-xs">Shell Pernis</Badge>
                    </div>
                    <div className="absolute top-12 left-1/3 flex flex-col gap-1">
                      <Badge className="bg-orange-500/80 text-white text-xs">ExxonMobil</Badge>
                    </div>
                    <div className="absolute top-16 right-1/4 flex flex-col gap-1">
                      <Badge className="bg-green-500/80 text-white text-xs">BP Rotterdam</Badge>
                    </div>
                    <div className="text-center z-10">
                      <Satellite className="w-12 h-12 text-blue-400 mx-auto mb-2 opacity-50" />
                      <p className="text-gray-500 text-sm">Sentinel-2 Coverage Area</p>
                      <p className="text-gray-600 text-xs mt-1">51.85°N - 51.98°N, 4.00°E - 4.25°E</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="col-span-4 bg-gray-900/50 border-gray-700">
                <CardHeader>
                  <CardTitle className="text-white">Weekly History</CardTitle>
                  <CardDescription className="text-gray-400">Recent activity readings</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2 max-h-64 overflow-y-auto" data-testid="weekly-history">
                    {timeline.slice(0, 8).map((entry, i) => (
                      <div 
                        key={entry.id} 
                        className={`flex items-center justify-between p-2 rounded-lg ${i === 0 ? 'bg-blue-500/20 border border-blue-500/30' : 'bg-gray-800/50'}`}
                      >
                        <div className="flex items-center gap-2">
                          <div className={`w-2 h-2 rounded-full ${getActivityColor(parseFloat(entry.activityIndex))}`} />
                          <span className="text-sm text-gray-300">{formatWeek(entry.weekStart)}</span>
                        </div>
                        <div className="flex items-center gap-3">
                          <span className="text-sm font-medium text-white">
                            {parseFloat(entry.activityIndex).toFixed(0)}
                          </span>
                          {getTrendIcon(entry.activityTrend)}
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </div>

            <div className="p-4 bg-gray-800/30 rounded-lg border border-gray-700">
              <div className="flex items-start gap-3">
                <Info className="w-5 h-5 text-blue-400 mt-0.5" />
                <div>
                  <h4 className="text-sm font-medium text-gray-300 mb-1">Methodology Note</h4>
                  <p className="text-xs text-gray-500">
                    Activity indices are derived from Sentinel-2 L2A imagery using SWIR band anomaly detection for flaring/combustion, 
                    RGB segmentation for steam/plume visibility, and multi-temporal reflectance analysis for surface changes. 
                    Confidence scores reflect cloud-free coverage and scene quality. Data refreshes weekly with best available scene selection.
                  </p>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
