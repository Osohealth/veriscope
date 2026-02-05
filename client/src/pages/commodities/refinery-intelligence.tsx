import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Factory, Gauge, TrendingUp, DollarSign, Wrench, BarChart3 } from "lucide-react";
import { Link } from "wouter";
import type { Refinery } from "@shared/schema";
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { MLPredictionCard } from "@/components/ml-prediction-card";
import { getAuthHeaders } from "@/lib/queryClient";

interface RefineryUtilization {
  id: string;
  date: string;
  plant: string;
  utilizationPct: string;
}

export default function RefineryIntelligencePage() {
  const [regionFilter, setRegionFilter] = useState<string>("all");
  const [selectedPlant, setSelectedPlant] = useState<string>("all");

  const queryParams = regionFilter !== "all" ? `?region=${regionFilter}` : '';
  const { data: refineries = [], isLoading } = useQuery<Refinery[]>({
    queryKey: [`/api/refineries${queryParams}`],
  });

  // Fetch real CSV-based utilization data
  const { data: utilizationData = [], isLoading: isLoadingUtilization } = useQuery<RefineryUtilization[]>({
    queryKey: ['/api/refinery/utilization', selectedPlant],
    queryFn: async () => {
      const params = selectedPlant !== "all" ? `?plant=${selectedPlant}` : '';
      const response = await fetch(`/api/refinery/utilization${params}`, { headers: getAuthHeaders() });
      return response.json();
    }
  });

  // Calculate statistics
  const totalRefineries = refineries.length;
  const totalCapacity = refineries.reduce((sum, r) => sum + parseFloat(r.capacity as string || '0'), 0);
  const avgUtilization = refineries.length > 0 
    ? refineries.reduce((sum, r) => sum + parseFloat(r.utilizationRate as string || '0'), 0) / refineries.length 
    : 0;
  const avgMargin = refineries.length > 0
    ? refineries.reduce((sum, r) => sum + parseFloat(r.marginPerBarrel as string || '0'), 0) / refineries.length
    : 0;

  const regions = ["all", "North America", "Europe", "Asia Pacific", "Middle East"];

  // Prepare chart data
  const yieldChartData = refineries.map(r => ({
    name: r.name.split(' ')[0],
    gasoline: parseFloat(r.yieldGasoline as string || '0'),
    diesel: parseFloat(r.yieldDiesel as string || '0'),
    jetFuel: parseFloat(r.yieldJetFuel as string || '0'),
    other: parseFloat(r.yieldOther as string || '0')
  }));

  // Prepare utilization time-series chart data (ensure utilizationData is an array)
  const safeUtilizationData = Array.isArray(utilizationData) ? utilizationData : [];
  const utilizationChartData = safeUtilizationData
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime())
    .map(u => ({
      date: new Date(u.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      utilization: parseFloat(u.utilizationPct),
      plant: u.plant
    }));

  // Get unique plants from utilization data
  const plants = ["all", ...Array.from(new Set(safeUtilizationData.map(u => u.plant)))];

  // Calculate real-time stats from CSV data
  const csvStats = {
    avgUtilization: safeUtilizationData.length > 0
      ? safeUtilizationData.reduce((sum, u) => sum + parseFloat(u.utilizationPct), 0) / safeUtilizationData.length
      : 0,
    maxUtilization: safeUtilizationData.length > 0
      ? Math.max(...safeUtilizationData.map(u => parseFloat(u.utilizationPct)))
      : 0,
    dataPoints: safeUtilizationData.length
  };

  const getMaintenanceStatusColor = (status: string) => {
    switch (status) {
      case 'operational': return 'bg-green-500/20 text-green-400 border-green-500/30';
      case 'planned_maintenance': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
      case 'unplanned_outage': return 'bg-red-500/20 text-red-400 border-red-500/30';
      default: return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
    }
  };

  const getMaintenanceStatusLabel = (status: string) => {
    switch (status) {
      case 'operational': return 'Operational';
      case 'planned_maintenance': return 'Maintenance';
      case 'unplanned_outage': return 'Outage';
      default: return status;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 text-white p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <Link href="/commodities">
              <Button variant="ghost" className="mb-4 text-slate-400 hover:text-white" data-testid="button-back">
                <ArrowLeft className="mr-2 h-4 w-4" />
                Back to Commodities
              </Button>
            </Link>
            <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent" data-testid="text-page-title">
              Refinery / Plant Intelligence
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Capacity tracking, utilization rates, product yields, and maintenance schedules
            </p>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Factory className="h-4 w-4" />
                Total Refineries
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-refineries">{totalRefineries}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-capacity">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Gauge className="h-4 w-4" />
                Total Capacity
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-capacity">{(totalCapacity / 1000).toFixed(1)}K bpd</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-utilization">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <TrendingUp className="h-4 w-4" />
                Avg Utilization
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-avg-utilization">{avgUtilization.toFixed(1)}%</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-margin">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <DollarSign className="h-4 w-4" />
                Avg Margin
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-avg-margin">${avgMargin.toFixed(2)}/bbl</div>
            </CardContent>
          </Card>
        </div>

        {/* ML Price Prediction */}
        <MLPredictionCard 
          commodityType="refined_products"
          commodityLabel="Refined Products"
          currentPrice={85}
        />

        {/* Main Content */}
        <Tabs defaultValue="realtime" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="realtime" className="data-[state=active]:bg-slate-800" data-testid="tab-realtime">
                Real-Time Data (CSV)
              </TabsTrigger>
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Refinery Data
              </TabsTrigger>
              <TabsTrigger value="analytics" className="data-[state=active]:bg-slate-800" data-testid="tab-analytics">
                Yield Analytics
              </TabsTrigger>
            </TabsList>

            <div className="flex gap-2">
              <Select value={selectedPlant} onValueChange={setSelectedPlant}>
                <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-plant">
                  <SelectValue placeholder="Filter by plant" />
                </SelectTrigger>
                <SelectContent className="bg-slate-900 border-slate-800">
                  {plants.map(plant => (
                    <SelectItem key={plant} value={plant} className="text-white">
                      {plant === "all" ? "All Plants" : plant}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={regionFilter} onValueChange={setRegionFilter}>
                <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-region">
                  <SelectValue placeholder="Filter by region" />
                </SelectTrigger>
                <SelectContent className="bg-slate-900 border-slate-800">
                  {regions.map(region => (
                    <SelectItem key={region} value={region} className="text-white">
                      {region === "all" ? "All Regions" : region}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <TabsContent value="realtime" className="space-y-4">
            {/* Real-time CSV Data Stats */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-slate-400">Avg Utilization (CSV)</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-green-400" data-testid="text-csv-avg-utilization">
                    {csvStats.avgUtilization.toFixed(1)}%
                  </div>
                </CardContent>
              </Card>
              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-slate-400">Max Utilization</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-blue-400" data-testid="text-csv-max-utilization">
                    {csvStats.maxUtilization.toFixed(1)}%
                  </div>
                </CardContent>
              </Card>
              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-slate-400">Data Points</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-cyan-400" data-testid="text-csv-data-points">
                    {csvStats.dataPoints}
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Utilization Time Series Chart */}
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="h-5 w-5 text-blue-400" />
                  Refinery Utilization - Time Series (Real CSV Data)
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoadingUtilization ? (
                  <div className="text-center text-slate-400 py-8">Loading utilization data...</div>
                ) : utilizationChartData.length === 0 ? (
                  <div className="text-center text-slate-400 py-8">No utilization data available</div>
                ) : (
                  <ResponsiveContainer width="100%" height={400}>
                    <BarChart data={utilizationChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="date" stroke="#94a3b8" />
                      <YAxis stroke="#94a3b8" label={{ value: 'Utilization %', angle: -90, position: 'insideLeft', style: { fill: '#94a3b8' } }} />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '6px' }}
                        labelStyle={{ color: '#f1f5f9' }}
                      />
                      <Legend wrapperStyle={{ color: '#94a3b8' }} />
                      <Bar dataKey="utilization" fill="#3b82f6" name="Utilization %" />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </CardContent>
            </Card>

            {/* Raw CSV Data Table */}
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Factory className="h-5 w-5 text-blue-400" />
                  Raw CSV Data - Refinery Utilization
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-800 hover:bg-slate-800/50">
                        <TableHead className="text-slate-400">Date</TableHead>
                        <TableHead className="text-slate-400">Plant</TableHead>
                        <TableHead className="text-slate-400">Utilization %</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {isLoadingUtilization ? (
                        <TableRow>
                          <TableCell colSpan={3} className="text-center text-slate-400">Loading...</TableCell>
                        </TableRow>
                      ) : safeUtilizationData.slice(0, 30).map((util, index) => (
                        <TableRow key={util.id} className="border-slate-800 hover:bg-slate-800/50" data-testid={`row-utilization-${index}`}>
                          <TableCell className="text-slate-300">{new Date(util.date).toLocaleDateString()}</TableCell>
                          <TableCell className="font-medium text-white">{util.plant}</TableCell>
                          <TableCell className="text-green-400">{parseFloat(util.utilizationPct).toFixed(2)}%</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  {safeUtilizationData.length > 30 && (
                    <div className="text-center text-slate-400 text-sm mt-4">
                      Showing 30 of {safeUtilizationData.length} records
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="data" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Factory className="h-5 w-5 text-blue-400" />
                  Refinery Operations
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-800 hover:bg-slate-800/50">
                        <TableHead className="text-slate-400">Refinery</TableHead>
                        <TableHead className="text-slate-400">Region</TableHead>
                        <TableHead className="text-slate-400">Operator</TableHead>
                        <TableHead className="text-slate-400">Capacity (bpd)</TableHead>
                        <TableHead className="text-slate-400">Utilization</TableHead>
                        <TableHead className="text-slate-400">Complexity</TableHead>
                        <TableHead className="text-slate-400">Margin ($/bbl)</TableHead>
                        <TableHead className="text-slate-400">Status</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {isLoading ? (
                        <TableRow>
                          <TableCell colSpan={8} className="text-center text-slate-400">Loading...</TableCell>
                        </TableRow>
                      ) : refineries.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={8} className="text-center text-slate-400">No refineries found</TableCell>
                        </TableRow>
                      ) : (
                        refineries.map((refinery, index) => (
                          <TableRow key={refinery.id} className="border-slate-800 hover:bg-slate-800/50" data-testid={`row-refinery-${index}`}>
                            <TableCell className="font-medium text-white">{refinery.name}</TableCell>
                            <TableCell className="text-slate-300">{refinery.region}</TableCell>
                            <TableCell className="text-slate-300">{refinery.operator}</TableCell>
                            <TableCell className="text-slate-300">{parseFloat(refinery.capacity as string).toLocaleString()}</TableCell>
                            <TableCell className="text-slate-300">{refinery.utilizationRate}%</TableCell>
                            <TableCell className="text-slate-300">{refinery.complexityIndex}</TableCell>
                            <TableCell className="text-green-400">${refinery.marginPerBarrel}</TableCell>
                            <TableCell>
                              <Badge className={getMaintenanceStatusColor(refinery.maintenanceStatus as string)}>
                                {getMaintenanceStatusLabel(refinery.maintenanceStatus as string)}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="analytics" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <BarChart3 className="h-5 w-5 text-blue-400" />
                  Product Yield Distribution
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={400}>
                  <BarChart data={yieldChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="name" stroke="#94a3b8" />
                    <YAxis stroke="#94a3b8" label={{ value: 'Yield %', angle: -90, position: 'insideLeft', style: { fill: '#94a3b8' } }} />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '6px' }}
                      labelStyle={{ color: '#f1f5f9' }}
                    />
                    <Legend wrapperStyle={{ color: '#94a3b8' }} />
                    <Bar dataKey="gasoline" fill="#3b82f6" name="Gasoline" />
                    <Bar dataKey="diesel" fill="#10b981" name="Diesel" />
                    <Bar dataKey="jetFuel" fill="#f59e0b" name="Jet Fuel" />
                    <Bar dataKey="other" fill="#6366f1" name="Other" />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
