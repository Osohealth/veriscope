import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Scale, TrendingUp, TrendingDown, Package, Globe, BarChart3 } from "lucide-react";
import { Link } from "wouter";
import type { SupplyDemandBalance } from "@shared/schema";
import { Line, LineChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { MLPredictionCard } from "@/components/ml-prediction-card";

interface SdModelDaily {
  id: string;
  date: string;
  region: string;
  supplyMt: number;
  demandMt: number;
  balanceMt: number;
}

export default function SupplyDemandPage() {
  const [commodityFilter, setCommodityFilter] = useState<string>("all");
  const [regionFilter, setRegionFilter] = useState<string>("all");
  const [csvRegionFilter, setCsvRegionFilter] = useState<string>("all");

  const queryParams = new URLSearchParams();
  if (commodityFilter !== "all") queryParams.append("commodity", commodityFilter);
  if (regionFilter !== "all") queryParams.append("region", regionFilter);
  const queryString = queryParams.toString() ? `?${queryParams.toString()}` : '';

  const { data: balances = [], isLoading } = useQuery<SupplyDemandBalance[]>({
    queryKey: [`/api/supply-demand-balances${queryString}`],
  });

  // Fetch real CSV S&D data
  const { data: csvModels = [], isLoading: isLoadingCsv } = useQuery<SdModelDaily[]>({
    queryKey: ['/api/supply-demand/models-daily', csvRegionFilter],
    queryFn: async () => {
      const params = csvRegionFilter !== "all" ? `?region=${csvRegionFilter}` : '';
      const response = await fetch(`/api/supply-demand/models-daily${params}`);
      return response.json();
    }
  });

  // Calculate statistics
  const totalBalances = balances.length;
  const totalProduction = balances.reduce((sum, b) => sum + parseFloat(b.production as string || '0'), 0);
  const totalConsumption = balances.reduce((sum, b) => sum + parseFloat(b.consumption as string || '0'), 0);
  const netBalance = totalProduction - totalConsumption;

  const commodities = ["all", "crude_oil", "gasoline", "diesel", "lng"];
  const regions = ["all", "global", "north_america", "europe", "asia", "middle_east"];

  // Prepare chart data
  const balanceChartData = balances.map(b => ({
    period: b.period.replace('_', ' '),
    production: parseFloat(b.production as string || '0'),
    consumption: parseFloat(b.consumption as string || '0'),
    balance: parseFloat(b.balanceValue as string || '0')
  }));

  // Prepare CSV chart data (ensure csvModels is an array)
  const safeCsvModels = Array.isArray(csvModels) ? csvModels : [];
  const csvChartData = safeCsvModels
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime())
    .map(m => ({
      date: new Date(m.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      supply: m.supplyMt,
      demand: m.demandMt,
      balance: m.balanceMt
    }));

  // Get unique CSV regions
  const csvRegions = ["all", ...Array.from(new Set(safeCsvModels.map(m => m.region)))];

  // Calculate CSV stats
  const csvStats = {
    totalSupply: safeCsvModels.reduce((sum, m) => sum + m.supplyMt, 0),
    totalDemand: safeCsvModels.reduce((sum, m) => sum + m.demandMt, 0),
    avgBalance: safeCsvModels.length > 0 ? safeCsvModels.reduce((sum, m) => sum + m.balanceMt, 0) / safeCsvModels.length : 0,
    dataPoints: safeCsvModels.length
  };

  const getForecastTypeBadge = (type: string) => {
    switch (type) {
      case 'actual': return 'bg-green-500/20 text-green-400 border-green-500/30';
      case 'estimate': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
      case 'forecast': return 'bg-blue-500/20 text-blue-400 border-blue-500/30';
      default: return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
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
              Supply & Demand Balances
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Global and regional production, consumption, trade flows, and inventory tracking
            </p>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Scale className="h-4 w-4" />
                Total Balances
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-balances">{totalBalances}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-production">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <TrendingUp className="h-4 w-4" />
                Total Production
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-production">{(totalProduction / 1000).toFixed(1)}M</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-consumption">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <TrendingDown className="h-4 w-4" />
                Total Consumption
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-consumption">{(totalConsumption / 1000).toFixed(1)}M</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-balance">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Package className="h-4 w-4" />
                Net Balance
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`text-2xl font-bold ${netBalance >= 0 ? 'text-green-400' : 'text-red-400'}`} data-testid="text-net-balance">
                {netBalance > 0 ? '+' : ''}{(netBalance / 1000).toFixed(1)}M
              </div>
            </CardContent>
          </Card>
        </div>

        {/* ML Price Prediction */}
        <MLPredictionCard 
          commodityType="crude_oil"
          commodityLabel="Crude Oil"
          currentPrice={80}
        />

        {/* Main Content */}
        <Tabs defaultValue="csv" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="csv" className="data-[state=active]:bg-slate-800" data-testid="tab-csv">
                Real CSV Data
              </TabsTrigger>
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Balance Data
              </TabsTrigger>
              <TabsTrigger value="analytics" className="data-[state=active]:bg-slate-800" data-testid="tab-analytics">
                Trend Analytics
              </TabsTrigger>
            </TabsList>

            <div className="flex gap-2">
              <Select value={commodityFilter} onValueChange={setCommodityFilter}>
                <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-commodity">
                  <SelectValue placeholder="Filter by commodity" />
                </SelectTrigger>
                <SelectContent className="bg-slate-900 border-slate-800">
                  {commodities.map(commodity => (
                    <SelectItem key={commodity} value={commodity} className="text-white">
                      {commodity === "all" ? "All Commodities" : commodity.replace('_', ' ').toUpperCase()}
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
                      {region === "all" ? "All Regions" : region.replace('_', ' ').charAt(0).toUpperCase() + region.replace('_', ' ').slice(1)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <TabsContent value="csv" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <BarChart3 className="h-5 w-5 text-blue-400" />
                  Real CSV Data - Supply & Demand Models ({csvStats.dataPoints} records)
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoadingCsv ? (
                  <div className="text-center text-slate-400 py-8">Loading CSV data...</div>
                ) : csvChartData.length === 0 ? (
                  <div className="text-center text-slate-400 py-8">No CSV data available</div>
                ) : (
                  <ResponsiveContainer width="100%" height={400}>
                    <LineChart data={csvChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="date" stroke="#94a3b8" />
                      <YAxis stroke="#94a3b8" label={{ value: 'MT', angle: -90, position: 'insideLeft', style: { fill: '#94a3b8' } }} />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '6px' }}
                        labelStyle={{ color: '#f1f5f9' }}
                      />
                      <Legend wrapperStyle={{ color: '#94a3b8' }} />
                      <Line type="monotone" dataKey="supply" stroke="#3b82f6" name="Supply (MT)" />
                      <Line type="monotone" dataKey="demand" stroke="#ef4444" name="Demand (MT)" />
                      <Line type="monotone" dataKey="balance" stroke="#10b981" name="Balance (MT)" />
                    </LineChart>
                  </ResponsiveContainer>
                )}
                <div className="mt-6 grid grid-cols-3 gap-4 text-center">
                  <div>
                    <div className="text-sm text-slate-400">Total Supply</div>
                    <div className="text-xl font-bold text-blue-400">{csvStats.totalSupply.toLocaleString()} MT</div>
                  </div>
                  <div>
                    <div className="text-sm text-slate-400">Total Demand</div>
                    <div className="text-xl font-bold text-red-400">{csvStats.totalDemand.toLocaleString()} MT</div>
                  </div>
                  <div>
                    <div className="text-sm text-slate-400">Avg Balance</div>
                    <div className="text-xl font-bold text-green-400">{csvStats.avgBalance.toFixed(0)} MT</div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="data" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Globe className="h-5 w-5 text-blue-400" />
                  Supply & Demand Metrics
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-800 hover:bg-slate-800/50">
                        <TableHead className="text-slate-400">Commodity</TableHead>
                        <TableHead className="text-slate-400">Region</TableHead>
                        <TableHead className="text-slate-400">Period</TableHead>
                        <TableHead className="text-slate-400">Production</TableHead>
                        <TableHead className="text-slate-400">Consumption</TableHead>
                        <TableHead className="text-slate-400">Balance</TableHead>
                        <TableHead className="text-slate-400">Inventory</TableHead>
                        <TableHead className="text-slate-400">Type</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {isLoading ? (
                        <TableRow>
                          <TableCell colSpan={8} className="text-center text-slate-400">Loading...</TableCell>
                        </TableRow>
                      ) : balances.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={8} className="text-center text-slate-400">No balances found</TableCell>
                        </TableRow>
                      ) : (
                        balances.map((balance, index) => (
                          <TableRow key={balance.id} className="border-slate-800 hover:bg-slate-800/50" data-testid={`row-balance-${index}`}>
                            <TableCell className="font-medium text-white">{balance.commodity.replace('_', ' ').toUpperCase()}</TableCell>
                            <TableCell className="text-slate-300">{balance.region.replace('_', ' ').charAt(0).toUpperCase() + balance.region.replace('_', ' ').slice(1)}</TableCell>
                            <TableCell className="text-slate-300">{balance.period.replace('_', ' ')}</TableCell>
                            <TableCell className="text-slate-300">{parseFloat(balance.production as string).toLocaleString()} {balance.unit}</TableCell>
                            <TableCell className="text-slate-300">{parseFloat(balance.consumption as string).toLocaleString()} {balance.unit}</TableCell>
                            <TableCell className={parseFloat(balance.balanceValue as string) >= 0 ? 'text-green-400' : 'text-red-400'}>
                              {parseFloat(balance.balanceValue as string) > 0 ? '+' : ''}{balance.balanceValue}
                            </TableCell>
                            <TableCell className="text-slate-300">{balance.closingInventory}</TableCell>
                            <TableCell>
                              <Badge className={getForecastTypeBadge(balance.forecastType as string)}>
                                {balance.forecastType?.toUpperCase()}
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
                  Production vs Consumption Trends
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={400}>
                  <LineChart data={balanceChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="period" stroke="#94a3b8" />
                    <YAxis stroke="#94a3b8" />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '6px' }}
                      labelStyle={{ color: '#f1f5f9' }}
                    />
                    <Legend wrapperStyle={{ color: '#94a3b8' }} />
                    <Line type="monotone" dataKey="production" stroke="#10b981" strokeWidth={2} name="Production" />
                    <Line type="monotone" dataKey="consumption" stroke="#ef4444" strokeWidth={2} name="Consumption" />
                    <Line type="monotone" dataKey="balance" stroke="#3b82f6" strokeWidth={2} name="Balance" />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
