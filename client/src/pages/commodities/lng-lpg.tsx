import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Ship, AlertTriangle, DollarSign, TrendingUp, Activity } from "lucide-react";
import { Link } from "wouter";
import type { LngCargo } from "@shared/schema";

export default function LngLpgPage() {
  const [cargoTypeFilter, setCargoTypeFilter] = useState<string>("all");

  const queryParams = cargoTypeFilter !== "all" ? `?cargoType=${cargoTypeFilter}` : '';
  const { data: cargoes = [], isLoading } = useQuery<LngCargo[]>({
    queryKey: [`/api/lng-cargoes${queryParams}`],
  });

  // Calculate statistics
  const totalCargoes = cargoes.length;
  const diversionCount = cargoes.filter(c => c.isDiversion).length;
  const avgVolume = cargoes.length > 0
    ? cargoes.reduce((sum, c) => sum + parseFloat(c.volume as string || '0'), 0) / cargoes.length
    : 0;
  const totalValue = cargoes.reduce((sum, c) => {
    const volume = parseFloat(c.volume as string || '0');
    const price = parseFloat(c.price as string || '0');
    return sum + (volume * price);
  }, 0);

  const cargoTypes = ["all", "LNG", "LPG"];

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
            <h1 className="text-3xl font-bold bg-gradient-to-r from-cyan-400 to-blue-400 bg-clip-text text-transparent" data-testid="text-page-title">
              LNG / LPG Pack
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Cargo tracking, diversions, terminals, and price curves
            </p>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Ship className="h-4 w-4" />
                Total Cargoes
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-cargoes">{totalCargoes}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-diversions">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <AlertTriangle className="h-4 w-4" />
                Diversions
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-orange-400" data-testid="text-diversion-count">{diversionCount}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-volume">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Activity className="h-4 w-4" />
                Avg Volume
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-avg-volume">{avgVolume.toFixed(0)} CBM</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-value">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <DollarSign className="h-4 w-4" />
                Total Value
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-400" data-testid="text-total-value">${(totalValue / 1000000).toFixed(1)}M</div>
            </CardContent>
          </Card>
        </div>

        {/* Main Content */}
        <Tabs defaultValue="data" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Cargo Data
              </TabsTrigger>
              <TabsTrigger value="analytics" className="data-[state=active]:bg-slate-800" data-testid="tab-analytics">
                Analytics
              </TabsTrigger>
            </TabsList>

            <Select value={cargoTypeFilter} onValueChange={setCargoTypeFilter}>
              <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-cargo-type">
                <SelectValue placeholder="Filter by cargo type" />
              </SelectTrigger>
              <SelectContent>
                {cargoTypes.map(type => (
                  <SelectItem key={type} value={type}>
                    {type === "all" ? "All Cargo Types" : type}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <TabsContent value="data" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Ship className="h-5 w-5 text-cyan-400" />
                  LNG/LPG Cargo Tracking
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-loading">Loading cargoes...</div>
                ) : cargoes.length === 0 ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-no-data">No cargoes found</div>
                ) : (
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="border-slate-800">
                          <TableHead className="text-slate-400">Cargo ID</TableHead>
                          <TableHead className="text-slate-400">Type</TableHead>
                          <TableHead className="text-slate-400">Volume (CBM)</TableHead>
                          <TableHead className="text-slate-400">Load Port</TableHead>
                          <TableHead className="text-slate-400">Discharge Port</TableHead>
                          <TableHead className="text-slate-400">Load Date</TableHead>
                          <TableHead className="text-slate-400">Status</TableHead>
                          <TableHead className="text-slate-400">Value</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {cargoes.map((cargo) => (
                          <TableRow key={cargo.id} className="border-slate-800" data-testid={`row-cargo-${cargo.cargoId}`}>
                            <TableCell className="font-mono text-cyan-400" data-testid={`text-id-${cargo.cargoId}`}>
                              {cargo.cargoId}
                            </TableCell>
                            <TableCell>
                              <Badge variant="outline" className="border-slate-700 text-slate-300">
                                {cargo.cargoType}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-slate-300" data-testid={`text-volume-${cargo.cargoId}`}>
                              {parseFloat(cargo.volume as string || '0').toLocaleString()}
                            </TableCell>
                            <TableCell className="text-slate-300">{cargo.loadPortId}</TableCell>
                            <TableCell className="text-slate-300">{cargo.dischargePortId}</TableCell>
                            <TableCell className="text-slate-300">
                              {cargo.loadDate ? new Date(cargo.loadDate).toLocaleDateString() : 'N/A'}
                            </TableCell>
                            <TableCell>
                              {cargo.isDiversion ? (
                                <Badge className="bg-orange-500/20 text-orange-400 border-orange-500/30">
                                  <AlertTriangle className="h-3 w-3 mr-1" />
                                  Diversion
                                </Badge>
                              ) : (
                                <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
                                  Normal
                                </Badge>
                              )}
                            </TableCell>
                            <TableCell className="text-white font-medium" data-testid={`text-value-${cargo.cargoId}`}>
                              ${((parseFloat(cargo.volume as string || '0') * parseFloat(cargo.price as string || '0')) / 1000000).toFixed(1)}M
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="analytics" className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2">
                    <TrendingUp className="h-5 w-5 text-cyan-400" />
                    Cargo Value Distribution
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {cargoes.slice(0, 5).map((cargo) => {
                      const value = parseFloat(cargo.volume as string || '0') * parseFloat(cargo.price as string || '0');
                      const maxValue = Math.max(...cargoes.map(c => parseFloat(c.volume as string || '0') * parseFloat(c.price as string || '0')));
                      const widthPercent = maxValue > 0 ? (value / maxValue) * 100 : 0;
                      
                      return (
                        <div key={cargo.id} className="space-y-1" data-testid={`chart-value-${cargo.cargoId}`}>
                          <div className="flex justify-between text-sm">
                            <span className="text-slate-300">{cargo.cargoId}</span>
                            <span className="text-white font-medium">${(value / 1000000).toFixed(1)}M</span>
                          </div>
                          <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-gradient-to-r from-cyan-500 to-blue-400"
                              style={{ width: `${widthPercent}%` }}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-slate-900/50 border-slate-800">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2">
                    <AlertTriangle className="h-5 w-5 text-orange-400" />
                    Diversion Analysis
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <div className="flex justify-between mb-2">
                        <span className="text-slate-400 text-sm">Diverted Cargoes</span>
                        <span className="text-white font-medium">
                          {diversionCount} ({totalCargoes > 0 ? ((diversionCount / totalCargoes) * 100).toFixed(0) : 0}%)
                        </span>
                      </div>
                      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-orange-500"
                          style={{ width: `${totalCargoes > 0 ? (diversionCount / totalCargoes) * 100 : 0}%` }}
                        />
                      </div>
                    </div>
                    <div>
                      <div className="flex justify-between mb-2">
                        <span className="text-slate-400 text-sm">Normal Cargoes</span>
                        <span className="text-white font-medium">
                          {totalCargoes - diversionCount} ({totalCargoes > 0 ? (((totalCargoes - diversionCount) / totalCargoes) * 100).toFixed(0) : 0}%)
                        </span>
                      </div>
                      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-green-500"
                          style={{ width: `${totalCargoes > 0 ? ((totalCargoes - diversionCount) / totalCargoes) * 100 : 0}%` }}
                        />
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
