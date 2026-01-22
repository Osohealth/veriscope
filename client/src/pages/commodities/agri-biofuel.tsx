import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Leaf, TrendingUp, DollarSign, Droplets } from "lucide-react";
import { Link } from "wouter";
import type { AgriBiofuelFlow } from "@shared/schema";

export default function AgriBiofuelPage() {
  const [productFilter, setProductFilter] = useState<string>("all");

  const queryParams = productFilter !== "all" ? `?commodityType=${productFilter}` : '';
  const { data: flows = [], isLoading } = useQuery<AgriBiofuelFlow[]>({
    queryKey: [`/api/agri-biofuel-flows${queryParams}`],
  });

  const totalFlows = flows.length;
  const sustainableFlows = flows.filter(f => f.sustainabilityCert).length;
  const totalVolume = flows.reduce((sum, f) => sum + parseFloat(f.volume as string || '0'), 0);
  const avgCarbonIntensity = flows.length > 0
    ? flows.reduce((sum, f) => sum + parseFloat(f.carbonIntensity as string || '0'), 0) / flows.length
    : 0;

  const productTypes = ["all", "Soybean Oil", "Palm Oil", "Rapeseed Oil", "Corn Ethanol", "Sugar Ethanol"];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 text-white p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <Link href="/commodities">
              <Button variant="ghost" className="mb-4 text-slate-400 hover:text-white" data-testid="button-back">
                <ArrowLeft className="mr-2 h-4 w-4" />
                Back to Commodities
              </Button>
            </Link>
            <h1 className="text-3xl font-bold bg-gradient-to-r from-green-400 to-emerald-400 bg-clip-text text-transparent" data-testid="text-page-title">
              Agri / Biofuel Pack
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Oilseed flows, biofuel production, and sustainability overlay
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Leaf className="h-4 w-4" />
                Total Flows
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-flows">{totalFlows}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <DollarSign className="h-4 w-4" />
                Total Volume
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{(totalVolume / 1000).toFixed(0)}K MT</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Droplets className="h-4 w-4" />
                Avg Carbon Intensity
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-400">{avgCarbonIntensity.toFixed(1)} gCO2e/MJ</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Leaf className="h-4 w-4" />
                Sustainable
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-400">{sustainableFlows}</div>
            </CardContent>
          </Card>
        </div>

        <Tabs defaultValue="data" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Flow Data
              </TabsTrigger>
              <TabsTrigger value="analytics" className="data-[state=active]:bg-slate-800" data-testid="tab-analytics">
                Analytics
              </TabsTrigger>
            </TabsList>

            <Select value={productFilter} onValueChange={setProductFilter}>
              <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-product">
                <SelectValue placeholder="Filter by product" />
              </SelectTrigger>
              <SelectContent>
                {productTypes.map(type => (
                  <SelectItem key={type} value={type}>
                    {type === "all" ? "All Products" : type}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <TabsContent value="data">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Leaf className="h-5 w-5 text-green-400" />
                  Agricultural & Biofuel Flows
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-loading">Loading flows...</div>
                ) : flows.length === 0 ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-no-data">No flows found</div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-800">
                        <TableHead className="text-slate-400">Flow ID</TableHead>
                        <TableHead className="text-slate-400">Commodity</TableHead>
                        <TableHead className="text-slate-400">Volume (MT)</TableHead>
                        <TableHead className="text-slate-400">Origin</TableHead>
                        <TableHead className="text-slate-400">Destination</TableHead>
                        <TableHead className="text-slate-400">Biofuel Type</TableHead>
                        <TableHead className="text-slate-400">Sustainability</TableHead>
                        <TableHead className="text-slate-400">Price</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {flows.map((flow) => (
                        <TableRow key={flow.id} className="border-slate-800" data-testid={`row-flow-${flow.flowId}`}>
                          <TableCell className="font-mono text-green-400">{flow.flowId}</TableCell>
                          <TableCell className="text-white font-medium">{flow.commodityType}</TableCell>
                          <TableCell className="text-slate-300">{parseFloat(flow.volume as string || '0').toLocaleString()}</TableCell>
                          <TableCell className="text-slate-300">{flow.originCountry || 'N/A'}</TableCell>
                          <TableCell className="text-slate-300">{flow.destinationCountry || 'N/A'}</TableCell>
                          <TableCell className="text-blue-400">{flow.biofuelType || 'N/A'}</TableCell>
                          <TableCell>
                            {flow.sustainabilityCert ? (
                              <Badge className="bg-green-500/20 text-green-400 border-green-500/50">{flow.sustainabilityCert}</Badge>
                            ) : (
                              <Badge variant="outline" className="border-slate-700 text-slate-400">Standard</Badge>
                            )}
                          </TableCell>
                          <TableCell className="text-white font-medium">${parseFloat(flow.price as string || '0').toFixed(2)}/ton</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="analytics">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <TrendingUp className="h-5 w-5 text-green-400" />
                  Sustainability Analysis
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {productTypes.filter(t => t !== "all").map(product => {
                    const count = flows.filter(f => f.commodityType === product).length;
                    const maxCount = Math.max(...productTypes.filter(t => t !== "all").map(p => 
                      flows.filter(f => f.commodityType === p).length
                    ));
                    return (
                      <div key={product}>
                        <div className="flex justify-between mb-2">
                          <span className="text-slate-400 text-sm">{product}</span>
                          <span className="text-white font-medium">{count}</span>
                        </div>
                        <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                          <div className="h-full bg-green-500" style={{ width: `${maxCount > 0 ? (count / maxCount) * 100 : 0}%` }} />
                        </div>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
