import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Anchor, TrendingUp, DollarSign, Ship } from "lucide-react";
import { Link } from "wouter";
import type { DryBulkFixture } from "@shared/schema";

export default function DryBulkPage() {
  const [commodityFilter, setCommodityFilter] = useState<string>("all");

  const queryParams = commodityFilter !== "all" ? `?commodityType=${commodityFilter}` : '';
  const { data: fixtures = [], isLoading } = useQuery<DryBulkFixture[]>({
    queryKey: [`/api/dry-bulk-fixtures${queryParams}`],
  });

  const totalFixtures = fixtures.length;
  const avgFreightRate = fixtures.length > 0
    ? fixtures.reduce((sum, f) => sum + parseFloat(f.freightRate as string || '0'), 0) / fixtures.length
    : 0;
  const totalVolume = fixtures.reduce((sum, f) => sum + parseFloat(f.quantity as string || '0'), 0);

  const commodityTypes = ["all", "Coal", "Iron Ore", "Grain", "Bauxite"];

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
            <h1 className="text-3xl font-bold bg-gradient-to-r from-amber-400 to-orange-400 bg-clip-text text-transparent" data-testid="text-page-title">
              Dry Bulk Pack
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Coal, iron ore, grain flows, vessel fixtures, and freight rates
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Anchor className="h-4 w-4" />
                Total Fixtures
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-fixtures">{totalFixtures}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <DollarSign className="h-4 w-4" />
                Avg Freight Rate
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">${avgFreightRate.toFixed(2)}/day</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Ship className="h-4 w-4" />
                Total Volume
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{(totalVolume / 1000).toFixed(0)}K MT</div>
            </CardContent>
          </Card>
        </div>

        <Tabs defaultValue="data" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Fixture Data
              </TabsTrigger>
              <TabsTrigger value="analytics" className="data-[state=active]:bg-slate-800" data-testid="tab-analytics">
                Analytics
              </TabsTrigger>
            </TabsList>

            <Select value={commodityFilter} onValueChange={setCommodityFilter}>
              <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-commodity">
                <SelectValue placeholder="Filter by commodity" />
              </SelectTrigger>
              <SelectContent>
                {commodityTypes.map(type => (
                  <SelectItem key={type} value={type}>
                    {type === "all" ? "All Commodities" : type}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <TabsContent value="data">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Anchor className="h-5 w-5 text-amber-400" />
                  Dry Bulk Fixtures
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-loading">Loading fixtures...</div>
                ) : fixtures.length === 0 ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-no-data">No fixtures found</div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-800">
                        <TableHead className="text-slate-400">Fixture ID</TableHead>
                        <TableHead className="text-slate-400">Commodity</TableHead>
                        <TableHead className="text-slate-400">Quantity (MT)</TableHead>
                        <TableHead className="text-slate-400">Load Port</TableHead>
                        <TableHead className="text-slate-400">Discharge Port</TableHead>
                        <TableHead className="text-slate-400">Vessel Size</TableHead>
                        <TableHead className="text-slate-400">Freight Rate</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {fixtures.map((fixture) => (
                        <TableRow key={fixture.id} className="border-slate-800" data-testid={`row-fixture-${fixture.fixtureId}`}>
                          <TableCell className="font-mono text-amber-400">{fixture.fixtureId}</TableCell>
                          <TableCell>
                            <Badge variant="outline" className="border-slate-700 text-slate-300">
                              {fixture.commodityType}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-slate-300">{parseFloat(fixture.quantity as string || '0').toLocaleString()}</TableCell>
                          <TableCell className="text-slate-300">{fixture.loadPortId}</TableCell>
                          <TableCell className="text-slate-300">{fixture.dischargePortId}</TableCell>
                          <TableCell className="text-slate-300">{fixture.vesselSize}</TableCell>
                          <TableCell className="text-white font-medium">${parseFloat(fixture.freightRate as string || '0').toFixed(2)}/day</TableCell>
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
                  <TrendingUp className="h-5 w-5 text-amber-400" />
                  Freight Rate Analysis
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {["Capesize", "Panamax", "Supramax", "Handysize"].map(size => {
                    const count = fixtures.filter(f => f.vesselSize === size).length;
                    const maxCount = Math.max(...["Capesize", "Panamax", "Supramax", "Handysize"].map(s => 
                      fixtures.filter(f => f.vesselSize === s).length
                    ));
                    return (
                      <div key={size}>
                        <div className="flex justify-between mb-2">
                          <span className="text-slate-400 text-sm">{size}</span>
                          <span className="text-white font-medium">{count}</span>
                        </div>
                        <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                          <div className="h-full bg-amber-500" style={{ width: `${maxCount > 0 ? (count / maxCount) * 100 : 0}%` }} />
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
