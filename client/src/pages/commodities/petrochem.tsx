import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Beaker, TrendingUp, DollarSign, Activity } from "lucide-react";
import { Link } from "wouter";
import type { PetrochemProduct } from "@shared/schema";

export default function PetrochemPage() {
  const [categoryFilter, setCategoryFilter] = useState<string>("all");

  const queryParams = categoryFilter !== "all" ? `?category=${categoryFilter}` : '';
  const { data: products = [], isLoading } = useQuery<PetrochemProduct[]>({
    queryKey: [`/api/petrochem-products${queryParams}`],
  });

  const totalProducts = products.length;
  const avgMargin = products.length > 0
    ? products.reduce((sum, p) => sum + parseFloat(p.marginSpread as string || '0'), 0) / products.length
    : 0;
  const avgUtilization = products.length > 0
    ? products.reduce((sum, p) => sum + parseFloat(p.utilizationRate as string || '0'), 0) / products.length
    : 0;

  const categories = ["all", "Olefins", "Aromatics", "Polymers", "Intermediates"];

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
            <h1 className="text-3xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent" data-testid="text-page-title">
              Petrochem Pack
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Product taxonomy, yields, margins, and flow reconciliation
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Beaker className="h-4 w-4" />
                Total Products
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-products">{totalProducts}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <DollarSign className="h-4 w-4" />
                Avg Margin
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-400">${avgMargin.toFixed(2)}/ton</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Activity className="h-4 w-4" />
                Avg Utilization
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{avgUtilization.toFixed(1)}%</div>
            </CardContent>
          </Card>
        </div>

        <Tabs defaultValue="data" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Product Data
              </TabsTrigger>
              <TabsTrigger value="analytics" className="data-[state=active]:bg-slate-800" data-testid="tab-analytics">
                Analytics
              </TabsTrigger>
            </TabsList>

            <Select value={categoryFilter} onValueChange={setCategoryFilter}>
              <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-category">
                <SelectValue placeholder="Filter by category" />
              </SelectTrigger>
              <SelectContent>
                {categories.map(cat => (
                  <SelectItem key={cat} value={cat}>
                    {cat === "all" ? "All Categories" : cat}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <TabsContent value="data">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Beaker className="h-5 w-5 text-purple-400" />
                  Petrochemical Products
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-loading">Loading products...</div>
                ) : products.length === 0 ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-no-data">No products found</div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-800">
                        <TableHead className="text-slate-400">Product Code</TableHead>
                        <TableHead className="text-slate-400">Name</TableHead>
                        <TableHead className="text-slate-400">Category</TableHead>
                        <TableHead className="text-slate-400">Feedstock</TableHead>
                        <TableHead className="text-slate-400">Yield Rate</TableHead>
                        <TableHead className="text-slate-400">Margin</TableHead>
                        <TableHead className="text-slate-400">Price</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {products.map((product) => (
                        <TableRow key={product.id} className="border-slate-800" data-testid={`row-product-${product.productCode}`}>
                          <TableCell className="font-mono text-purple-400">{product.productCode}</TableCell>
                          <TableCell className="text-white font-medium">{product.productName}</TableCell>
                          <TableCell>
                            <Badge variant="outline" className="border-slate-700 text-slate-300">
                              {product.category}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-slate-300">{product.feedstock || 'N/A'}</TableCell>
                          <TableCell className="text-slate-300">{product.yieldRate}%</TableCell>
                          <TableCell className="text-green-400">${parseFloat(product.marginSpread as string || '0').toFixed(2)}</TableCell>
                          <TableCell className="text-white font-medium">${parseFloat(product.currentPrice as string || '0').toFixed(2)}/ton</TableCell>
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
                  <TrendingUp className="h-5 w-5 text-purple-400" />
                  Category Analysis
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {["Olefins", "Aromatics", "Polymers", "Intermediates"].map(cat => {
                    const count = products.filter(p => p.category === cat).length;
                    const maxCount = Math.max(...["Olefins", "Aromatics", "Polymers", "Intermediates"].map(c => 
                      products.filter(p => p.category === c).length
                    ));
                    return (
                      <div key={cat}>
                        <div className="flex justify-between mb-2">
                          <span className="text-slate-400 text-sm">{cat}</span>
                          <span className="text-white font-medium">{count}</span>
                        </div>
                        <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                          <div className="h-full bg-purple-500" style={{ width: `${maxCount > 0 ? (count / maxCount) * 100 : 0}%` }} />
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
