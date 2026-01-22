import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Droplet, TrendingUp, TrendingDown, DollarSign, BarChart3, Gauge } from "lucide-react";
import { Link } from "wouter";
import type { CrudeGrade } from "@shared/schema";

export default function CrudeProductsPage() {
  const [categoryFilter, setCategoryFilter] = useState<string>("all");

  const queryParams = categoryFilter !== "all" ? `?category=${categoryFilter}` : '';
  const { data: grades = [], isLoading } = useQuery<CrudeGrade[]>({
    queryKey: [`/api/crude-grades${queryParams}`],
  });

  // Calculate statistics
  const totalGrades = grades.length;
  const avgApiGravity = grades.length > 0 
    ? grades.reduce((sum, g) => sum + parseFloat(g.apiGravity as string || '0'), 0) / grades.length 
    : 0;
  const avgSulfurContent = grades.length > 0
    ? grades.reduce((sum, g) => sum + parseFloat(g.sulfurContent as string || '0'), 0) / grades.length
    : 0;
  const avgYield = grades.length > 0
    ? grades.reduce((sum, g) => {
        const profile = g.yieldProfile as any;
        return sum + parseFloat(profile?.distillate || '0');
      }, 0) / grades.length
    : 0;

  const categories = ["all", "light", "medium", "heavy", "extra_heavy"];

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
              Crude & Products Pack
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Grade library, quality specs, pricing, and arbitrage monitors
            </p>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Droplet className="h-4 w-4" />
                Total Grades
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-grades">{totalGrades}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-api">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Gauge className="h-4 w-4" />
                Avg API Gravity
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-avg-api">{avgApiGravity.toFixed(1)}°</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-sulfur">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <BarChart3 className="h-4 w-4" />
                Avg Sulfur Content
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-avg-sulfur">{avgSulfurContent.toFixed(2)}%</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-yield">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <TrendingUp className="h-4 w-4" />
                Avg Distillate Yield
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-avg-yield">{avgYield.toFixed(1)}%</div>
            </CardContent>
          </Card>
        </div>

        {/* Main Content */}
        <Tabs defaultValue="data" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="data" className="data-[state=active]:bg-slate-800" data-testid="tab-data">
                Grade Data
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
                    {cat === "all" ? "All Categories" : cat.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <TabsContent value="data" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Droplet className="h-5 w-5 text-blue-400" />
                  Crude Grade Library
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-loading">Loading grades...</div>
                ) : grades.length === 0 ? (
                  <div className="text-center py-8 text-slate-400" data-testid="text-no-data">No grades found</div>
                ) : (
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="border-slate-800">
                          <TableHead className="text-slate-400">Grade Code</TableHead>
                          <TableHead className="text-slate-400">Name</TableHead>
                          <TableHead className="text-slate-400">Category</TableHead>
                          <TableHead className="text-slate-400">Origin</TableHead>
                          <TableHead className="text-slate-400">API Gravity</TableHead>
                          <TableHead className="text-slate-400">Sulfur %</TableHead>
                          <TableHead className="text-slate-400">Benchmark</TableHead>
                          <TableHead className="text-slate-400">Price</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {grades.map((grade) => (
                          <TableRow key={grade.id} className="border-slate-800" data-testid={`row-grade-${grade.gradeCode}`}>
                            <TableCell className="font-mono text-blue-400" data-testid={`text-code-${grade.gradeCode}`}>
                              {grade.gradeCode}
                            </TableCell>
                            <TableCell className="text-white font-medium" data-testid={`text-name-${grade.gradeCode}`}>
                              {grade.name}
                            </TableCell>
                            <TableCell>
                              <Badge variant="outline" className="border-slate-700 text-slate-300">
                                {grade.category}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-slate-300" data-testid={`text-origin-${grade.gradeCode}`}>
                              {grade.origin}
                            </TableCell>
                            <TableCell className="text-slate-300" data-testid={`text-api-${grade.gradeCode}`}>
                              {grade.apiGravity}°
                            </TableCell>
                            <TableCell className="text-slate-300" data-testid={`text-sulfur-${grade.gradeCode}`}>
                              {grade.sulfurContent}%
                            </TableCell>
                            <TableCell className="text-slate-300" data-testid={`text-benchmark-${grade.gradeCode}`}>
                              {grade.priceBenchmark}
                            </TableCell>
                            <TableCell className="text-white font-medium" data-testid={`text-price-${grade.gradeCode}`}>
                              ${parseFloat(grade.currentPrice as string || '0').toFixed(2)}/{grade.priceUnit?.split('/')[1] || 'bbl'}
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
                    <DollarSign className="h-5 w-5 text-green-400" />
                    Price Distribution
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {grades.slice(0, 5).map((grade) => {
                      const price = parseFloat(grade.currentPrice as string || '0');
                      const maxPrice = Math.max(...grades.map(g => parseFloat(g.currentPrice as string || '0')));
                      const widthPercent = maxPrice > 0 ? (price / maxPrice) * 100 : 0;
                      
                      return (
                        <div key={grade.id} className="space-y-1" data-testid={`chart-price-${grade.gradeCode}`}>
                          <div className="flex justify-between text-sm">
                            <span className="text-slate-300">{grade.name}</span>
                            <span className="text-white font-medium">${price.toFixed(2)}</span>
                          </div>
                          <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-gradient-to-r from-green-500 to-emerald-400"
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
                    <TrendingUp className="h-5 w-5 text-blue-400" />
                    Quality Analysis
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <div className="flex justify-between mb-2">
                        <span className="text-slate-400 text-sm">Light Crudes</span>
                        <span className="text-white font-medium">
                          {grades.filter(g => g.category === 'light').length}
                        </span>
                      </div>
                      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-blue-500"
                          style={{ width: `${totalGrades > 0 ? (grades.filter(g => g.category === 'light').length / totalGrades) * 100 : 0}%` }}
                        />
                      </div>
                    </div>
                    <div>
                      <div className="flex justify-between mb-2">
                        <span className="text-slate-400 text-sm">Medium Crudes</span>
                        <span className="text-white font-medium">
                          {grades.filter(g => g.category === 'medium').length}
                        </span>
                      </div>
                      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-cyan-500"
                          style={{ width: `${totalGrades > 0 ? (grades.filter(g => g.category === 'medium').length / totalGrades) * 100 : 0}%` }}
                        />
                      </div>
                    </div>
                    <div>
                      <div className="flex justify-between mb-2">
                        <span className="text-slate-400 text-sm">Heavy Crudes</span>
                        <span className="text-white font-medium">
                          {grades.filter(g => g.category === 'heavy').length}
                        </span>
                      </div>
                      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-purple-500"
                          style={{ width: `${totalGrades > 0 ? (grades.filter(g => g.category === 'heavy').length / totalGrades) * 100 : 0}%` }}
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
