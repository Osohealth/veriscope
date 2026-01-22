import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, FileText, TrendingUp, TrendingDown, Minus, Brain, Calendar, Target } from "lucide-react";
import { Link } from "wouter";
import type { ResearchReport } from "@shared/schema";
import { MLPredictionCard } from "@/components/ml-prediction-card";

interface ResearchInsightCsv {
  id: string;
  date: string;
  title: string;
  summary: string;
  impactScore: string;
  createdAt: string;
}

export default function ResearchInsightsPage() {
  const [categoryFilter, setCategoryFilter] = useState<string>("all");

  const queryParams = categoryFilter !== "all" ? `?category=${categoryFilter}` : '';
  const { data: reports = [], isLoading } = useQuery<ResearchReport[]>({
    queryKey: [`/api/research-reports${queryParams}`],
  });

  // Fetch CSV insights data
  const { data: csvInsights = [], isLoading: isLoadingCsv } = useQuery<ResearchInsightCsv[]>({
    queryKey: ['/api/research-insights/daily'],
  });

  // Calculate statistics
  const totalReports = reports.length;
  const bullishReports = reports.filter(r => r.priceOutlook === 'bullish').length;
  const bearishReports = reports.filter(r => r.priceOutlook === 'bearish').length;
  const neutralReports = reports.filter(r => r.priceOutlook === 'neutral').length;

  const categories = ["all", "market_analysis", "price_forecast", "trade_flow", "supply_demand"];

  const getOutlookIcon = (outlook: string) => {
    switch (outlook) {
      case 'bullish': return <TrendingUp className="h-4 w-4" />;
      case 'bearish': return <TrendingDown className="h-4 w-4" />;
      case 'neutral': return <Minus className="h-4 w-4" />;
      default: return null;
    }
  };

  const getOutlookColor = (outlook: string) => {
    switch (outlook) {
      case 'bullish': return 'bg-green-500/20 text-green-400 border-green-500/30';
      case 'bearish': return 'bg-red-500/20 text-red-400 border-red-500/30';
      case 'neutral': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
      default: return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
    }
  };

  const getConfidenceColor = (level: string) => {
    switch (level) {
      case 'high': return 'bg-blue-500/20 text-blue-400 border-blue-500/30';
      case 'medium': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
      case 'low': return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
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
              Research & Insight Layer
            </h1>
            <p className="text-slate-400 mt-2" data-testid="text-page-description">
              Market analysis, price forecasts, trade flow insights, and expert commentary
            </p>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-total">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <FileText className="h-4 w-4" />
                Total Reports
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white" data-testid="text-total-reports">{totalReports}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-bullish">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <TrendingUp className="h-4 w-4" />
                Bullish Outlook
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-400" data-testid="text-bullish-reports">{bullishReports}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-bearish">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <TrendingDown className="h-4 w-4" />
                Bearish Outlook
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-red-400" data-testid="text-bearish-reports">{bearishReports}</div>
            </CardContent>
          </Card>

          <Card className="bg-slate-900/50 border-slate-800" data-testid="card-stats-neutral">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-slate-400 flex items-center gap-2">
                <Minus className="h-4 w-4" />
                Neutral Outlook
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-yellow-400" data-testid="text-neutral-reports">{neutralReports}</div>
            </CardContent>
          </Card>
        </div>

        {/* ML Price Prediction */}
        <MLPredictionCard 
          commodityType="lng"
          commodityLabel="LNG"
          currentPrice={92}
        />

        {/* Main Content */}
        <Tabs defaultValue="csv" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList className="bg-slate-900/50 border border-slate-800">
              <TabsTrigger value="csv" className="data-[state=active]:bg-slate-800" data-testid="tab-csv">
                Real CSV Data
              </TabsTrigger>
              <TabsTrigger value="reports" className="data-[state=active]:bg-slate-800" data-testid="tab-reports">
                Research Reports
              </TabsTrigger>
              <TabsTrigger value="insights" className="data-[state=active]:bg-slate-800" data-testid="tab-insights">
                Key Insights
              </TabsTrigger>
            </TabsList>

            <Select value={categoryFilter} onValueChange={setCategoryFilter}>
              <SelectTrigger className="w-48 bg-slate-900/50 border-slate-800" data-testid="select-category">
                <SelectValue placeholder="Filter by category" />
              </SelectTrigger>
              <SelectContent className="bg-slate-900 border-slate-800">
                {categories.map(category => (
                  <SelectItem key={category} value={category} className="text-white">
                    {category === "all" ? "All Categories" : category.replace('_', ' ').split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <TabsContent value="csv" className="space-y-4">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <FileText className="h-5 w-5 text-blue-400" />
                  Real CSV Data - Research Insights ({csvInsights.length} records)
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoadingCsv ? (
                  <div className="text-center text-slate-400 py-8">Loading CSV insights...</div>
                ) : csvInsights.length === 0 ? (
                  <div className="text-center text-slate-400 py-8">No CSV insights available</div>
                ) : (
                  <div className="space-y-4">
                    {csvInsights.slice(0, 10).map((insight, index) => {
                      const impact = parseFloat(insight.impactScore);
                      const impactColor = impact > 0.6 ? 'bg-red-500/20 text-red-400' : impact > 0.3 ? 'bg-yellow-500/20 text-yellow-400' : 'bg-green-500/20 text-green-400';
                      const impactIcon = impact > 0.6 ? <TrendingUp className="h-3 w-3 mr-1" /> : impact > 0.3 ? <Minus className="h-3 w-3 mr-1" /> : <TrendingDown className="h-3 w-3 mr-1" />;
                      return (
                        <div key={insight.id} className="p-4 bg-slate-800/50 rounded-lg border border-slate-700" data-testid={`csv-insight-${index}`}>
                          <div className="flex items-start justify-between mb-2">
                            <h3 className="text-lg font-semibold text-white">{insight.title}</h3>
                            <Badge className={impactColor}>
                              {impactIcon}
                              Impact: {(impact * 100).toFixed(0)}%
                            </Badge>
                          </div>
                          <p className="text-sm text-slate-400 mb-3">{insight.summary}</p>
                          <div className="flex items-center gap-4 text-xs text-slate-500">
                            <span className="flex items-center gap-1">
                              <Calendar className="h-3 w-3" />
                              {new Date(insight.date).toLocaleDateString()}
                            </span>
                          </div>
                        </div>
                      );
                    })}
                    {csvInsights.length > 10 && (
                      <div className="text-center text-slate-400 text-sm">
                        Showing 10 of {csvInsights.length} insights
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="reports" className="space-y-4">
            {isLoading ? (
              <Card className="bg-slate-900/50 border-slate-800">
                <CardContent className="p-8 text-center text-slate-400">Loading...</CardContent>
              </Card>
            ) : reports.length === 0 ? (
              <Card className="bg-slate-900/50 border-slate-800">
                <CardContent className="p-8 text-center text-slate-400">No reports found</CardContent>
              </Card>
            ) : (
              reports.map((report, index) => (
                <Card key={report.id} className="bg-slate-900/50 border-slate-800 hover:bg-slate-800/30 transition-colors" data-testid={`card-report-${index}`}>
                  <CardHeader>
                    <div className="flex items-start justify-between">
                      <div className="space-y-1 flex-1">
                        <CardTitle className="text-xl text-white">{report.title}</CardTitle>
                        <div className="flex items-center gap-2 text-sm text-slate-400">
                          <Calendar className="h-4 w-4" />
                          <span>{new Date(report.publishDate).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}</span>
                          <span className="mx-2">•</span>
                          <Brain className="h-4 w-4" />
                          <span>{report.analyst}</span>
                        </div>
                      </div>
                      <div className="flex flex-col gap-2 items-end">
                        <Badge className={getOutlookColor(report.priceOutlook as string)}>
                          <span className="flex items-center gap-1">
                            {getOutlookIcon(report.priceOutlook as string)}
                            {report.priceOutlook?.toUpperCase()}
                          </span>
                        </Badge>
                        <Badge className={getConfidenceColor(report.confidenceLevel as string)}>
                          {report.confidenceLevel?.toUpperCase()} CONFIDENCE
                        </Badge>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <p className="text-slate-300 leading-relaxed">{report.summary}</p>
                    
                    {report.shortTermForecast && (
                      <div className="space-y-2">
                        <div className="flex items-center gap-2 text-sm font-medium text-slate-400">
                          <Target className="h-4 w-4" />
                          Short-term Forecast (1-3 months)
                        </div>
                        <p className="text-slate-300 text-sm pl-6">{report.shortTermForecast}</p>
                      </div>
                    )}

                    {report.tags && report.tags.length > 0 && (
                      <div className="flex flex-wrap gap-2 pt-2">
                        {report.tags.map((tag, i) => (
                          <Badge key={i} variant="outline" className="bg-slate-800/50 text-slate-300 border-slate-700">
                            {tag}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </CardContent>
                </Card>
              ))
            )}
          </TabsContent>

          <TabsContent value="insights" className="space-y-4">
            {isLoading ? (
              <Card className="bg-slate-900/50 border-slate-800">
                <CardContent className="p-8 text-center text-slate-400">Loading...</CardContent>
              </Card>
            ) : reports.length === 0 ? (
              <Card className="bg-slate-900/50 border-slate-800">
                <CardContent className="p-8 text-center text-slate-400">No insights found</CardContent>
              </Card>
            ) : (
              reports.map((report, index) => {
                const insights = typeof report.keyInsights === 'string' 
                  ? JSON.parse(report.keyInsights) 
                  : report.keyInsights || [];
                
                return insights.length > 0 ? (
                  <Card key={report.id} className="bg-slate-900/50 border-slate-800" data-testid={`card-insight-${index}`}>
                    <CardHeader>
                      <CardTitle className="text-lg text-white flex items-center gap-2">
                        <Brain className="h-5 w-5 text-blue-400" />
                        {report.title}
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <ul className="space-y-2">
                        {insights.map((insight: string, i: number) => (
                          <li key={i} className="flex items-start gap-3 text-slate-300">
                            <span className="text-blue-400 mt-1">•</span>
                            <span>{insight}</span>
                          </li>
                        ))}
                      </ul>
                    </CardContent>
                  </Card>
                ) : null;
              })
            )}
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
