import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useSignals } from "@/hooks/use-signals";
import { usePredictions } from "@/hooks/use-predictions";
import { useWatchlistFilter } from "@/hooks/use-watchlist-filter";
import { WatchlistFilter } from "@/components/watchlist-filter";
import { Zap, TrendingUp, TrendingDown, AlertTriangle, Clock, MapPin, Activity, HelpCircle, ChevronDown, Database, Timer, Shield } from "lucide-react";
import { Link } from "wouter";
import type { Prediction } from "@shared/schema";

interface SignalExplainability {
  triggerReason: string;
  dataSources: string[];
  timeWindow: string;
  confidence: 'low' | 'medium' | 'high';
  methodology: string;
}

interface Signal {
  id: string;
  timestamp?: string;
  entityType: string;
  entityId: string;
  signalType: string;
  severity: number;
  title: string;
  description?: string;
  metadata?: any;
  isActive: boolean;
  explainability?: SignalExplainability;
}

export default function SignalsPage() {
  const { data: signals = [], isLoading: signalsLoading } = useSignals();
  const { data: predictions = [], isLoading: predictionsLoading } = usePredictions();
  const [selectedTab, setSelectedTab] = useState("all");
  const { activeWatchlist, isItemInActiveWatchlist } = useWatchlistFilter();

  const getSeverityColor = (severity: number) => {
    if (severity >= 4) return "bg-red-500";
    if (severity >= 3) return "bg-orange-500";
    return "bg-yellow-500";
  };

  const getSeverityLabel = (severity: number) => {
    if (severity >= 4) return "Critical";
    if (severity >= 3) return "High";
    if (severity >= 2) return "Medium";
    return "Low";
  };

  const watchlistFilteredSignals = useMemo(() => {
    if (!activeWatchlist) return signals;
    
    return signals.filter((signal) => {
      if (activeWatchlist.type === 'ports') {
        const portCode = signal.metadata?.portCode || signal.metadata?.port;
        return portCode && isItemInActiveWatchlist(portCode, 'ports');
      }
      if (activeWatchlist.type === 'vessels') {
        return isItemInActiveWatchlist(signal.entityId, 'vessels');
      }
      return true;
    });
  }, [signals, activeWatchlist, isItemInActiveWatchlist]);

  const filteredSignals = selectedTab === "all" 
    ? watchlistFilteredSignals 
    : watchlistFilteredSignals.filter(s => s.signalType === selectedTab);

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <Link href="/platform">
                <a className="text-muted-foreground hover:text-foreground transition-colors" data-testid="link-back">
                  ← Back to Dashboard
                </a>
              </Link>
              <div className="h-6 w-px bg-border"></div>
              <div>
                <h1 className="text-2xl font-bold text-foreground flex items-center gap-2" data-testid="text-page-title">
                  <Zap className="w-6 h-6 text-primary" />
                  Alerts & Active Events
                </h1>
                <p className="text-sm text-muted-foreground">Real-time market alerts and active maritime intelligence signals</p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <WatchlistFilter filterType="all" size="sm" />
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
                <div className="w-2 h-2 bg-emerald-400 rounded-full pulse-dot"></div>
                <span className="text-sm text-emerald-400">Live Feed Active</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-6 py-8">
        <Tabs defaultValue="signals" className="space-y-6">
          <TabsList className="grid w-full max-w-md grid-cols-2">
            <TabsTrigger value="signals" data-testid="tab-signals">
              <Zap className="w-4 h-4 mr-2" />
              Active Signals
            </TabsTrigger>
            <TabsTrigger value="predictions" data-testid="tab-predictions">
              <TrendingUp className="w-4 h-4 mr-2" />
              AI Predictions
            </TabsTrigger>
          </TabsList>

          {/* Signals Tab */}
          <TabsContent value="signals" className="space-y-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-xl font-semibold text-foreground">Active Signals</h2>
                <p className="text-sm text-muted-foreground">
                  {signals.length} signal{signals.length !== 1 ? 's' : ''} detected
                </p>
              </div>
              <div className="flex gap-2">
                <Button
                  variant={selectedTab === "all" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedTab("all")}
                  data-testid="button-filter-all"
                >
                  All
                </Button>
                <Button
                  variant={selectedTab === "CONGESTION_ALERT" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedTab("CONGESTION_ALERT")}
                  data-testid="button-filter-congestion"
                >
                  Congestion
                </Button>
                <Button
                  variant={selectedTab === "HIGH_STORAGE_FILL" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedTab("HIGH_STORAGE_FILL")}
                  data-testid="button-filter-storage"
                >
                  Storage
                </Button>
                <Button
                  variant={selectedTab === "THROUGHPUT_SURGE" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedTab("THROUGHPUT_SURGE")}
                  data-testid="button-filter-throughput"
                >
                  Throughput
                </Button>
                <Button
                  variant={selectedTab === "PRICE_MOVEMENT" ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedTab("PRICE_MOVEMENT")}
                  data-testid="button-filter-price"
                >
                  Price
                </Button>
              </div>
            </div>

            {signalsLoading ? (
              <div className="text-center py-12 text-muted-foreground">
                Loading signals...
              </div>
            ) : filteredSignals.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center">
                  <AlertTriangle className="w-12 h-12 mx-auto mb-4 text-muted-foreground" />
                  <p className="text-muted-foreground">No signals detected</p>
                </CardContent>
              </Card>
            ) : (
              <div className="grid gap-4">
                {filteredSignals.map((signal) => (
                  <Card key={signal.id} className="hover:border-primary/50 transition-colors" data-testid={`card-signal-${signal.id}`}>
                    <CardHeader>
                      <div className="flex items-start justify-between">
                        <div className="flex items-start gap-3">
                          <div className={`w-3 h-3 rounded-full mt-1.5 ${getSeverityColor(signal.severity)}`}></div>
                          <div>
                            <CardTitle className="text-lg mb-1">{signal.title}</CardTitle>
                            <p className="text-sm text-muted-foreground">{signal.description}</p>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          {signal.explainability?.confidence && (
                            <Badge 
                              variant="outline" 
                              className={
                                signal.explainability.confidence === 'high' 
                                  ? 'border-emerald-500 text-emerald-500' 
                                  : signal.explainability.confidence === 'medium'
                                    ? 'border-yellow-500 text-yellow-500'
                                    : 'border-gray-500 text-gray-500'
                              }
                            >
                              <Shield className="w-3 h-3 mr-1" />
                              {signal.explainability.confidence} confidence
                            </Badge>
                          )}
                          <Badge variant={signal.severity >= 4 ? "destructive" : "secondary"}>
                            {getSeverityLabel(signal.severity)}
                          </Badge>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="flex items-center gap-6 text-sm text-muted-foreground">
                        <div className="flex items-center gap-2">
                          <Activity className="w-4 h-4" />
                          <span className="capitalize">{signal.signalType.replace(/_/g, ' ').toLowerCase()}</span>
                        </div>
                        {signal.metadata?.portCode && (
                          <div className="flex items-center gap-2">
                            <MapPin className="w-4 h-4" />
                            <span>{signal.metadata.portCode}</span>
                          </div>
                        )}
                        {signal.timestamp && (
                          <div className="flex items-center gap-2">
                            <Clock className="w-4 h-4" />
                            <span>{new Date(signal.timestamp).toLocaleString()}</span>
                          </div>
                        )}
                      </div>

                      {signal.explainability && (
                        <Collapsible>
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-between hover:bg-muted/50" data-testid={`button-explain-${signal.id}`}>
                              <span className="flex items-center gap-2 text-muted-foreground">
                                <HelpCircle className="w-4 h-4" />
                                Why am I seeing this?
                              </span>
                              <ChevronDown className="w-4 h-4 text-muted-foreground transition-transform duration-200 [&[data-state=open]]:rotate-180" />
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2">
                            <div className="p-4 bg-blue-500/5 border border-blue-500/20 rounded-lg space-y-3">
                              <div>
                                <p className="text-xs font-semibold text-blue-400 mb-1 flex items-center gap-1">
                                  <Zap className="w-3 h-3" /> Trigger Reason
                                </p>
                                <p className="text-sm text-foreground">{signal.explainability.triggerReason}</p>
                              </div>
                              <div className="grid grid-cols-2 gap-4">
                                <div>
                                  <p className="text-xs font-semibold text-blue-400 mb-1 flex items-center gap-1">
                                    <Database className="w-3 h-3" /> Data Sources
                                  </p>
                                  <div className="flex flex-wrap gap-1">
                                    {signal.explainability.dataSources.map((source: string, i: number) => (
                                      <Badge key={i} variant="outline" className="text-xs">
                                        {source}
                                      </Badge>
                                    ))}
                                  </div>
                                </div>
                                <div>
                                  <p className="text-xs font-semibold text-blue-400 mb-1 flex items-center gap-1">
                                    <Timer className="w-3 h-3" /> Time Window
                                  </p>
                                  <p className="text-sm text-foreground">{signal.explainability.timeWindow}</p>
                                </div>
                              </div>
                              <div>
                                <p className="text-xs font-semibold text-blue-400 mb-1">Methodology</p>
                                <p className="text-sm text-muted-foreground">{signal.explainability.methodology}</p>
                              </div>
                            </div>
                          </CollapsibleContent>
                        </Collapsible>
                      )}

                      {signal.metadata && Object.keys(signal.metadata).length > 0 && (
                        <div className="p-3 bg-muted/50 rounded-lg">
                          <p className="text-xs font-semibold text-muted-foreground mb-2">Signal Data</p>
                          <div className="grid grid-cols-2 gap-2 text-sm">
                            {Object.entries(signal.metadata).map(([key, value]) => (
                              key !== 'portCode' && (
                                <div key={key}>
                                  <span className="text-muted-foreground capitalize">{key.replace(/([A-Z])/g, ' $1').trim()}: </span>
                                  <span className="font-semibold">{typeof value === 'number' ? value.toFixed(2) : String(value)}</span>
                                </div>
                              )
                            ))}
                          </div>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>

          {/* Predictions Tab */}
          <TabsContent value="predictions" className="space-y-6">
            <div>
              <h2 className="text-xl font-semibold text-foreground">AI-Powered Price Predictions</h2>
              <p className="text-sm text-muted-foreground">
                Machine learning forecasts for crude oil markets
              </p>
            </div>

            {predictionsLoading ? (
              <div className="text-center py-12 text-muted-foreground">
                Loading predictions...
              </div>
            ) : predictions.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center">
                  <TrendingUp className="w-12 h-12 mx-auto mb-4 text-muted-foreground" />
                  <p className="text-muted-foreground">No predictions available</p>
                  <p className="text-xs text-muted-foreground mt-2">Predictions are generated every 6 hours</p>
                </CardContent>
              </Card>
            ) : (
              <div className="grid gap-4 md:grid-cols-2">
                {predictions.map((prediction: Prediction) => {
                  const current = parseFloat(String(prediction.currentPrice));
                  const predicted = parseFloat(String(prediction.predictedPrice));
                  const change = predicted - current;
                  const changePercent = (change / current) * 100;
                  const isUp = prediction.direction === 'up';

                  return (
                    <Card key={prediction.id} className="hover:border-primary/50 transition-colors" data-testid={`card-prediction-${prediction.id}`}>
                      <CardHeader>
                        <div className="flex items-start justify-between">
                          <div>
                            <CardTitle className="text-lg mb-1">
                              Crude Oil Forecast
                            </CardTitle>
                            <p className="text-sm text-muted-foreground">
                              {prediction.timeframe} Forecast
                            </p>
                          </div>
                          <Badge variant={isUp ? "default" : "destructive"} className="flex items-center gap-1">
                            {isUp ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                            {isUp ? 'Bullish' : 'Bearish'}
                          </Badge>
                        </div>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <p className="text-xs text-muted-foreground mb-1">Current Price</p>
                            <p className="text-2xl font-bold">${current.toFixed(2)}</p>
                          </div>
                          <div>
                            <p className="text-xs text-muted-foreground mb-1">Predicted Price</p>
                            <p className="text-2xl font-bold">${predicted.toFixed(2)}</p>
                          </div>
                        </div>

                        <div className="p-3 bg-muted/50 rounded-lg">
                          <div className="flex items-center justify-between mb-2">
                            <span className="text-sm text-muted-foreground">Expected Change</span>
                            <span className={`font-semibold ${isUp ? 'text-emerald-500' : 'text-red-500'}`}>
                              {isUp ? '+' : ''}{change.toFixed(2)} ({changePercent.toFixed(2)}%)
                            </span>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">Confidence</span>
                            <span className="font-semibold">{(parseFloat(String(prediction.confidence || 0)) * 100).toFixed(1)}%</span>
                          </div>
                        </div>

                        <div className="pt-2 border-t border-border">
                          <div className="flex items-center justify-between text-xs text-muted-foreground">
                            <div className="flex items-center gap-1">
                              <Clock className="w-3 h-3" />
                              Valid until {prediction.validUntil ? new Date(prediction.validUntil).toLocaleString() : 'N/A'}
                            </div>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  );
                })}
              </div>
            )}
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
