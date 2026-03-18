import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "wouter";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { 
  ArrowLeft, Ship, Anchor, TrendingUp, TrendingDown, 
  ArrowRight, MapPin, Package, GitMerge, Fuel
} from "lucide-react";

interface CargoLeg {
  id: string;
  sequence: number;
  portId: string;
  legType: string;
  arrivalDate: string | null;
  departureDate: string | null;
  volumeLoaded: number;
  volumeDischargedRounded: number;
  activity: string | null;
  waitTimeHours: string | null;
}

interface STSEvent {
  id: string;
  motherVesselId: string;
  daughterVesselId: string;
  commodityId: string;
  volumeTransferred: number;
  grade: string | null;
  startTime: string;
  endTime: string | null;
  status: string;
  reason: string | null;
}

interface CargoSplit {
  id: string;
  splitSequence: number;
  commodityId: string;
  grade: string;
  volume: number;
  percentage: string;
  buyer: string | null;
  price: string | null;
}

interface TradeFlow {
  id: string;
  vesselId: string;
  commodityId: string;
  cargoVolume: number;
  grade: string | null;
  status: string;
  hasSTS: boolean;
  isSplit: boolean;
  cargoChain: CargoLeg[];
  stsEvents: STSEvent[];
  splits: CargoSplit[];
}

interface FlowForecast {
  id: string;
  originPortId: string;
  destinationPortId: string;
  commodityId: string;
  timeframe: string;
  forecastedVolume: number;
  forecastedVesselCount: number;
  confidence: string;
  trend: string;
  historicalAverage: number | null;
  validFrom: string;
  validUntil: string;
}

export default function TradesFlowsPage() {
  const [selectedFlow, setSelectedFlow] = useState<string | null>(null);

  const { data: tradeFlows, isLoading: flowsLoading } = useQuery<TradeFlow[]>({
    queryKey: ['/api/trade-flows'],
  });

  const { data: flowForecasts, isLoading: forecastsLoading } = useQuery<FlowForecast[]>({
    queryKey: ['/api/flow-forecasts'],
  });

  const selectedFlowData = tradeFlows?.find(f => f.id === selectedFlow);

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-blue-950 to-slate-900">
      <div className="container mx-auto px-6 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-4">
            <Link href="/commodities">
              <Button variant="ghost" size="sm" data-testid="button-back-to-commodities">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back to Commodities
              </Button>
            </Link>
            <div>
              <h1 className="text-3xl font-bold text-white" data-testid="text-page-title">Trades & Flows</h1>
              <p className="text-slate-400" data-testid="text-page-description">
                Cargo chain view, STS linking, and ML-based flow projections
              </p>
            </div>
          </div>
        </div>

        <Tabs defaultValue="cargo-chains" className="space-y-6">
          <TabsList className="bg-slate-900/50 border border-slate-800" data-testid="tabs-trades-flows">
            <TabsTrigger value="cargo-chains" data-testid="tab-cargo-chains">
              <Ship className="h-4 w-4 mr-2" />
              Cargo Chains
            </TabsTrigger>
            <TabsTrigger value="sts-events" data-testid="tab-sts-events">
              <GitMerge className="h-4 w-4 mr-2" />
              STS Transfers
            </TabsTrigger>
            <TabsTrigger value="forecasts" data-testid="tab-forecasts">
              <TrendingUp className="h-4 w-4 mr-2" />
              Flow Forecasts
            </TabsTrigger>
          </TabsList>

          {/* Cargo Chains Tab */}
          <TabsContent value="cargo-chains" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Trade Flows List */}
              <Card className="lg:col-span-1 bg-slate-900/50 border-slate-800">
                <CardHeader>
                  <CardTitle className="text-white" data-testid="text-active-flows-title">Active Trade Flows</CardTitle>
                  <CardDescription>Click to view cargo chain details</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  {flowsLoading ? (
                    <div className="text-slate-400" data-testid="text-loading-flows">Loading flows...</div>
                  ) : tradeFlows && tradeFlows.length > 0 ? (
                    tradeFlows.map((flow) => (
                      <Card
                        key={flow.id}
                        className={`cursor-pointer transition-all ${
                          selectedFlow === flow.id
                            ? 'bg-blue-900/30 border-blue-500'
                            : 'bg-slate-800/30 border-slate-700 hover:border-slate-600'
                        }`}
                        onClick={() => setSelectedFlow(flow.id)}
                        data-testid={`card-flow-${flow.id}`}
                      >
                        <CardContent className="p-4">
                          <div className="flex items-center justify-between mb-2">
                            <div className="flex items-center gap-2">
                              <Ship className="h-4 w-4 text-blue-400" />
                              <span className="font-medium text-white text-sm">Flow #{flow.id.slice(0, 8)}</span>
                            </div>
                            <Badge variant={flow.status === 'in_transit' ? 'default' : 'secondary'} data-testid={`badge-status-${flow.id}`}>
                              {flow.status}
                            </Badge>
                          </div>
                          <div className="space-y-1 text-sm">
                            <div className="text-slate-400" data-testid={`text-volume-${flow.id}`}>
                              Volume: {flow.cargoVolume.toLocaleString()} MT
                            </div>
                            {flow.grade && (
                              <div className="text-slate-400" data-testid={`text-grade-${flow.id}`}>Grade: {flow.grade}</div>
                            )}
                            {flow.hasSTS && (
                              <Badge variant="outline" className="text-xs" data-testid={`badge-sts-${flow.id}`}>
                                <GitMerge className="h-3 w-3 mr-1" />
                                STS Transfer
                              </Badge>
                            )}
                            {flow.isSplit && (
                              <Badge variant="outline" className="text-xs" data-testid={`badge-split-${flow.id}`}>
                                <Package className="h-3 w-3 mr-1" />
                                Cargo Split
                              </Badge>
                            )}
                          </div>
                        </CardContent>
                      </Card>
                    ))
                  ) : (
                    <div className="text-slate-400 text-center py-8" data-testid="text-no-flows">
                      No active trade flows
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Cargo Chain Visualization */}
              <Card className="lg:col-span-2 bg-slate-900/50 border-slate-800">
                <CardHeader>
                  <CardTitle className="text-white" data-testid="text-cargo-chain-title">
                    Cargo Chain View
                  </CardTitle>
                  <CardDescription>
                    Complete journey: Origin → Waypoints → Destination
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {selectedFlowData ? (
                    <div className="space-y-6">
                      {/* Flow Summary */}
                      <div className="grid grid-cols-3 gap-4 p-4 bg-slate-800/30 rounded-lg">
                        <div>
                          <div className="text-xs text-slate-400 mb-1">Total Volume</div>
                          <div className="text-lg font-semibold text-white" data-testid="text-total-volume">
                            {selectedFlowData.cargoVolume.toLocaleString()} MT
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-slate-400 mb-1">Status</div>
                          <Badge data-testid="badge-flow-status">{selectedFlowData.status}</Badge>
                        </div>
                        <div>
                          <div className="text-xs text-slate-400 mb-1">Grade</div>
                          <div className="text-lg font-semibold text-white" data-testid="text-flow-grade">
                            {selectedFlowData.grade || 'Standard'}
                          </div>
                        </div>
                      </div>

                      {/* Cargo Chain */}
                      {selectedFlowData.cargoChain && selectedFlowData.cargoChain.length > 0 ? (
                        <div className="space-y-4">
                          <h3 className="text-lg font-semibold text-white" data-testid="text-journey-title">Journey Legs</h3>
                          {selectedFlowData.cargoChain.map((leg, index) => (
                            <div key={leg.id} className="relative">
                              <Card className="bg-slate-800/30 border-slate-700" data-testid={`card-leg-${leg.id}`}>
                                <CardContent className="p-4">
                                  <div className="flex items-start gap-4">
                                    <div className="flex flex-col items-center">
                                      <div className={`rounded-full p-2 ${
                                        leg.legType === 'origin' ? 'bg-green-500/20 text-green-400' :
                                        leg.legType === 'destination' ? 'bg-red-500/20 text-red-400' :
                                        leg.legType === 'sts_point' ? 'bg-purple-500/20 text-purple-400' :
                                        'bg-blue-500/20 text-blue-400'
                                      }`}>
                                        {leg.legType === 'origin' ? <MapPin className="h-5 w-5" /> :
                                         leg.legType === 'destination' ? <Anchor className="h-5 w-5" /> :
                                         leg.legType === 'sts_point' ? <GitMerge className="h-5 w-5" /> :
                                         <Fuel className="h-5 w-5" />}
                                      </div>
                                      {index < selectedFlowData.cargoChain.length - 1 && (
                                        <div className="h-12 w-0.5 bg-slate-700 my-2" />
                                      )}
                                    </div>
                                    <div className="flex-1">
                                      <div className="flex items-center justify-between mb-2">
                                        <div>
                                          <div className="font-medium text-white capitalize" data-testid={`text-leg-type-${leg.id}`}>
                                            {leg.legType.replace('_', ' ')}
                                          </div>
                                          <div className="text-sm text-slate-400" data-testid={`text-port-${leg.id}`}>
                                            Port ID: {leg.portId.slice(0, 8)}
                                          </div>
                                        </div>
                                        <Badge variant="outline" data-testid={`badge-seq-${leg.id}`}>
                                          Leg {leg.sequence}
                                        </Badge>
                                      </div>
                                      <div className="grid grid-cols-2 gap-4 text-sm">
                                        {leg.arrivalDate && (
                                          <div>
                                            <div className="text-slate-400">Arrival</div>
                                            <div className="text-white" data-testid={`text-arrival-${leg.id}`}>
                                              {new Date(leg.arrivalDate).toLocaleDateString()}
                                            </div>
                                          </div>
                                        )}
                                        {leg.departureDate && (
                                          <div>
                                            <div className="text-slate-400">Departure</div>
                                            <div className="text-white" data-testid={`text-departure-${leg.id}`}>
                                              {new Date(leg.departureDate).toLocaleDateString()}
                                            </div>
                                          </div>
                                        )}
                                        {leg.volumeLoaded > 0 && (
                                          <div>
                                            <div className="text-slate-400">Loaded</div>
                                            <div className="text-green-400" data-testid={`text-loaded-${leg.id}`}>
                                              +{leg.volumeLoaded.toLocaleString()} MT
                                            </div>
                                          </div>
                                        )}
                                        {leg.volumeDischargedRounded > 0 && (
                                          <div>
                                            <div className="text-slate-400">Discharged</div>
                                            <div className="text-red-400" data-testid={`text-discharged-${leg.id}`}>
                                              -{leg.volumeDischargedRounded.toLocaleString()} MT
                                            </div>
                                          </div>
                                        )}
                                      </div>
                                      {leg.activity && (
                                        <div className="mt-2 text-sm">
                                          <Badge variant="secondary" data-testid={`badge-activity-${leg.id}`}>
                                            {leg.activity}
                                          </Badge>
                                        </div>
                                      )}
                                    </div>
                                  </div>
                                </CardContent>
                              </Card>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-center py-8 text-slate-400" data-testid="text-no-chain-data">
                          No cargo chain data available
                        </div>
                      )}

                      {/* Cargo Splits */}
                      {selectedFlowData.splits && selectedFlowData.splits.length > 0 && (
                        <div className="space-y-4">
                          <Separator className="bg-slate-800" />
                          <h3 className="text-lg font-semibold text-white" data-testid="text-splits-title">Cargo Splits</h3>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            {selectedFlowData.splits.map((split) => (
                              <Card key={split.id} className="bg-slate-800/30 border-slate-700" data-testid={`card-split-${split.id}`}>
                                <CardContent className="p-4">
                                  <div className="flex items-center justify-between mb-2">
                                    <div className="font-medium text-white" data-testid={`text-split-grade-${split.id}`}>
                                      Grade: {split.grade}
                                    </div>
                                    <Badge data-testid={`badge-split-pct-${split.id}`}>
                                      {parseFloat(split.percentage).toFixed(1)}%
                                    </Badge>
                                  </div>
                                  <div className="text-sm text-slate-400 space-y-1">
                                    <div data-testid={`text-split-volume-${split.id}`}>
                                      Volume: {split.volume.toLocaleString()} MT
                                    </div>
                                    {split.buyer && (
                                      <div data-testid={`text-split-buyer-${split.id}`}>Buyer: {split.buyer}</div>
                                    )}
                                    {split.price && (
                                      <div className="text-green-400" data-testid={`text-split-price-${split.id}`}>
                                        ${parseFloat(split.price).toFixed(2)}/bbl
                                      </div>
                                    )}
                                  </div>
                                </CardContent>
                              </Card>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* STS Events */}
                      {selectedFlowData.stsEvents && selectedFlowData.stsEvents.length > 0 && (
                        <div className="space-y-4">
                          <Separator className="bg-slate-800" />
                          <h3 className="text-lg font-semibold text-white" data-testid="text-sts-title">STS Transfer Events</h3>
                          {selectedFlowData.stsEvents.map((sts) => (
                            <Card key={sts.id} className="bg-purple-900/20 border-purple-500/30" data-testid={`card-sts-${sts.id}`}>
                              <CardContent className="p-4">
                                <div className="flex items-center gap-2 mb-3">
                                  <GitMerge className="h-5 w-5 text-purple-400" />
                                  <span className="font-medium text-white">Ship-to-Ship Transfer</span>
                                  <Badge variant={sts.status === 'completed' ? 'default' : 'secondary'} data-testid={`badge-sts-status-${sts.id}`}>
                                    {sts.status}
                                  </Badge>
                                </div>
                                <div className="grid grid-cols-2 gap-4 text-sm">
                                  <div>
                                    <div className="text-slate-400">Volume Transferred</div>
                                    <div className="text-white font-medium" data-testid={`text-sts-volume-${sts.id}`}>
                                      {sts.volumeTransferred.toLocaleString()} MT
                                    </div>
                                  </div>
                                  {sts.grade && (
                                    <div>
                                      <div className="text-slate-400">Grade</div>
                                      <div className="text-white" data-testid={`text-sts-grade-${sts.id}`}>{sts.grade}</div>
                                    </div>
                                  )}
                                  <div>
                                    <div className="text-slate-400">Start Time</div>
                                    <div className="text-white" data-testid={`text-sts-start-${sts.id}`}>
                                      {new Date(sts.startTime).toLocaleString()}
                                    </div>
                                  </div>
                                  {sts.reason && (
                                    <div>
                                      <div className="text-slate-400">Reason</div>
                                      <div className="text-white capitalize" data-testid={`text-sts-reason-${sts.id}`}>
                                        {sts.reason.replace('_', ' ')}
                                      </div>
                                    </div>
                                  )}
                                </div>
                              </CardContent>
                            </Card>
                          ))}
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="text-center py-12 text-slate-400" data-testid="text-select-flow">
                      Select a trade flow to view cargo chain details
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* STS Events Tab */}
          <TabsContent value="sts-events">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-sts-transfers-title">
                  <GitMerge className="h-5 w-5" />
                  Ship-to-Ship Transfers
                </CardTitle>
                <CardDescription>Recent transshipment events and vessel-to-vessel cargo transfers</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {tradeFlows?.flatMap(flow => flow.stsEvents || []).slice(0, 9).map((sts) => (
                    <Card key={sts.id} className="bg-slate-800/30 border-slate-700" data-testid={`card-sts-event-${sts.id}`}>
                      <CardContent className="p-4">
                        <div className="flex items-center justify-between mb-3">
                          <GitMerge className="h-5 w-5 text-purple-400" />
                          <Badge variant={sts.status === 'completed' ? 'default' : 'secondary'} data-testid={`badge-sts-event-status-${sts.id}`}>
                            {sts.status}
                          </Badge>
                        </div>
                        <div className="space-y-2 text-sm">
                          <div>
                            <div className="text-slate-400">Volume</div>
                            <div className="text-white font-medium" data-testid={`text-sts-event-volume-${sts.id}`}>
                              {sts.volumeTransferred.toLocaleString()} MT
                            </div>
                          </div>
                          {sts.grade && (
                            <div>
                              <div className="text-slate-400">Grade</div>
                              <div className="text-white" data-testid={`text-sts-event-grade-${sts.id}`}>{sts.grade}</div>
                            </div>
                          )}
                          {sts.reason && (
                            <div>
                              <div className="text-slate-400">Reason</div>
                              <div className="text-white capitalize" data-testid={`text-sts-event-reason-${sts.id}`}>
                                {sts.reason.replace('_', ' ')}
                              </div>
                            </div>
                          )}
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Flow Forecasts Tab */}
          <TabsContent value="forecasts">
            <Card className="bg-slate-900/50 border-slate-800">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2" data-testid="text-forecasts-title">
                  <TrendingUp className="h-5 w-5" />
                  ML-Based Flow Forecasts
                </CardTitle>
                <CardDescription>Short-term trade flow predictions with confidence intervals</CardDescription>
              </CardHeader>
              <CardContent>
                {forecastsLoading ? (
                  <div className="text-slate-400" data-testid="text-loading-forecasts">Loading forecasts...</div>
                ) : flowForecasts && flowForecasts.length > 0 ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {flowForecasts.map((forecast) => (
                      <Card key={forecast.id} className="bg-slate-800/30 border-slate-700" data-testid={`card-forecast-${forecast.id}`}>
                        <CardContent className="p-4">
                          <div className="flex items-center justify-between mb-3">
                            <Badge variant="outline" data-testid={`badge-forecast-timeframe-${forecast.id}`}>
                              {forecast.timeframe}
                            </Badge>
                            <div className="flex items-center gap-1">
                              {forecast.trend === 'increasing' ? (
                                <TrendingUp className="h-4 w-4 text-green-400" />
                              ) : forecast.trend === 'decreasing' ? (
                                <TrendingDown className="h-4 w-4 text-red-400" />
                              ) : (
                                <ArrowRight className="h-4 w-4 text-slate-400" />
                              )}
                              <span className={`text-sm ${
                                forecast.trend === 'increasing' ? 'text-green-400' :
                                forecast.trend === 'decreasing' ? 'text-red-400' :
                                'text-slate-400'
                              }`} data-testid={`text-forecast-trend-${forecast.id}`}>
                                {forecast.trend}
                              </span>
                            </div>
                          </div>
                          <div className="space-y-3">
                            <div>
                              <div className="text-slate-400 text-sm">Forecasted Volume</div>
                              <div className="text-2xl font-bold text-white" data-testid={`text-forecast-volume-${forecast.id}`}>
                                {forecast.forecastedVolume.toLocaleString()}
                                <span className="text-sm text-slate-400 ml-1">MT</span>
                              </div>
                            </div>
                            <div>
                              <div className="text-slate-400 text-sm">Vessel Count</div>
                              <div className="text-white font-medium" data-testid={`text-forecast-vessels-${forecast.id}`}>
                                {forecast.forecastedVesselCount} vessels
                              </div>
                            </div>
                            <div>
                              <div className="text-slate-400 text-sm">Confidence</div>
                              <div className="flex items-center gap-2">
                                <div className="flex-1 bg-slate-700 rounded-full h-2">
                                  <div 
                                    className="bg-blue-500 h-2 rounded-full" 
                                    style={{ width: `${parseFloat(forecast.confidence) * 100}%` }}
                                    data-testid={`bar-forecast-confidence-${forecast.id}`}
                                  />
                                </div>
                                <span className="text-white text-sm" data-testid={`text-forecast-confidence-${forecast.id}`}>
                                  {(parseFloat(forecast.confidence) * 100).toFixed(0)}%
                                </span>
                              </div>
                            </div>
                            {forecast.historicalAverage && (
                              <div className="text-sm text-slate-400" data-testid={`text-forecast-avg-${forecast.id}`}>
                                Hist. Avg: {forecast.historicalAverage.toLocaleString()} MT
                              </div>
                            )}
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-12 text-slate-400" data-testid="text-no-forecasts">
                    No flow forecasts available
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
