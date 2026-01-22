import { useQuery } from "@tanstack/react-query";
import { useState, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ChevronLeft, Fuel, DollarSign, TrendingUp, Droplet, Ship, MapPin, Calendar, User } from "lucide-react";
import { useLocation } from "wouter";
import type { BunkeringEvent, Port, Vessel } from "@shared/schema";
import { Skeleton } from "@/components/ui/skeleton";

type EnrichedBunkeringEvent = BunkeringEvent & {
  vessel?: Vessel;
  port?: Port;
};

export default function BunkeringFuelEvents() {
  const [, setLocation] = useLocation();
  const [selectedFuelType, setSelectedFuelType] = useState<string>("all");
  const [selectedPortId, setSelectedPortId] = useState<string>("all");

  // Fetch ports and vessels for enrichment
  const { data: ports = [] } = useQuery<Port[]>({ queryKey: ["/api/ports"] });
  const { data: vessels = [] } = useQuery<Vessel[]>({ queryKey: ["/api/vessels"] });

  // Build API URL with port filter
  const eventsUrl = selectedPortId !== "all" 
    ? `/api/bunkering-events?portId=${selectedPortId}` 
    : "/api/bunkering-events";

  // Fetch bunkering events
  const { data: events = [], isLoading } = useQuery<BunkeringEvent[]>({
    queryKey: [eventsUrl]
  });

  // Enrich events with port and vessel data
  const enrichedEvents: EnrichedBunkeringEvent[] = events.map(event => ({
    ...event,
    port: event.portId ? ports.find(p => p.id === event.portId) : undefined,
    vessel: vessels.find(v => v.id === event.vesselId)
  }));

  // Filter by fuel type
  const filteredEvents = selectedFuelType === "all" 
    ? enrichedEvents 
    : enrichedEvents.filter(e => e.fuelType.toLowerCase() === selectedFuelType.toLowerCase());

  // Calculate statistics from filtered events
  const pricedEventsCount = filteredEvents.filter(e => e.pricePerMT).length;
  const stats = {
    totalEvents: filteredEvents.length,
    totalVolume: filteredEvents.reduce((sum, e) => sum + parseFloat(e.volumeMT || "0"), 0),
    avgPrice: pricedEventsCount > 0 
      ? filteredEvents.reduce((sum, e) => sum + parseFloat(e.pricePerMT || "0"), 0) / pricedEventsCount
      : 0,
    totalCost: filteredEvents.reduce((sum, e) => sum + parseFloat(e.totalCost || "0"), 0),
    vlsfoEvents: filteredEvents.filter(e => e.fuelType.toLowerCase().includes('vlsfo')).length,
    hsfoEvents: filteredEvents.filter(e => e.fuelType.toLowerCase().includes('hsfo')).length,
    mgoEvents: filteredEvents.filter(e => e.fuelType.toLowerCase().includes('mgo')).length,
    lngEvents: filteredEvents.filter(e => e.fuelType.toLowerCase().includes('lng')).length
  };

  // Fuel type distribution for analytics
  const fuelDistribution = [
    { name: "VLSFO", value: stats.vlsfoEvents },
    { name: "HSFO", value: stats.hsfoEvents },
    { name: "MGO", value: stats.mgoEvents },
    { name: "LNG", value: stats.lngEvents }
  ];

  // Event type distribution
  const eventTypeDistribution = filteredEvents.reduce((acc, event) => {
    const type = event.eventType || 'unknown';
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  return (
    <div className="min-h-screen bg-background p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button 
              variant="ghost" 
              size="icon" 
              onClick={() => setLocation("/maritime")}
              data-testid="button-back"
            >
              <ChevronLeft className="h-5 w-5" />
            </Button>
            <div>
              <h1 className="text-3xl font-bold" data-testid="text-page-title">Bunkering / Fuel Events</h1>
              <p className="text-muted-foreground">LNG bunkering and fuel supply zones</p>
            </div>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid gap-4 md:grid-cols-3 lg:grid-cols-6">
          <Card data-testid="card-total-events">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Events</CardTitle>
              <Fuel className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-total-events">{stats.totalEvents}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-total-volume">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Volume</CardTitle>
              <Droplet className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-total-volume">
                {stats.totalVolume.toLocaleString()} MT
              </div>
            </CardContent>
          </Card>

          <Card data-testid="card-avg-price">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Avg Price/MT</CardTitle>
              <DollarSign className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-avg-price">
                ${stats.avgPrice.toFixed(2)}
              </div>
            </CardContent>
          </Card>

          <Card data-testid="card-total-cost">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Cost</CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-total-cost">
                ${stats.totalCost.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
              </div>
            </CardContent>
          </Card>

          <Card data-testid="card-vlsfo-events">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">VLSFO Events</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-vlsfo-events">{stats.vlsfoEvents}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-lng-events">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">LNG Events</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-lng-events">{stats.lngEvents}</div>
            </CardContent>
          </Card>
        </div>

        {/* Filters */}
        <div className="flex gap-4">
          <Select value={selectedFuelType} onValueChange={setSelectedFuelType}>
            <SelectTrigger className="w-[200px]" data-testid="select-fuel-type">
              <SelectValue placeholder="Filter by fuel type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Fuel Types</SelectItem>
              <SelectItem value="vlsfo">VLSFO</SelectItem>
              <SelectItem value="hsfo">HSFO</SelectItem>
              <SelectItem value="mgo">MGO</SelectItem>
              <SelectItem value="lng">LNG</SelectItem>
              <SelectItem value="methanol">Methanol</SelectItem>
            </SelectContent>
          </Select>

          <Select value={selectedPortId} onValueChange={setSelectedPortId}>
            <SelectTrigger className="w-[200px]" data-testid="select-port-filter">
              <SelectValue placeholder="Filter by port" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Ports</SelectItem>
              {ports.map(port => (
                <SelectItem key={port.id} value={port.id}>
                  {port.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Content Tabs */}
        <Tabs defaultValue="events" className="space-y-4">
          <TabsList>
            <TabsTrigger value="events" data-testid="tab-events">Events</TabsTrigger>
            <TabsTrigger value="analytics" data-testid="tab-analytics">Analytics</TabsTrigger>
          </TabsList>

          <TabsContent value="events" className="space-y-4">
            {isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map(i => (
                  <Skeleton key={i} className="h-48 w-full" />
                ))}
              </div>
            ) : filteredEvents.length === 0 ? (
              <Card>
                <CardContent className="pt-6">
                  <p className="text-center text-muted-foreground">No bunkering events found</p>
                </CardContent>
              </Card>
            ) : (
              <div className="grid gap-4 md:grid-cols-2">
                {filteredEvents.map(event => (
                  <Card key={event.id} data-testid={`card-event-${event.id}`}>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Fuel className="h-5 w-5" />
                        <span data-testid={`text-fuel-type-${event.id}`}>{event.fuelType.toUpperCase()}</span>
                        <span className="text-sm font-normal text-muted-foreground">
                          ({event.eventType})
                        </span>
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <p className="text-xs text-muted-foreground mb-1">Vessel</p>
                          <div className="flex items-center gap-2">
                            <Ship className="h-3 w-3 text-muted-foreground" />
                            <span className="text-sm font-medium" data-testid={`text-vessel-${event.id}`}>
                              {event.vessel?.name || 'Unknown'}
                            </span>
                          </div>
                        </div>

                        <div>
                          <p className="text-xs text-muted-foreground mb-1">Port</p>
                          <div className="flex items-center gap-2">
                            <MapPin className="h-3 w-3 text-muted-foreground" />
                            <span className="text-sm" data-testid={`text-port-${event.id}`}>
                              {event.port?.name || 'At Sea'}
                            </span>
                          </div>
                        </div>

                        <div>
                          <p className="text-xs text-muted-foreground mb-1">Volume</p>
                          <span className="text-sm font-medium" data-testid={`text-volume-${event.id}`}>
                            {parseFloat(event.volumeMT).toLocaleString()} MT
                          </span>
                        </div>

                        <div>
                          <p className="text-xs text-muted-foreground mb-1">Price/MT</p>
                          <span className="text-sm" data-testid={`text-price-${event.id}`}>
                            {event.pricePerMT ? `$${parseFloat(event.pricePerMT).toFixed(2)}` : 'N/A'}
                          </span>
                        </div>

                        <div>
                          <p className="text-xs text-muted-foreground mb-1">Total Cost</p>
                          <span className="text-sm font-medium" data-testid={`text-cost-${event.id}`}>
                            {event.totalCost ? `$${parseFloat(event.totalCost).toLocaleString(undefined, { minimumFractionDigits: 2 })}` : 'N/A'}
                          </span>
                        </div>

                        <div>
                          <p className="text-xs text-muted-foreground mb-1">Supplier</p>
                          <div className="flex items-center gap-2">
                            <User className="h-3 w-3 text-muted-foreground" />
                            <span className="text-sm" data-testid={`text-supplier-${event.id}`}>
                              {event.supplier || 'N/A'}
                            </span>
                          </div>
                        </div>

                        <div className="col-span-2">
                          <p className="text-xs text-muted-foreground mb-1">Start Time</p>
                          <div className="flex items-center gap-2">
                            <Calendar className="h-3 w-3 text-muted-foreground" />
                            <span className="text-sm" data-testid={`text-start-time-${event.id}`}>
                              {event.startTime ? new Date(event.startTime).toLocaleString() : 'N/A'}
                            </span>
                          </div>
                        </div>

                        {event.grade && (
                          <div className="col-span-2">
                            <p className="text-xs text-muted-foreground mb-1">Grade/Specs</p>
                            <span className="text-sm" data-testid={`text-grade-${event.id}`}>
                              {event.grade}
                            </span>
                          </div>
                        )}
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="analytics" className="space-y-4">
            <div className="grid gap-4 md:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Fuel Type Distribution</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {fuelDistribution.map(fuel => (
                      <div key={fuel.name} className="flex items-center justify-between">
                        <span className="text-sm">{fuel.name}</span>
                        <div className="flex items-center gap-2">
                          <div className="w-32 h-2 bg-secondary rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-primary" 
                              style={{ width: stats.totalEvents > 0 ? `${(fuel.value / stats.totalEvents) * 100}%` : '0%' }}
                            />
                          </div>
                          <span className="text-sm font-medium w-12 text-right">{fuel.value}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Event Type Distribution</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {Object.entries(eventTypeDistribution).map(([type, count]) => (
                      <div key={type} className="flex items-center justify-between">
                        <span className="text-sm capitalize">{type.replace('_', ' ')}</span>
                        <div className="flex items-center gap-2">
                          <div className="w-32 h-2 bg-secondary rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-primary" 
                              style={{ width: stats.totalEvents > 0 ? `${(count / stats.totalEvents) * 100}%` : '0%' }}
                            />
                          </div>
                          <span className="text-sm font-medium w-12 text-right">{count}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <Card className="md:col-span-2">
                <CardHeader>
                  <CardTitle>Volume & Cost Insights</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Avg Volume/Event</p>
                      <p className="text-2xl font-bold">
                        {stats.totalEvents > 0 ? (stats.totalVolume / stats.totalEvents).toFixed(1) : '0'} MT
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Avg Cost/Event</p>
                      <p className="text-2xl font-bold">
                        ${stats.totalEvents > 0 ? (stats.totalCost / stats.totalEvents).toLocaleString(undefined, { maximumFractionDigits: 0 }) : '0'}
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Port Bunkering</p>
                      <p className="text-2xl font-bold">
                        {eventTypeDistribution['port_bunkering'] || 0}
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">STS Bunkering</p>
                      <p className="text-2xl font-bold">
                        {eventTypeDistribution['sts_bunkering'] || 0}
                      </p>
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
