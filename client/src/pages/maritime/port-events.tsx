import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Link } from "wouter";
import { ArrowLeft, Anchor, Ship, Clock, TrendingUp, Calendar, MapPin } from "lucide-react";
import type { PortCall, Port, Vessel } from "@shared/schema";

interface EnrichedPortCall extends PortCall {
  port?: Port;
  vessel?: Vessel;
}

export default function PortEventEngine() {
  const [selectedPort, setSelectedPort] = useState<string>("all");
  const [selectedVessel, setSelectedVessel] = useState<string | null>(null);

  // Fetch ports
  const { data: ports = [] } = useQuery<Port[]>({
    queryKey: ['/api/ports']
  });

  // Fetch vessels
  const { data: vessels = [] } = useQuery<Vessel[]>({
    queryKey: ['/api/vessels']
  });

  // Fetch port calls with proper port filtering
  const portCallsUrl = selectedPort !== "all" 
    ? `/api/port-calls?portId=${selectedPort}` 
    : '/api/port-calls';
  
  const { data: portCalls = [], isLoading } = useQuery<PortCall[]>({
    queryKey: [portCallsUrl],
    enabled: true
  });

  // Enrich port calls with port and vessel data
  const enrichedPortCalls: EnrichedPortCall[] = portCalls.map(call => ({
    ...call,
    port: ports.find(p => p.id === call.portId),
    vessel: vessels.find(v => v.id === call.vesselId)
  }));

  // Calculate statistics from filtered port calls
  const stats = {
    totalCalls: enrichedPortCalls.length,
    activeBerths: enrichedPortCalls.filter(c => c.status === 'berthed').length,
    atAnchor: enrichedPortCalls.filter(c => c.status === 'anchored').length,
    avgBerthTime: enrichedPortCalls.filter(c => c.departureTime && c.arrivalTime).reduce((sum, c) => {
      if (!c.departureTime || !c.arrivalTime) return sum;
      const arrival = new Date(c.arrivalTime).getTime();
      const departure = new Date(c.departureTime).getTime();
      return sum + (departure - arrival);
    }, 0) / Math.max(enrichedPortCalls.filter(c => c.departureTime).length, 1) / (1000 * 60 * 60) // Convert to hours
  };

  const getStatusColor = (status: string): "secondary" | "default" | "destructive" | "outline" => {
    switch (status.toLowerCase()) {
      case 'berthed': return "default";
      case 'anchored': return "outline";
      case 'departed': return "secondary";
      case 'delayed': return "destructive";
      default: return "secondary";
    }
  };

  const formatDuration = (start: Date | null, end?: Date | null) => {
    if (!start) return 'N/A';
    if (!end) return 'In progress';
    const hours = Math.round((new Date(end).getTime() - new Date(start).getTime()) / (1000 * 60 * 60));
    return `${hours}h`;
  };

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/maritime">
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Maritime
              </Button>
            </Link>
            <div>
              <div className="flex items-center gap-2">
                <Anchor className="w-6 h-6 text-primary" />
                <h1 className="text-2xl font-bold" data-testid="text-page-title">Port / Event Engine</h1>
              </div>
              <p className="text-sm text-muted-foreground">Port calls, berth times, and congestion metrics</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card data-testid="card-total-calls">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Total Port Calls</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-total-calls">{stats.totalCalls}</div>
              <p className="text-xs text-muted-foreground mt-1">Active operations</p>
            </CardContent>
          </Card>

          <Card data-testid="card-active-berths">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">At Berth</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-500" data-testid="text-active-berths">{stats.activeBerths}</div>
              <p className="text-xs text-muted-foreground mt-1">Currently berthed</p>
            </CardContent>
          </Card>

          <Card data-testid="card-at-anchor">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">At Anchor</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-yellow-500" data-testid="text-at-anchor">{stats.atAnchor}</div>
              <p className="text-xs text-muted-foreground mt-1">Waiting</p>
            </CardContent>
          </Card>

          <Card data-testid="card-avg-berth-time">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Avg Berth Time</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-500" data-testid="text-avg-berth-time">
                {stats.avgBerthTime.toFixed(1)}h
              </div>
              <p className="text-xs text-muted-foreground mt-1">Turnaround time</p>
            </CardContent>
          </Card>
        </div>

        {/* Filters and Content */}
        <Tabs defaultValue="calls" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList>
              <TabsTrigger value="calls" data-testid="tab-port-calls">Port Calls</TabsTrigger>
              <TabsTrigger value="timeline" data-testid="tab-timeline">Event Timeline</TabsTrigger>
              <TabsTrigger value="analytics" data-testid="tab-analytics">Analytics</TabsTrigger>
            </TabsList>

            <div className="flex items-center gap-3">
              <Select value={selectedPort} onValueChange={setSelectedPort}>
                <SelectTrigger className="w-48" data-testid="select-port-filter">
                  <SelectValue placeholder="Select port" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Ports</SelectItem>
                  {ports.map(port => (
                    <SelectItem key={port.id} value={port.id}>{port.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <TabsContent value="calls" className="space-y-4">
            {isLoading ? (
              <div className="space-y-3">
                {[...Array(5)].map((_, i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : enrichedPortCalls.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center text-muted-foreground">
                  <Anchor className="w-12 h-12 mx-auto mb-3 opacity-50" />
                  <p>No port calls found</p>
                  <p className="text-sm mt-1">Port call data will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <div className="space-y-3">
                {enrichedPortCalls.map((call) => (
                  <Card 
                    key={call.id}
                    className="hover:border-primary transition-colors cursor-pointer"
                    onClick={() => setSelectedVessel(call.vesselId)}
                    data-testid={`card-port-call-${call.id}`}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between">
                        <div className="flex-1 space-y-3">
                          <div className="flex items-center gap-3">
                            <Ship className="w-5 h-5 text-primary" />
                            <div>
                              <h3 className="font-semibold" data-testid={`text-vessel-name-${call.id}`}>
                                {call.vessel?.name || `Vessel ${call.vesselId}`}
                              </h3>
                              <p className="text-sm text-muted-foreground">
                                {call.port?.name || `Port ${call.portId}`}
                              </p>
                            </div>
                          </div>

                          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Arrival</p>
                              <div className="flex items-center gap-1">
                                <Calendar className="w-3 h-3" />
                                <span className="text-sm" data-testid={`text-arrival-${call.id}`}>
                                  {call.arrivalTime ? new Date(call.arrivalTime).toLocaleDateString() : 'N/A'}
                                </span>
                              </div>
                              <p className="text-xs text-muted-foreground">
                                {call.arrivalTime && new Date(call.arrivalTime).toLocaleTimeString()}
                              </p>
                            </div>

                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Departure</p>
                              <div className="flex items-center gap-1">
                                <Calendar className="w-3 h-3" />
                                <span className="text-sm" data-testid={`text-departure-${call.id}`}>
                                  {call.departureTime 
                                    ? new Date(call.departureTime).toLocaleDateString()
                                    : 'In port'}
                                </span>
                              </div>
                              <p className="text-xs text-muted-foreground">
                                {call.departureTime && new Date(call.departureTime).toLocaleTimeString()}
                              </p>
                            </div>

                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Duration</p>
                              <div className="flex items-center gap-1">
                                <Clock className="w-3 h-3" />
                                <span className="text-sm font-medium" data-testid={`text-duration-${call.id}`}>
                                  {formatDuration(call.arrivalTime, call.departureTime)}
                                </span>
                              </div>
                            </div>

                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Status</p>
                              <Badge variant={getStatusColor(call.status)} data-testid={`badge-status-${call.id}`}>
                                {call.status}
                              </Badge>
                            </div>
                          </div>

                          {call.purpose && (
                            <div className="flex items-start gap-2 text-sm">
                              <MapPin className="w-4 h-4 text-muted-foreground mt-0.5" />
                              <span className="text-muted-foreground">Purpose: {call.purpose}</span>
                            </div>
                          )}
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="timeline" className="space-y-4">
            <Card>
              <CardContent className="py-12 text-center text-muted-foreground">
                <TrendingUp className="w-12 h-12 mx-auto mb-3 opacity-50" />
                <p>Event Timeline Visualization</p>
                <p className="text-sm mt-1">Chronological view of port events coming soon</p>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="analytics" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle>Port Performance Analytics</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground mb-2">Congestion Level</p>
                    <div className="flex items-center gap-2">
                      <div className="flex-1 bg-secondary h-2 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-yellow-500"
                          style={{ width: `${(stats.atAnchor / Math.max(stats.totalCalls, 1)) * 100}%` }}
                        />
                      </div>
                      <span className="text-sm font-medium">
                        {((stats.atAnchor / Math.max(stats.totalCalls, 1)) * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>

                  <div>
                    <p className="text-sm text-muted-foreground mb-2">Berth Utilization</p>
                    <div className="flex items-center gap-2">
                      <div className="flex-1 bg-secondary h-2 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-blue-500"
                          style={{ width: `${(stats.activeBerths / Math.max(stats.totalCalls, 1)) * 100}%` }}
                        />
                      </div>
                      <span className="text-sm font-medium">
                        {((stats.activeBerths / Math.max(stats.totalCalls, 1)) * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                </div>

                <div className="pt-4 border-t border-border">
                  <p className="text-sm text-muted-foreground mb-3">Insights</p>
                  <ul className="space-y-2 text-sm">
                    <li className="flex items-start gap-2">
                      <div className="w-1.5 h-1.5 rounded-full bg-blue-500 mt-1.5" />
                      <span>Average berth time is {stats.avgBerthTime.toFixed(1)} hours</span>
                    </li>
                    <li className="flex items-start gap-2">
                      <div className="w-1.5 h-1.5 rounded-full bg-yellow-500 mt-1.5" />
                      <span>{stats.atAnchor} vessels currently waiting at anchor</span>
                    </li>
                    <li className="flex items-start gap-2">
                      <div className="w-1.5 h-1.5 rounded-full bg-green-500 mt-1.5" />
                      <span>{stats.activeBerths} active berth operations</span>
                    </li>
                  </ul>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
