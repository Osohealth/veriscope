import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Link, useLocation } from "wouter";
import { ArrowLeft, Radio, Ship, MapPin, Activity, Search, Filter, RefreshCw, LogIn } from "lucide-react";
import MapPanel from "@/components/MapPanel";
import type { Vessel } from "@shared/schema";

interface VesselWithPosition extends Vessel {
  position?: {
    latitude: number;
    longitude: number;
    speed?: number;
    heading?: number;
    navigationStatus?: string;
    timestamp?: Date;
  };
}

export default function AisVesselTracking() {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedVesselType, setSelectedVesselType] = useState<string>("all");
  const [selectedVessel, setSelectedVessel] = useState<VesselWithPosition | null>(null);
  const [mapCenter, setMapCenter] = useState<[number, number] | undefined>(undefined);
  const [, setLocation] = useLocation();

  // Check if user is logged in
  const isLoggedIn = typeof window !== 'undefined' && !!localStorage.getItem('access_token');

  // Fetch all vessels
  const { data: vessels = [], isLoading, error, refetch } = useQuery<VesselWithPosition[]>({
    queryKey: ['/api/vessels'],
    enabled: isLoggedIn,
    retry: false
  });

  // Check if error is authentication related
  const isAuthError = error && (error.message.includes('401') || error.message.includes('Unauthorized'));

  // Fetch AIS positions for selected vessel
  const { data: positions = [] } = useQuery<any[]>({
    queryKey: ['/api/ais-positions', selectedVessel?.mmsi],
    enabled: !!selectedVessel?.mmsi
  });

  // Filter vessels based on search and type
  const filteredVessels = vessels.filter(vessel => {
    const matchesSearch = !searchQuery || 
      vessel.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      vessel.mmsi?.toString().includes(searchQuery) ||
      vessel.imo?.toString().includes(searchQuery);
    
    const matchesType = selectedVesselType === "all" || vessel.vesselType === selectedVesselType;
    
    return matchesSearch && matchesType;
  });

  // Update selected vessel with latest position
  useEffect(() => {
    if (selectedVessel && positions.length > 0) {
      const latestPosition = positions[0];
      setSelectedVessel({
        ...selectedVessel,
        position: latestPosition
      });
    }
  }, [positions]);

  const handleVesselClick = (vessel: VesselWithPosition) => {
    setSelectedVessel(vessel);
    if (vessel.position?.latitude && vessel.position?.longitude) {
      setMapCenter([vessel.position.latitude, vessel.position.longitude]);
    }
  };

  const getStatusColor = (status?: string): "secondary" | "default" | "destructive" | "outline" => {
    if (!status) return "secondary";
    const lowerStatus = status.toLowerCase();
    if (lowerStatus.includes("moored") || lowerStatus.includes("anchored")) return "default";
    if (lowerStatus.includes("underway")) return "outline";
    if (lowerStatus.includes("not under command")) return "destructive";
    return "secondary";
  };

  const getVesselTypeLabel = (type: string) => {
    const labels: Record<string, string> = {
      vlcc: "VLCC",
      suezmax: "Suezmax",
      aframax: "Aframax",
      panamax: "Panamax"
    };
    return labels[type] || type.toUpperCase();
  };

  const formatCoordinate = (value: unknown, decimals: number = 6): string => {
    if (value == null) return 'N/A';
    const num = parseFloat(String(value));
    if (Number.isNaN(num)) return 'N/A';
    return num.toFixed(decimals);
  };

  // Show login prompt if not authenticated
  if (!isLoggedIn || isAuthError) {
    return (
      <div className="min-h-screen bg-background">
        <header className="bg-card border-b border-border px-6 py-4">
          <div className="flex items-center gap-4">
            <Link href="/maritime">
              <Button variant="ghost" size="sm">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Maritime
              </Button>
            </Link>
            <div>
              <div className="flex items-center gap-2">
                <Radio className="w-6 h-6 text-primary" />
                <h1 className="text-2xl font-bold">AIS / Vessel Tracking</h1>
              </div>
              <p className="text-sm text-muted-foreground">Live vessel positions and tracking data</p>
            </div>
          </div>
        </header>
        <div className="flex items-center justify-center h-[calc(100vh-5rem)]">
          <Card className="w-full max-w-md mx-4">
            <CardHeader className="text-center">
              <CardTitle className="flex items-center justify-center gap-2">
                <LogIn className="w-6 h-6" />
                Authentication Required
              </CardTitle>
            </CardHeader>
            <CardContent className="text-center space-y-4">
              <p className="text-muted-foreground">
                Please log in to access vessel tracking data. Your session may have expired.
              </p>
              <Link href="/auth/login">
                <Button className="w-full">
                  <LogIn className="w-4 h-4 mr-2" />
                  Go to Login
                </Button>
              </Link>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

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
                <Radio className="w-6 h-6 text-primary" />
                <h1 className="text-2xl font-bold" data-testid="text-page-title">AIS / Vessel Tracking</h1>
              </div>
              <p className="text-sm text-muted-foreground">Live vessel positions and tracking data</p>
            </div>
          </div>
          <Button variant="outline" size="sm" onClick={() => refetch()} data-testid="button-refresh">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </header>

      <div className="flex h-[calc(100vh-5rem)]">
        {/* Sidebar - Vessel List */}
        <div className="w-96 border-r border-border flex flex-col bg-card">
          {/* Filters */}
          <div className="p-4 border-b border-border space-y-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                placeholder="Search by name, MMSI, or IMO..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9"
                data-testid="input-search-vessel"
              />
            </div>
            <div>
              <Label className="text-xs text-muted-foreground mb-1 block">Vessel Type</Label>
              <Select value={selectedVesselType} onValueChange={setSelectedVesselType}>
                <SelectTrigger data-testid="select-vessel-type">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Types</SelectItem>
                  <SelectItem value="vlcc">VLCC</SelectItem>
                  <SelectItem value="suezmax">Suezmax</SelectItem>
                  <SelectItem value="aframax">Aframax</SelectItem>
                  <SelectItem value="panamax">Panamax</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>Showing {filteredVessels.length} vessels</span>
              {searchQuery && (
                <Button 
                  variant="ghost" 
                  size="sm" 
                  onClick={() => setSearchQuery("")}
                  className="h-6 text-xs"
                  data-testid="button-clear-search"
                >
                  Clear
                </Button>
              )}
            </div>
          </div>

          {/* Vessel List */}
          <div className="flex-1 overflow-y-auto">
            {isLoading ? (
              <div className="p-4 space-y-3">
                {[...Array(8)].map((_, i) => (
                  <Skeleton key={i} className="h-24 w-full" />
                ))}
              </div>
            ) : filteredVessels.length === 0 ? (
              <div className="p-8 text-center text-muted-foreground">
                <Ship className="w-12 h-12 mx-auto mb-3 opacity-50" />
                <p>No vessels found</p>
              </div>
            ) : (
              <div className="p-2 space-y-2">
                {filteredVessels.map((vessel) => (
                  <Card
                    key={vessel.mmsi}
                    className={`cursor-pointer transition-all hover:border-primary ${
                      selectedVessel?.mmsi === vessel.mmsi ? 'border-primary bg-accent' : ''
                    }`}
                    onClick={() => handleVesselClick(vessel)}
                    data-testid={`card-vessel-${vessel.mmsi}`}
                  >
                    <CardContent className="p-3">
                      <div className="flex items-start justify-between">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <Ship className="w-4 h-4 text-primary flex-shrink-0" />
                            <h3 className="font-semibold text-sm truncate" data-testid={`text-vessel-name-${vessel.mmsi}`}>
                              {vessel.name}
                            </h3>
                          </div>
                          <div className="space-y-1 text-xs text-muted-foreground">
                            <div className="flex items-center gap-2">
                              <span>MMSI: {vessel.mmsi}</span>
                              {vessel.imo && <span>IMO: {vessel.imo}</span>}
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant="outline" className="text-xs">
                                {getVesselTypeLabel(vessel.vesselType)}
                              </Badge>
                              {vessel.deadweight && (
                                <span className="text-xs">{vessel.deadweight.toLocaleString()} DWT</span>
                              )}
                            </div>
                          </div>
                        </div>
                        {vessel.position && (
                          <Activity className="w-4 h-4 text-green-500 flex-shrink-0" />
                        )}
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Main Content - Map and Details */}
        <div className="flex-1 flex flex-col">
          {/* Map */}
          <div className="flex-1">
            <MapPanel
              selectedPort=""
              timeRange="24h"
              scope="tankscope"
              mapCenter={mapCenter}
              onMapCenterReset={() => setMapCenter(undefined)}
            />
          </div>

          {/* Vessel Details Panel */}
          {selectedVessel && (
            <div className="h-64 border-t border-border bg-card overflow-y-auto">
              <Tabs defaultValue="details" className="h-full">
                <div className="border-b border-border px-4">
                  <TabsList className="h-12">
                    <TabsTrigger value="details" data-testid="tab-details">Vessel Details</TabsTrigger>
                    <TabsTrigger value="position" data-testid="tab-position">Position Data</TabsTrigger>
                    <TabsTrigger value="track" data-testid="tab-track">Track History</TabsTrigger>
                  </TabsList>
                </div>

                <TabsContent value="details" className="p-4 space-y-3 mt-0">
                  <div className="grid grid-cols-3 gap-4">
                    <div>
                      <Label className="text-xs text-muted-foreground">Vessel Name</Label>
                      <p className="font-medium" data-testid="text-detail-name">{selectedVessel.name}</p>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">MMSI</Label>
                      <p className="font-medium" data-testid="text-detail-mmsi">{selectedVessel.mmsi}</p>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">IMO</Label>
                      <p className="font-medium" data-testid="text-detail-imo">{selectedVessel.imo || 'N/A'}</p>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">Vessel Type</Label>
                      <Badge variant="outline" data-testid="badge-vessel-type">
                        {getVesselTypeLabel(selectedVessel.vesselType)}
                      </Badge>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">Deadweight</Label>
                      <p className="font-medium" data-testid="text-detail-dwt">
                        {selectedVessel.deadweight?.toLocaleString() || 'N/A'} MT
                      </p>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">Flag</Label>
                      <p className="font-medium" data-testid="text-detail-flag">{selectedVessel.flag || 'N/A'}</p>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="position" className="p-4 space-y-3 mt-0">
                  {selectedVessel.position ? (
                    <div className="grid grid-cols-3 gap-4">
                      <div>
                        <Label className="text-xs text-muted-foreground">Latitude</Label>
                        <p className="font-medium" data-testid="text-position-lat">
                          {formatCoordinate(selectedVessel.position.latitude, 6)}°
                        </p>
                      </div>
                      <div>
                        <Label className="text-xs text-muted-foreground">Longitude</Label>
                        <p className="font-medium" data-testid="text-position-lng">
                          {formatCoordinate(selectedVessel.position.longitude, 6)}°
                        </p>
                      </div>
                      <div>
                        <Label className="text-xs text-muted-foreground">Speed</Label>
                        <p className="font-medium" data-testid="text-position-speed">
                          {formatCoordinate(selectedVessel.position.speed, 1)} knots
                        </p>
                      </div>
                      <div>
                        <Label className="text-xs text-muted-foreground">Heading</Label>
                        <p className="font-medium" data-testid="text-position-heading">
                          {selectedVessel.position.heading || 'N/A'}°
                        </p>
                      </div>
                      <div>
                        <Label className="text-xs text-muted-foreground">Status</Label>
                        <Badge variant={getStatusColor(selectedVessel.position.navigationStatus)} data-testid="badge-nav-status">
                          {selectedVessel.position.navigationStatus || 'Unknown'}
                        </Badge>
                      </div>
                      <div>
                        <Label className="text-xs text-muted-foreground">Last Update</Label>
                        <p className="font-medium text-xs" data-testid="text-position-timestamp">
                          {selectedVessel.position.timestamp 
                            ? new Date(selectedVessel.position.timestamp).toLocaleString()
                            : 'N/A'}
                        </p>
                      </div>
                    </div>
                  ) : (
                    <div className="text-center text-muted-foreground py-8">
                      <MapPin className="w-12 h-12 mx-auto mb-3 opacity-50" />
                      <p>No position data available</p>
                    </div>
                  )}
                </TabsContent>

                <TabsContent value="track" className="p-4 mt-0">
                  <div className="text-center text-muted-foreground py-8">
                    <Activity className="w-12 h-12 mx-auto mb-3 opacity-50" />
                    <p className="mb-2">Track history visualization</p>
                    <p className="text-xs">Historical vessel positions will be displayed here</p>
                  </div>
                </TabsContent>
              </Tabs>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
