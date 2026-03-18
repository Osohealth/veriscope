import { useQuery } from "@tanstack/react-query";
import { useParams, Link } from "wouter";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ArrowLeft, Anchor, Ship, Clock, MapPin, Activity, Users } from "lucide-react";
import { getAuthHeaders, getAuthToken } from "@/lib/queryClient";

interface PortDetail {
  id: string;
  name: string;
  unlocode: string;
  country_code: string;
  latitude: number;
  longitude: number;
  timezone: string;
  metrics_7d: {
    arrivals: number;
    departures: number;
    unique_vessels: number;
    avg_dwell_hours: number;
    median_dwell_hours?: number;
    open_calls?: number;
  };
}

interface PortCall {
  id: string;
  vessel_id: string;
  vessel_name: string;
  arrival_time_utc: string;
  departure_time_utc: string | null;
  dwell_hours: number | null;
}

export default function PortDetailPage() {
  const { portId } = useParams<{ portId: string }>();
  const token = getAuthToken();
  const isAuthed = !!token;
  
  const { data: port, isLoading: portLoading } = useQuery<PortDetail>({
    queryKey: ['/v1/ports', portId],
    queryFn: async () => {
      const res = await fetch(`/v1/ports/${portId}`, {
        headers: getAuthHeaders()
      });
      if (!res.ok) throw new Error('Failed to fetch port');
      return res.json();
    },
    enabled: isAuthed && !!portId
  });

  const { data: callsData, isLoading: callsLoading } = useQuery<{ items: PortCall[] }>({
    queryKey: ['/v1/ports', portId, 'calls'],
    queryFn: async () => {
      const res = await fetch(`/v1/ports/${portId}/calls?limit=20`, {
        headers: getAuthHeaders()
      });
      if (!res.ok) throw new Error('Failed to fetch port calls');
      return res.json();
    },
    enabled: isAuthed && !!portId
  });

  const calls = callsData?.items || [];
  const hasServerMetrics = !!port?.metrics_7d &&
    (port.metrics_7d.arrivals +
      port.metrics_7d.departures +
      port.metrics_7d.unique_vessels +
      (port.metrics_7d.open_calls ?? 0) > 0);

  const derivedMetrics = (() => {
    const arrivals = calls.length;
    const departures = calls.filter(call => !!call.departure_time_utc).length;
    const uniqueVessels = new Set(calls.map(call => call.vessel_id)).size;
    const openCalls = calls.filter(call => !call.departure_time_utc).length;
    const dwellValues = calls
      .map(call => call.dwell_hours)
      .filter((value): value is number => typeof value === 'number' && !Number.isNaN(value));

    const avgDwell = dwellValues.length
      ? Math.round((dwellValues.reduce((sum, v) => sum + v, 0) / dwellValues.length) * 10) / 10
      : 0;

    const medianDwell = dwellValues.length
      ? (() => {
          const sorted = [...dwellValues].sort((a, b) => a - b);
          const mid = Math.floor(sorted.length / 2);
          return sorted.length % 2 === 0
            ? Math.round(((sorted[mid - 1] + sorted[mid]) / 2) * 10) / 10
            : sorted[mid];
        })()
      : undefined;

    return {
      arrivals,
      departures,
      unique_vessels: uniqueVessels,
      avg_dwell_hours: avgDwell,
      median_dwell_hours: medianDwell,
      open_calls: openCalls,
    };
  })();

  const metrics = hasServerMetrics ? port?.metrics_7d : derivedMetrics;

  if (portLoading) {
    return (
      <div className="min-h-screen bg-background p-6">
        <Skeleton className="h-8 w-48 mb-4" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (!port) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold mb-2">Port Not Found</h2>
          <Link href="/maritime/ais-tracking">
            <Button variant="outline">Back to Map</Button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center gap-4">
          <Link href="/maritime/ais-tracking">
            <Button variant="ghost" size="sm">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Map
            </Button>
          </Link>
          <div>
            <div className="flex items-center gap-2">
              <Anchor className="w-6 h-6 text-primary" />
              <h1 className="text-2xl font-bold">{port.name}</h1>
              <Badge variant="outline">{port.unlocode}</Badge>
            </div>
            <p className="text-sm text-muted-foreground">
              {port.country_code} | Lat: {port.latitude.toFixed(4)}, Lng: {port.longitude.toFixed(4)}
            </p>
          </div>
        </div>
      </header>

      <main className="p-6 space-y-6">
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Ship className="w-4 h-4" />
                Arrivals (7d)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{metrics?.arrivals ?? 0}</div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Ship className="w-4 h-4" />
                Departures (7d)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{metrics?.departures ?? 0}</div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Unique Vessels
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{metrics?.unique_vessels ?? 0}</div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Clock className="w-4 h-4" />
                Avg Dwell Time
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{metrics?.avg_dwell_hours ?? 0}h</div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Clock className="w-4 h-4" />
                Median Dwell
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{metrics?.median_dwell_hours ?? '-'}h</div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Users className="w-4 h-4" />
                In Port Now
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-green-500">{metrics?.open_calls ?? 0}</div>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <MapPin className="w-5 h-5" />
              Recent Port Calls
            </CardTitle>
          </CardHeader>
          <CardContent>
            {callsLoading ? (
              <div className="space-y-2">
                {[1, 2, 3].map(i => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : calls.length === 0 ? (
              <p className="text-muted-foreground text-center py-8">No recent port calls</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-border">
                      <th className="text-left py-2 px-3 text-sm font-medium text-muted-foreground">Vessel</th>
                      <th className="text-left py-2 px-3 text-sm font-medium text-muted-foreground">Arrival</th>
                      <th className="text-left py-2 px-3 text-sm font-medium text-muted-foreground">Departure</th>
                      <th className="text-left py-2 px-3 text-sm font-medium text-muted-foreground">Dwell</th>
                    </tr>
                  </thead>
                  <tbody>
                    {calls.map(call => (
                      <tr key={call.id} className="border-b border-border hover:bg-muted/50">
                        <td className="py-3 px-3">
                          <span className="font-medium">{call.vessel_name}</span>
                        </td>
                        <td className="py-3 px-3 text-sm text-muted-foreground">
                          {call.arrival_time_utc ? new Date(call.arrival_time_utc).toLocaleString() : '-'}
                        </td>
                        <td className="py-3 px-3 text-sm text-muted-foreground">
                          {call.departure_time_utc ? new Date(call.departure_time_utc).toLocaleString() : (
                            <Badge variant="outline" className="text-green-500 border-green-500">In Port</Badge>
                          )}
                        </td>
                        <td className="py-3 px-3 text-sm">
                          {call.dwell_hours ? `${call.dwell_hours}h` : '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </main>
    </div>
  );
}
