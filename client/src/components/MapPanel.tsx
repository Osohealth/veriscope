import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { useVessels } from "@/hooks/use-vessels";
import { MapContainer, TileLayer, Marker, Popup, Tooltip } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

interface ApiPort {
  id: string;
  name: string;
  unlocode: string;
  country_code: string;
  latitude: number;
  longitude: number;
  timezone: string;
}

// Fix default markers
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

interface MapPanelProps {
  selectedPort: string;
  timeRange: string;
  scope: string;
  layers?: any;
  vesselTypes?: any;
  mapCenter?: [number, number] | null;
  onMapCenterReset?: () => void;
}

export default function MapPanel({ selectedPort, timeRange, scope, layers: externalLayers, vesselTypes: externalVesselTypes, mapCenter, onMapCenterReset }: MapPanelProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const [, navigate] = useLocation();
  
  // Use external layers/vesselTypes if provided, otherwise use local state
  const layers = externalLayers || (scope === 'flightscope' ? {
    flights: true,
    airports: true,
    airspace: false,
    routes: false,
  } : {
    vessels: true,
    portAreas: true,
    storageFarms: false,
    shippingLanes: false,
  });
  
  const vesselTypes = externalVesselTypes || (scope === 'flightscope' ? {
    passenger: true,
    cargo: true,
    private: true,
  } : {
    vlcc: true,
    suezmax: true,
    aframax: true,
  });

  const { data: vessels, isLoading } = useVessels();
  
  // Fetch ports from API
  const { data: portsData } = useQuery<{ items: ApiPort[] }>({
    queryKey: ['/v1/ports'],
    queryFn: async () => {
      const res = await fetch('/v1/ports');
      if (!res.ok) throw new Error('Failed to fetch ports');
      return res.json();
    }
  });
  
  const apiPorts = portsData?.items || [];

  // Fallback port coordinates (consistent with Sidebar)
  const ports = {
    fujairah: { lat: 25.1204, lng: 56.3541, name: 'Fujairah Port' },
    rotterdam: { lat: 51.9225, lng: 4.4792, name: 'Port of Rotterdam' },
    dxb: { lat: 25.2532, lng: 55.3657, name: 'Dubai International Airport' },
    ams: { lat: 52.3105, lng: 4.7683, name: 'Amsterdam Schiphol Airport' }
  };

  // Get map center based on selected port
  const getMapCenter = (): [number, number] => {
    const port = ports[selectedPort as keyof typeof ports];
    return port ? [port.lat, port.lng] : [30, 30]; // Default center
  };

  // Handle map centering when mapCenter prop changes
  useEffect(() => {
    if (mapCenter && onMapCenterReset) {
      // Reset the mapCenter prop after using it to avoid stale centering
      onMapCenterReset();
    }
  }, [mapCenter, onMapCenterReset]);

  // Create custom icons for vessels and ports
  const createVesselIcon = (status?: string) => {
    const color = getVesselColor(status);
    return L.divIcon({
      className: 'custom-vessel-marker',
      html: `<div style="background-color: ${color}; width: 12px; height: 12px; border-radius: 50%; border: 2px solid white; box-shadow: 0 0 4px rgba(0,0,0,0.3);"></div>`,
      iconSize: [12, 12],
      iconAnchor: [6, 6]
    });
  };

  const createPortIcon = () => {
    return L.divIcon({
      className: 'custom-port-marker',
      html: `<div style="background-color: #dc2626; width: 16px; height: 16px; border-radius: 2px; border: 2px solid white; box-shadow: 0 0 6px rgba(0,0,0,0.4);"></div>`,
      iconSize: [16, 16],
      iconAnchor: [8, 8]
    });
  };

  useEffect(() => {
    setMapLoaded(true);
  }, [selectedPort, vessels, scope]);

  const getVesselColor = (status?: string) => {
    switch (status) {
      case 'AT_ANCHOR': return '#F59E0B'; // amber
      case 'AT_BERTH': return '#EF4444'; // red
      case 'UNDERWAY': return '#10B981'; // emerald
      default: return '#60A5FA'; // blue
    }
  };


  const getVesselCount = (type: string) => {
    if (!vessels) return 0;
    return vessels.filter(v => v.vesselClass?.toLowerCase().includes(type.toLowerCase())).length;
  };

  return (
    <div className="flex-1 relative">
      {/* Map Container */}
      <div className="w-full h-full" data-testid="map-container">
        {scope !== 'flightscope' ? (
          <MapContainer
            key={`${selectedPort}-${Object.values(layers).join('-')}-${mapCenter ? mapCenter.join(',') : 'default'}`}
            center={getMapCenter()}
            zoom={selectedPort === 'fujairah' || selectedPort === 'rotterdam' ? 6 : 4}
            style={{ height: '100%', width: '100%' }}
            className="leaflet-container"
          >
            <TileLayer
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            />
            
            {/* Port Markers from API */}
            {apiPorts.map((port) => (
              <Marker
                key={port.id}
                position={[port.latitude, port.longitude]}
                icon={createPortIcon()}
                eventHandlers={{
                  click: () => navigate(`/ports/${port.id}`)
                }}
              >
                <Popup>
                  <div className="text-sm">
                    <div className="font-semibold">{port.name}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      {port.unlocode} | {port.country_code}
                    </div>
                    <div className="text-xs text-blue-500 mt-2 cursor-pointer" onClick={() => navigate(`/ports/${port.id}`)}>
                      View Details →
                    </div>
                  </div>
                </Popup>
                <Tooltip direction="top" offset={[0, -10]} opacity={1}>
                  {port.name}
                </Tooltip>
              </Marker>
            ))}
            
            {/* Vessel Markers */}
            {vessels && layers.vessels && vessels.map((vessel) => {
              if (!vessel.position?.latitude || !vessel.position?.longitude) return null;
              
              const lat = typeof vessel.position.latitude === 'string' ? parseFloat(vessel.position.latitude) : vessel.position.latitude;
              const lng = typeof vessel.position.longitude === 'string' ? parseFloat(vessel.position.longitude) : vessel.position.longitude;
              
              return (
                <Marker
                  key={vessel.mmsi}
                  position={[lat, lng]}
                  icon={createVesselIcon(vessel.position.navigationStatus)}
                >
                  <Popup>
                    <div className="text-sm">
                      <div className="font-semibold">{vessel.name}</div>
                      <div className="text-xs text-muted-foreground mt-1">
                        MMSI: {vessel.mmsi}<br/>
                        Class: {vessel.vesselClass || 'Unknown'}<br/>
                        Status: {vessel.position.navigationStatus || 'Unknown'}<br/>
                        Speed: {vessel.position.speedOverGround?.toFixed(1) || 0} knots<br/>
                        Course: {vessel.position.navigationStatus || 'Unknown'}
                      </div>
                    </div>
                  </Popup>
                  <Tooltip direction="top" offset={[0, -10]} opacity={1}>
                    {vessel.name}
                  </Tooltip>
                </Marker>
              );
            })}
          </MapContainer>
        ) : (
          // Placeholder for FlightScope
          <div className="w-full h-full bg-muted flex items-center justify-center">
            <div className="text-center">
              <div className="text-lg font-semibold text-foreground mb-2">Aviation Map View</div>
              <div className="text-sm text-muted-foreground mb-4">
                Flight tracking for {selectedPort === 'dxb' ? 'Middle East' : 'Europe'}
              </div>
              <div className="text-xs text-muted-foreground">124 flights currently tracked</div>
            </div>
          </div>
        )}
      </div>
      
      {/* Loading Overlay */}
      {isLoading && scope !== 'flightscope' && (
        <div className="absolute inset-0 bg-background/50 flex items-center justify-center z-[1000]">
          <div className="text-foreground">Loading vessel data...</div>
        </div>
      )}
    </div>
  );
}
