import { useMemo } from "react";
import { MapContainer, TileLayer, Marker, Tooltip, Polyline } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders } from "@/lib/queryClient";
import { useTerminalStore } from "@/hooks/use-terminal-store";

type FlowLane = {
  id: string;
  originId: string | null;
  originName: string;
  originLat: number | null;
  originLng: number | null;
  destinationId: string | null;
  destinationName: string;
  destinationLat: number | null;
  destinationLng: number | null;
  commodity: string;
  volume: number;
  unit: string;
  deltaPct?: number;
  zScore?: number;
};

const markerIcon = new L.DivIcon({
  className: "flow-port-marker",
  html: '<div style="background:#38bdf8;width:10px;height:10px;border-radius:50%;border:2px solid #0f172a;"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
});

const selectedIcon = new L.DivIcon({
  className: "flow-port-marker-selected",
  html: '<div style="background:#f97316;width:14px;height:14px;border-radius:50%;border:2px solid #0f172a;box-shadow:0 0 8px rgba(249,115,22,0.6);"></div>',
  iconSize: [18, 18],
  iconAnchor: [9, 9],
});

const buildQuery = (filters: any) => {
  const params = new URLSearchParams();
  if (filters.commodity?.[0]) params.set("commodity", filters.commodity[0]);
  if (filters.origin?.[0]) params.set("origin", filters.origin[0]);
  if (filters.destination?.[0]) params.set("destination", filters.destination[0]);
  if (filters.hub?.[0]) params.set("hub", filters.hub[0]);
  if (filters.region?.[0]) params.set("region", filters.region[0]);
  if (filters.timeMode === "range" && filters.timeWindow) params.set("time", filters.timeWindow);
  return params.toString();
};

export default function FlowMap() {
  const { filters, selectedEntity, setSelectedEntity } = useTerminalStore();
  const query = buildQuery(filters);

  const { data } = useQuery<{ items: FlowLane[] }>({
    queryKey: ["/v1/flows/lanes", query],
    queryFn: async () => {
      const res = await fetch(`/v1/flows/lanes?${query}`, { headers: getAuthHeaders() });
      if (!res.ok) throw new Error("Failed to load lanes");
      return res.json();
    },
  });

  const lanes = data?.items ?? [];

  const nodes = useMemo(() => {
    const map = new Map<string, { id: string; name: string; lat: number; lng: number }>();
    lanes.forEach((lane) => {
      if (lane.originId && lane.originLat && lane.originLng) {
        map.set(lane.originId, {
          id: lane.originId,
          name: lane.originName,
          lat: lane.originLat,
          lng: lane.originLng,
        });
      }
      if (lane.destinationId && lane.destinationLat && lane.destinationLng) {
        map.set(lane.destinationId, {
          id: lane.destinationId,
          name: lane.destinationName,
          lat: lane.destinationLat,
          lng: lane.destinationLng,
        });
      }
    });
    return Array.from(map.values());
  }, [lanes]);

  return (
    <MapContainer center={[20, 20]} zoom={2} className="h-[360px] w-full rounded-lg">
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
      />

      {lanes.slice(0, 20).map((lane) => {
        if (!lane.originLat || !lane.originLng || !lane.destinationLat || !lane.destinationLng) return null;
        const isSelected = selectedEntity?.id === lane.id;
        return (
          <Polyline
            key={lane.id}
            positions={[
              [lane.originLat, lane.originLng],
              [lane.destinationLat, lane.destinationLng],
            ]}
            pathOptions={{
              color: isSelected ? "#f97316" : "#38bdf8",
              weight: isSelected ? 3 : 2,
              opacity: isSelected ? 0.9 : 0.6,
            }}
            eventHandlers={{
              click: () =>
                setSelectedEntity({
                  id: lane.id,
                  name: `${lane.originName} -> ${lane.destinationName}`,
                  type: "lane",
                }),
            }}
          />
        );
      })}

      {nodes.map((node) => {
        const isSelected = selectedEntity?.id === node.id;
        return (
          <Marker
            key={node.id}
            position={[node.lat, node.lng]}
            icon={isSelected ? selectedIcon : markerIcon}
            eventHandlers={{
              click: () =>
                setSelectedEntity({
                  id: node.id,
                  name: node.name,
                  type: "port",
                }),
            }}
          >
            <Tooltip direction="top" offset={[0, -8]}>
              <div className="text-xs font-medium">{node.name}</div>
            </Tooltip>
          </Marker>
        );
      })}
    </MapContainer>
  );
}
