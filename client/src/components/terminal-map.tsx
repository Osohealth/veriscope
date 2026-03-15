import { useMemo } from "react";
import { MapContainer, TileLayer, Marker, Tooltip } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders } from "@/lib/queryClient";
import { useTerminalStore } from "@/hooks/use-terminal-store";

type ApiPort = {
  id: string;
  name: string;
  unlocode: string;
  code: string;
  latitude: number;
  longitude: number;
};

const defaultIcon = new L.DivIcon({
  className: "terminal-port-marker",
  html: '<div style="background:#0ea5e9;width:10px;height:10px;border-radius:50%;border:2px solid #0f172a;"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
});

const selectedIcon = new L.DivIcon({
  className: "terminal-port-marker-selected",
  html: '<div style="background:#f97316;width:14px;height:14px;border-radius:50%;border:2px solid #0f172a;box-shadow:0 0 8px rgba(249,115,22,0.6);"></div>',
  iconSize: [18, 18],
  iconAnchor: [9, 9],
});

export default function TerminalMap() {
  const { filters, selectedEntity, setSelectedEntity } = useTerminalStore();
  const { data } = useQuery<{ items: ApiPort[] }>({
    queryKey: ["/v1/ports"],
    queryFn: async () => {
      const res = await fetch("/v1/ports", { headers: getAuthHeaders() });
      if (!res.ok) throw new Error("Failed to load ports");
      return res.json();
    },
  });

  const filteredPorts = useMemo(() => {
    const ports = data?.items ?? [];
    const hub = filters.hub?.[0];
    if (!hub) return ports;
    const normalized = hub.toLowerCase();
    return ports.filter((port) =>
      [port.name, port.unlocode, port.code].some((value) => value?.toLowerCase().includes(normalized))
    );
  }, [data?.items, filters.hub]);

  const center: [number, number] = [20, 20];

  return (
    <MapContainer center={center} zoom={2} className="h-[360px] w-full rounded-lg">
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
      />
      {filteredPorts.map((port) => {
        const isSelected = selectedEntity?.id === port.id;
        return (
          <Marker
            key={port.id}
            position={[port.latitude, port.longitude]}
            icon={isSelected ? selectedIcon : defaultIcon}
            eventHandlers={{
              click: () =>
                setSelectedEntity({
                  id: port.id,
                  name: port.name,
                  type: "port",
                }),
            }}
          >
            <Tooltip direction="top" offset={[0, -10]}>
              <div className="text-xs font-medium">{port.name}</div>
              <div className="text-[10px] text-muted-foreground">{port.unlocode ?? port.code}</div>
            </Tooltip>
          </Marker>
        );
      })}
    </MapContainer>
  );
}
