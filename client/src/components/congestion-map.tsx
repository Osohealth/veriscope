import { useMemo } from "react";
import { MapContainer, Marker, TileLayer, Tooltip } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useTerminalStore } from "@/hooks/use-terminal-store";

type CongestionPort = {
  id: string;
  portId: string;
  portName: string;
  vesselCount: number;
  queueCount: number;
  avgWaitHours: number;
  dwellHours: number;
  throughputEstimate?: number;
  riskScore: number;
  severity?: "low" | "medium" | "high";
  whyItMatters?: string;
  latitude?: number | null;
  longitude?: number | null;
};

const getSeverityColor = (severity?: string) => {
  if (severity === "high") return "#ef4444";
  if (severity === "medium") return "#f59e0b";
  return "#38bdf8";
};

const buildIcon = (color: string, size: number, selected: boolean) =>
  new L.DivIcon({
    className: "congestion-port-marker",
    html: `<div style="background:${color};width:${size}px;height:${size}px;border-radius:50%;border:2px solid #0f172a;${selected ? "box-shadow:0 0 10px rgba(249,115,22,0.6);" : ""}"></div>`,
    iconSize: [size + 4, size + 4],
    iconAnchor: [(size + 4) / 2, (size + 4) / 2],
  });

export default function CongestionMap({ ports }: { ports: CongestionPort[] }) {
  const { selectedEntity, setSelectedEntity } = useTerminalStore();

  const markers = useMemo(
    () =>
      ports
        .filter((port) => port.latitude != null && port.longitude != null)
        .map((port) => {
          const riskSize = Math.min(18, 8 + Math.round((port.riskScore || 0) / 12));
          const color = getSeverityColor(port.severity);
          const isSelected = selectedEntity?.id === port.portId;
          return {
            ...port,
            icon: buildIcon(color, riskSize, isSelected),
          };
        }),
    [ports, selectedEntity?.id]
  );

  return (
    <MapContainer center={[20, 20]} zoom={2} className="h-[360px] w-full rounded-lg">
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
      />
      {markers.map((port) => (
        <Marker
          key={port.portId}
          position={[port.latitude as number, port.longitude as number]}
          icon={port.icon}
          eventHandlers={{
            click: () =>
              setSelectedEntity({
                id: port.portId,
                name: port.portName,
                type: "port",
              }),
          }}
        >
          <Tooltip direction="top" offset={[0, -8]}>
            <div className="text-xs font-medium">{port.portName}</div>
            <div className="text-[10px] text-muted-foreground">
              Risk {port.riskScore} | Wait {port.avgWaitHours.toFixed(1)}h
            </div>
          </Tooltip>
        </Marker>
      ))}
    </MapContainer>
  );
}
