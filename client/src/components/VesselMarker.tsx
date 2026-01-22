import { cn } from "@/lib/utils";

interface VesselMarkerProps {
  vessel: {
    mmsi: string;
    name: string;
    vesselClass?: string;
    position?: {
      navigationStatus?: string;
      speedOverGround?: number;
    };
  };
  onClick?: () => void;
}

export default function VesselMarker({ vessel, onClick }: VesselMarkerProps) {
  const getStatusColor = (status?: string) => {
    switch (status) {
      case 'AT_ANCHOR': return 'bg-amber-400';
      case 'AT_BERTH': return 'bg-destructive';
      case 'UNDERWAY': return 'bg-emerald-400';
      default: return 'bg-blue-400';
    }
  };

  const getStatusLabel = (status?: string) => {
    switch (status) {
      case 'AT_ANCHOR': return 'At Anchor';
      case 'AT_BERTH': return 'At Berth';
      case 'UNDERWAY': return 'Underway';
      default: return 'Ballast';
    }
  };

  return (
    <div 
      className={cn(
        "w-3 h-3 rounded-full cursor-pointer transition-all hover:scale-150 vessel-marker",
        getStatusColor(vessel.position?.navigationStatus)
      )}
      onClick={onClick}
      title={`${vessel.name} - ${vessel.vesselClass || 'Unknown'} - ${getStatusLabel(vessel.position?.navigationStatus)}`}
      data-testid={`vessel-marker-${vessel.mmsi}`}
    />
  );
}
