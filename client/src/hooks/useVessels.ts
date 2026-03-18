import { useQuery } from "@tanstack/react-query";

interface Vessel {
  id: string;
  mmsi: string;
  name: string;
  vesselType: string;
  vesselClass: string;
  dwt: number;
  position?: {
    latitude: string;
    longitude: string;
    speedOverGround: number;
    navigationStatus: string;
    timestamp: string;
  };
}

export function useVessels() {
  return useQuery<Vessel[]>({
    queryKey: ['/api/vessels'],
    // Uses default queryFn from queryClient which includes JWT Authorization header
    refetchInterval: 30000, // Refetch every 30 seconds for live data
  });
}
