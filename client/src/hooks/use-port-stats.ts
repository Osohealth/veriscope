import { useQuery } from "@tanstack/react-query";

interface PortStats {
  id: string;
  portId: string;
  date: string;
  arrivals: number;
  departures: number;
  queueLength: number;
  averageWaitHours: number;
  totalVessels: number;
  throughputMT: number;
  byClass: {
    VLCC: number;
    Suezmax: number;
    Aframax: number;
  };
}

export function usePortStats(portId: string) {
  return useQuery<PortStats>({
    queryKey: ['/api/ports', portId, 'stats'],
    queryFn: async () => {
      const response = await fetch(`/api/ports/${portId}/stats`);
      if (!response.ok) throw new Error('Failed to fetch port statistics');
      return response.json();
    },
    refetchInterval: 60000, // Refetch every minute
  });
}
