import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders } from "@/lib/queryClient";
import type { Prediction } from "@shared/schema";

export function usePredictions(target?: string) {
  return useQuery<Prediction[]>({
    queryKey: ['/api/predictions', ...(target ? [target] : [])],
    queryFn: async () => {
      const url = target ? `/api/predictions?target=${target}` : '/api/predictions';
      const response = await fetch(url, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch predictions');
      return response.json();
    },
    refetchInterval: 360000, // Refetch every 6 minutes
  });
}
