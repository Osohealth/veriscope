import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders } from "@/lib/queryClient";

interface SignalExplainability {
  triggerReason: string;
  dataSources: string[];
  timeWindow: string;
  confidence: 'low' | 'medium' | 'high';
  methodology: string;
}

interface Signal {
  id: string;
  timestamp?: string;
  entityType: string;
  entityId: string;
  signalType: string;
  severity: number;
  title: string;
  description?: string;
  metadata?: any;
  isActive: boolean;
  explainability?: SignalExplainability;
}

export function useSignals() {
  return useQuery<Signal[]>({
    queryKey: ['/api/signals'],
    queryFn: async () => {
      const response = await fetch('/api/signals', { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch signals');
      return response.json();
    },
    refetchInterval: 120000, // Refetch every 2 minutes
  });
}
