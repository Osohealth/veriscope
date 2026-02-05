import { useQuery } from "@tanstack/react-query";
import { getAuthHeaders, getAuthToken } from "@/lib/queryClient";

export interface RotterdamDataPoint {
  date: string;
  brentPrice: number;
  rotterdamPrice: number;
  localSpread: number;
  arrivalsCount: number;
  departuresCount: number;
  avgTankerDwt: number;
  receipts: number;
  exports: number;
  storageBbl: number;
  storageUtilization: number;
  berthedCount: number;
  anchoredCount: number;
  portWaitDays: number;
  congestionIndex: number;
  supplyPressureIndex: number;
  satelliteAnomalyFlag: number;
}

export interface RotterdamStats {
  avgBrentPrice: number;
  avgRotterdamPrice: number;
  avgSpread: number;
  totalArrivals: number;
  totalDepartures: number;
  avgStorageUtilization: number;
  avgCongestionIndex: number;
  totalReceipts: number;
  totalExports: number;
}

export interface RotterdamDataResponse {
  data: RotterdamDataPoint[];
  stats: RotterdamStats;
}

export function useRotterdamData(month?: string, enabled: boolean = true) {
  const token = getAuthToken();
  const isAuthed = !!token;

  return useQuery<RotterdamDataResponse>({
    queryKey: ['/api/rotterdam-data', month],
    queryFn: async () => {
      const url = month 
        ? `/api/rotterdam-data?month=${month}` 
        : '/api/rotterdam-data';
      const response = await fetch(url, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch Rotterdam data');
      return response.json();
    },
    enabled: isAuthed && enabled,
  });
}

export function useRotterdamMonths(enabled: boolean = true) {
  const token = getAuthToken();
  const isAuthed = !!token;

  return useQuery<string[]>({
    queryKey: ['/api/rotterdam-data/months'],
    queryFn: async () => {
      const response = await fetch('/api/rotterdam-data/months', { headers: getAuthHeaders() });
      if (!response.ok) throw new Error('Failed to fetch Rotterdam months');
      return response.json();
    },
    enabled: isAuthed && enabled,
  });
}

export function useLatestRotterdamData() {
  const token = getAuthToken();
  const isAuthed = !!token;

  return useQuery<RotterdamDataPoint>({
    queryKey: ['/api/rotterdam-data/latest'],
    enabled: isAuthed,
  });
}
