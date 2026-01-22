import { useState, useEffect, useCallback, useMemo, createContext, useContext, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import type { Watchlist } from "@shared/schema";

interface WatchlistFilterContextValue {
  watchlists: Watchlist[];
  isLoading: boolean;
  activeWatchlistId: string | null;
  activeWatchlist: Watchlist | null;
  setActiveWatchlistId: (id: string | null) => void;
  isItemInActiveWatchlist: (itemId: string, itemType?: string) => boolean;
  getWatchlistItems: (type?: string) => string[];
  clearFilter: () => void;
}

const WatchlistFilterContext = createContext<WatchlistFilterContextValue | null>(null);

export function WatchlistFilterProvider({ children }: { children: ReactNode }) {
  const [activeWatchlistId, setActiveWatchlistId] = useState<string | null>(() => {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('activeWatchlistId') || null;
    }
    return null;
  });

  const { data: watchlists = [], isLoading } = useQuery<Watchlist[]>({
    queryKey: ["/api/watchlists"],
  });

  useEffect(() => {
    if (activeWatchlistId) {
      localStorage.setItem('activeWatchlistId', activeWatchlistId);
    } else {
      localStorage.removeItem('activeWatchlistId');
    }
  }, [activeWatchlistId]);

  const activeWatchlist = useMemo(() => {
    if (!activeWatchlistId) return null;
    return watchlists.find(w => w.id === activeWatchlistId) || null;
  }, [watchlists, activeWatchlistId]);

  const isItemInActiveWatchlist = useCallback((itemId: string, itemType?: string): boolean => {
    if (!activeWatchlist) return true;
    if (itemType && activeWatchlist.type !== itemType) return true;
    const items = Array.isArray(activeWatchlist.items) ? activeWatchlist.items : [];
    return items.includes(itemId);
  }, [activeWatchlist]);

  const getWatchlistItems = useCallback((type?: string): string[] => {
    if (!activeWatchlist) return [];
    if (type && activeWatchlist.type !== type) return [];
    return Array.isArray(activeWatchlist.items) ? activeWatchlist.items : [];
  }, [activeWatchlist]);

  const clearFilter = useCallback(() => {
    setActiveWatchlistId(null);
  }, []);

  const value = useMemo(() => ({
    watchlists,
    isLoading,
    activeWatchlistId,
    activeWatchlist,
    setActiveWatchlistId,
    isItemInActiveWatchlist,
    getWatchlistItems,
    clearFilter,
  }), [watchlists, isLoading, activeWatchlistId, activeWatchlist, isItemInActiveWatchlist, getWatchlistItems, clearFilter]);

  return (
    <WatchlistFilterContext.Provider value={value}>
      {children}
    </WatchlistFilterContext.Provider>
  );
}

export function useWatchlistFilter(): WatchlistFilterContextValue {
  const context = useContext(WatchlistFilterContext);
  if (!context) {
    throw new Error("useWatchlistFilter must be used within a WatchlistFilterProvider");
  }
  return context;
}
