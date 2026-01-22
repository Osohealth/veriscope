import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Star, X, Ship, Anchor, Droplets, Eye, Filter } from "lucide-react";
import { useWatchlistFilter } from "@/hooks/use-watchlist-filter";
import { cn } from "@/lib/utils";

interface WatchlistFilterProps {
  filterType?: "vessels" | "ports" | "commodities" | "routes" | "all";
  showClearButton?: boolean;
  size?: "sm" | "md";
  className?: string;
}

const typeIcons: Record<string, typeof Ship> = {
  vessels: Ship,
  ports: Anchor,
  commodities: Droplets,
  routes: Eye,
};

export function WatchlistFilter({ 
  filterType = "all", 
  showClearButton = true,
  size = "md",
  className 
}: WatchlistFilterProps) {
  const { 
    watchlists, 
    isLoading, 
    activeWatchlistId, 
    activeWatchlist,
    setActiveWatchlistId, 
    clearFilter 
  } = useWatchlistFilter();

  const filteredWatchlists = filterType === "all" 
    ? watchlists 
    : watchlists.filter(w => w.type === filterType);

  if (isLoading) {
    return (
      <div className={cn("flex items-center gap-2", className)}>
        <div className="w-32 h-8 bg-slate-800 animate-pulse rounded" />
      </div>
    );
  }

  if (filteredWatchlists.length === 0) {
    return null;
  }

  const IconComponent = activeWatchlist ? typeIcons[activeWatchlist.type] || Star : Filter;
  const itemCount = activeWatchlist && Array.isArray(activeWatchlist.items) 
    ? activeWatchlist.items.length 
    : 0;

  return (
    <div className={cn("flex items-center gap-2", className)}>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Star className={cn("w-4 h-4", activeWatchlistId && "text-amber-400 fill-amber-400")} />
            {size === "md" && <span className="text-sm">Watchlist:</span>}
          </div>
        </TooltipTrigger>
        <TooltipContent>
          <p>Filter data by your watchlist items</p>
        </TooltipContent>
      </Tooltip>

      <Select
        value={activeWatchlistId || "all"}
        onValueChange={(value) => setActiveWatchlistId(value === "all" ? null : value)}
      >
        <SelectTrigger 
          className={cn(
            "bg-slate-800 border-slate-700 text-white",
            size === "sm" ? "w-28 h-8 text-xs" : "w-40"
          )}
          data-testid="select-watchlist-filter"
        >
          <div className="flex items-center gap-2 truncate">
            {activeWatchlist ? (
              <>
                <IconComponent className="w-3 h-3 shrink-0" />
                <span className="truncate">{activeWatchlist.name}</span>
              </>
            ) : (
              <span>All Items</span>
            )}
          </div>
        </SelectTrigger>
        <SelectContent className="bg-slate-800 border-slate-700">
          <SelectItem value="all" className="text-slate-300">
            <span className="flex items-center gap-2">
              <Filter className="w-3 h-3" />
              All Items (No Filter)
            </span>
          </SelectItem>
          {filteredWatchlists.map((watchlist) => {
            const WatchlistIcon = typeIcons[watchlist.type] || Star;
            const count = Array.isArray(watchlist.items) ? watchlist.items.length : 0;
            return (
              <SelectItem key={watchlist.id} value={watchlist.id}>
                <span className="flex items-center gap-2">
                  <WatchlistIcon className="w-3 h-3" />
                  {watchlist.name}
                  <Badge variant="secondary" className="ml-1 text-xs px-1.5 py-0">
                    {count}
                  </Badge>
                </span>
              </SelectItem>
            );
          })}
        </SelectContent>
      </Select>

      {showClearButton && activeWatchlistId && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-slate-400 hover:text-white"
              onClick={clearFilter}
              data-testid="button-clear-watchlist-filter"
            >
              <X className="w-4 h-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>
            <p>Clear watchlist filter</p>
          </TooltipContent>
        </Tooltip>
      )}

      {activeWatchlist && (
        <Badge 
          variant="outline" 
          className="border-amber-500/50 text-amber-400 text-xs"
          data-testid="badge-watchlist-active"
        >
          Filtering {itemCount} {activeWatchlist.type}
        </Badge>
      )}
    </div>
  );
}

export function WatchlistFilterCompact({ 
  filterType = "all",
  className 
}: { filterType?: WatchlistFilterProps["filterType"]; className?: string }) {
  const { activeWatchlist, activeWatchlistId, setActiveWatchlistId, watchlists, clearFilter } = useWatchlistFilter();

  const filteredWatchlists = filterType === "all" 
    ? watchlists 
    : watchlists.filter(w => w.type === filterType);

  if (filteredWatchlists.length === 0) {
    return null;
  }

  return (
    <div className={cn("flex items-center gap-1", className)}>
      <Select
        value={activeWatchlistId || "all"}
        onValueChange={(value) => setActiveWatchlistId(value === "all" ? null : value)}
      >
        <SelectTrigger 
          className="w-8 h-8 p-0 border-none bg-transparent hover:bg-slate-800"
          data-testid="select-watchlist-filter-compact"
        >
          <Star className={cn(
            "w-4 h-4",
            activeWatchlistId ? "text-amber-400 fill-amber-400" : "text-slate-400"
          )} />
        </SelectTrigger>
        <SelectContent className="bg-slate-800 border-slate-700">
          <SelectItem value="all" className="text-slate-300">
            All Items
          </SelectItem>
          {filteredWatchlists.map((watchlist) => (
            <SelectItem key={watchlist.id} value={watchlist.id}>
              {watchlist.name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
