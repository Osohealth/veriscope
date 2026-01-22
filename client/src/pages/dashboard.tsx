import { useState, useEffect, useMemo } from "react";
import Sidebar from "@/components/Sidebar";
import MapPanel from "@/components/MapPanel";
import RightPanel from "@/components/RightPanel";
import { NotificationPanel } from "@/components/NotificationPanel";
import { DataFreshnessIndicator } from "@/components/data-freshness-indicator";
import { WatchlistFilter } from "@/components/watchlist-filter";
import { useWatchlistFilter } from "@/hooks/use-watchlist-filter";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Bell, Calendar, ArrowLeft } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useVessels } from "@/hooks/use-vessels";
import { useNotifications } from "@/hooks/use-notifications";
import { useRotterdamMonths } from "@/hooks/use-rotterdam-data";
import { Link } from "wouter";

export default function Dashboard() {
  const [scope, setScope] = useState<string>("crude-oil");
  const [selectedPort, setSelectedPort] = useState<string>("fujairah");
  const [timeRange, setTimeRange] = useState<string>("live");
  const [activeTab, setActiveTab] = useState<string>("stats");
  const [selectedMonth, setSelectedMonth] = useState<string | undefined>(undefined);
  
  // Fetch available months for Rotterdam data
  const { data: availableMonths } = useRotterdamMonths(selectedPort === 'rotterdam');
  
  // Layer and vessel type state - now based on dashboard type
  const getDefaultLayers = (dashboardType: string): Record<string, boolean> => {
    switch (dashboardType) {
      case 'crude-oil':
        return { vessels: true, portAreas: true, storageFarms: true, shippingLanes: true };
      case 'refined-products':
        return { vessels: true, portAreas: true, storageFarms: true, refineries: true };
      case 'lng':
        return { vessels: true, portAreas: true, storageFarms: true, terminals: true };
      case 'maritime':
        return { vessels: true, portAreas: true, shippingLanes: true, anchorages: true };
      case 'trade-flows':
        return { vessels: true, tradeRoutes: true, loadingPorts: true, dischargePorts: true };
      case 'market-analytics':
        return { priceNodes: true, marketCenters: true, storageHubs: true, facilities: true };
      default: // tankscope, flightscope
        return { vessels: true, portAreas: true, storageFarms: false, shippingLanes: false };
    }
  };

  const getDefaultVesselTypes = (dashboardType: string): Record<string, boolean> => {
    switch (dashboardType) {
      case 'crude-oil':
        return { vlcc: true, suezmax: true, aframax: true, panamax: true };
      case 'refined-products':
        return { mr: true, lr1: true, lr2: true, handysize: true };
      case 'lng':
        return { 'q-max': true, 'q-flex': true, conventional: true, fsru: true };
      case 'maritime':
        return { tanker: true, bulk: true, container: true, lng: true };
      case 'trade-flows':
        return { export: true, import: true, coastal: true, storage: true };
      case 'market-analytics':
        return { spot: true, term: true, storage: true, floating: true };
      default:
        return { vlcc: true, suezmax: true, aframax: true };
    }
  };

  const [layers, setLayers] = useState<Record<string, boolean>>(getDefaultLayers(scope));
  const [vesselTypes, setVesselTypes] = useState<Record<string, boolean>>(getDefaultVesselTypes(scope));
  
  const { data: vessels, dataUpdatedAt } = useVessels();
  const { activeWatchlist, isItemInActiveWatchlist, getWatchlistItems } = useWatchlistFilter();
  
  // Filter vessels based on active watchlist
  const filteredVessels = useMemo(() => {
    if (!vessels) return [];
    if (!activeWatchlist || activeWatchlist.type !== 'vessels') return vessels;
    return vessels.filter(v => isItemInActiveWatchlist(v.id, 'vessels'));
  }, [vessels, activeWatchlist, isItemInActiveWatchlist]);
  
  // Track last data update for freshness indicator - undefined when no data yet
  const [lastDataUpdate, setLastDataUpdate] = useState<Date | undefined>(undefined);
  
  useEffect(() => {
    if (dataUpdatedAt) {
      setLastDataUpdate(new Date(dataUpdatedAt));
    }
  }, [dataUpdatedAt]);
  
  // Notifications state
  const {
    unreadCount,
    isNotificationPanelOpen,
    toggleNotificationPanel,
    closeNotificationPanel,
  } = useNotifications();

  // Get the default tab for each dashboard type
  const getDefaultTab = (dashboardType: string): string => {
    switch (dashboardType) {
      case 'trade-flows':
        return 'flows'; // Primary focus is trade flows
      case 'market-analytics':
        return 'analytics'; // Primary focus is market analytics
      case 'crude-oil':
      case 'refined-products':
      case 'lng':
        return 'stats'; // Commodity prices first
      case 'maritime':
        return 'stats'; // Port stats first
      case 'flightscope':
        return 'stats'; // Airport stats first
      default:
        return 'stats';
    }
  };

  // Handle scope changes and set appropriate configurations
  const handleScopeChange = (newScope: string) => {
    setScope(newScope);
    
    // Set appropriate default port based on dashboard type
    switch (newScope) {
      case 'crude-oil':
        setSelectedPort('fujairah'); // Major crude oil hub
        break;
      case 'refined-products':
        setSelectedPort('rotterdam'); // Major refining center
        break;
      case 'lng':
        setSelectedPort('fujairah'); // LNG terminal
        break;
      case 'maritime':
      case 'trade-flows':
        setSelectedPort('fujairah'); // Central maritime hub
        break;
      case 'market-analytics':
        setSelectedPort('rotterdam'); // Market center
        break;
      case 'flightscope':
        setSelectedPort('dxb'); // Aviation
        break;
      default:
        setSelectedPort('fujairah');
    }
    
    // Update layers and vessel types based on dashboard type
    setLayers(getDefaultLayers(newScope));
    setVesselTypes(getDefaultVesselTypes(newScope));
    
    // Set appropriate default tab for new dashboard type
    setActiveTab(getDefaultTab(newScope));
  };
  
  const handleLayerChange = (layer: string) => {
    setLayers(prev => ({ ...prev, [layer]: !prev[layer] }));
  };

  const handleVesselTypeChange = (type: string) => {
    setVesselTypes(prev => ({ ...prev, [type]: !prev[type] }));
  };

  const getVesselCount = (type: string) => {
    if (!filteredVessels || filteredVessels.length === 0) return 0;
    
    // For dashboard-specific categories, map to vessel classes or return mock counts
    switch (scope) {
      case 'crude-oil':
      case 'refined-products':
      case 'lng':
      case 'maritime':
        // Standard vessel class filtering
        return filteredVessels.filter(v => v.vesselClass?.toLowerCase().includes(type.toLowerCase())).length;
      case 'trade-flows':
        // Mock counts for trade flow types
        const tradeFlowCounts: Record<string, number> = {
          export: 12, import: 8, coastal: 5, storage: 3
        };
        return tradeFlowCounts[type] || 0;
      case 'market-analytics':
        // Mock counts for market segments
        const marketCounts: Record<string, number> = {
          spot: 15, term: 7, storage: 4, floating: 2
        };
        return marketCounts[type] || 0;
      default:
        return filteredVessels.filter(v => v.vesselClass?.toLowerCase().includes(type.toLowerCase())).length;
    }
  };

  // Map control state
  const [mapCenter, setMapCenter] = useState<[number, number] | null>(null);

  const handlePortClick = (port: { name: string; code: string; coordinates: [number, number] }) => {
    setSelectedPort(port.code);
    setMapCenter(port.coordinates);
  };

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar 
        layers={layers}
        vesselTypes={vesselTypes}
        onLayerChange={handleLayerChange}
        onVesselTypeChange={handleVesselTypeChange}
        getVesselCount={getVesselCount}
        scope={scope}
        selectedPort={selectedPort}
        onPortClick={handlePortClick}
        dashboardType={scope}
      />
      
      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top Header */}
        <header className="bg-card border-b border-border px-6 py-4 flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Link href="/">
              <Button variant="ghost" size="sm" data-testid="button-back-to-home">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Home
              </Button>
            </Link>
            <div className="flex items-center space-x-4">
              <Select value={scope} onValueChange={handleScopeChange} data-testid="select-scope">
                <SelectTrigger className="w-40">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="crude-oil">Crude Oil Dashboard</SelectItem>
                  <SelectItem value="refined-products">Refined Products</SelectItem>
                  <SelectItem value="lng">LNG Dashboard</SelectItem>
                  <SelectItem value="maritime">Maritime Analytics</SelectItem>
                  <SelectItem value="trade-flows">Trade Flows</SelectItem>
                  <SelectItem value="market-analytics">Market Analytics</SelectItem>
                  <SelectItem value="tankscope">TankScope (Legacy)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-sm text-muted-foreground">Region:</span>
              <Select defaultValue="global" data-testid="select-region">
                <SelectTrigger className="w-32">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="global">Global</SelectItem>
                  <SelectItem value="middle-east">Middle East</SelectItem>
                  <SelectItem value="europe">Europe</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          
          <div className="flex items-center space-x-4">
            {/* Watchlist Filter */}
            <WatchlistFilter filterType="all" size="sm" showClearButton={true} />
            
            {/* Rotterdam Month Filter */}
            {selectedPort === 'rotterdam' && availableMonths && availableMonths.length > 0 && (
              <div className="flex items-center space-x-2">
                <Calendar className="w-4 h-4 text-muted-foreground" />
                <Select value={selectedMonth || "all"} onValueChange={(value) => setSelectedMonth(value === "all" ? undefined : value)} data-testid="select-month">
                  <SelectTrigger className="w-32">
                    <SelectValue placeholder="All Months" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Months</SelectItem>
                    {availableMonths.map((month) => {
                      const [year, monthNum] = month.split('-');
                      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                      const monthName = monthNames[parseInt(monthNum) - 1];
                      return (
                        <SelectItem key={month} value={month}>
                          {monthName} {year}
                        </SelectItem>
                      );
                    })}
                  </SelectContent>
                </Select>
              </div>
            )}
            
            {/* Data Freshness Indicator */}
            <DataFreshnessIndicator 
              lastUpdate={lastDataUpdate}
              streamName="AIS Feed"
              showLabel={true}
            />
            
            {/* Time Range Selector */}
            <div className="flex items-center space-x-2">
              <span className="text-sm text-muted-foreground">Time:</span>
              <div className="flex rounded-lg border border-border overflow-hidden">
                {['live', '1h', '6h', '1d'].map((range) => (
                  <Button
                    key={range}
                    variant={timeRange === range ? "default" : "ghost"}
                    size="sm"
                    className="rounded-none px-3 py-1 text-sm"
                    onClick={() => setTimeRange(range)}
                    data-testid={`button-time-${range}`}
                  >
                    {range === 'live' ? 'Live' : range.toUpperCase()}
                  </Button>
                ))}
              </div>
            </div>
            
            {/* Alert Bell */}
            <Button 
              variant="ghost" 
              size="sm" 
              className="relative" 
              onClick={toggleNotificationPanel}
              data-testid="button-alerts"
            >
              <Bell className="w-5 h-5" />
              {unreadCount > 0 && (
                <Badge variant="destructive" className="absolute -top-1 -right-1 w-5 h-5 text-xs p-0 flex items-center justify-center">
                  {unreadCount > 99 ? '99+' : unreadCount}
                </Badge>
              )}
            </Button>
            
            {/* User Profile */}
            <div className="flex items-center space-x-2">
              <div className="w-8 h-8 bg-primary rounded-full flex items-center justify-center" data-testid="avatar-user">
                <span className="text-sm font-medium text-primary-foreground">JD</span>
              </div>
            </div>
          </div>
        </header>
        
        {/* Dashboard Content */}
        <main className="flex-1 overflow-hidden">
          <div className="h-full flex">
            <MapPanel 
              selectedPort={selectedPort} 
              timeRange={timeRange} 
              scope={scope}
              layers={layers}
              vesselTypes={vesselTypes}
              mapCenter={mapCenter}
              onMapCenterReset={() => setMapCenter(null)}
            />
            <RightPanel 
              selectedPort={selectedPort} 
              activeTab={activeTab} 
              onTabChange={setActiveTab}
              onPortChange={setSelectedPort}
              scope={scope}
              selectedMonth={selectedMonth}
            />
          </div>
        </main>
      </div>
      
      {/* Notification Panel */}
      <NotificationPanel 
        isOpen={isNotificationPanelOpen}
        onClose={closeNotificationPanel}
      />
    </div>
  );
}
