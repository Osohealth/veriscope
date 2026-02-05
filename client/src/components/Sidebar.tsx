import { Link, useLocation } from "wouter";
import { Ship, Plane, Zap, FileText, TrendingUp, List, Bell, Download } from "lucide-react";
import { cn } from "@/lib/utils";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";

const navigation = [
  { name: "TankScope", href: "/dashboard", icon: Ship, active: true },
  { name: "FlightScope", href: "/flightscope", icon: Plane, badge: "V1", disabled: true },
  { name: "Signals & Predict", href: "/signals", icon: Zap, active: true },
  { name: "News & Notes", href: "/news", icon: FileText, disabled: true },
];

const userTools = [
  { name: "Watchlists", href: "/watchlists", icon: List },
  { name: "Alert Activity", href: "/alerts", icon: Bell },
  { name: "Alert Subscriptions", href: "/alerts/subscriptions", icon: Bell },
  { name: "Alert Rules", href: "/alert-rules", icon: Bell },
  { name: "Exports", href: "/exports", icon: Download },
];

// Dashboard-specific port configurations
const getPortsForDashboard = (dashboardType: string) => {
  switch (dashboardType) {
    case 'crude-oil':
      return [
        { name: "Fujairah", status: "online", code: "fujairah", coordinates: [25.1204, 56.3541] as [number, number] },
        { name: "Rotterdam", status: "online", code: "rotterdam", coordinates: [51.9225, 4.4792] as [number, number] },
        { name: "Singapore", status: "online", code: "singapore", coordinates: [1.2966, 103.7764] as [number, number] },
        { name: "Houston", status: "online", code: "houston", coordinates: [29.7372, -95.2618] as [number, number] },
      ];
    case 'refined-products':
      return [
        { name: "Amsterdam", status: "online", code: "amsterdam", coordinates: [52.3727, 4.8936] as [number, number] },
        { name: "New York", status: "online", code: "newyork", coordinates: [40.6892, -74.0445] as [number, number] },
        { name: "Singapore", status: "online", code: "singapore", coordinates: [1.2966, 103.7764] as [number, number] },
        { name: "LA/Long Beach", status: "online", code: "longbeach", coordinates: [33.7701, -118.1937] as [number, number] },
      ];
    case 'lng':
      return [
        { name: "Sabine Pass", status: "online", code: "sabinepass", coordinates: [29.7272, -93.8707] as [number, number] },
        { name: "Yamal", status: "online", code: "yamal", coordinates: [70.7619, 72.7811] as [number, number] },
        { name: "Qatargas", status: "online", code: "qatargas", coordinates: [25.5000, 51.2500] as [number, number] },
        { name: "Gladstone", status: "online", code: "gladstone", coordinates: [-23.8512, 151.2621] as [number, number] },
      ];
    case 'maritime':
    case 'trade-flows':
    case 'market-analytics':
    default:
      return [
        { name: "Fujairah", status: "online", code: "fujairah", coordinates: [25.1204, 56.3541] as [number, number] },
        { name: "Rotterdam", status: "online", code: "rotterdam", coordinates: [51.9225, 4.4792] as [number, number] },
        { name: "Singapore", status: "online", code: "singapore", coordinates: [1.2966, 103.7764] as [number, number] },
      ];
  }
};

interface SidebarProps {
  layers?: Record<string, boolean>;
  vesselTypes?: Record<string, boolean>;
  onLayerChange?: (layer: any) => void;
  onVesselTypeChange?: (type: any) => void;
  getVesselCount?: (type: string) => number;
  scope?: string;
  selectedPort?: string;
  onPortClick?: (port: { name: string; code: string; coordinates: [number, number] }) => void;
  dashboardType?: string;
}

export default function Sidebar({ 
  layers = {}, 
  vesselTypes = {}, 
  onLayerChange, 
  onVesselTypeChange, 
  getVesselCount, 
  scope = 'tankscope',
  selectedPort,
  onPortClick,
  dashboardType = 'crude-oil'
}: SidebarProps) {
  const [location] = useLocation();
  
  const ports = getPortsForDashboard(dashboardType);
  
  // Get dashboard-specific legend and vessel types
  const getDashboardConfig = () => {
    switch (dashboardType) {
      case 'crude-oil':
        return {
          title: 'Crude Oil',
          vesselTypes: ['VLCC', 'Suezmax', 'Aframax', 'Panamax'],
          legend: [
            { color: 'bg-emerald-400', label: 'Loaded' },
            { color: 'bg-blue-400', label: 'Ballast' },
            { color: 'bg-amber-400', label: 'At Anchor' },
            { color: 'bg-red-400', label: 'At Terminal' }
          ]
        };
      case 'refined-products':
        return {
          title: 'Refined Products',
          vesselTypes: ['MR', 'LR1', 'LR2', 'Handysize'],
          legend: [
            { color: 'bg-emerald-400', label: 'Gasoline' },
            { color: 'bg-blue-400', label: 'Diesel' },
            { color: 'bg-amber-400', label: 'Jet Fuel' },
            { color: 'bg-red-400', label: 'Naphtha' }
          ]
        };
      case 'lng':
        return {
          title: 'LNG',
          vesselTypes: ['Q-Max', 'Q-Flex', 'Conventional', 'FSRU'],
          legend: [
            { color: 'bg-emerald-400', label: 'Laden' },
            { color: 'bg-blue-400', label: 'Ballast' },
            { color: 'bg-amber-400', label: 'Loading' },
            { color: 'bg-red-400', label: 'Discharging' }
          ]
        };
      case 'maritime':
        return {
          title: 'Maritime',
          vesselTypes: ['Tanker', 'Bulk', 'Container', 'LNG'],
          legend: [
            { color: 'bg-emerald-400', label: 'Underway' },
            { color: 'bg-blue-400', label: 'Anchored' },
            { color: 'bg-amber-400', label: 'At Berth' },
            { color: 'bg-red-400', label: 'Restricted' }
          ]
        };
      case 'trade-flows':
        return {
          title: 'Trade Flows',
          vesselTypes: ['Export', 'Import', 'Coastal', 'Storage'],
          legend: [
            { color: 'bg-emerald-400', label: 'Active Flow' },
            { color: 'bg-blue-400', label: 'Pending' },
            { color: 'bg-amber-400', label: 'Transit' },
            { color: 'bg-red-400', label: 'Delayed' }
          ]
        };
      case 'market-analytics':
        return {
          title: 'Market Analytics',
          vesselTypes: ['Spot', 'Term', 'Storage', 'Floating'],
          legend: [
            { color: 'bg-emerald-400', label: 'Supply' },
            { color: 'bg-blue-400', label: 'Demand' },
            { color: 'bg-amber-400', label: 'Storage' },
            { color: 'bg-red-400', label: 'Arbitrage' }
          ]
        };
      default:
        return {
          title: 'TankScope',
          vesselTypes: ['VLCC', 'Suezmax', 'Aframax'],
          legend: [
            { color: 'bg-emerald-400', label: 'Underway' },
            { color: 'bg-blue-400', label: 'Ballast' },
            { color: 'bg-amber-400', label: 'At Anchor' },
            { color: 'bg-red-400', label: 'At Berth' }
          ]
        };
    }
  };
  
  const config = getDashboardConfig();

  return (
    <div className="w-64 bg-sidebar border-r border-sidebar-border flex flex-col">
      {/* Logo and Header */}
      <div className="p-6 border-b border-sidebar-border">
        <div className="flex items-center space-x-3">
          <div className="w-8 h-8 bg-sidebar-primary rounded-lg flex items-center justify-center">
            <TrendingUp className="w-5 h-5 text-sidebar-primary-foreground" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-sidebar-foreground" data-testid="text-logo">
              Veriscope
            </h1>
            <p className="text-xs text-muted-foreground">Maritime Intelligence</p>
          </div>
        </div>
      </div>
      
      {/* Navigation */}
      <nav className="flex-1 p-4">
        <ul className="space-y-2">
          {navigation.map((item) => {
            const isActive = location === item.href;
            const Icon = item.icon;
            
            return (
              <li key={item.name}>
                {item.disabled ? (
                  <div className={cn(
                    "flex items-center space-x-3 px-3 py-2 rounded-lg",
                    "text-muted-foreground cursor-not-allowed opacity-50"
                  )}>
                    <Icon className="w-5 h-5" />
                    <span>{item.name}</span>
                    {item.badge && (
                      <span className="text-xs bg-muted text-muted-foreground px-2 py-1 rounded">
                        {item.badge}
                      </span>
                    )}
                  </div>
                ) : (
                  <Link href={item.href}>
                    <a
                      className={cn(
                        "flex items-center space-x-3 px-3 py-2 rounded-lg font-medium transition-colors",
                        isActive
                          ? "bg-sidebar-primary text-sidebar-primary-foreground"
                          : "text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                      )}
                      data-testid={`link-${item.name.toLowerCase().replace(/\s+/g, '-')}`}
                    >
                      <Icon className="w-5 h-5" />
                      <span>{item.name}</span>
                      {item.badge && (
                        <span className="text-xs bg-muted text-muted-foreground px-2 py-1 rounded">
                          {item.badge}
                        </span>
                      )}
                    </a>
                  </Link>
                )}
              </li>
            );
          })}
        </ul>
        
        {/* User Tools Section */}
        <div className="mt-6">
          <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
            Tools
          </h3>
          <ul className="space-y-1">
            {userTools.map((item) => {
              const isActive = location === item.href;
              const Icon = item.icon;
              
              return (
                <li key={item.name}>
                  <Link href={item.href}>
                    <a
                      className={cn(
                        "flex items-center space-x-3 px-3 py-2 rounded-lg font-medium transition-colors",
                        isActive
                          ? "bg-sidebar-primary text-sidebar-primary-foreground"
                          : "text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                      )}
                      data-testid={`link-${item.name.toLowerCase().replace(/\s+/g, '-')}`}
                    >
                      <Icon className="w-4 h-4" />
                      <span className="text-sm">{item.name}</span>
                    </a>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
        
        <div className="mt-8">
          <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
            {config.title} Ports
          </h3>
          <ul className="space-y-1">
            {ports.map((port) => (
              <li key={port.name}>
                <div 
                  className={cn(
                    "flex items-center justify-between px-3 py-1.5 rounded text-sm transition-colors cursor-pointer",
                    selectedPort === port.code 
                      ? "bg-sidebar-primary text-sidebar-primary-foreground" 
                      : "text-sidebar-foreground hover:bg-sidebar-accent"
                  )}
                  onClick={() => onPortClick?.(port)}
                  data-testid={`port-${port.name.toLowerCase()}`}
                >
                  <span>{port.name}</span>
                  <span className={cn(
                    "text-xs",
                    port.status === "online" ? "text-emerald-400" : "text-destructive"
                  )}>
                    ●
                  </span>
                </div>
              </li>
            ))}
          </ul>
        </div>
        
        {/* Map Controls */}
        {Object.keys(layers).length > 0 && (
          <div className="mt-8">
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
              Layers
            </h3>
            <Card className="bg-sidebar-accent/50 border-sidebar-border">
              <CardContent className="p-3">
                <div className="space-y-2">
                  {Object.entries(layers).map(([key, checked]) => (
                    <div key={key} className="flex items-center space-x-2">
                      <Checkbox
                        id={key}
                        checked={checked as boolean}
                        onCheckedChange={() => onLayerChange?.(key)}
                        data-testid={`checkbox-layer-${key}`}
                      />
                      <Label htmlFor={key} className="text-sm text-sidebar-foreground capitalize">
                        {key.replace(/([A-Z])/g, ' $1').trim()}
                      </Label>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>
        )}
        
        {/* Vessel Types */}
        {Object.keys(vesselTypes).length > 0 && (
          <div className="mt-6">
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
              {scope === 'flightscope' ? 'Aircraft Types' : 
               dashboardType === 'trade-flows' ? 'Flow Types' :
               dashboardType === 'market-analytics' ? 'Market Segments' : 
               'Vessel Types'}
            </h3>
            <Card className="bg-sidebar-accent/50 border-sidebar-border">
              <CardContent className="p-3">
                <div className="space-y-2">
                  {Object.entries(vesselTypes).map(([key, checked]) => {
                    const count = getVesselCount?.(key) || 0;
                    return (
                      <div key={key} className="flex items-center space-x-2">
                        <Checkbox
                          id={key}
                          checked={checked as boolean}
                          onCheckedChange={() => onVesselTypeChange?.(key)}
                          data-testid={`checkbox-vessel-${key}`}
                        />
                        <Label htmlFor={key} className="text-sm text-sidebar-foreground capitalize flex-1">
                          {key}
                        </Label>
                        <span className="text-xs text-muted-foreground" data-testid={`text-count-${key}`}>
                          ({count})
                        </span>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>
          </div>
        )}
        
        {/* Legend */}
        <div className="mt-6">
          <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
            {scope === 'flightscope' ? 'Aircraft Status' : 
             dashboardType === 'trade-flows' ? 'Flow Status' :
             dashboardType === 'market-analytics' ? 'Market Indicators' :
             'Status Legend'}
          </h3>
          <Card className="bg-sidebar-accent/50 border-sidebar-border">
            <CardContent className="p-3">
              <div className="space-y-1">
                {scope === 'flightscope' ? (
                  <>
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-emerald-400 rounded-full"></div>
                      <span className="text-xs text-sidebar-foreground">In Flight</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-blue-400 rounded-full"></div>
                      <span className="text-xs text-sidebar-foreground">Climbing</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-amber-400 rounded-full"></div>
                      <span className="text-xs text-sidebar-foreground">Descending</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-red-400 rounded-full"></div>
                      <span className="text-xs text-sidebar-foreground">On Ground</span>
                    </div>
                  </>
                ) : (
                  config.legend.map((item, index) => (
                    <div key={index} className="flex items-center space-x-2">
                      <div className={`w-3 h-3 ${item.color} rounded-full`}></div>
                      <span className="text-xs text-sidebar-foreground">{item.label}</span>
                    </div>
                  ))
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </nav>
      
      {/* Status Footer */}
      <div className="p-4 border-t border-sidebar-border">
        <div className="flex items-center space-x-2">
          <div className="w-2 h-2 bg-emerald-400 rounded-full pulse-dot"></div>
          <span className="text-xs text-muted-foreground">Live Data Feed</span>
        </div>
        <div className="text-xs text-muted-foreground mt-1" data-testid="text-last-update">
          Last update: 2 min ago
        </div>
      </div>
    </div>
  );
}
