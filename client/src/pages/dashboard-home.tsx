import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import { 
  TrendingUp, 
  TrendingDown, 
  Ship, 
  Warehouse, 
  Activity, 
  AlertTriangle,
  BarChart3,
  Globe,
  Droplet,
  Fuel,
  Factory,
  Wind,
  Zap,
  FileText,
  Settings,
  ArrowLeft,
  Satellite
} from "lucide-react";

export default function DashboardHome() {
  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/">
              <Button variant="ghost" size="sm" data-testid="button-back-home">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Home
              </Button>
            </Link>
            <div>
              <h1 className="text-2xl font-bold" data-testid="text-dashboard-title">Veriscope Intelligence Platform</h1>
              <p className="text-sm text-muted-foreground">Global Maritime & Commodity Analytics</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="flex items-center gap-1">
              <div className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse"></div>
              Live Feed Active
            </Badge>
            <Button variant="outline" size="sm" data-testid="button-settings">
              <Settings className="w-4 h-4 mr-2" />
              Settings
            </Button>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Global Summary Section */}
        <section>
          <h2 className="text-xl font-semibold mb-4" data-testid="text-global-summary">Global Summary</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Inventories */}
            <Card className="hover:border-primary/50 transition-colors cursor-pointer" data-testid="card-inventories">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  <Warehouse className="w-5 h-5 text-primary" />
                  Global Inventories
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Crude Oil</span>
                    <span className="font-semibold">2,847 MMbbl</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Refined Products</span>
                    <span className="font-semibold">1,234 MMbbl</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">LNG</span>
                    <span className="font-semibold">456 MMcf</span>
                  </div>
                  <div className="flex items-center gap-2 pt-2 border-t">
                    <TrendingDown className="w-4 h-4 text-red-500" />
                    <span className="text-sm text-muted-foreground">-2.3% vs last week</span>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Flows */}
            <Card className="hover:border-primary/50 transition-colors cursor-pointer" data-testid="card-flows">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  <Activity className="w-5 h-5 text-primary" />
                  Global Flows
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Active Vessels</span>
                    <span className="font-semibold">2,456</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Daily Throughput</span>
                    <span className="font-semibold">18.4 MMbbl</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Transit Time Avg</span>
                    <span className="font-semibold">24.3 days</span>
                  </div>
                  <div className="flex items-center gap-2 pt-2 border-t">
                    <TrendingUp className="w-4 h-4 text-emerald-500" />
                    <span className="text-sm text-muted-foreground">+5.1% vs last week</span>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Disruptions */}
            <Card className="hover:border-primary/50 transition-colors cursor-pointer" data-testid="card-disruptions">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  <AlertTriangle className="w-5 h-5 text-destructive" />
                  Active Disruptions
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Port Congestion</span>
                    <Badge variant="destructive">High</Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Weather Delays</span>
                    <Badge variant="outline" className="text-amber-500 border-amber-500">Medium</Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Geopolitical</span>
                    <Badge variant="outline">Low</Badge>
                  </div>
                  <div className="text-sm text-muted-foreground pt-2 border-t">
                    <span className="font-semibold">12</span> active alerts
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </section>

        {/* Alerts / Active Events */}
        <section>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold" data-testid="text-active-alerts">Alerts & Active Events</h2>
            <Link href="/signals">
              <Button variant="outline" size="sm" data-testid="button-view-all-signals">
                View All Signals
              </Button>
            </Link>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card className="border-l-4 border-l-destructive">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base">Rotterdam Port Congestion</CardTitle>
                  <Badge variant="destructive">Critical</Badge>
                </div>
              </CardHeader>
              <CardContent className="text-sm text-muted-foreground">
                <p>Queue length: 14 vessels. Average wait time increased to 18.5 hours.</p>
                <p className="text-xs mt-2">2 minutes ago</p>
              </CardContent>
            </Card>

            <Card className="border-l-4 border-l-amber-500">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base">Fujairah Storage at 85%</CardTitle>
                  <Badge variant="outline" className="text-amber-500 border-amber-500">Warning</Badge>
                </div>
              </CardHeader>
              <CardContent className="text-sm text-muted-foreground">
                <p>Tank farm capacity approaching critical levels. Estimated 3 days to full.</p>
                <p className="text-xs mt-2">15 minutes ago</p>
              </CardContent>
            </Card>
          </div>
        </section>

        {/* Quick Links */}
        <section>
          <h2 className="text-xl font-semibold mb-4" data-testid="text-quick-links">Quick Access</h2>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            <Link href="/commodities/trades">
              <Card className="hover:bg-accent transition-colors cursor-pointer" data-testid="card-quick-trades">
                <CardContent className="p-4 flex flex-col items-center text-center gap-2">
                  <Droplet className="w-8 h-8 text-primary" />
                  <span className="text-sm font-medium">Trades & Flows</span>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/inventories">
              <Card className="hover:bg-accent transition-colors cursor-pointer" data-testid="card-quick-inventories">
                <CardContent className="p-4 flex flex-col items-center text-center gap-2">
                  <Warehouse className="w-8 h-8 text-primary" />
                  <span className="text-sm font-medium">Inventories</span>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime/vessels">
              <Card className="hover:bg-accent transition-colors cursor-pointer" data-testid="card-quick-vessels">
                <CardContent className="p-4 flex flex-col items-center text-center gap-2">
                  <Ship className="w-8 h-8 text-primary" />
                  <span className="text-sm font-medium">Vessel Tracking</span>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/refinery-intelligence">
              <Card className="hover:bg-accent transition-colors cursor-pointer" data-testid="card-quick-terminals">
                <CardContent className="p-4 flex flex-col items-center text-center gap-2">
                  <Factory className="w-8 h-8 text-primary" />
                  <span className="text-sm font-medium">Terminals</span>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/research-insights">
              <Card className="hover:bg-accent transition-colors cursor-pointer" data-testid="card-quick-research">
                <CardContent className="p-4 flex flex-col items-center text-center gap-2">
                  <FileText className="w-8 h-8 text-primary" />
                  <span className="text-sm font-medium">Research</span>
                </CardContent>
              </Card>
            </Link>

            <Link href="/energy/power">
              <Card className="hover:bg-accent transition-colors cursor-pointer" data-testid="card-quick-power">
                <CardContent className="p-4 flex flex-col items-center text-center gap-2">
                  <Zap className="w-8 h-8 text-primary" />
                  <span className="text-sm font-medium">Power Markets</span>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>

        {/* Main Modules */}
        <section>
          <h2 className="text-xl font-semibold mb-4">Platform Modules</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Link href="/commodities">
              <Card className="hover:border-primary transition-colors cursor-pointer h-full" data-testid="card-module-commodities">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <BarChart3 className="w-5 h-5 text-primary" />
                    Commodities Intelligence
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>Trades, inventories, freight analytics, supply & demand models, and pillar packs.</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime">
              <Card className="hover:border-primary transition-colors cursor-pointer h-full" data-testid="card-module-maritime">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Globe className="w-5 h-5 text-primary" />
                    Maritime Intelligence
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>AIS tracking, port intelligence, predictive schedules, and container tracking.</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/energy">
              <Card className="hover:border-primary transition-colors cursor-pointer h-full" data-testid="card-module-energy">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Wind className="w-5 h-5 text-primary" />
                    Energy Transition
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>Emissions intelligence, power markets, renewable dispatch, and carbon analytics.</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/dashboard">
              <Card className="hover:border-primary transition-colors cursor-pointer h-full" data-testid="card-module-tankscope">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Fuel className="w-5 h-5 text-primary" />
                    TankScope (Classic)
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>Legacy dashboard with real-time vessel tracking and port monitoring.</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/refinery-satellite">
              <Card className="hover:border-primary transition-colors cursor-pointer h-full" data-testid="card-module-refinery-satellite">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Satellite className="w-5 h-5 text-primary" />
                    Refinery Satellite
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>Rotterdam refinery cluster monitoring with Sentinel-2 satellite intelligence.</p>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>
      </div>
    </div>
  );
}
