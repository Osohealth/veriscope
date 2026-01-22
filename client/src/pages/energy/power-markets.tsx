import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import { ArrowLeft, Zap, TrendingUp, TrendingDown, Wind, Droplets, Flame } from "lucide-react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area } from "recharts";

export default function PowerMarkets() {
  const priceData = [
    { time: "00:00", spot: 45, intraday: 48, forward: 50 },
    { time: "04:00", spot: 42, intraday: 45, forward: 48 },
    { time: "08:00", spot: 52, intraday: 55, forward: 58 },
    { time: "12:00", spot: 65, intraday: 68, forward: 70 },
    { time: "16:00", spot: 72, intraday: 75, forward: 78 },
    { time: "20:00", spot: 58, intraday: 61, forward: 63 },
    { time: "24:00", spot: 48, intraday: 50, forward: 52 },
  ];

  const generationMix = [
    { source: "Nuclear", capacity: 8000, dispatch: 7900, color: "#00bcd4" },
    { source: "Wind", capacity: 6500, dispatch: 4200, color: "#4caf50" },
    { source: "Solar", capacity: 5200, dispatch: 3100, color: "#ffc107" },
    { source: "Gas", capacity: 4800, dispatch: 2400, color: "#ff9800" },
    { source: "Coal", capacity: 3500, dispatch: 1800, color: "#757575" },
  ];

  const supplyDemand = [
    { hour: "00:00", supply: 28000, demand: 25000 },
    { hour: "04:00", supply: 27500, demand: 24000 },
    { hour: "08:00", supply: 29000, demand: 27000 },
    { hour: "12:00", supply: 30500, demand: 31000 },
    { hour: "16:00", supply: 31000, demand: 32000 },
    { hour: "20:00", supply: 28500, demand: 29500 },
    { hour: "24:00", supply: 27000, demand: 26000 },
  ];

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/energy">
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Energy
              </Button>
            </Link>
            <div>
              <h1 className="text-2xl font-bold flex items-center gap-2" data-testid="text-page-title">
                <Zap className="w-6 h-6 text-primary" />
                Power Markets
              </h1>
              <p className="text-sm text-muted-foreground">Supply/demand fundamentals, price curves, and market analytics</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Summary Cards */}
        <section className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Zap className="w-5 h-5 text-primary" />
                Spot Price
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">€65.30</div>
              <p className="text-xs text-muted-foreground mt-2">/MWh</p>
              <Badge variant="outline" className="mt-2 text-red-500">
                <TrendingUp className="w-3 h-3 mr-1" />
                +8.2%
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Wind className="w-5 h-5 text-primary" />
                Wind Generation
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">4.2 GW</div>
              <p className="text-xs text-muted-foreground mt-2">Current output</p>
              <Badge variant="outline" className="mt-2 text-green-500">
                64.6% capacity
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Droplets className="w-5 h-5 text-primary" />
                Demand
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">32 GW</div>
              <p className="text-xs text-muted-foreground mt-2">Current demand</p>
              <Badge variant="outline" className="mt-2 text-orange-500">
                103% peak
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Flame className="w-5 h-5 text-primary" />
                Gas Price
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">$12.50</div>
              <p className="text-xs text-muted-foreground mt-2">/MBtu</p>
              <Badge variant="outline" className="mt-2 text-green-500">
                <TrendingDown className="w-3 h-3 mr-1" />
                -3.5%
              </Badge>
            </CardContent>
          </Card>
        </section>

        {/* Price Curves */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Price Curves: Spot, Intraday & Forward</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={priceData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="spot" stroke="#00e5ff" strokeWidth={2} name="Spot (€/MWh)" />
                  <Line type="monotone" dataKey="intraday" stroke="#4caf50" strokeWidth={2} name="Intraday (€/MWh)" />
                  <Line type="monotone" dataKey="forward" stroke="#ff9800" strokeWidth={2} name="Forward (€/MWh)" />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Supply & Demand */}
        <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Supply vs Demand Balance</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={supplyDemand}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="supply" fill="#4ade80" stroke="#22c55e" name="Supply (MW)" />
                  <Area type="monotone" dataKey="demand" fill="#ff6b6b" stroke="#ff6b6b" name="Demand (MW)" />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Generation Mix & Dispatch</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {generationMix.map((source) => (
                  <div key={source.source} className="space-y-1">
                    <div className="flex justify-between text-sm">
                      <span className="font-medium flex items-center gap-2">
                        <div className="w-3 h-3 rounded-full" style={{ backgroundColor: source.color }} />
                        {source.source}
                      </span>
                      <span className="text-muted-foreground">{source.dispatch.toLocaleString()} MW</span>
                    </div>
                    <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
                      <div 
                        className="h-full transition-all" 
                        style={{ 
                          width: `${(source.dispatch / source.capacity) * 100}%`,
                          backgroundColor: source.color
                        }} 
                      />
                    </div>
                    <div className="text-xs text-muted-foreground">{((source.dispatch / source.capacity) * 100).toFixed(1)}% capacity</div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </section>

        {/* Outages & Weather */}
        <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Outages & Maintenance</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="p-3 bg-red-500/10 border border-red-500/20 rounded-lg">
                <p className="text-sm font-semibold text-red-400">Scheduled Maintenance</p>
                <p className="text-xs text-muted-foreground mt-1">Gas Plant #3: 2.5 GW offline until 18:00</p>
              </div>
              <div className="p-3 bg-orange-500/10 border border-orange-500/20 rounded-lg">
                <p className="text-sm font-semibold text-orange-400">Unplanned Outage</p>
                <p className="text-xs text-muted-foreground mt-1">Wind Farm #7: 450 MW capacity reduction</p>
              </div>
              <div className="p-3 bg-green-500/10 border border-green-500/20 rounded-lg">
                <p className="text-sm font-semibold text-green-400">Status</p>
                <p className="text-xs text-muted-foreground mt-1">Total available capacity: 27.8 GW (94.3%)</p>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Weather Impact & Forecasts</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="p-3 bg-secondary/50 rounded-lg">
                <p className="text-sm font-semibold mb-2">Current Weather</p>
                <p className="text-xs text-muted-foreground space-y-1">
                  <div>• Temperature: 12°C</div>
                  <div>• Wind Speed: 8.5 m/s (Good for wind)</div>
                  <div>• Cloud Cover: 35% (Moderate solar)</div>
                  <div>• Precipitation: 0 mm</div>
                </p>
              </div>
              <div className="p-3 bg-secondary/50 rounded-lg">
                <p className="text-sm font-semibold mb-2">24h Forecast Impact</p>
                <p className="text-xs text-muted-foreground space-y-1">
                  <div>• Wind +15% (higher generation expected)</div>
                  <div>• Temp ↓2°C (increased heating demand)</div>
                  <div>• Solar -10% (cloud increase)</div>
                </p>
              </div>
            </CardContent>
          </Card>
        </section>

        {/* Cross-Fuel Sensitivities */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Cross-Fuel Sensitivities</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm font-semibold mb-2">Gas Price Sensitivity</p>
                  <p className="text-2xl font-bold text-orange-400">€0.85</p>
                  <p className="text-xs text-muted-foreground mt-2">Impact on power price per €1/MBtu</p>
                </div>
                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm font-semibold mb-2">Carbon Price Correlation</p>
                  <p className="text-2xl font-bold text-red-400">0.68</p>
                  <p className="text-xs text-muted-foreground mt-2">Correlation with CO2 prices</p>
                </div>
                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm font-semibold mb-2">Renewable Generation</p>
                  <p className="text-2xl font-bold text-green-400">-€12.50</p>
                  <p className="text-xs text-muted-foreground mt-2">Price impact per 1 GW increase</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
