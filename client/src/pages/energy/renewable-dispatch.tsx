import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import { ArrowLeft, Wind, Sun, Zap, TrendingUp, TrendingDown } from "lucide-react";
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function RenewableDispatch() {
  const windData = [
    { hour: "00:00", capacity: 6500, generation: 2100, forecast: 2200 },
    { hour: "04:00", capacity: 6500, generation: 3800, forecast: 3700 },
    { hour: "08:00", capacity: 6500, generation: 4900, forecast: 5100 },
    { hour: "12:00", capacity: 6500, generation: 5200, forecast: 5300 },
    { hour: "16:00", capacity: 6500, generation: 4200, forecast: 4400 },
    { hour: "20:00", capacity: 6500, generation: 2800, forecast: 2700 },
    { hour: "24:00", capacity: 6500, generation: 1800, forecast: 1900 },
  ];

  const solarData = [
    { hour: "00:00", capacity: 5200, generation: 0, forecast: 0 },
    { hour: "06:00", capacity: 5200, generation: 150, forecast: 180 },
    { hour: "09:00", capacity: 5200, generation: 2800, forecast: 2900 },
    { hour: "12:00", capacity: 5200, generation: 4200, forecast: 4100 },
    { hour: "15:00", capacity: 5200, generation: 3900, forecast: 3800 },
    { hour: "18:00", capacity: 5200, generation: 1200, forecast: 1300 },
    { hour: "21:00", capacity: 5200, generation: 0, forecast: 0 },
  ];

  const capacityFactorData = [
    { region: "North Sea", wind: 45, solar: 12, hydro: 38 },
    { region: "Atlantic", wind: 52, solar: 8, hydro: 35 },
    { region: "Mediterranean", wind: 28, solar: 42, hydro: 25 },
    { region: "Central", wind: 35, solar: 38, hydro: 32 },
    { region: "Baltic", wind: 48, solar: 10, hydro: 40 },
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
                <Wind className="w-6 h-6 text-primary" />
                Renewable Dispatch
              </h1>
              <p className="text-sm text-muted-foreground">Wind and solar generation forecasts with capacity factor analysis</p>
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
                <Wind className="w-5 h-5 text-primary" />
                Wind Generation
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">4.2 GW</div>
              <p className="text-xs text-muted-foreground mt-2">Current output</p>
              <Badge variant="outline" className="mt-2 text-blue-500">
                <TrendingUp className="w-3 h-3 mr-1" />
                64.6% capacity
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Sun className="w-5 h-5 text-primary" />
                Solar Generation
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">3.1 GW</div>
              <p className="text-xs text-muted-foreground mt-2">Current output</p>
              <Badge variant="outline" className="mt-2 text-yellow-500">
                59.6% capacity
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Zap className="w-5 h-5 text-primary" />
                Total Renewable
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">7.3 GW</div>
              <p className="text-xs text-muted-foreground mt-2">Combined output</p>
              <Badge variant="outline" className="mt-2 text-green-500">
                24.2% of grid
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <TrendingDown className="w-5 h-5 text-primary" />
                24h Avg CF
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">42.8%</div>
              <p className="text-xs text-muted-foreground mt-2">Capacity factor</p>
              <Badge variant="outline" className="mt-2 text-emerald-500">
                +3.2% vs avg
              </Badge>
            </CardContent>
          </Card>
        </section>

        {/* Wind Generation */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Wind Generation vs Forecast</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={windData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="generation" fill="#00e5ff" stroke="#00e5ff" name="Generation (MW)" />
                  <Area type="monotone" dataKey="forecast" fill="#4caf50" stroke="#4caf50" name="Forecast (MW)" opacity={0.3} />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Solar Generation */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Solar Generation vs Forecast</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={solarData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="generation" fill="#ffc107" stroke="#ffc107" name="Generation (MW)" />
                  <Area type="monotone" dataKey="forecast" fill="#ff9800" stroke="#ff9800" name="Forecast (MW)" opacity={0.3} />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Regional Capacity Factors */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Capacity Factors by Region</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={capacityFactorData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="region" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="wind" fill="#00e5ff" name="Wind %" />
                  <Bar dataKey="solar" fill="#ffc107" name="Solar %" />
                  <Bar dataKey="hydro" fill="#4caf50" name="Hydro %" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Performance Metrics */}
        <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Wind Performance</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>Installed Capacity</span>
                  <span className="font-semibold">6.5 GW</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Average CF (yearly)</span>
                  <span className="font-semibold">43.2%</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Best Hour CF</span>
                  <span className="font-semibold">80.0%</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Forecast Accuracy</span>
                  <span className="font-semibold text-green-500">92.3%</span>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Solar Performance</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>Installed Capacity</span>
                  <span className="font-semibold">5.2 GW</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Average CF (yearly)</span>
                  <span className="font-semibold">38.5%</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Peak Hour CF</span>
                  <span className="font-semibold">80.8%</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Forecast Accuracy</span>
                  <span className="font-semibold text-green-500">88.7%</span>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Grid Integration</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>Renewable Penetration</span>
                  <span className="font-semibold">24.2%</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Variability Index</span>
                  <span className="font-semibold">0.68</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Reserve Margin</span>
                  <span className="font-semibold text-green-500">18.5%</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span>Curtailment Rate</span>
                  <span className="font-semibold">2.3%</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
