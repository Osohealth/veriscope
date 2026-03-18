import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import { ArrowLeft, Leaf, TrendingUp, TrendingDown, BarChart3, PieChart } from "lucide-react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function CarbonMarkets() {
  const eua_data = [
    { date: "Mon", price: 82.5, volume: 450 },
    { date: "Tue", price: 84.2, volume: 520 },
    { date: "Wed", price: 83.8, volume: 490 },
    { date: "Thu", price: 86.5, volume: 610 },
    { date: "Fri", price: 87.3, volume: 680 },
    { date: "Sat", price: 86.0, volume: 350 },
    { date: "Sun", price: 85.2, volume: 420 },
  ];

  const compliance_allocation = [
    { category: "Energy Sector", allocated: 40, used: 35, surplus: 5 },
    { category: "Industry", allocated: 35, used: 32, surplus: 3 },
    { category: "Aviation", allocated: 15, used: 14, surplus: 1 },
    { category: "Maritime", allocated: 10, used: 9, surplus: 1 },
  ];

  const creditData = [
    { type: "EUA 2024", volume: 450, price: 85.50 },
    { type: "EUA 2025", volume: 380, price: 82.25 },
    { type: "EUA 2026", volume: 290, price: 78.75 },
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
                <Leaf className="w-6 h-6 text-primary" />
                Carbon Markets
              </h1>
              <p className="text-sm text-muted-foreground">ETS pricing, carbon credit tracking, and regulatory compliance</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Market Summary */}
        <section className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <BarChart3 className="w-5 h-5 text-primary" />
                EUA Price
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">€85.20</div>
              <p className="text-xs text-muted-foreground mt-2">Per tonne CO2</p>
              <Badge variant="outline" className="mt-2 text-red-500">
                <TrendingUp className="w-3 h-3 mr-1" />
                +3.3%
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Leaf className="w-5 h-5 text-primary" />
                Credits Holdings
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">2.5M</div>
              <p className="text-xs text-muted-foreground mt-2">EUA credits owned</p>
              <Badge variant="outline" className="mt-2 text-green-500">
                Value: €213M
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <TrendingDown className="w-5 h-5 text-primary" />
                Est. Liability
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">$42K</div>
              <p className="text-xs text-muted-foreground mt-2">Monthly cost</p>
              <Badge variant="outline" className="mt-2 text-orange-500">
                504k tonnes/month
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <PieChart className="w-5 h-5 text-primary" />
                Compliance
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">87%</div>
              <p className="text-xs text-muted-foreground mt-2">Portfolio aligned</p>
              <Badge variant="outline" className="mt-2 text-green-500">
                On track
              </Badge>
            </CardContent>
          </Card>
        </section>

        {/* EUA Price Trend */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>EUA Spot Price & Trading Volume</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={eua_data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Legend />
                  <Line yAxisId="left" type="monotone" dataKey="price" stroke="#00e5ff" strokeWidth={2} name="EUA Price (€/tonne)" />
                  <Bar yAxisId="right" dataKey="volume" fill="#4caf50" name="Trading Volume (MtCO2)" opacity={0.6} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Credit Allocation */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>EU ETS Allocation by Sector</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={compliance_allocation}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="category" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="allocated" fill="#4caf50" name="Allocated" />
                  <Bar dataKey="used" fill="#ff9800" name="Used" />
                  <Bar dataKey="surplus" fill="#2196f3" name="Surplus" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Credit Market */}
        <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Active Credit Positions</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {creditData.map((credit, i) => (
                <div key={credit.type} className="p-3 bg-secondary/50 rounded-lg">
                  <div className="flex justify-between mb-2">
                    <p className="font-semibold">{credit.type}</p>
                    <p className="text-sm font-bold text-primary">€{credit.price}</p>
                  </div>
                  <div className="w-full h-2 bg-secondary rounded-full">
                    <div 
                      className="h-full bg-gradient-to-r from-emerald-400 to-cyan-400" 
                      style={{ width: `${(credit.volume / 500) * 100}%` }}
                    />
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">{credit.volume}k credits</p>
                </div>
              ))}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Regulatory Requirements</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="p-3 bg-green-500/10 border border-green-500/20 rounded-lg">
                <p className="text-sm font-semibold text-green-400">Phase 4 (2021-2030)</p>
                <p className="text-xs text-muted-foreground mt-1">Free allocation: 57% of baseline emissions</p>
              </div>
              <div className="p-3 bg-orange-500/10 border border-orange-500/20 rounded-lg">
                <p className="text-sm font-semibold text-orange-400">Phase 5 (2031-2038)</p>
                <p className="text-xs text-muted-foreground mt-1">Free allocation: Phased out to 0%</p>
              </div>
              <div className="p-3 bg-blue-500/10 border border-blue-500/20 rounded-lg">
                <p className="text-sm font-semibold text-blue-400">Your Status</p>
                <p className="text-xs text-muted-foreground mt-1">Compliant through 2024 | Action needed 2025</p>
              </div>
            </CardContent>
          </Card>
        </section>

        {/* Market Insights */}
        <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Price Drivers</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">EU climate policy</span>
                <span className="font-semibold">+35%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Economic growth</span>
                <span className="font-semibold">+18%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Renewable expansion</span>
                <span className="font-semibold">-12%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Supply tightness</span>
                <span className="font-semibold">+22%</span>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Compliance Costs</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Annual liability</span>
                <span className="font-bold">€504K</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Hedging costs</span>
                <span className="font-bold">€28K</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Admin/reporting</span>
                <span className="font-bold">€12K</span>
              </div>
              <div className="border-t pt-2 flex justify-between font-semibold">
                <span>Total Cost</span>
                <span>€544K</span>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Risk Factors</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Price volatility</span>
                <Badge variant="outline" className="text-orange-500">High</Badge>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Policy changes</span>
                <Badge variant="outline" className="text-yellow-500">Medium</Badge>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Supply risk</span>
                <Badge variant="outline" className="text-green-500">Low</Badge>
              </div>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
