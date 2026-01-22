import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import { ArrowLeft, TrendingUp, TrendingDown, Cloud, AlertTriangle, Leaf } from "lucide-react";
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

export default function EmissionsIntelligence() {
  const emissionsData = [
    { vessel: "VLCC Alpha", co2: 450, nox: 85, sox: 12 },
    { vessel: "Suezmax Beta", co2: 320, nox: 62, sox: 9 },
    { vessel: "Aframax Gamma", co2: 240, nox: 48, sox: 7 },
    { vessel: "Panamax Delta", co2: 180, nox: 35, sox: 5 },
    { vessel: "MR Epsilon", co2: 120, nox: 24, sox: 3 },
  ];

  const complianceData = [
    { regulation: "EEXI", compliant: 65, noncompliant: 35 },
    { regulation: "CII", compliant: 72, noncompliant: 28 },
    { regulation: "ETS", compliant: 58, noncompliant: 42 },
  ];

  const regulatoryScenarios = [
    { name: "IMO 2030 (30%)", value: 30, color: "bg-yellow-500" },
    { name: "IMO 2040 (70%)", value: 40, color: "bg-orange-500" },
    { name: "Net Zero 2050", value: 30, color: "bg-red-500" },
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
                <Cloud className="w-6 h-6 text-primary" />
                Emissions Intelligence
              </h1>
              <p className="text-sm text-muted-foreground">Voyage emissions, regulatory compliance, and carbon analytics</p>
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
                <Cloud className="w-5 h-5 text-primary" />
                Total CO2 Emissions
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">1,310</div>
              <p className="text-xs text-muted-foreground mt-2">Tonnes CO2/day</p>
              <Badge variant="outline" className="mt-2 text-emerald-500">
                <TrendingDown className="w-3 h-3 mr-1" />
                -5.2%
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Leaf className="w-5 h-5 text-primary" />
                Fleet Average
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">6.2</div>
              <p className="text-xs text-muted-foreground mt-2">g CO2/tonne-km</p>
              <Badge variant="outline" className="mt-2 text-blue-500">
                Industry avg: 8.1g
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-primary" />
                Compliance Status
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">87%</div>
              <p className="text-xs text-muted-foreground mt-2">EEXI/CII compliant</p>
              <Badge variant="outline" className="mt-2 text-green-500">
                13 vessels need action
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <TrendingUp className="w-5 h-5 text-primary" />
                Carbon Cost
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">$42K</div>
              <p className="text-xs text-muted-foreground mt-2">Est. ETS liability/month</p>
              <Badge variant="outline" className="mt-2 text-orange-500">
                €50-80/tonne
              </Badge>
            </CardContent>
          </Card>
        </section>

        {/* Emissions by Vessel */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Voyage Emissions (Top Vessels)</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={emissionsData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="vessel" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="co2" fill="#00e5ff" name="CO2 (tonnes)" />
                  <Bar dataKey="nox" fill="#ff9800" name="NOx (tonnes)" />
                  <Bar dataKey="sox" fill="#f44336" name="SOx (tonnes)" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Regulatory Compliance */}
        <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Compliance by Regulation</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={complianceData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="regulation" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="compliant" fill="#4ade80" name="Compliant %" />
                  <Bar dataKey="noncompliant" fill="#ef4444" name="Non-compliant %" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>IMO Reduction Targets</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {regulatoryScenarios.map((scenario) => (
                <div key={scenario.name} className="space-y-2">
                  <div className="flex justify-between text-sm">
                    <span className="font-medium">{scenario.name}</span>
                    <span className="text-muted-foreground">{scenario.value}% reduction</span>
                  </div>
                  <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
                    <div className={`h-full ${scenario.color}`} style={{ width: `${scenario.value}%` }} />
                  </div>
                </div>
              ))}
            </CardContent>
          </Card>
        </section>

        {/* Carbon Attribution */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Cargo Attribution & Fleet Summary</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm text-muted-foreground mb-1">Crude Oil Cargoes</p>
                  <p className="text-2xl font-bold">45%</p>
                  <p className="text-xs text-muted-foreground mt-2">445 tonnes CO2/day</p>
                </div>
                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm text-muted-foreground mb-1">Refined Products</p>
                  <p className="text-2xl font-bold">35%</p>
                  <p className="text-xs text-muted-foreground mt-2">375 tonnes CO2/day</p>
                </div>
                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm text-muted-foreground mb-1">LNG/Specialty</p>
                  <p className="text-2xl font-bold">20%</p>
                  <p className="text-xs text-muted-foreground mt-2">220 tonnes CO2/day</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </section>

        {/* Benchmarking */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Peer Benchmarking & Industry Metrics</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-3">
                <div>
                  <div className="flex justify-between text-sm mb-2">
                    <span>Your Fleet Average</span>
                    <span className="font-semibold text-primary">6.2 g CO2/t-km</span>
                  </div>
                  <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
                    <div className="h-full bg-green-500" style={{ width: "62%" }} />
                  </div>
                </div>
                <div>
                  <div className="flex justify-between text-sm mb-2">
                    <span>Industry Average (IMO)</span>
                    <span className="font-semibold text-muted-foreground">8.1 g CO2/t-km</span>
                  </div>
                  <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
                    <div className="h-full bg-yellow-500" style={{ width: "81%" }} />
                  </div>
                </div>
                <div>
                  <div className="flex justify-between text-sm mb-2">
                    <span>Best-in-class Target</span>
                    <span className="font-semibold text-muted-foreground">3.5 g CO2/t-km</span>
                  </div>
                  <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
                    <div className="h-full bg-orange-500" style={{ width: "35%" }} />
                  </div>
                </div>
              </div>
              <p className="text-sm text-muted-foreground pt-2 border-t">
                Your fleet is <span className="font-semibold text-green-500">23% better</span> than industry average. Continue monitoring to reach best-in-class status.
              </p>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
