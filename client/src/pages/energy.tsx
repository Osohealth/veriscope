import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link } from "wouter";
import { 
  ArrowLeft,
  Cloud,
  Zap,
  Wind,
  Thermometer
} from "lucide-react";

export default function EnergyTransition() {
  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/">
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Home
              </Button>
            </Link>
            <div>
              <h1 className="text-2xl font-bold" data-testid="text-page-title">Energy Transition / Power</h1>
              <p className="text-sm text-muted-foreground">Emissions intelligence and power market analytics</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        <section>
          <h2 className="text-xl font-semibold mb-4">Energy Transition Modules</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Link href="/energy/emissions">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-emissions-intelligence">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Cloud className="w-5 h-5 text-primary" />
                    Emissions Intelligence
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Voyage emissions (CO2, NOx, etc.)</p>
                  <p>• Cargo attribution & fleet / company summarization</p>
                  <p>• Regulatory scenario (EEXI, CII, ETS) overlays</p>
                  <p>• Benchmark / peer metrics</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/energy/power">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-power-markets">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Zap className="w-5 h-5 text-primary" />
                    Power Markets
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Supply / Demand fundamentals (generation, transmission)</p>
                  <p>• Price curves: spot, intraday, forward</p>
                  <p>• Outages & thermal / renewable dispatch</p>
                  <p>• Weather / forecast integration (wind, temp, etc.)</p>
                  <p>• Cross-fuel sensitivities (gas, coal, emissions)</p>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>

        {/* Additional Features */}
        <section>
          <h2 className="text-xl font-semibold mb-4">Analytics & Insights</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Link href="/energy/renewable">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-renewable-dispatch">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Wind className="w-5 h-5 text-primary" />
                    Renewable Dispatch
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>Wind and solar generation forecasts with capacity factor analysis</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/energy/weather">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-weather-integration">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Thermometer className="w-5 h-5 text-primary" />
                    Weather Integration
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>Temperature, wind speed, and precipitation impact models</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/energy/carbon">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-carbon-markets">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Cloud className="w-5 h-5 text-primary" />
                    Carbon Markets
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  <p>ETS pricing, carbon credit tracking, and regulatory compliance</p>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>
      </div>
    </div>
  );
}
