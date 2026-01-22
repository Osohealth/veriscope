import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link } from "wouter";
import { 
  ArrowLeft,
  TrendingUp,
  Warehouse,
  Ship,
  Factory,
  BarChart3,
  Layers,
  FileText,
  Droplet,
  Fuel,
  Package,
  Leaf,
  Beaker
} from "lucide-react";

export default function CommoditiesIntelligence() {
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
              <h1 className="text-2xl font-bold" data-testid="text-page-title">Commodities Intelligence</h1>
              <p className="text-sm text-muted-foreground">Comprehensive commodity tracking and analytics</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Core Analytics */}
        <section>
          <h2 className="text-xl font-semibold mb-4">Core Analytics</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <Link href="/commodities/trades">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-trades-flows">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <TrendingUp className="w-5 h-5 text-primary" />
                    Trades & Flows
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Cargo chain view (origin → destination)</p>
                  <p>• STS / transshipment linking</p>
                  <p>• Grade / product splitting</p>
                  <p>• Forecasted flows (ML projection)</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/inventories">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-inventories-storage">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Warehouse className="w-5 h-5 text-primary" />
                    Inventories & Storage
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Tank level estimates by installation</p>
                  <p>• Floating storage (vessel + offshore)</p>
                  <p>• SPR vs commercial split</p>
                  <p>• Historical time series (5+ yrs)</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/freight">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-freight-analytics">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Ship className="w-5 h-5 text-primary" />
                    Freight Analytics
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Ton-miles / Ton-days metrics</p>
                  <p>• Laden vs Ballast analysis</p>
                  <p>• Port / canal congestion impacts</p>
                  <p>• Freight cost derivation</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/refinery-intelligence">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-refinery-intelligence">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Factory className="w-5 h-5 text-primary" />
                    Refinery / Plant Intelligence
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Plant / unit hierarchy (CDU, FCC)</p>
                  <p>• Operational status / outages</p>
                  <p>• Utilization / run forecasts</p>
                  <p>• Margin calculators / crack spreads</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/supply-demand">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-supply-demand">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <BarChart3 className="w-5 h-5 text-primary" />
                    Supply & Demand / Balances
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Country / region S&D models</p>
                  <p>• Forward forecasts (short & mid term)</p>
                  <p>• Disruption simulation</p>
                  <p>• Price sensitivity / elasticities</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/research-insights">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-research-insights">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <FileText className="w-5 h-5 text-primary" />
                    Research / Insight Layer
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Analyst outlook reports</p>
                  <p>• Change summary deltas</p>
                  <p>• Scenario builder</p>
                  <p>• Methodology / provenance viewer</p>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>

        {/* Pillar Packs */}
        <section>
          <h2 className="text-xl font-semibold mb-4">Commodity Pillar Packs</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <Link href="/commodities/crude-products">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-crude-products-pack">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Droplet className="w-5 h-5 text-primary" />
                    Crude & Products Pack
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Grade library & assays</p>
                  <p>• Buyer / seller network graph</p>
                  <p>• Arbitrage monitors</p>
                  <p>• Clean vs dirty routing analysis</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/lng-lpg">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-lng-lpg-pack">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Fuel className="w-5 h-5 text-primary" />
                    LNG / LPG Pack
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Cargo diversions & re-routing detection</p>
                  <p>• Contract & tender tree (SPAs, TTAs)</p>
                  <p>• Regas constraints & capacity overlays</p>
                  <p>• Price curves across hubs (TTF, Henry, JKM)</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/dry-bulk">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-dry-bulk-pack">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Package className="w-5 h-5 text-primary" />
                    Dry Bulk Pack
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Coal / iron ore / grain flows</p>
                  <p>• Port draft / lock constraints</p>
                  <p>• Seasonal / monsoon disruption flags</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/petrochem">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-petrochem-pack">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Beaker className="w-5 h-5 text-primary" />
                    Petrochem / Chemicals Pack
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Product taxonomy & route database</p>
                  <p>• Multi-parcel flow reconciliation</p>
                  <p>• Plant-to-plant edge tracing</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/commodities/agri-biofuel">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-agri-biofuel-pack">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Leaf className="w-5 h-5 text-primary" />
                    Agri / Biofuel Pack
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Oilseed / veg oil / crushing flows</p>
                  <p>• Biofuel (FAME / HVO / SAF) overlay</p>
                  <p>• Land / seasonal constraint integration</p>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>
      </div>
    </div>
  );
}
