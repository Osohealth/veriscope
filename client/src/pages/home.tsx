import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { 
  Ship, 
  TrendingUp, 
  BarChart3, 
  Zap,
  Building2,
  PieChart,
  Globe,
  ArrowRight,
  Activity,
  Users,
  MapPin,
  Radar,
  Satellite,
  Waves,
  ChevronDown
} from "lucide-react";
import veriscopeLogo from "@assets/ChatGPT Image Sep 28, 2025, 11_38_52 PM_1759345566852.png";
import commoditiesImage from "@assets/istockphoto-1645923179-612x612_1764451895248.jpg";
import tankerImage from "@assets/oil-tanker-sails-ocean-clear-sky-maritime-shipping-international-trade-concept-cargo-ship-carries-crude-fuels-385597174_1764443231197.webp";
import energyImage from "@assets/renewable_energy-768x555_1764452420992.jpg";
import satelliteImage from "@assets/The-Importance-of-Vessel-Tracking-System_1764443231200.webp";

export default function Home() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Navigation Header */}
      <header className="border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 sticky top-0 z-50">
        <div className="container mx-auto px-4 h-20 flex items-center justify-between">
          <div className="flex items-center">
            <Link href="/" className="flex items-center gap-3">
              <img src={veriscopeLogo} alt="Veriscope AI" className="h-14 w-auto" data-testid="logo-veriscope" />
              <span className="text-lg md:text-2xl font-semibold tracking-tight text-foreground">
                Veriscope AI
              </span>
            </Link>
          </div>
          <nav className="hidden md:flex flex-1 items-center justify-start gap-10 pl-12 text-base md:text-lg font-medium">
              <Link href="/platform" className="text-muted-foreground hover:text-primary transition-colors" data-testid="nav-dashboard">
                Dashboard
              </Link>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <button className="flex items-center gap-1 text-muted-foreground hover:text-primary transition-colors cursor-pointer bg-transparent border-none p-0 text-base md:text-lg" data-testid="nav-products">
                    Products
                    <ChevronDown className="w-4 h-4" />
                  </button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="w-[60vw] max-w-5xl p-6">
                  <div className="grid grid-cols-4 gap-8">
                    <div className="col-span-3 grid grid-cols-3 gap-8">
                      <div className="space-y-4">
                        <div className="flex items-start gap-3">
                          <BarChart3 className="w-5 h-5 mt-0.5 text-primary" />
                          <div>
                            <p className="text-base font-semibold">Commodities</p>
                            <p className="text-sm text-muted-foreground">
                              Real-time market intelligence across crude, products, and storage.
                            </p>
                          </div>
                        </div>
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          <li>Crude & Products</li>
                          <li>Inventories</li>
                          <li>Supply & Demand</li>
                          <li>Pricing & Spreads</li>
                          <li>Risk & Compliance</li>
                        </ul>
                      </div>
                      <div className="space-y-4">
                        <div className="flex items-start gap-3">
                          <Ship className="w-5 h-5 mt-0.5 text-primary" />
                          <div>
                            <p className="text-base font-semibold">Maritime</p>
                            <p className="text-sm text-muted-foreground">
                              Vessel tracking, port congestion, and voyage analytics.
                            </p>
                          </div>
                        </div>
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          <li>Ship Tracking</li>
                          <li>Port Congestion</li>
                          <li>ETA Forecasts</li>
                          <li>Route Analytics</li>
                          <li>Marine Traffic Inbox</li>
                        </ul>
                      </div>
                      <div className="space-y-4">
                        <div className="flex items-start gap-3">
                          <Zap className="w-5 h-5 mt-0.5 text-primary" />
                          <div>
                            <p className="text-base font-semibold">Energy Transition</p>
                            <p className="text-sm text-muted-foreground">
                              Power, carbon, and renewables intelligence for the new grid.
                            </p>
                          </div>
                        </div>
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          <li>Power Markets</li>
                          <li>Carbon Markets</li>
                          <li>Renewables</li>
                          <li>Fuel Switching</li>
                          <li>Regulatory Signals</li>
                        </ul>
                      </div>
                      <div className="space-y-4">
                        <div className="flex items-start gap-3">
                          <Activity className="w-5 h-5 mt-0.5 text-primary" />
                          <div>
                            <p className="text-base font-semibold">Signals & Alerts</p>
                            <p className="text-sm text-muted-foreground">
                              Explainable anomaly detection with clustered event context.
                            </p>
                          </div>
                        </div>
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          <li>Port Disruption Signals</li>
                          <li>Delay Risk Alerts</li>
                          <li>Inventory Shocks</li>
                          <li>Market Event Clusters</li>
                          <li>Confidence Scoring</li>
                        </ul>
                      </div>
                      <div className="space-y-4">
                        <div className="flex items-start gap-3">
                          <BarChart3 className="w-5 h-5 mt-0.5 text-primary" />
                          <div>
                            <p className="text-base font-semibold">Analytics & Reporting</p>
                            <p className="text-sm text-muted-foreground">
                              Dashboards, exports, and decision-ready reporting.
                            </p>
                          </div>
                        </div>
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          <li>Custom Dashboards</li>
                          <li>Data Exports</li>
                          <li>API Access</li>
                          <li>Weekly Briefs</li>
                          <li>Alert Routing</li>
                        </ul>
                      </div>
                      <div className="space-y-4">
                        <div className="flex items-start gap-3">
                          <Globe className="w-5 h-5 mt-0.5 text-primary" />
                          <div>
                            <p className="text-base font-semibold">Coverage & Integrations</p>
                            <p className="text-sm text-muted-foreground">
                              Global coverage with data feeds and workflow integrations.
                            </p>
                          </div>
                        </div>
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          <li>Global Port Network</li>
                          <li>Data API</li>
                          <li>Python SDK</li>
                          <li>Excel Add-in</li>
                          <li>Slack / Email Alerts</li>
                        </ul>
                      </div>
                    </div>
                    <div className="col-span-1 rounded-xl border border-border bg-muted/40 p-4">
                      <div className="aspect-[4/3] overflow-hidden rounded-lg bg-muted">
                        <img src={tankerImage} alt="Market brief" className="h-full w-full object-cover" />
                      </div>
                      <div className="mt-4 space-y-2">
                        <p className="text-xs uppercase tracking-wide text-muted-foreground">Featured brief</p>
                        <p className="text-sm font-semibold">
                          Rotterdam congestion builds as arrivals dip and dwell spikes
                        </p>
                        <p className="text-sm text-muted-foreground">
                          See the drivers behind the latest port disruption signal.
                        </p>
                      </div>
                    </div>
                  </div>
                </DropdownMenuContent>
              </DropdownMenu>
              <span className="text-muted-foreground hover:text-primary transition-colors cursor-pointer" data-testid="nav-markets">
                Markets
              </span>
              <span className="text-muted-foreground hover:text-primary transition-colors cursor-pointer" data-testid="nav-company">
                Company
              </span>
          </nav>
          <div className="flex items-center space-x-4 pl-6">
            <Link href="/auth/login">
              <Button variant="outline" size="sm" data-testid="button-login">
                Sign In
              </Button>
            </Link>
            <Link href="/auth/register">
              <Button size="sm" data-testid="button-request-demo">
                Request demo
              </Button>
            </Link>
          </div>
        </div>
      </header>

      {/* Hero Section with Image */}
      <section className="relative py-0 px-0 overflow-hidden">
        <div className="absolute inset-0">
          <img 
            src={tankerImage} 
            alt="Global Maritime" 
            className="w-full h-full object-cover"
          />
          <div className="absolute inset-0 bg-gradient-to-r from-background via-background/70 to-background/40"></div>
        </div>
        <div className="relative container mx-auto max-w-6xl px-4 py-32">
          <div className="space-y-8 max-w-2xl">
            <Badge variant="outline" className="px-4 py-1 bg-primary/10 border-primary/30 text-primary w-fit">
              Real-time Maritime Intelligence
            </Badge>
            <h1 className="text-5xl md:text-7xl font-bold tracking-tight leading-tight">
              Trade with Confidence
            </h1>
            <p className="text-xl text-muted-foreground leading-relaxed max-w-xl">
              With actionable real-time data and intelligence, we enable forward-thinking businesses to plan, grow, and move sustainably into the future
            </p>
            <div className="flex flex-col sm:flex-row gap-4 pt-4">
              <Link href="/platform">
                <Button size="lg" className="px-8" data-testid="button-hero-demo">
                  Explore Dashboard
                  <ArrowRight className="ml-2 w-4 h-4" />
                </Button>
              </Link>
              <Link href="/signals">
                <Button size="lg" variant="outline">
                  View Signals
                </Button>
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Stats Bar */}
      <section className="bg-secondary/50 border-y border-border py-8 px-4">
        <div className="container mx-auto max-w-6xl">
          <div className="grid grid-cols-3 md:grid-cols-6 gap-8">
            {[
              { label: "Global Vessels", value: "45,000+" },
              { label: "Ports Monitored", value: "500+" },
              { label: "Data Points/Day", value: "10M+" },
              { label: "Real-time Events", value: "24/7" },
              { label: "Countries", value: "180+" },
              { label: "Traders", value: "10K+" }
            ].map((stat) => (
              <div key={stat.label} className="text-center">
                <div className="text-2xl md:text-3xl font-bold text-primary mb-2">
                  {stat.value}
                </div>
                <p className="text-xs md:text-sm text-muted-foreground">
                  {stat.label}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Three Pillars Section with Images */}
      <section className="py-24 px-4">
        <div className="container mx-auto max-w-6xl">
          <div className="text-center space-y-4 mb-20">
            <h2 className="text-4xl md:text-5xl font-bold">Enterprise-Grade Intelligence</h2>
            <p className="text-xl text-muted-foreground max-w-3xl mx-auto">
              Three integrated platforms powering global trade and maritime operations
            </p>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            {/* Commodities */}
            <div className="group relative">
              <div className="absolute inset-0 bg-gradient-to-b from-blue-500/20 to-transparent rounded-lg -z-10 group-hover:from-blue-500/40 transition-all duration-300"></div>
              <Card className="border-border hover:border-primary/50 transition-all overflow-hidden h-full">
                <div className="h-48 bg-gradient-to-br from-primary/20 to-blue-600/20 relative overflow-hidden">
                  <img 
                    src={commoditiesImage} 
                    alt="Trading Analytics" 
                    className="w-full h-full object-cover opacity-60 group-hover:opacity-80 transition-opacity"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-background to-transparent"></div>
                </div>
                <CardHeader>
                  <div className="flex items-center gap-3 mb-2">
                    <BarChart3 className="w-6 h-6 text-primary" />
                    <CardTitle className="text-2xl">Commodities</CardTitle>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <p className="text-muted-foreground">
                    Master 40+ commodities. Spot trends, control storage, and predict prices with ML-powered insights.
                  </p>
                  <div className="flex flex-col gap-2">
                    <Link href="/commodities">
                      <Button className="w-full" data-testid="button-commodities-demo">
                        Explore Commodities
                      </Button>
                    </Link>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Maritime */}
            <div className="group relative">
              <div className="absolute inset-0 bg-gradient-to-b from-blue-500/20 to-transparent rounded-lg -z-10 group-hover:from-blue-500/40 transition-all duration-300"></div>
              <Card className="border-border hover:border-primary/50 transition-all overflow-hidden h-full">
                <div className="h-48 bg-gradient-to-br from-primary/20 to-blue-600/20 relative overflow-hidden">
                  <img 
                    src={tankerImage} 
                    alt="Maritime Vessels" 
                    className="w-full h-full object-cover opacity-60 group-hover:opacity-80 transition-opacity"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-background to-transparent"></div>
                </div>
                <CardHeader>
                  <div className="flex items-center gap-3 mb-2">
                    <Ship className="w-6 h-6 text-primary" />
                    <CardTitle className="text-2xl">Maritime</CardTitle>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <p className="text-muted-foreground">
                    Track 45,000+ vessels globally. Monitor ports, optimize freight routes, and unlock supply chain insights.
                  </p>
                  <div className="flex flex-col gap-2">
                    <Link href="/maritime">
                      <Button className="w-full" data-testid="button-maritime-demo">
                        Explore Maritime
                      </Button>
                    </Link>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Energy Transition */}
            <div className="group relative">
              <div className="absolute inset-0 bg-gradient-to-b from-blue-500/20 to-transparent rounded-lg -z-10 group-hover:from-blue-500/40 transition-all duration-300"></div>
              <Card className="border-border hover:border-primary/50 transition-all overflow-hidden h-full">
                <div className="h-48 bg-gradient-to-br from-primary/20 to-blue-600/20 relative overflow-hidden">
                  <img 
                    src={energyImage} 
                    alt="Renewable Energy" 
                    className="w-full h-full object-cover opacity-60 group-hover:opacity-80 transition-opacity"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-background to-transparent"></div>
                </div>
                <CardHeader>
                  <div className="flex items-center gap-3 mb-2">
                    <Zap className="w-6 h-6 text-primary" />
                    <CardTitle className="text-2xl">Energy</CardTitle>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <p className="text-muted-foreground">
                    Navigate the energy transition. Real-time power markets and renewable fuel intelligence for sustainable growth.
                  </p>
                  <div className="flex flex-col gap-2">
                    <Link href="/energy">
                      <Button className="w-full" data-testid="button-energy-demo">
                        Explore Energy
                      </Button>
                    </Link>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-24 px-4 bg-secondary/30 border-y border-border">
        <div className="container mx-auto max-w-6xl">
          <h2 className="text-4xl md:text-5xl font-bold text-center mb-16">Why Veriscope</h2>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
            {[
              {
                icon: Activity,
                title: "Real-Time Monitoring",
                description: "Live AIS tracking, port events, storage levels, and market movements updated 24/7"
              },
              {
                icon: TrendingUp,
                title: "Predictive Analytics",
                description: "ML-powered price forecasts and demand predictions based on maritime data"
              },
              {
                icon: Globe,
                title: "Global Coverage",
                description: "180+ countries, 500+ ports, 45,000+ vessels monitored across all oceans"
              },
              {
                icon: BarChart3,
                title: "Deep Analytics",
                description: "40+ commodities with comprehensive supply/demand balances and market research"
              },
              {
                icon: Radar,
                title: "Smart Signals",
                description: "Automated alerts for congestion, storage anomalies, and market opportunities"
              },
              {
                icon: Users,
                title: "Enterprise Ready",
                description: "Trusted by 10,000+ organizations including Shell, BP, Maersk, and Goldman Sachs"
              }
            ].map((feature, idx) => (
              <Card key={idx} className="border-border bg-background/50 hover:bg-background/80 transition-colors">
                <CardHeader>
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-primary/10 rounded-lg">
                      <feature.icon className="w-6 h-6 text-primary" />
                    </div>
                    <CardTitle className="text-lg">{feature.title}</CardTitle>
                  </div>
                </CardHeader>
                <CardContent>
                  <p className="text-muted-foreground text-sm">
                    {feature.description}
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="relative py-24 px-4 overflow-hidden">
        <div className="absolute inset-0 opacity-30">
          <img src={satelliteImage} alt="background" className="w-full h-full object-cover" />
        </div>
        <div className="relative container mx-auto max-w-4xl">
          <div className="text-center space-y-8 bg-background/80 backdrop-blur rounded-lg p-12">
            <h2 className="text-4xl md:text-5xl font-bold">
              Ready to transform your trade operations?
            </h2>
            <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
              Get started with Veriscope and unlock the power of real-time maritime and commodity intelligence
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link href="/auth/register">
                <Button size="lg" className="px-8" data-testid="button-cta-demo">
                  Request Demo
                  <ArrowRight className="ml-2 w-4 h-4" />
                </Button>
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border bg-background/50 py-12 px-4">
        <div className="container mx-auto max-w-6xl">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
            <div>
              <img src={veriscopeLogo} alt="Veriscope" className="h-8 mb-4" />
              <p className="text-sm text-muted-foreground">
                Real-time intelligence for global trade
              </p>
            </div>
            {[
              {
                title: "Product",
                links: [
                  { label: "Commodities", href: "/commodities" },
                  { label: "Maritime", href: "/maritime" },
                  { label: "Energy", href: "/energy" }
                ]
              },
              {
                title: "Company",
                links: [
                  { label: "About", href: "/about" },
                  { label: "Blog", href: "/blog" },
                  { label: "Careers", href: "/careers" }
                ]
              },
              {
                title: "Resources",
                links: [
                  { label: "Documentation", href: "/documentation" },
                  { label: "API", href: "#" },
                  { label: "Contact", href: "/contact" }
                ]
              }
            ].map((col, idx) => (
              <div key={idx}>
                <h3 className="font-semibold text-sm mb-4">{col.title}</h3>
                <ul className="space-y-2">
                  {col.links.map((link, lidx) => (
                    <li key={lidx}>
                      <Link href={link.href}>
                        <a className="text-sm text-muted-foreground hover:text-primary transition-colors">
                          {link.label}
                        </a>
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
          <div className="border-t border-border pt-8 text-center text-sm text-muted-foreground">
            <p>&copy; 2024 Veriscope AI. All rights reserved.</p>
          </div>
        </div>
      </footer>
    </div>
  );
}
