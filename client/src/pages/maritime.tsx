import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link } from "wouter";
import { 
  ArrowLeft,
  Radio,
  Anchor,
  Navigation,
  Container,
  Droplets,
  Mail
} from "lucide-react";

export default function MaritimeIntelligence() {
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
              <h1 className="text-2xl font-bold" data-testid="text-page-title">Maritime / Vessel Intelligence</h1>
              <p className="text-sm text-muted-foreground">Real-time vessel tracking and maritime analytics</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        <section>
          <h2 className="text-xl font-semibold mb-4">Maritime Intelligence Modules</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <Link href="/maritime/ais-tracking">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-ais-tracking">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Radio className="w-5 h-5 text-primary" />
                    AIS / Vessel Tracking
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Live AIS (terrestrial + satellite)</p>
                  <p>• Historical tracks / replay</p>
                  <p>• Vessel static / dynamic attributes</p>
                  <p>• Route forecasting / ETA / predicted trajectories</p>
                  <p>• Anomaly / spoofing detection</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime/port-events">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-port-events">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Anchor className="w-5 h-5 text-primary" />
                    Port / Event Engine
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Port calls / berth times / anchorage</p>
                  <p>• STS, bunkering, pilot transfers, canal transits</p>
                  <p>• Congestion / waiting time metrics</p>
                  <p>• Terminal / yard throughput overlays</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime/predictive-schedules">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-predictive-schedules">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Navigation className="w-5 h-5 text-primary" />
                    Predictive Schedules / Forecasting
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Event forecast up to 6 weeks ahead</p>
                  <p>• ETA / ETD for vessels even if carriers silent</p>
                  <p>• Slack / delay risk probability</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime/containers">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-container-intelligence">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Container className="w-5 h-5 text-primary" />
                    Container Intelligence
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Shipment tracking (container-level)</p>
                  <p>• Port terminal congestion view (carrier splits)</p>
                  <p>• ETA predictive for container vessels</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime/bunkering">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-bunkering-fuel">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Droplets className="w-5 h-5 text-primary" />
                    Bunkering / Fuel Events
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• LNG bunkering (load / transfer) detection & volume estimation</p>
                  <p>• Bunker supply / demand zones</p>
                  <p>• Dual-fuel vessel interaction</p>
                </CardContent>
              </Card>
            </Link>

            <Link href="/maritime/inbox">
              <Card className="hover:border-primary transition-colors cursor-pointer" data-testid="card-inbox-communications">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Mail className="w-5 h-5 text-primary" />
                    Inbox / Communications Overlay
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground space-y-1">
                  <p>• Integrated maritime email / document hub</p>
                  <p>• Auto-tag events from mails</p>
                  <p>• Link mails → events / vessels / ports</p>
                </CardContent>
              </Card>
            </Link>
          </div>
        </section>

        {/* Quick Action to Classic Dashboard */}
        <section>
          <Card className="bg-accent/50">
            <CardHeader>
              <CardTitle>Live Vessel Tracking (TankScope)</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">
                Access the classic TankScope dashboard for real-time AIS vessel tracking with interactive map and port monitoring.
              </p>
              <Link href="/dashboard">
                <Button data-testid="button-launch-tankscope">
                  Launch TankScope Dashboard
                </Button>
              </Link>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
