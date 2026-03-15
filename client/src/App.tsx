import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { WatchlistFilterProvider } from "@/hooks/use-watchlist-filter";
import { useEffect } from "react";
import Dashboard from "@/pages/dashboard";
import DashboardHome from "@/pages/dashboard-home";
import Home from "@/pages/home";
import SignalsPage from "@/pages/signals";
import Blog from "@/pages/blog";
import BlogPost from "@/pages/blog/post";
import CommoditiesIntelligence from "@/pages/commodities";
import MaritimeIntelligence from "@/pages/maritime";
import EnergyTransition from "@/pages/energy";
import EmissionsIntelligence from "@/pages/energy/emissions";
import PowerMarkets from "@/pages/energy/power-markets";
import RenewableDispatch from "@/pages/energy/renewable-dispatch";
import WeatherIntegration from "@/pages/energy/weather-integration";
import CarbonMarkets from "@/pages/energy/carbon-markets";
import TradesFlowsPage from "@/pages/trades-flows";
import InventoriesStoragePage from "@/pages/inventories-storage";
import FreightAnalyticsPage from "@/pages/freight-analytics";
import AisVesselTracking from "@/pages/maritime/ais-tracking";
import PortEventEngine from "@/pages/maritime/port-events";
import ContainerIntelligence from "@/pages/maritime/containers";
import BunkeringFuelEvents from "@/pages/maritime/bunkering";
import MaritimeInbox from "@/pages/maritime/inbox";
import CrudeProductsPage from "@/pages/commodities/crude-products";
import LngLpgPage from "@/pages/commodities/lng-lpg";
import DryBulkPage from "@/pages/commodities/dry-bulk";
import PetrochemPage from "@/pages/commodities/petrochem";
import AgriBiofuelPage from "@/pages/commodities/agri-biofuel";
import RefineryIntelligencePage from "@/pages/commodities/refinery-intelligence";
import SupplyDemandPage from "@/pages/commodities/supply-demand";
import ResearchInsightsPage from "@/pages/commodities/research-insights";
import { createModulePage } from "@/pages/module-page";
import NotFound from "@/pages/not-found";
import Register from "@/pages/auth/register";
import Login from "@/pages/auth/login";
import About from "@/pages/about";
import Contact from "@/pages/contact";
import Careers from "@/pages/careers";
import Documentation from "@/pages/documentation";
import TankScope from "@/pages/tankscope";
import WatchlistsPage from "@/pages/watchlists";
import AlertRulesPage from "@/pages/alert-rules";
import AlertsPage from "@/pages/alerts";
import AlertsCommandPage from "@/pages/alerts-command";
import ViewsPage from "@/pages/views";
import InvestigationsPage from "@/pages/investigations";
import InvestigationDetailPage from "@/pages/investigation-detail";
import AlertsHealthPage from "@/pages/alerts-health";
import AlertsDestinationsPage from "@/pages/alerts-destinations";
import AlertSubscriptionsPage from "@/pages/alert-subscriptions";
import IncidentsPage from "@/pages/incidents";
import InviteAcceptPage from "@/pages/invite-accept";
import TeamPage from "@/pages/team";
import AuditPage from "@/pages/audit";
import EscalationsPage from "@/pages/escalations";
import DataExportsPage from "@/pages/data-exports";
import RefinerySatellite from "@/pages/refinery-satellite";
import PortDetailPage from "@/pages/port-detail";
import AuthGate from "@/components/auth-gate";
import RouteBoundary from "@/components/route-boundary";
import TerminalPage from "@/pages/terminal";
import FlowsPage from "@/pages/flows";
import CongestionPage from "@/pages/congestion";
import CommandPage from "@/pages/command";

// Maritime subsections (AIS, Port Events, Containers, Bunkering, and Inbox have full pages, Predictive Schedules is a placeholder)
const PredictiveSchedulesPage = createModulePage("Predictive Schedules", "Event forecasts up to 6 weeks ahead", "/maritime", "Back to Maritime");

function Router() {
  return (
    <Switch>
      <Route path="/" component={Home} />
      <Route path="/platform">
        {() => (
          <AuthGate
            title="Explore Dashboard"
            description="Sign in or create an account to unlock the Veriscope platform."
          >
            <DashboardHome />
          </AuthGate>
        )}
      </Route>
      <Route path="/home">
        {() => (
          <AuthGate
            title="Explore Dashboard"
            description="Sign in or create an account to unlock the Veriscope platform."
          >
            <DashboardHome />
          </AuthGate>
        )}
      </Route>
      <Route path="/landing" component={Home} />
      <Route path="/dashboard">
        {() => (
          <AuthGate
            title="Dashboard Locked"
            description="Sign in or create an account to access the classic dashboard."
          >
            <Dashboard />
          </AuthGate>
        )}
      </Route>
      <Route path="/terminal">
        {() => (
          <RouteBoundary name="terminal">
            <AuthGate
              title="Terminal Locked"
              description="Sign in or create an account to access Terminal Mode."
            >
              <TerminalPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/flows">
        {() => (
          <RouteBoundary name="flows">
            <AuthGate
              title="Flows Locked"
              description="Sign in or create an account to access Flow Intelligence."
            >
              <FlowsPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/congestion">
        {() => (
          <RouteBoundary name="congestion">
            <AuthGate
              title="Congestion Locked"
              description="Sign in or create an account to access Port Congestion."
            >
              <CongestionPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/command">
        {() => (
          <RouteBoundary name="command">
            <AuthGate
              title="Command Locked"
              description="Sign in or create an account to access Command Mode."
            >
              <CommandPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/signals">
        {() => (
          <AuthGate
            title="Signals Locked"
            description="Sign in or create an account to view real-time signals."
          >
            <SignalsPage />
          </AuthGate>
        )}
      </Route>
      <Route path="/alerts">
        {() => (
          <RouteBoundary name="alerts">
            <AuthGate
              title="Alerts Locked"
              description="Sign in or create an account to access Alert Command."
            >
              <AlertsCommandPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/alerts/legacy" component={AlertsPage} />
      <Route path="/views">
        {() => (
          <RouteBoundary name="views">
            <AuthGate
              title="Views Locked"
              description="Sign in or create an account to access saved views."
            >
              <ViewsPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/investigations">
        {() => (
          <RouteBoundary name="investigations">
            <AuthGate
              title="Investigations Locked"
              description="Sign in or create an account to access investigations."
            >
              <InvestigationsPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/investigations/:id">
        {() => (
          <RouteBoundary name="investigation detail">
            <AuthGate
              title="Investigations Locked"
              description="Sign in or create an account to access investigations."
            >
              <InvestigationDetailPage />
            </AuthGate>
          </RouteBoundary>
        )}
      </Route>
      <Route path="/incidents" component={IncidentsPage} />
      <Route path="/alerts/health" component={AlertsHealthPage} />
      <Route path="/alerts/destinations" component={AlertsDestinationsPage} />
      <Route path="/alerts/subscriptions" component={AlertSubscriptionsPage} />
      <Route path="/invite/accept" component={InviteAcceptPage} />
      <Route path="/settings/team" component={TeamPage} />
      <Route path="/settings/audit" component={AuditPage} />
      <Route path="/settings/escalations" component={EscalationsPage} />
      <Route path="/about" component={About} />
      <Route path="/contact" component={Contact} />
      <Route path="/careers" component={Careers} />
      <Route path="/documentation" component={Documentation} />
      <Route path="/blog" component={Blog} />
      <Route path="/blog/:slug" component={BlogPost} />
      
      {/* Auth routes */}
      <Route path="/auth/register" component={Register} />
      <Route path="/auth/login" component={Login} />
      
      {/* Commodities routes */}
      <Route path="/commodities" component={CommoditiesIntelligence} />
      <Route path="/commodities/trades" component={TradesFlowsPage} />
      <Route path="/commodities/inventories" component={InventoriesStoragePage} />
      <Route path="/commodities/freight" component={FreightAnalyticsPage} />
      <Route path="/commodities/refinery-intelligence" component={RefineryIntelligencePage} />
      <Route path="/commodities/supply-demand" component={SupplyDemandPage} />
      <Route path="/commodities/research-insights" component={ResearchInsightsPage} />
      <Route path="/commodities/crude-products" component={CrudeProductsPage} />
      <Route path="/commodities/lng-lpg" component={LngLpgPage} />
      <Route path="/commodities/dry-bulk" component={DryBulkPage} />
      <Route path="/commodities/petrochem" component={PetrochemPage} />
      <Route path="/commodities/agri-biofuel" component={AgriBiofuelPage} />
      
      {/* Maritime routes */}
      <Route path="/maritime" component={MaritimeIntelligence} />
      <Route path="/maritime/ais-tracking" component={AisVesselTracking} />
      <Route path="/maritime/port-events" component={PortEventEngine} />
      <Route path="/maritime/predictive-schedules" component={PredictiveSchedulesPage} />
      <Route path="/maritime/containers" component={ContainerIntelligence} />
      <Route path="/maritime/bunkering" component={BunkeringFuelEvents} />
      <Route path="/maritime/inbox" component={MaritimeInbox} />
      <Route path="/maritime/vessels" component={Dashboard} />
      <Route path="/ports/:portId" component={PortDetailPage} />
      
      {/* TankScope classic dashboard */}
      <Route path="/tankscope" component={TankScope} />
      
      {/* Refinery Satellite Monitoring */}
      <Route path="/refinery-satellite" component={RefinerySatellite} />
      
      {/* Energy routes */}
      <Route path="/energy" component={EnergyTransition} />
      <Route path="/energy/emissions" component={EmissionsIntelligence} />
      <Route path="/energy/power" component={PowerMarkets} />
      <Route path="/energy/renewable" component={RenewableDispatch} />
      <Route path="/energy/weather" component={WeatherIntegration} />
      <Route path="/energy/carbon" component={CarbonMarkets} />
      
      {/* User features */}
      <Route path="/watchlists" component={WatchlistsPage} />
      <Route path="/alert-rules" component={AlertRulesPage} />
      <Route path="/exports" component={DataExportsPage} />
      
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  useEffect(() => {
    // Initialize services on app load
    fetch('/api/init', { method: 'POST' })
      .then(() => console.log('Veriscope services initialized'))
      .catch(console.error);
  }, []);

  return (
    <div className="dark">
      <QueryClientProvider client={queryClient}>
        <WatchlistFilterProvider>
          <TooltipProvider>
            <Toaster />
            <Router />
          </TooltipProvider>
        </WatchlistFilterProvider>
      </QueryClientProvider>
    </div>
  );
}

export default App;
