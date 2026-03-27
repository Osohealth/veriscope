import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { WatchlistFilterProvider } from "@/hooks/useWatchlistFilter";
import { useEffect } from "react";
import Dashboard from "@/pages/Dashboard";
import DashboardHome from "@/pages/DashboardHome";
import Home from "@/pages/Home";
import SignalsPage from "@/pages/Signals";
import Blog from "@/pages/Blog";
import BlogPost from "@/pages/blog/Post";
import CommoditiesIntelligence from "@/pages/Commodities";
import MaritimeIntelligence from "@/pages/Maritime";
import EnergyTransition from "@/pages/Energy";
import EmissionsIntelligence from "@/pages/energy/Emissions";
import PowerMarkets from "@/pages/energy/PowerMarkets";
import RenewableDispatch from "@/pages/energy/RenewableDispatch";
import WeatherIntegration from "@/pages/energy/WeatherIntegration";
import CarbonMarkets from "@/pages/energy/CarbonMarkets";
import TradesFlowsPage from "@/pages/TradesFlows";
import InventoriesStoragePage from "@/pages/InventoriesStorage";
import FreightAnalyticsPage from "@/pages/FreightAnalytics";
import AisVesselTracking from "@/pages/maritime/AisTracking";
import PortEventEngine from "@/pages/maritime/PortEvents";
import ContainerIntelligence from "@/pages/maritime/Containers";
import BunkeringFuelEvents from "@/pages/maritime/Bunkering";
import MaritimeInbox from "@/pages/maritime/Inbox";
import CrudeProductsPage from "@/pages/commodities/CrudeProducts";
import LngLpgPage from "@/pages/commodities/LngLpg";
import DryBulkPage from "@/pages/commodities/DryBulk";
import PetrochemPage from "@/pages/commodities/Petrochem";
import AgriBiofuelPage from "@/pages/commodities/AgriBiofuel";
import RefineryIntelligencePage from "@/pages/commodities/RefineryIntelligence";
import SupplyDemandPage from "@/pages/commodities/SupplyDemand";
import ResearchInsightsPage from "@/pages/commodities/ResearchInsights";
import { createModulePage } from "@/pages/ModulePage";
import NotFound from "@/pages/NotFound";
import Register from "@/pages/auth/Register";
import Login from "@/pages/auth/Login";
import About from "@/pages/About";
import Contact from "@/pages/Contact";
import Careers from "@/pages/Careers";
import Documentation from "@/pages/Documentation";
import TankScope from "@/pages/Tankscope";
import WatchlistsPage from "@/pages/Watchlists";
import AlertRulesPage from "@/pages/AlertRules";
import AlertsPage from "@/pages/Alerts";
import AlertsHealthPage from "@/pages/AlertsHealth";
import AlertSubscriptionsPage from "@/pages/AlertSubscriptions";
import DataExportsPage from "@/pages/DataExports";
import RefinerySatellite from "@/pages/RefinerySatellite";
import PortDetailPage from "@/pages/PortDetail";
import AlertsCommandPage from "@/pages/AlertsCommand";
import AlertsDestinationsPage from "@/pages/AlertsDestinations";
import AuditPage from "@/pages/Audit";
import CommandPage from "@/pages/Command";
import CongestionPage from "@/pages/Congestion";
import EscalationsPage from "@/pages/Escalations";
import FlowsPage from "@/pages/Flows";
import IncidentsPage from "@/pages/Incidents";
import InvestigationDetailPage from "@/pages/InvestigationDetail";
import InvestigationsPage from "@/pages/Investigations";
import InviteAcceptPage from "@/pages/InviteAccept";
import TeamPage from "@/pages/Team";
import TerminalPage from "@/pages/Terminal";
import ViewsPage from "@/pages/Views";
import AuthGate from "@/components/AuthGate";
import RouteBoundary from "@/components/RouteBoundary";

// Maritime subsections (AIS, Port Events, Containers, Bunkering, and Inbox have full pages, Predictive Schedules is a placeholder)
const PredictiveSchedulesPage = createModulePage("Predictive Schedules", "Event forecasts up to 6 weeks ahead", "/maritime", "Back to Maritime");

function Router() {
  return (
    <Switch>
      <Route path="/" component={Home} />
      <Route path="/platform" component={DashboardHome} />
      <Route path="/home" component={DashboardHome} />
      <Route path="/landing" component={Home} />
      <Route path="/dashboard" component={Dashboard} />
      <Route path="/signals" component={SignalsPage} />
      <Route path="/alerts" component={AlertsPage} />
      <Route path="/alerts/health" component={AlertsHealthPage} />
      <Route path="/alerts/subscriptions" component={AlertSubscriptionsPage} />
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
      <Route path="/alerts/command" component={AlertsCommandPage} />
      <Route path="/alerts/destinations" component={AlertsDestinationsPage} />
      <Route path="/exports" component={DataExportsPage} />
      <Route path="/audit" component={AuditPage} />
      <Route path="/command" component={CommandPage} />
      <Route path="/congestion" component={CongestionPage} />
      <Route path="/escalations" component={EscalationsPage} />
      <Route path="/flows" component={FlowsPage} />
      <Route path="/incidents" component={IncidentsPage} />
      <Route path="/investigations/:id" component={InvestigationDetailPage} />
      <Route path="/investigations" component={InvestigationsPage} />
      <Route path="/invite/accept" component={InviteAcceptPage} />
      <Route path="/team" component={TeamPage} />
      <Route path="/terminal" component={TerminalPage} />
      <Route path="/views" component={ViewsPage} />

      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  useEffect(() => {
    // Initialize services on app load
    fetch('/api/init', { method: 'POST' })
      .then(() => { /* services initialized */ })
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
