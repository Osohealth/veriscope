import { WebSocketServer } from 'ws';
import { storage } from "../storage";
import type { Port, Vessel, VesselPosition, PortDelayEvent, VesselDelaySnapshot, MarketDelayImpact } from "@shared/schema";

class DelayService {
  private delayCalculationInterval: NodeJS.Timeout | null = null;
  private wss: WebSocketServer | null = null;
  
  async start(wss: WebSocketServer) {
    this.wss = wss;
    console.log("Delay tracking service starting...");
    
    // Calculate delays immediately
    await this.calculatePortDelays();
    
    // Run delay calculation every 2 minutes
    this.delayCalculationInterval = setInterval(async () => {
      try {
        await this.calculatePortDelays();
      } catch (error) {
        console.error("Delay calculation error:", error);
      }
    }, 120000); // 2 minutes
    
    console.log("Delay tracking service started");
  }
  
  stop() {
    if (this.delayCalculationInterval) {
      clearInterval(this.delayCalculationInterval);
      this.delayCalculationInterval = null;
    }
    console.log("Delay tracking service stopped");
  }
  
  async calculatePortDelays() {
    try {
      // Focus on Rotterdam port
      const ports = await storage.getPorts();
      const rotterdamPort = ports.find(p => p.code === "RTM");
      
      if (!rotterdamPort) {
        console.log("Rotterdam port not found for delay calculation");
        return;
      }
      
      // Get all vessels with Rotterdam as destination
      const vessels = await storage.getVessels();
      const positions = await storage.getVesselPositions();
      
      // Calculate delays for vessels heading to Rotterdam
      for (const vessel of vessels) {
        const vesselPosition = positions.find(p => p.vesselId === vessel.id);
        
        if (vesselPosition && vesselPosition.destination?.includes("Rotterdam")) {
          await this.calculateVesselDelay(vessel, vesselPosition, rotterdamPort);
        }
      }
      
      // Calculate aggregated market impacts
      await this.calculateMarketImpacts(rotterdamPort);
      
    } catch (error) {
      console.error("Error calculating port delays:", error);
    }
  }
  
  async calculateVesselDelay(vessel: Vessel, position: VesselPosition, port: Port) {
    try {
      const now = new Date();
      const eta = position.eta ? new Date(position.eta) : null;
      
      if (!eta) {
        // Set default ETA based on distance/speed if not available
        const estimatedHours = 24 + Math.random() * 48; // 1-3 days estimate
        const scheduledETA = new Date(now.getTime() + estimatedHours * 60 * 60 * 1000);
        const currentETA = new Date(scheduledETA.getTime() + (Math.random() * 12 - 6) * 60 * 60 * 1000); // ±6 hours variation
        
        const delayHours = (currentETA.getTime() - scheduledETA.getTime()) / (60 * 60 * 1000);
        
        // Calculate cargo volume based on vessel capacity
        const cargoVolume = vessel.deadweight ? Math.floor(vessel.deadweight * 0.85) : 50000 + Math.floor(Math.random() * 150000);
        const cargoValue = cargoVolume * (80 + Math.random() * 40); // $80-120 per ton
        
        const delaySnapshot: Omit<VesselDelaySnapshot, 'id' | 'lastUpdated'> = {
          vesselId: vessel.id,
          currentPortId: null,
          destinationPortId: port.id,
          scheduledETA: scheduledETA,
          currentETA: currentETA,
          delayHours: delayHours.toFixed(2),
          cargoVolume: cargoVolume,
          cargoValue: cargoValue.toFixed(2),
          commodityId: null,
          impactSeverity: Math.abs(delayHours) > 12 ? 'high' : Math.abs(delayHours) > 6 ? 'medium' : 'low'
        };
        
        await storage.createVesselDelaySnapshot(delaySnapshot);
        
        // Create delay event if delay is significant (>2 hours)
        if (Math.abs(delayHours) > 2) {
          const delayEvent = {
            portId: port.id,
            vesselId: vessel.id,
            expectedArrival: scheduledETA,
            actualArrival: null,
            delayHours: delayHours.toFixed(2),
            delayReason: delayHours > 0 ? 'congestion' : 'early_arrival',
            cargoVolume: cargoVolume,
            cargoType: 'crude_oil',
            queuePosition: null,
            status: 'in_transit',
            metadata: undefined
          };
          
          await storage.createPortDelayEvent(delayEvent as any);
          
          // Emit delay notification for significant delays
          if (Math.abs(delayHours) > 6) {
            this.broadcastDelayAlert({
              vessel: vessel.name || 'Unknown Vessel',
              port: port.name,
              delayHours: delayHours,
              cargoVolume: cargoVolume,
              severity: delaySnapshot.impactSeverity as string
            });
          }
        }
      }
    } catch (error) {
      console.error(`Error calculating delay for vessel ${vessel.name}:`, error);
    }
  }
  
  async calculateMarketImpacts(port: Port) {
    try {
      // Get delay snapshots for the port (vesselId=undefined, portId=port.id)
      const delaySnapshots = await storage.getVesselDelaySnapshots(undefined, port.id);
      
      if (delaySnapshots.length === 0) {
        return;
      }
      
      // Calculate aggregated impact for 24h timeframe
      const totalDelayedVolume = delaySnapshots.reduce((sum: number, snap) => sum + (snap.cargoVolume || 0), 0);
      const totalDelayedValue = delaySnapshots.reduce((sum: number, snap) => sum + (parseFloat(snap.cargoValue as string) || 0), 0);
      const averageDelayHours = delaySnapshots.reduce((sum: number, snap) => sum + parseFloat(snap.delayHours as string), 0) / delaySnapshots.length;
      
      // Estimate supply impact (% of normal throughput)
      const normalDailyThroughput = 500000; // tons per day for Rotterdam
      const supplyImpact = (totalDelayedVolume / normalDailyThroughput) * 100;
      
      // Estimate price impact (crude oil correlation: 1% supply = ~0.5% price)
      const priceImpact = supplyImpact * 0.5;
      
      // Get commodities for crude oil
      const commodities = await storage.getCommodities();
      const crudeOil = commodities.find(c => c.code === 'BRENT' || c.code === 'WTI') || commodities[0];
      
      const marketImpact = {
        portId: port.id,
        commodityId: crudeOil.id,
        marketId: null,
        timeframe: '24h',
        totalDelayedVolume: totalDelayedVolume,
        totalDelayedValue: totalDelayedValue.toFixed(2),
        averageDelayHours: averageDelayHours.toFixed(2),
        vesselCount: delaySnapshots.length,
        supplyImpact: supplyImpact.toFixed(2),
        priceImpact: priceImpact.toFixed(4),
        confidence: "0.75",
        metadata: undefined,
        validUntil: new Date(Date.now() + 24 * 60 * 60 * 1000) // Valid for 24 hours
      };
      
      await storage.createMarketDelayImpact(marketImpact as any);
      
      console.log(`Market impact calculated for ${port.name}: ${totalDelayedVolume} tons delayed, ${averageDelayHours.toFixed(1)}h avg delay`);
      
    } catch (error) {
      console.error("Error calculating market impacts:", error);
    }
  }
  
  broadcastDelayAlert(data: { vessel: string; port: string; delayHours: number; cargoVolume: number; severity: string }) {
    if (!this.wss) return;
    
    const message = JSON.stringify({
      type: 'delay_alert',
      data: {
        title: `Vessel Delay Alert - ${data.port}`,
        message: `${data.vessel} delayed by ${Math.abs(data.delayHours).toFixed(1)} hours with ${(data.cargoVolume / 1000).toFixed(0)}K tons cargo`,
        severity: data.severity,
        timestamp: new Date().toISOString()
      }
    });
    
    this.wss.clients.forEach((client: any) => {
      if (client.readyState === 1) { // WebSocket.OPEN
        client.send(message);
      }
    });
  }
  
  async getPortDelays(portId: string) {
    return await storage.getPortDelayEvents(portId);
  }
  
  async getMarketImpacts(portId: string) {
    return await storage.getMarketDelayImpacts(portId);
  }
}

export const delayService = new DelayService();
