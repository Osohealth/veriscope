import { storage } from '../storage';
import { logger } from '../middleware/observability';
import type { Port, VesselPosition } from '@shared/schema';

function haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

interface VesselPortState {
  vesselId: string;
  currentPortId: string | null;
  enteredAt: Date | null;
  lastPositionTime: Date;
}

class PortCallService {
  private intervalId: NodeJS.Timeout | null = null;
  private vesselStates: Map<string, VesselPortState> = new Map();
  private checkIntervalMs = 60000;
  private portCallsCreated = 0;
  private portCallsCompleted = 0;

  start() {
    logger.info('Starting port call detection service', { 
      checkIntervalMs: this.checkIntervalMs 
    });
    
    this.intervalId = setInterval(async () => {
      try {
        await this.detectPortCalls();
      } catch (error) {
        logger.error('Port call detection error', { error: (error as Error).message });
      }
    }, this.checkIntervalMs);
    
    this.detectPortCalls();
  }

  stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    logger.info('Port call detection service stopped');
  }

  private async detectPortCalls() {
    const ports = await storage.getPorts();
    const vessels = await storage.getVessels();
    const latestPositions = await storage.getLatestVesselPositions();
    
    const positionMap = new Map<string, VesselPosition>();
    for (const pos of latestPositions) {
      positionMap.set(pos.vesselId, pos);
    }
    
    for (const vessel of vessels) {
      const position = positionMap.get(vessel.id);
      if (!position) continue;
      
      const vesselLat = parseFloat(String(position.latitude));
      const vesselLon = parseFloat(String(position.longitude));
      
      if (isNaN(vesselLat) || isNaN(vesselLon)) continue;
      
      let currentPort: Port | null = null;
      let minDistance = Infinity;
      
      for (const port of ports) {
        const portLat = parseFloat(String(port.latitude));
        const portLon = parseFloat(String(port.longitude));
        const geofenceRadius = parseFloat(String(port.geofenceRadiusKm || '10'));
        
        if (isNaN(portLat) || isNaN(portLon)) continue;
        
        const distance = haversineDistance(vesselLat, vesselLon, portLat, portLon);
        
        if (distance <= geofenceRadius && distance < minDistance) {
          currentPort = port;
          minDistance = distance;
        }
      }
      
      await this.updateVesselState(vessel.id, currentPort, position);
    }
  }

  private async updateVesselState(vesselId: string, currentPort: Port | null, position: VesselPosition) {
    const state = this.vesselStates.get(vesselId) || {
      vesselId,
      currentPortId: null,
      enteredAt: null,
      lastPositionTime: new Date()
    };
    
    const previousPortId = state.currentPortId;
    const now = new Date();
    
    if (currentPort && !previousPortId) {
      state.currentPortId = currentPort.id;
      state.enteredAt = now;
      state.lastPositionTime = now;
      
      try {
        await storage.createPortCall({
          vesselId,
          portId: currentPort.id,
          arrivalTime: now,
          departureTime: null,
          callType: 'arrival',
          status: 'in_port',
          waitTimeHours: '0',
          berthTimeHours: null
        });
        
        this.portCallsCreated++;
        logger.info('Port call created (arrival)', {
          vesselId,
          portId: currentPort.id,
          portName: currentPort.name
        });
      } catch (error) {
        logger.error('Failed to create port call', { error: (error as Error).message });
      }
    } else if (!currentPort && previousPortId) {
      try {
        const recentCalls = await storage.getPortCallsByPort(previousPortId, 
          new Date(Date.now() - 7 * 24 * 60 * 60 * 1000), new Date());
        
        const openCall = recentCalls.find(c => 
          c.vesselId === vesselId && c.status === 'in_port' && !c.departureTime
        );
        
        if (openCall) {
          const dwellMs = now.getTime() - new Date(openCall.arrivalTime!).getTime();
          const berthHours = (dwellMs / (1000 * 60 * 60)).toFixed(1);
          
          await storage.updatePortCall(openCall.id, {
            departureTime: now,
            status: 'completed',
            berthTimeHours: berthHours
          });
          
          this.portCallsCompleted++;
          logger.info('Port call completed (departure)', {
            vesselId,
            portId: previousPortId,
            berthHours
          });
        }
      } catch (error) {
        logger.error('Failed to update port call', { error: (error as Error).message });
      }
      
      state.currentPortId = null;
      state.enteredAt = null;
    } else if (currentPort && previousPortId && currentPort.id !== previousPortId) {
      try {
        const recentCalls = await storage.getPortCallsByPort(previousPortId,
          new Date(Date.now() - 7 * 24 * 60 * 60 * 1000), new Date());
        
        const openCall = recentCalls.find(c =>
          c.vesselId === vesselId && c.status === 'in_port' && !c.departureTime
        );
        
        if (openCall) {
          await storage.updatePortCall(openCall.id, {
            departureTime: now,
            status: 'completed'
          });
        }
        
        await storage.createPortCall({
          vesselId,
          portId: currentPort.id,
          arrivalTime: now,
          departureTime: null,
          callType: 'arrival',
          status: 'in_port',
          waitTimeHours: '0',
          berthTimeHours: null
        });
        
        this.portCallsCreated++;
        logger.info('Vessel moved to different port', {
          vesselId,
          fromPort: previousPortId,
          toPort: currentPort.id
        });
      } catch (error) {
        logger.error('Failed to handle port transition', { error: (error as Error).message });
      }
      
      state.currentPortId = currentPort.id;
      state.enteredAt = now;
    }
    
    state.lastPositionTime = now;
    this.vesselStates.set(vesselId, state);
  }

  getStatus() {
    return {
      isRunning: this.intervalId !== null,
      checkIntervalMs: this.checkIntervalMs,
      trackedVessels: this.vesselStates.size,
      portCallsCreated: this.portCallsCreated,
      portCallsCompleted: this.portCallsCompleted,
      vesselStates: Array.from(this.vesselStates.entries()).map(([id, state]) => ({
        vesselId: id,
        inPort: state.currentPortId !== null,
        portId: state.currentPortId,
        enteredAt: state.enteredAt
      }))
    };
  }
}

export const portCallService = new PortCallService();
