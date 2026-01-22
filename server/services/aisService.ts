import { WebSocketServer, WebSocket } from 'ws';
import { createHash } from 'crypto';
import { storage } from '../storage';
import { logger, metricsCollector, setAisHealth } from '../middleware/observability';
import type { VesselPosition } from '@shared/schema';

interface AISMessage {
  mmsi: string;
  timestamp: Date;
  latitude: string;
  longitude: string;
  speed: string;
  course: string;
  heading: string;
  status: string;
  destination?: string;
  eta?: Date;
}

interface QueuedMessage {
  hash: string;
  message: AISMessage;
  receivedAt: number;
}

class AISService {
  private intervalId: NodeJS.Timeout | null = null;
  private wss: WebSocketServer | null = null;
  private aisStreamClient: WebSocket | null = null;
  
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private baseReconnectDelayMs = 1000;
  private maxReconnectDelayMs = 60000;
  private reconnectTimeoutId: NodeJS.Timeout | null = null;
  
  private recentMessageHashes = new Set<string>();
  private maxHashSetSize = 10000;
  private hashCleanupIntervalId: NodeJS.Timeout | null = null;
  
  private messageQueue: QueuedMessage[] = [];
  private maxQueueSize = 5000;
  private processingQueue = false;
  private queueProcessIntervalId: NodeJS.Timeout | null = null;
  
  private messagesDropped = 0;
  private duplicatesFiltered = 0;
  private lastConnectionStatus: 'connected' | 'disconnected' | 'reconnecting' = 'disconnected';
  private mode: 'simulation' | 'live' = 'simulation';

  startSimulation(wss: WebSocketServer) {
    this.wss = wss;
    
    this.hashCleanupIntervalId = setInterval(() => this.cleanupHashSet(), 60000);
    this.queueProcessIntervalId = setInterval(() => this.processQueue(), 100);
    
    const apiKey = process.env.AISSTREAM_API_KEY;
    
    if (apiKey) {
      this.mode = 'live';
      logger.info('AIS stream API key found, connecting to live AIS stream', {
        mode: this.mode,
        maxQueueSize: this.maxQueueSize,
        maxHashSetSize: this.maxHashSetSize,
        maxReconnectAttempts: this.maxReconnectAttempts
      });
      this.connectToAISStream(apiKey);
    } else {
      this.mode = 'simulation';
      this.intervalId = setInterval(async () => {
        try {
          await this.generateAISUpdates();
        } catch (error) {
          logger.error('AIS simulation error', { error: (error as Error).message });
        }
      }, 30000);
      
      setAisHealth(true);
      this.lastConnectionStatus = 'connected';
      logger.info('AIS simulation service started (no API key, using mock data)', {
        maxQueueSize: this.maxQueueSize,
        maxHashSetSize: this.maxHashSetSize
      });
    }
  }

  async connectToAISStream(apiKey?: string) {
    if (!apiKey) {
      apiKey = process.env.AISSTREAM_API_KEY;
    }
    
    if (!apiKey) {
      logger.warn('No AIS stream API key configured, using simulation mode');
      return;
    }
    
    try {
      this.lastConnectionStatus = 'reconnecting';
      setAisHealth(false);
      
      const streamUrl = 'wss://stream.aisstream.io/v0/stream';
      this.aisStreamClient = new WebSocket(streamUrl);
      
      this.aisStreamClient.on('open', () => {
        this.reconnectAttempts = 0;
        this.lastConnectionStatus = 'connected';
        setAisHealth(true);
        
        logger.info('Connected to AIS stream', { url: streamUrl });
        
        const subscribeMessage = {
          APIKey: apiKey,
          BoundingBoxes: [
            [[-180, -90], [180, 90]]
          ],
          FilterMessageTypes: ['PositionReport']
        };
        
        this.aisStreamClient?.send(JSON.stringify(subscribeMessage));
      });
      
      this.aisStreamClient.on('message', (data) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleAISMessage(message);
        } catch (error) {
          logger.error('Failed to parse AIS message', { error: (error as Error).message });
        }
      });
      
      this.aisStreamClient.on('error', (error) => {
        logger.error('AIS stream error', { error: error.message });
        setAisHealth(false);
      });
      
      this.aisStreamClient.on('close', (code, reason) => {
        this.lastConnectionStatus = 'disconnected';
        setAisHealth(false);
        
        logger.warn('AIS stream connection closed', { 
          code, 
          reason: reason.toString(),
          willReconnect: this.reconnectAttempts < this.maxReconnectAttempts
        });
        
        this.scheduleReconnect(apiKey!);
      });
      
    } catch (error) {
      logger.error('Failed to connect to AIS stream', { error: (error as Error).message });
      setAisHealth(false);
      this.scheduleReconnect(apiKey);
    }
  }

  private scheduleReconnect(apiKey: string) {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      logger.error('Max reconnection attempts reached, giving up', {
        attempts: this.reconnectAttempts,
        max: this.maxReconnectAttempts
      });
      return;
    }
    
    const delay = Math.min(
      this.baseReconnectDelayMs * Math.pow(2, this.reconnectAttempts),
      this.maxReconnectDelayMs
    );
    
    const jitter = Math.random() * 1000;
    const totalDelay = delay + jitter;
    
    this.reconnectAttempts++;
    this.lastConnectionStatus = 'reconnecting';
    
    logger.info('Scheduling AIS stream reconnection', {
      attempt: this.reconnectAttempts,
      delayMs: Math.round(totalDelay)
    });
    
    this.reconnectTimeoutId = setTimeout(() => {
      this.connectToAISStream(apiKey);
    }, totalDelay);
  }

  private handleAISMessage(rawMessage: any) {
    metricsCollector.recordAisMessage();
    
    const messageHash = this.computeMessageHash(rawMessage);
    
    if (this.recentMessageHashes.has(messageHash)) {
      this.duplicatesFiltered++;
      return;
    }
    
    this.recentMessageHashes.add(messageHash);
    
    const aisMessage = this.parseAISMessage(rawMessage);
    if (!aisMessage) return;
    
    const queuedMessage: QueuedMessage = {
      hash: messageHash,
      message: aisMessage,
      receivedAt: Date.now()
    };
    
    if (this.messageQueue.length >= this.maxQueueSize) {
      const dropped = this.messageQueue.shift();
      if (dropped) {
        this.messagesDropped++;
        logger.warn('Queue full, dropping oldest message', {
          droppedHash: dropped.hash,
          queueSize: this.messageQueue.length,
          totalDropped: this.messagesDropped
        });
      }
    }
    
    this.messageQueue.push(queuedMessage);
  }

  private computeMessageHash(message: any): string {
    const relevantFields = {
      mmsi: message?.MetaData?.MMSI,
      timestamp: message?.MetaData?.time_utc,
      lat: message?.Message?.PositionReport?.Latitude,
      lon: message?.Message?.PositionReport?.Longitude
    };
    
    const content = JSON.stringify(relevantFields);
    return createHash('sha256').update(content).digest('hex').substring(0, 16);
  }

  private parseAISMessage(rawMessage: any): AISMessage | null {
    try {
      const meta = rawMessage?.MetaData;
      const pos = rawMessage?.Message?.PositionReport;
      
      if (!meta?.MMSI || !pos) return null;
      
      return {
        mmsi: String(meta.MMSI),
        timestamp: new Date(meta.time_utc || Date.now()),
        latitude: String(pos.Latitude || 0),
        longitude: String(pos.Longitude || 0),
        speed: String(pos.Sog || 0),
        course: String(pos.Cog || 0),
        heading: String(pos.TrueHeading || 0),
        status: this.mapNavigationalStatus(pos.NavigationalStatus),
        destination: meta.Destination,
        eta: meta.ETA ? new Date(meta.ETA) : undefined
      };
    } catch {
      return null;
    }
  }

  private mapNavigationalStatus(status: number): string {
    const statusMap: Record<number, string> = {
      0: 'underway',
      1: 'anchored',
      2: 'not_under_command',
      3: 'restricted_maneuverability',
      4: 'constrained_by_draught',
      5: 'moored',
      6: 'aground',
      7: 'fishing',
      8: 'underway_sailing'
    };
    return statusMap[status] || 'unknown';
  }

  private async processQueue() {
    if (this.processingQueue || this.messageQueue.length === 0) return;
    
    this.processingQueue = true;
    const batchSize = 50;
    const batch = this.messageQueue.splice(0, batchSize);
    
    try {
      for (const queued of batch) {
        await this.persistAISMessage(queued.message);
      }
      
      if (batch.length > 0) {
        this.broadcastUpdate('ais_update', { 
          timestamp: new Date().toISOString(),
          messageCount: batch.length
        });
      }
    } catch (error) {
      logger.error('Failed to process AIS message batch', { 
        error: (error as Error).message,
        batchSize: batch.length
      });
      
      this.messageQueue.unshift(...batch);
    } finally {
      this.processingQueue = false;
    }
  }

  private async persistAISMessage(message: AISMessage) {
    const vessel = await storage.getVesselByMMSI(message.mmsi);
    if (!vessel) return;
    
    const position: Omit<VesselPosition, 'id'> = {
      vesselId: vessel.id,
      timestamp: message.timestamp,
      latitude: message.latitude,
      longitude: message.longitude,
      speed: message.speed,
      course: message.course,
      heading: message.heading,
      status: message.status,
      destination: message.destination ?? null,
      eta: message.eta ?? null
    };
    
    await storage.createAisPosition(position);
  }

  private cleanupHashSet() {
    if (this.recentMessageHashes.size > this.maxHashSetSize) {
      const excess = this.recentMessageHashes.size - Math.floor(this.maxHashSetSize * 0.8);
      const iterator = this.recentMessageHashes.values();
      
      for (let i = 0; i < excess; i++) {
        const value = iterator.next().value;
        if (value) {
          this.recentMessageHashes.delete(value);
        }
      }
      
      logger.debug('Cleaned up message hash set', {
        removed: excess,
        remaining: this.recentMessageHashes.size
      });
    }
  }

  stopSimulation() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    
    if (this.hashCleanupIntervalId) {
      clearInterval(this.hashCleanupIntervalId);
      this.hashCleanupIntervalId = null;
    }
    
    if (this.queueProcessIntervalId) {
      clearInterval(this.queueProcessIntervalId);
      this.queueProcessIntervalId = null;
    }
    
    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }
    
    if (this.aisStreamClient) {
      this.aisStreamClient.close();
      this.aisStreamClient = null;
    }
    
    setAisHealth(false);
    this.lastConnectionStatus = 'disconnected';
    logger.info('AIS service stopped');
  }

  getStatus() {
    return {
      mode: this.mode,
      connectionStatus: this.lastConnectionStatus,
      reconnectAttempts: this.reconnectAttempts,
      queueSize: this.messageQueue.length,
      maxQueueSize: this.maxQueueSize,
      hashSetSize: this.recentMessageHashes.size,
      maxHashSetSize: this.maxHashSetSize,
      messagesDropped: this.messagesDropped,
      duplicatesFiltered: this.duplicatesFiltered,
      isHealthy: this.lastConnectionStatus === 'connected'
    };
  }

  private async generateAISUpdates() {
    const vessels = await storage.getVessels();
    const latestPositions = await storage.getLatestVesselPositions();
    
    const positionMap = new Map<string, VesselPosition>();
    for (const pos of latestPositions) {
      positionMap.set(pos.vesselId, pos);
    }
    
    for (const vessel of vessels) {
      const lastPosition = positionMap.get(vessel.id);
      const newPosition = this.simulateMovement(vessel, lastPosition);
      await storage.createAisPosition(newPosition);
      metricsCollector.recordAisMessage();
    }

    this.broadcastUpdate('ais_update', { timestamp: new Date().toISOString() });
  }

  private simulateMovement(vessel: any, lastPosition?: VesselPosition): Omit<VesselPosition, 'id'> {
    let lat = parseFloat(lastPosition?.latitude || '') || this.getDefaultLatitude(vessel.mmsi);
    let lon = parseFloat(lastPosition?.longitude || '') || this.getDefaultLongitude(vessel.mmsi);
    
    const movementRange = 0.001;
    lat += (Math.random() - 0.5) * movementRange;
    lon += (Math.random() - 0.5) * movementRange;
    
    const statuses = ['anchored', 'underway', 'moored', 'not_under_command'];
    const speeds = { 'anchored': 0.1, 'underway': 12.5, 'moored': 0.0, 'not_under_command': 6.2 };
    
    const status = statuses[Math.floor(Math.random() * statuses.length)];
    const speed = (speeds as any)[status] + (Math.random() - 0.5) * 2;
    const course = Math.floor(Math.random() * 360);
    const heading = Math.floor(Math.random() * 360);

    return {
      vesselId: vessel.id,
      timestamp: new Date(),
      latitude: lat.toFixed(7),
      longitude: lon.toFixed(7),
      speed: Math.max(0, speed).toFixed(2),
      course: course.toFixed(2),
      heading: heading.toFixed(2),
      status: status,
      destination: this.getRandomDestination(),
      eta: new Date(Date.now() + Math.random() * 24 * 60 * 60 * 1000)
    };
  }

  private getDefaultLatitude(mmsi: string): number {
    const hash = this.simpleHash(mmsi);
    return hash % 2 === 0 ? 
      25.10 + (hash % 100) / 1000 :
      51.90 + (hash % 100) / 1000;
  }

  private getDefaultLongitude(mmsi: string): number {
    const hash = this.simpleHash(mmsi);
    return hash % 2 === 0 ? 
      56.30 + (hash % 100) / 1000 :
      4.40 + (hash % 100) / 1000;
  }

  private simpleHash(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return Math.abs(hash);
  }

  private getRandomDestination(): string {
    const destinations = [
      'Fujairah Anchorage', 'Rotterdam Port', 'Singapore', 'Houston',
      'Ras Tanura', 'Jebel Ali', 'Antwerp', 'Hamburg'
    ];
    return destinations[Math.floor(Math.random() * destinations.length)];
  }

  private broadcastUpdate(type: string, data: any) {
    if (!this.wss) return;
    
    const message = JSON.stringify({ type, data });
    this.wss.clients.forEach((client) => {
      if (client.readyState === client.OPEN) {
        client.send(message);
      }
    });
  }
}

export const aisService = new AISService();
