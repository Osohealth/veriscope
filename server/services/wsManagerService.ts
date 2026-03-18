import { WebSocket, WebSocketServer } from 'ws';
import { IncomingMessage } from 'http';
import { metricsCollector, setWsHealth, logger } from '../middleware/observability';

const MESSAGE_SCHEMA_VERSION = '1.0.0';

interface ClientInfo {
  id: string;
  ws: WebSocket;
  subscriptions: Set<string>;
  lastMessageTime: number;
  messageCount: number;
  isThrottled: boolean;
}

interface WsMessage {
  version: string;
  type: string;
  topic?: string;
  payload: any;
  timestamp: string;
  messageId: string;
}

interface ThrottleConfig {
  maxMessagesPerSecond: number;
  throttleDurationMs: number;
}

const DEFAULT_THROTTLE: ThrottleConfig = {
  maxMessagesPerSecond: 10,
  throttleDurationMs: 5000
};

class WebSocketManager {
  private clients: Map<string, ClientInfo> = new Map();
  private topics: Map<string, Set<string>> = new Map();
  private wss: WebSocketServer | null = null;
  private messageCounter = 0;
  private throttleConfig: ThrottleConfig = DEFAULT_THROTTLE;

  initialize(wss: WebSocketServer): void {
    this.wss = wss;
    
    wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      const clientId = this.generateClientId();
      this.handleConnection(clientId, ws);
      
      ws.on('message', (data) => {
        this.handleMessage(clientId, data.toString());
      });
      
      ws.on('close', () => {
        this.handleDisconnect(clientId);
      });
      
      ws.on('error', (error) => {
        logger.error('WebSocket client error', { clientId, error: error.message });
      });
    });
    
    setInterval(() => this.cleanupThrottles(), 1000);
    
    logger.info('WebSocket manager initialized');
  }

  private generateClientId(): string {
    return `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private generateMessageId(): string {
    return `msg_${++this.messageCounter}_${Date.now()}`;
  }

  private handleConnection(clientId: string, ws: WebSocket): void {
    const client: ClientInfo = {
      id: clientId,
      ws,
      subscriptions: new Set(),
      lastMessageTime: 0,
      messageCount: 0,
      isThrottled: false
    };
    
    this.clients.set(clientId, client);
    metricsCollector.setWsConnections(this.clients.size);
    setWsHealth(true);
    
    this.sendToClient(clientId, {
      type: 'connected',
      payload: { 
        clientId,
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        availableTopics: ['ais', 'alerts', 'delays', 'signals', 'prices']
      }
    });
    
    logger.info('WebSocket client connected', { clientId });
  }

  private handleDisconnect(clientId: string): void {
    const client = this.clients.get(clientId);
    if (client) {
      Array.from(client.subscriptions).forEach(topic => {
        const topicSet = this.topics.get(topic);
        if (topicSet) {
          topicSet.delete(clientId);
          // Remove empty topic sets to prevent stale data
          if (topicSet.size === 0) {
            this.topics.delete(topic);
          }
        }
      });
      this.clients.delete(clientId);
    }
    
    metricsCollector.setWsConnections(this.clients.size);
    logger.info('WebSocket client disconnected', { clientId });
  }

  private handleMessage(clientId: string, rawMessage: string): void {
    const client = this.clients.get(clientId);
    if (!client) return;
    
    if (this.isThrottled(client)) {
      this.sendToClient(clientId, {
        type: 'error',
        payload: { 
          code: 'RATE_LIMITED',
          message: 'Too many messages. Please slow down.',
          retryAfterMs: this.throttleConfig.throttleDurationMs
        }
      });
      return;
    }
    
    this.updateClientActivity(client);
    
    try {
      const message = JSON.parse(rawMessage);
      
      if (message.version && message.version !== MESSAGE_SCHEMA_VERSION) {
        this.sendToClient(clientId, {
          type: 'error',
          payload: {
            code: 'VERSION_MISMATCH',
            message: `Expected version ${MESSAGE_SCHEMA_VERSION}, got ${message.version}`,
            currentVersion: MESSAGE_SCHEMA_VERSION
          }
        });
        return;
      }
      
      switch (message.type) {
        case 'subscribe':
          this.handleSubscribe(clientId, message.topics || [message.topic]);
          break;
        case 'unsubscribe':
          this.handleUnsubscribe(clientId, message.topics || [message.topic]);
          break;
        case 'ping':
          this.sendToClient(clientId, { type: 'pong', payload: { timestamp: Date.now() } });
          break;
        default:
          this.sendToClient(clientId, {
            type: 'error',
            payload: { code: 'UNKNOWN_MESSAGE_TYPE', message: `Unknown type: ${message.type}` }
          });
      }
    } catch (error) {
      this.sendToClient(clientId, {
        type: 'error',
        payload: { code: 'PARSE_ERROR', message: 'Invalid JSON message' }
      });
    }
  }

  private handleSubscribe(clientId: string, topics: string[]): void {
    const client = this.clients.get(clientId);
    if (!client) return;
    
    const validTopics = ['ais', 'alerts', 'delays', 'signals', 'prices'];
    const subscribedTopics: string[] = [];
    
    for (const topic of topics) {
      if (!validTopics.includes(topic)) continue;
      
      client.subscriptions.add(topic);
      
      if (!this.topics.has(topic)) {
        this.topics.set(topic, new Set());
      }
      this.topics.get(topic)!.add(clientId);
      subscribedTopics.push(topic);
    }
    
    this.sendToClient(clientId, {
      type: 'subscribed',
      payload: { topics: subscribedTopics }
    });
  }

  private handleUnsubscribe(clientId: string, topics: string[]): void {
    const client = this.clients.get(clientId);
    if (!client) return;
    
    for (const topic of topics) {
      client.subscriptions.delete(topic);
      this.topics.get(topic)?.delete(clientId);
    }
    
    this.sendToClient(clientId, {
      type: 'unsubscribed',
      payload: { topics }
    });
  }

  private isThrottled(client: ClientInfo): boolean {
    const now = Date.now();
    const timeSinceLastMessage = now - client.lastMessageTime;
    
    // Reset throttle if enough time has passed
    if (client.isThrottled && timeSinceLastMessage > this.throttleConfig.throttleDurationMs) {
      client.isThrottled = false;
      client.messageCount = 0;
    }
    
    // Reset counter for new time window
    if (timeSinceLastMessage >= 1000) {
      client.messageCount = 1;
      client.lastMessageTime = now;
      return false;
    }
    
    client.messageCount++;
    
    if (client.messageCount > this.throttleConfig.maxMessagesPerSecond) {
      client.isThrottled = true;
      return true;
    }
    
    return false;
  }

  private updateClientActivity(client: ClientInfo): void {
    client.lastMessageTime = Date.now();
  }

  private cleanupThrottles(): void {
    const now = Date.now();
    Array.from(this.clients.values()).forEach(client => {
      if (client.isThrottled && now - client.lastMessageTime > this.throttleConfig.throttleDurationMs) {
        client.isThrottled = false;
        client.messageCount = 0;
      }
    });
  }

  private sendToClient(clientId: string, message: Omit<WsMessage, 'version' | 'timestamp' | 'messageId'>): void {
    const client = this.clients.get(clientId);
    if (!client || client.ws.readyState !== WebSocket.OPEN) return;
    
    const fullMessage: WsMessage = {
      version: MESSAGE_SCHEMA_VERSION,
      ...message,
      timestamp: new Date().toISOString(),
      messageId: this.generateMessageId()
    };
    
    client.ws.send(JSON.stringify(fullMessage));
  }

  broadcast(topic: string, type: string, payload: any): void {
    const subscribers = this.topics.get(topic);
    if (!subscribers) return;
    
    const message: WsMessage = {
      version: MESSAGE_SCHEMA_VERSION,
      type,
      topic,
      payload,
      timestamp: new Date().toISOString(),
      messageId: this.generateMessageId()
    };
    
    const messageStr = JSON.stringify(message);
    
    Array.from(subscribers).forEach(clientId => {
      const client = this.clients.get(clientId);
      if (client && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(messageStr);
      }
    });
  }

  broadcastAll(type: string, payload: any): void {
    const message: WsMessage = {
      version: MESSAGE_SCHEMA_VERSION,
      type,
      payload,
      timestamp: new Date().toISOString(),
      messageId: this.generateMessageId()
    };
    
    const messageStr = JSON.stringify(message);
    
    Array.from(this.clients.values()).forEach(client => {
      if (client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(messageStr);
      }
    });
  }

  getStats(): { clientCount: number; topicStats: Record<string, number> } {
    const topicStats: Record<string, number> = {};
    Array.from(this.topics.entries()).forEach(([topic, subscribers]) => {
      topicStats[topic] = subscribers.size;
    });
    
    return {
      clientCount: this.clients.size,
      topicStats
    };
  }
}

export const wsManager = new WebSocketManager();
