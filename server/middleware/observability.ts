import { Request, Response, NextFunction } from 'express';
import { randomUUID } from 'crypto';

declare global {
  namespace Express {
    interface Request {
      requestId: string;
      startTime: number;
    }
  }
}

export interface StructuredLog {
  timestamp: string;
  level: 'info' | 'warn' | 'error' | 'debug';
  requestId?: string;
  userId?: string;
  route?: string;
  method?: string;
  statusCode?: number;
  latencyMs?: number;
  message: string;
  metadata?: Record<string, any>;
}

class Logger {
  private formatLog(log: StructuredLog): string {
    return JSON.stringify(log);
  }

  info(message: string, metadata?: Record<string, any>, req?: Request) {
    const log: StructuredLog = {
      timestamp: new Date().toISOString(),
      level: 'info',
      requestId: req?.requestId,
      message,
      metadata
    };
    console.log(this.formatLog(log));
  }

  warn(message: string, metadata?: Record<string, any>, req?: Request) {
    const log: StructuredLog = {
      timestamp: new Date().toISOString(),
      level: 'warn',
      requestId: req?.requestId,
      message,
      metadata
    };
    console.warn(this.formatLog(log));
  }

  error(message: string, metadata?: Record<string, any>, req?: Request) {
    const log: StructuredLog = {
      timestamp: new Date().toISOString(),
      level: 'error',
      requestId: req?.requestId,
      message,
      metadata
    };
    console.error(this.formatLog(log));
  }

  debug(message: string, metadata?: Record<string, any>, req?: Request) {
    if (process.env.NODE_ENV === 'development') {
      const log: StructuredLog = {
        timestamp: new Date().toISOString(),
        level: 'debug',
        requestId: req?.requestId,
        message,
        metadata
      };
      console.log(this.formatLog(log));
    }
  }
}

export const logger = new Logger();

export interface Metrics {
  requestCount: number;
  errorCount: number;
  wsConnections: number;
  aisMessagesReceived: number;
  aisMessagesPerSecond: number;
  jobsCompleted: number;
  avgLatencyMs: number;
  lastUpdated: string;
}

class MetricsCollector {
  private metrics: Metrics = {
    requestCount: 0,
    errorCount: 0,
    wsConnections: 0,
    aisMessagesReceived: 0,
    aisMessagesPerSecond: 0,
    jobsCompleted: 0,
    avgLatencyMs: 0,
    lastUpdated: new Date().toISOString()
  };
  
  private latencies: number[] = [];
  private aisMessageTimestamps: number[] = [];

  incrementRequests() {
    this.metrics.requestCount++;
    this.metrics.lastUpdated = new Date().toISOString();
  }

  incrementErrors() {
    this.metrics.errorCount++;
    this.metrics.lastUpdated = new Date().toISOString();
  }

  setWsConnections(count: number) {
    this.metrics.wsConnections = count;
    this.metrics.lastUpdated = new Date().toISOString();
  }

  recordAisMessage() {
    this.metrics.aisMessagesReceived++;
    const now = Date.now();
    this.aisMessageTimestamps.push(now);
    const oneSecondAgo = now - 1000;
    this.aisMessageTimestamps = this.aisMessageTimestamps.filter(t => t > oneSecondAgo);
    this.metrics.aisMessagesPerSecond = this.aisMessageTimestamps.length;
    this.metrics.lastUpdated = new Date().toISOString();
  }

  recordLatency(ms: number) {
    this.latencies.push(ms);
    if (this.latencies.length > 1000) {
      this.latencies = this.latencies.slice(-1000);
    }
    this.metrics.avgLatencyMs = Math.round(
      this.latencies.reduce((a, b) => a + b, 0) / this.latencies.length
    );
    this.metrics.lastUpdated = new Date().toISOString();
  }

  incrementJobsCompleted() {
    this.metrics.jobsCompleted++;
    this.metrics.lastUpdated = new Date().toISOString();
  }

  getMetrics(): Metrics {
    return { ...this.metrics };
  }
}

export const metricsCollector = new MetricsCollector();

export interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  uptime: number;
  timestamp: string;
  checks: {
    database: boolean;
    websocket: boolean;
    aisStream: boolean;
  };
  version: string;
}

const startTime = Date.now();
let dbHealthy = true;
let wsHealthy = true;
let aisHealthy = true;

export function setDbHealth(healthy: boolean) {
  dbHealthy = healthy;
}

export function setWsHealth(healthy: boolean) {
  wsHealthy = healthy;
}

export function setAisHealth(healthy: boolean) {
  aisHealthy = healthy;
}

export function getHealthStatus(): HealthStatus {
  const allHealthy = dbHealthy && wsHealthy && aisHealthy;
  const anyHealthy = dbHealthy || wsHealthy || aisHealthy;
  
  return {
    status: allHealthy ? 'healthy' : anyHealthy ? 'degraded' : 'unhealthy',
    uptime: Math.floor((Date.now() - startTime) / 1000),
    timestamp: new Date().toISOString(),
    checks: {
      database: dbHealthy,
      websocket: wsHealthy,
      aisStream: aisHealthy
    },
    version: '1.0.0'
  };
}

export function requestTrackingMiddleware(req: Request, res: Response, next: NextFunction) {
  req.requestId = req.headers['x-request-id'] as string || randomUUID();
  req.startTime = Date.now();
  
  res.setHeader('X-Request-ID', req.requestId);
  
  res.on('finish', () => {
    const latency = Date.now() - req.startTime;
    metricsCollector.incrementRequests();
    metricsCollector.recordLatency(latency);
    
    if (res.statusCode >= 400) {
      metricsCollector.incrementErrors();
    }
    
    const log: StructuredLog = {
      timestamp: new Date().toISOString(),
      level: res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info',
      requestId: req.requestId,
      route: req.path,
      method: req.method,
      statusCode: res.statusCode,
      latencyMs: latency,
      message: `${req.method} ${req.path} ${res.statusCode} ${latency}ms`
    };
    
    // Log all requests with appropriate level
    if (res.statusCode >= 500) {
      console.error(JSON.stringify(log));
    } else if (res.statusCode >= 400) {
      console.warn(JSON.stringify(log));
    } else {
      console.log(JSON.stringify(log));
    }
  });
  
  next();
}

export function errorTrackingMiddleware(err: Error, req: Request, res: Response, next: NextFunction) {
  logger.error('Unhandled error', {
    error: err.message,
    stack: err.stack,
    route: req.path,
    method: req.method
  }, req);
  
  metricsCollector.incrementErrors();
  
  res.status(500).json({
    error: 'Internal server error',
    requestId: req.requestId
  });
}
