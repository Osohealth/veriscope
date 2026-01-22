import { Request, Response, NextFunction } from 'express';
import { logger } from './observability';

interface RateLimitEntry {
  count: number;
  resetTime: number;
  blocked: boolean;
  blockUntil?: number;
}

interface RateLimitConfig {
  windowMs: number;
  maxRequests: number;
  blockDurationMs?: number;
  maxBlockedAttempts?: number;
}

class RateLimiter {
  private store: Map<string, RateLimitEntry> = new Map();
  private blockedIPs: Map<string, number> = new Map();
  
  private cleanupInterval: NodeJS.Timeout;

  constructor() {
    this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
  }

  private cleanup() {
    const now = Date.now();
    Array.from(this.store.entries()).forEach(([key, entry]) => {
      if (entry.resetTime < now && (!entry.blockUntil || entry.blockUntil < now)) {
        this.store.delete(key);
      }
    });
    Array.from(this.blockedIPs.entries()).forEach(([ip, blockUntil]) => {
      if (blockUntil < now) {
        this.blockedIPs.delete(ip);
      }
    });
  }

  private getKey(req: Request, keyPrefix: string): string {
    const ip = req.ip || req.socket.remoteAddress || 'unknown';
    return `${keyPrefix}:${ip}`;
  }

  check(req: Request, config: RateLimitConfig, keyPrefix: string = 'default'): { allowed: boolean; remaining: number; resetIn: number } {
    const key = this.getKey(req, keyPrefix);
    const now = Date.now();
    
    const blockUntil = this.blockedIPs.get(req.ip || 'unknown');
    if (blockUntil && blockUntil > now) {
      return {
        allowed: false,
        remaining: 0,
        resetIn: Math.ceil((blockUntil - now) / 1000)
      };
    }
    
    let entry = this.store.get(key);
    
    if (!entry || entry.resetTime < now) {
      entry = {
        count: 0,
        resetTime: now + config.windowMs,
        blocked: false
      };
      this.store.set(key, entry);
    }
    
    entry.count++;
    
    const remaining = Math.max(0, config.maxRequests - entry.count);
    const resetIn = Math.ceil((entry.resetTime - now) / 1000);
    
    if (entry.count > config.maxRequests) {
      if (config.blockDurationMs && config.maxBlockedAttempts) {
        if (entry.count > config.maxRequests + config.maxBlockedAttempts) {
          this.blockedIPs.set(req.ip || 'unknown', now + config.blockDurationMs);
          logger.warn('IP blocked for excessive requests', {
            ip: req.ip,
            keyPrefix,
            blockDurationMs: config.blockDurationMs
          });
        }
      }
      
      return { allowed: false, remaining: 0, resetIn };
    }
    
    return { allowed: true, remaining, resetIn };
  }
}

const rateLimiter = new RateLimiter();

const authLimitConfig: RateLimitConfig = {
  windowMs: 15 * 60 * 1000,
  maxRequests: 5,
  blockDurationMs: 30 * 60 * 1000,
  maxBlockedAttempts: 10
};

const apiLimitConfig: RateLimitConfig = {
  windowMs: 60 * 1000,
  maxRequests: 100
};

const criticalLimitConfig: RateLimitConfig = {
  windowMs: 60 * 1000,
  maxRequests: 30
};

export function authRateLimiter(req: Request, res: Response, next: NextFunction) {
  const result = rateLimiter.check(req, authLimitConfig, 'auth');
  
  res.setHeader('X-RateLimit-Remaining', result.remaining);
  res.setHeader('X-RateLimit-Reset', result.resetIn);
  
  if (!result.allowed) {
    logger.warn('Auth rate limit exceeded', { ip: req.ip, path: req.path });
    return res.status(429).json({
      error: 'Too many authentication attempts. Please try again later.',
      retryAfter: result.resetIn
    });
  }
  
  next();
}

export function apiRateLimiter(req: Request, res: Response, next: NextFunction) {
  const result = rateLimiter.check(req, apiLimitConfig, 'api');
  
  res.setHeader('X-RateLimit-Remaining', result.remaining);
  res.setHeader('X-RateLimit-Reset', result.resetIn);
  
  if (!result.allowed) {
    return res.status(429).json({
      error: 'Rate limit exceeded. Please slow down.',
      retryAfter: result.resetIn
    });
  }
  
  next();
}

export function criticalRateLimiter(req: Request, res: Response, next: NextFunction) {
  const result = rateLimiter.check(req, criticalLimitConfig, 'critical');
  
  res.setHeader('X-RateLimit-Remaining', result.remaining);
  res.setHeader('X-RateLimit-Reset', result.resetIn);
  
  if (!result.allowed) {
    return res.status(429).json({
      error: 'Rate limit exceeded for this endpoint.',
      retryAfter: result.resetIn
    });
  }
  
  next();
}

export function resetAuthRateLimit(req: Request) {
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const key = `auth:${ip}`;
}
