import { createHash, createHmac, randomBytes } from 'crypto';
import { storage } from '../storage';
import { type User } from '@shared/schema';

interface TokenPayload {
  userId: string;
  email: string;
  role: string;
  iat: number;
  exp: number;
  type: 'access' | 'refresh';
}

interface SessionResult {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  user: Omit<User, 'passwordHash'>;
}

class SessionService {
  private readonly SECRET = process.env.JWT_SECRET || randomBytes(32).toString('hex');
  private readonly ACCESS_TOKEN_EXPIRY = 15 * 60; // 15 minutes
  private readonly REFRESH_TOKEN_EXPIRY = 7 * 24 * 60 * 60; // 7 days
  
  private refreshTokens: Map<string, { userId: string; expiresAt: number }> = new Map();

  private base64UrlEncode(str: string): string {
    return Buffer.from(str).toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');
  }

  private base64UrlDecode(str: string): string {
    str = str.replace(/-/g, '+').replace(/_/g, '/');
    while (str.length % 4) str += '=';
    return Buffer.from(str, 'base64').toString();
  }

  private createToken(payload: TokenPayload): string {
    const header = { alg: 'HS256', typ: 'JWT' };
    const encodedHeader = this.base64UrlEncode(JSON.stringify(header));
    const encodedPayload = this.base64UrlEncode(JSON.stringify(payload));
    const signature = createHmac('sha256', this.SECRET)
      .update(`${encodedHeader}.${encodedPayload}`)
      .digest('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');
    
    return `${encodedHeader}.${encodedPayload}.${signature}`;
  }

  verifyToken(token: string): TokenPayload | null {
    try {
      const parts = token.split('.');
      if (parts.length !== 3) return null;

      const [encodedHeader, encodedPayload, signature] = parts;
      
      const expectedSignature = createHmac('sha256', this.SECRET)
        .update(`${encodedHeader}.${encodedPayload}`)
        .digest('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=/g, '');

      if (signature !== expectedSignature) return null;

      const payload = JSON.parse(this.base64UrlDecode(encodedPayload)) as TokenPayload;
      
      if (payload.exp < Math.floor(Date.now() / 1000)) {
        return null;
      }

      return payload;
    } catch {
      return null;
    }
  }

  async createSession(user: User): Promise<SessionResult> {
    const now = Math.floor(Date.now() / 1000);
    
    const accessPayload: TokenPayload = {
      userId: user.id,
      email: user.email,
      role: user.role,
      iat: now,
      exp: now + this.ACCESS_TOKEN_EXPIRY,
      type: 'access'
    };

    const refreshPayload: TokenPayload = {
      userId: user.id,
      email: user.email,
      role: user.role,
      iat: now,
      exp: now + this.REFRESH_TOKEN_EXPIRY,
      type: 'refresh'
    };

    const accessToken = this.createToken(accessPayload);
    const refreshToken = this.createToken(refreshPayload);
    
    const refreshTokenHash = createHash('sha256').update(refreshToken).digest('hex');
    this.refreshTokens.set(refreshTokenHash, {
      userId: user.id,
      expiresAt: (now + this.REFRESH_TOKEN_EXPIRY) * 1000
    });

    const { passwordHash: _, ...safeUser } = user;

    return {
      accessToken,
      refreshToken,
      expiresIn: this.ACCESS_TOKEN_EXPIRY,
      user: safeUser as Omit<User, 'passwordHash'>
    };
  }

  async refreshSession(refreshToken: string): Promise<SessionResult | null> {
    const payload = this.verifyToken(refreshToken);
    if (!payload || payload.type !== 'refresh') {
      return null;
    }

    const refreshTokenHash = createHash('sha256').update(refreshToken).digest('hex');
    const storedToken = this.refreshTokens.get(refreshTokenHash);
    
    if (!storedToken || storedToken.expiresAt < Date.now()) {
      return null;
    }

    const user = await storage.getUserById(payload.userId);
    if (!user || !user.isActive) {
      return null;
    }

    this.refreshTokens.delete(refreshTokenHash);
    
    return this.createSession(user);
  }

  revokeRefreshToken(refreshToken: string): boolean {
    const refreshTokenHash = createHash('sha256').update(refreshToken).digest('hex');
    return this.refreshTokens.delete(refreshTokenHash);
  }

  revokeAllUserTokens(userId: string): number {
    let count = 0;
    Array.from(this.refreshTokens.entries()).forEach(([hash, data]) => {
      if (data.userId === userId) {
        this.refreshTokens.delete(hash);
        count++;
      }
    });
    return count;
  }

  cleanupExpiredTokens(): number {
    const now = Date.now();
    let count = 0;
    Array.from(this.refreshTokens.entries()).forEach(([hash, data]) => {
      if (data.expiresAt < now) {
        this.refreshTokens.delete(hash);
        count++;
      }
    });
    return count;
  }
}

export const sessionService = new SessionService();

setInterval(() => {
  const cleaned = sessionService.cleanupExpiredTokens();
  if (cleaned > 0) {
    console.log(`Cleaned up ${cleaned} expired refresh tokens`);
  }
}, 60 * 60 * 1000);
