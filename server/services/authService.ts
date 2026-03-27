import { randomUUID } from "node:crypto";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { db } from "../db";
import { logger } from "../middleware/observability";
import { users, organizations, failedLoginAttempts, revokedRefreshTokens } from "@shared/schema";
import { and, eq, gte, lt } from "drizzle-orm";

const JWT_SECRET = (() => {
  const secret = process.env.JWT_SECRET;
  if (!secret) {
    throw new Error("JWT_SECRET environment variable is required but not set. Refusing to start with an insecure default.");
  }
  return secret;
})();
const ACCESS_TOKEN_EXPIRY = "15m";
const REFRESH_TOKEN_EXPIRY = "7d";

interface TokenPayload {
  userId: string;
  email: string;
  role: string;
  type: "access" | "refresh";
  jti?: string;
  iat?: number;
  exp?: number;
}

export interface UserResponse {
  id: string;
  email: string;
  fullName: string | null;
  role: string;
  organizationId: string | null;
}

export interface AuthTokens {
  accessToken: string;
  refreshToken: string;
  tokenType: string;
  user: UserResponse;
}

async function hashPassword(password: string): Promise<string> {
  const saltRounds = 10;
  return bcrypt.hash(password, saltRounds);
}

async function verifyPassword(password: string, hash: string): Promise<boolean> {
  return bcrypt.compare(password, hash);
}

function generateAccessToken(payload: Omit<TokenPayload, "type" | "iat" | "exp">): string {
  return jwt.sign({ ...payload, type: "access" }, JWT_SECRET, { expiresIn: ACCESS_TOKEN_EXPIRY });
}

function generateRefreshToken(payload: Omit<TokenPayload, "type" | "iat" | "exp">): string {
  const jti = randomUUID();
  return jwt.sign({ ...payload, type: "refresh", jti }, JWT_SECRET, { expiresIn: REFRESH_TOKEN_EXPIRY });
}

export function verifyToken(token: string): TokenPayload | null {
  try {
    const decoded = jwt.verify(token, JWT_SECRET) as TokenPayload;
    return decoded;
  } catch {
    return null;
  }
}

class AuthService {
  async register(
    email: string,
    password: string,
    fullName: string,
    organizationName?: string
  ): Promise<{ success: boolean; data?: AuthTokens; error?: string }> {
    try {
      const existingUser = await db.select().from(users).where(eq(users.email, email)).limit(1);

      if (existingUser.length > 0) {
        logger.warn("Registration: duplicate email attempt", { email });
        return { success: false, error: "Invalid email or password" };
      }

      const passwordHash = await hashPassword(password);

      let organizationId: string | null = null;

      if (organizationName) {
        const [org] = await db.insert(organizations).values({
          name: organizationName,
        }).returning();
        organizationId = org.id;
      }

      const [user] = await db.insert(users).values({
        email,
        passwordHash,
        fullName,
        name: fullName,
        organizationId,
        role: "analyst",
        isActive: true,
      }).returning();

      const tokenPayload = {
        userId: user.id,
        email: user.email,
        role: user.role,
      };

      return {
        success: true,
        data: {
          accessToken: generateAccessToken(tokenPayload),
          refreshToken: generateRefreshToken(tokenPayload),
          tokenType: "bearer",
          user: {
            id: user.id,
            email: user.email,
            fullName: user.fullName,
            role: user.role,
            organizationId: user.organizationId,
          },
        },
      };
    } catch (error: any) {
      logger.error("Registration error", { error });
      return { success: false, error: error.message || "Registration failed" };
    }
  }

  async login(email: string, password: string, ipAddress?: string): Promise<{ success: boolean; data?: AuthTokens; error?: string }> {
    try {
      const [user] = await db.select().from(users).where(eq(users.email, email)).limit(1);

      if (!user) {
        return { success: false, error: "Invalid email or password" };
      }

      if (!user.isActive) {
        return { success: false, error: "Account is deactivated" };
      }

      // Per-account brute-force lockout: max 5 failures within 15 minutes
      const windowStart = new Date(Date.now() - 15 * 60 * 1000);
      const recentFailures = await db
        .select({ id: failedLoginAttempts.id })
        .from(failedLoginAttempts)
        .where(and(eq(failedLoginAttempts.userId, user.id), gte(failedLoginAttempts.attemptedAt, windowStart)))
        .limit(6);

      if (recentFailures.length >= 5) {
        return { success: false, error: "Account temporarily locked due to too many failed attempts. Try again in 15 minutes." };
      }

      const isValidPassword = await verifyPassword(password, user.passwordHash);

      if (!isValidPassword) {
        await db.insert(failedLoginAttempts).values({
          userId: user.id,
          emailAttempted: email,
          ipAddress: ipAddress ?? null,
        });
        return { success: false, error: "Invalid email or password" };
      }

      // Clear failed attempts on successful login
      await db.delete(failedLoginAttempts).where(eq(failedLoginAttempts.userId, user.id));

      await db.update(users)
        .set({ lastLogin: new Date() })
        .where(eq(users.id, user.id));

      const tokenPayload = {
        userId: user.id,
        email: user.email,
        role: user.role,
      };

      return {
        success: true,
        data: {
          accessToken: generateAccessToken(tokenPayload),
          refreshToken: generateRefreshToken(tokenPayload),
          tokenType: "bearer",
          user: {
            id: user.id,
            email: user.email,
            fullName: user.fullName,
            role: user.role,
            organizationId: user.organizationId,
          },
        },
      };
    } catch (error: any) {
      logger.error("Login error", { error });
      return { success: false, error: error.message || "Login failed" };
    }
  }

  async refreshTokens(refreshToken: string): Promise<{ success: boolean; data?: AuthTokens; error?: string }> {
    try {
      const payload = verifyToken(refreshToken);

      if (!payload || payload.type !== "refresh") {
        return { success: false, error: "Invalid refresh token" };
      }

      // JTI-based replay detection: reject if this token has already been used
      if (payload.jti) {
        const [revoked] = await db
          .select({ id: revokedRefreshTokens.id })
          .from(revokedRefreshTokens)
          .where(eq(revokedRefreshTokens.jti, payload.jti))
          .limit(1);

        if (revoked) {
          logger.warn("Refresh token replay attack detected", { userId: payload.userId, jti: payload.jti });
          return { success: false, error: "Invalid refresh token" };
        }

        // Mark this JTI as used before issuing new tokens
        await db.insert(revokedRefreshTokens).values({
          jti: payload.jti,
          userId: payload.userId,
          expiresAt: new Date((payload.exp ?? 0) * 1000),
        });
      }

      const [user] = await db.select().from(users).where(eq(users.id, payload.userId)).limit(1);

      if (!user || !user.isActive) {
        return { success: false, error: "User not found or deactivated" };
      }

      const tokenPayload = {
        userId: user.id,
        email: user.email,
        role: user.role,
      };

      return {
        success: true,
        data: {
          accessToken: generateAccessToken(tokenPayload),
          refreshToken: generateRefreshToken(tokenPayload),
          tokenType: "bearer",
          user: {
            id: user.id,
            email: user.email,
            fullName: user.fullName,
            role: user.role,
            organizationId: user.organizationId,
          },
        },
      };
    } catch (error: any) {
      logger.error("Token refresh error", { error });
      return { success: false, error: error.message || "Token refresh failed" };
    }
  }

  async getUser(userId: string) {
    const [user] = await db.select().from(users).where(eq(users.id, userId)).limit(1);
    if (!user) return null;

    const { passwordHash: _, ...safeUser } = user;
    return safeUser;
  }
}

export const authService = new AuthService();
