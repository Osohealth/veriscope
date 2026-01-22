import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { db } from "../db";
import { users, organizations } from "@shared/schema";
import { eq } from "drizzle-orm";

const JWT_SECRET = process.env.JWT_SECRET || "veriscope-secret-key-change-in-production";
const ACCESS_TOKEN_EXPIRY = "15m";
const REFRESH_TOKEN_EXPIRY = "7d";

interface TokenPayload {
  userId: string;
  email: string;
  role: string;
  type: "access" | "refresh";
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
  if (hash.includes(':')) {
    return false;
  }
  return bcrypt.compare(password, hash);
}

function generateAccessToken(payload: Omit<TokenPayload, "type" | "iat" | "exp">): string {
  return jwt.sign({ ...payload, type: "access" }, JWT_SECRET, { expiresIn: ACCESS_TOKEN_EXPIRY });
}

function generateRefreshToken(payload: Omit<TokenPayload, "type" | "iat" | "exp">): string {
  return jwt.sign({ ...payload, type: "refresh" }, JWT_SECRET, { expiresIn: REFRESH_TOKEN_EXPIRY });
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
        return { success: false, error: "User with this email already exists" };
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
      console.error("Registration error:", error);
      return { success: false, error: error.message || "Registration failed" };
    }
  }

  async login(email: string, password: string): Promise<{ success: boolean; data?: AuthTokens; error?: string }> {
    try {
      const [user] = await db.select().from(users).where(eq(users.email, email)).limit(1);

      if (!user) {
        return { success: false, error: "Invalid email or password" };
      }

      if (!user.isActive) {
        return { success: false, error: "Account is deactivated" };
      }

      const isValidPassword = await verifyPassword(password, user.passwordHash);
      
      if (!isValidPassword) {
        return { success: false, error: "Invalid email or password" };
      }

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
      console.error("Login error:", error);
      return { success: false, error: error.message || "Login failed" };
    }
  }

  async refreshTokens(refreshToken: string): Promise<{ success: boolean; data?: AuthTokens; error?: string }> {
    try {
      const payload = verifyToken(refreshToken);
      
      if (!payload || payload.type !== "refresh") {
        return { success: false, error: "Invalid refresh token" };
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
      console.error("Token refresh error:", error);
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
