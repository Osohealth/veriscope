import { Router } from "express";
import { z } from "zod";
import { authService } from "../services/authService";
import { sessionService } from "../services/sessionService";
import { auditService } from "../services/auditService";
import { authRateLimiter } from "../middleware/rateLimiter";
import { authenticate } from "../middleware/rbac";
import { logger } from "../middleware/observability";

export const authRouter = Router();

// ===== /api/auth/* =====

authRouter.post("/api/auth/register", authRateLimiter, async (req, res) => {
    try {
        const schema = z
            .object({
                email: z.string().email("Invalid email address"),
                password: z.string().min(8, "Password must be at least 8 characters"),
                name: z.string().optional(),
                full_name: z.string().optional(),
                organization_name: z.string().optional(),
            })
            .refine((d) => d.full_name || d.name, { message: "Name is required" });

        const parsed = schema.safeParse(req.body);
        if (!parsed.success) {
            return res.status(400).json({ error: parsed.error.errors[0].message });
        }
        const { email, password, name, full_name, organization_name } = parsed.data;
        const fullName = full_name || name!;

        const result = await authService.register(email, password, fullName, organization_name);
        if (!result.success) {
            return res.status(400).json({ error: result.error });
        }

        res.cookie("access_token", result.data!.accessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 15 * 60 * 1000,
        });
        res.cookie("refresh_token", result.data!.refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 7 * 24 * 60 * 60 * 1000,
            path: "/api/auth/refresh",
        });
        res.status(201).json({ message: "Registration successful", user: result.data!.user });
    } catch (error: any) {
        logger.error("Registration error", { error });
        res.status(500).json({ error: "Registration failed" });
    }
});

authRouter.post("/api/auth/login", authRateLimiter, async (req, res) => {
    try {
        const schema = z.object({
            email: z.string().email("Invalid email address"),
            password: z.string().min(1, "Password is required"),
        });
        const parsed = schema.safeParse(req.body);
        if (!parsed.success) {
            return res.status(400).json({ error: parsed.error.errors[0].message });
        }
        const { email, password } = parsed.data;

        const result = await authService.login(email, password);
        if (!result.success || !result.data) {
            await auditService.logLogin("", false, req, result.error);
            return res.status(401).json({ error: result.error });
        }

        await auditService.logLogin(result.data.user.id, true, req);

        res.cookie("access_token", result.data.accessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 15 * 60 * 1000,
        });
        res.cookie("refresh_token", result.data.refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 7 * 24 * 60 * 60 * 1000,
            path: "/api/auth/refresh",
        });
        res.json({ message: "Login successful", user: result.data.user });
    } catch (error: any) {
        logger.error("Login error", { error });
        res.status(500).json({ error: "Login failed" });
    }
});

authRouter.post("/api/auth/refresh", async (req, res) => {
    try {
        const token =
            req.cookies?.refresh_token || req.body?.refreshToken || req.body?.refresh_token;
        if (!token) {
            return res.status(400).json({ error: "Refresh token required" });
        }

        const result = await authService.refreshTokens(token);
        if (!result.success || !result.data) {
            res.clearCookie("access_token");
            res.clearCookie("refresh_token");
            return res
                .status(401)
                .json({ error: result.error || "Invalid or expired refresh token" });
        }

        res.cookie("access_token", result.data.accessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 15 * 60 * 1000,
        });
        res.cookie("refresh_token", result.data.refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 7 * 24 * 60 * 60 * 1000,
            path: "/api/auth/refresh",
        });
        res.json({ message: "Token refreshed", user: result.data.user });
    } catch (error: any) {
        logger.error("Token refresh error", { error });
        res.status(500).json({ error: "Token refresh failed" });
    }
});

authRouter.get("/api/auth/me", authenticate, (req, res) => {
    if (!req.user) {
        return res.status(401).json({ error: "Not authenticated" });
    }
    return res.json({ id: req.user.userId, email: req.user.email, role: req.user.role });
});

authRouter.post("/api/auth/logout", async (req, res) => {
    try {
        const token = req.cookies?.refresh_token || req.body?.refreshToken;
        if (token) {
            sessionService.revokeRefreshToken(token);
        }
        res.clearCookie("access_token");
        res.clearCookie("refresh_token");
        res.json({ message: "Logged out successfully" });
    } catch (error: any) {
        logger.error("Logout error", { error });
        res.status(500).json({ error: "Logout failed" });
    }
});

// ===== /v1/auth/* =====

authRouter.post("/v1/auth/register", authRateLimiter, async (req, res) => {
    try {
        const schema = z.object({
            email: z.string().email("Invalid email address"),
            password: z.string().min(8, "Password must be at least 8 characters"),
            full_name: z.string().min(1, "full_name is required"),
            organization_name: z.string().optional(),
        });
        const parsed = schema.safeParse(req.body);
        if (!parsed.success) {
            return res.status(400).json({ error: parsed.error.errors[0].message });
        }
        const { email, password, full_name, organization_name } = parsed.data;

        const result = await authService.register(email, password, full_name, organization_name);
        if (!result.success || !result.data) {
            return res.status(400).json({ error: result.error });
        }

        res.cookie("access_token", result.data.accessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 15 * 60 * 1000,
        });
        res.cookie("refresh_token", result.data.refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 7 * 24 * 60 * 60 * 1000,
            path: "/v1/auth/refresh",
        });
        res.status(201).json({
            access_token: result.data.accessToken,
            token_type: result.data.tokenType,
            user: {
                id: result.data.user.id,
                email: result.data.user.email,
                full_name: result.data.user.fullName,
                role: result.data.user.role,
            },
        });
    } catch (error: any) {
        logger.error("V1 Registration error", { error });
        res.status(500).json({ error: "Registration failed" });
    }
});

authRouter.post("/v1/auth/login", authRateLimiter, async (req, res) => {
    try {
        const schema = z.object({
            email: z.string().email("Invalid email address"),
            password: z.string().min(1, "Password is required"),
        });
        const parsed = schema.safeParse(req.body);
        if (!parsed.success) {
            return res.status(400).json({ error: parsed.error.errors[0].message });
        }
        const { email, password } = parsed.data;

        const result = await authService.login(email, password);
        if (!result.success || !result.data) {
            await auditService.logLogin("", false, req, result.error);
            return res.status(401).json({ error: result.error });
        }

        await auditService.logLogin(result.data.user.id, true, req);

        res.cookie("access_token", result.data.accessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 15 * 60 * 1000,
        });
        res.cookie("refresh_token", result.data.refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "strict",
            maxAge: 7 * 24 * 60 * 60 * 1000,
            path: "/v1/auth/refresh",
        });
        res.json({
            access_token: result.data.accessToken,
            token_type: result.data.tokenType,
            user: {
                id: result.data.user.id,
                email: result.data.user.email,
                full_name: result.data.user.fullName,
                role: result.data.user.role,
            },
        });
    } catch (error: any) {
        logger.error("V1 Login error", { error });
        res.status(500).json({ error: "Login failed" });
    }
});

authRouter.post("/v1/auth/refresh", async (req, res) => {
    try {
        const token = req.cookies?.refresh_token || req.body?.refresh_token;
        if (!token) {
            return res.status(400).json({ error: "refresh_token is required" });
        }

        const result = await authService.refreshTokens(token);
        if (!result.success || !result.data) {
            return res
                .status(401)
                .json({ error: result.error || "Invalid or expired refresh token" });
        }

        res.json({
            access_token: result.data.accessToken,
            refresh_token: result.data.refreshToken,
            token_type: result.data.tokenType,
            user: {
                id: result.data.user.id,
                email: result.data.user.email,
                full_name: result.data.user.fullName,
                role: result.data.user.role,
            },
        });
    } catch (error: any) {
        logger.error("V1 Token refresh error", { error });
        res.status(500).json({ error: "Token refresh failed" });
    }
});
