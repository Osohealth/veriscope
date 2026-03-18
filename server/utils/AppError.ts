/**
 * AppError — structured error for Express routes.
 *
 * Usage:
 *   throw new AppError("VALIDATION_ERROR", 400, "email is required");
 *   next(new AppError("NOT_FOUND", 404, "Port not found"));
 *
 * The global error handler in server/index.ts serialises this to:
 *   { version: "1", ok: false, error: { code, message } }
 */
export class AppError extends Error {
    readonly code: string;
    readonly status: number;

    constructor(code: string, status: number, message: string) {
        super(message);
        this.name = "AppError";
        this.code = code;
        this.status = status;
    }
}

/** Convenience factory for common HTTP errors */
export const Errors = {
    badRequest: (message: string, code = "BAD_REQUEST") => new AppError(code, 400, message),
    unauthorized: (message = "Authentication required") => new AppError("UNAUTHORIZED", 401, message),
    forbidden: (message = "Insufficient permissions") => new AppError("FORBIDDEN", 403, message),
    notFound: (resource = "Resource") => new AppError("NOT_FOUND", 404, `${resource} not found`),
    conflict: (message: string) => new AppError("CONFLICT", 409, message),
    internal: (message = "Internal server error") => new AppError("INTERNAL", 500, message),
} as const;
