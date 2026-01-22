import { Request, Response, NextFunction } from 'express';
import { sessionService } from '../services/sessionService';
import { verifyToken as verifyAuthToken } from '../services/authService';
import { logger } from './observability';

export type Role = 'admin' | 'analyst' | 'viewer' | 'operator';

export type Permission =
  | 'read:vessels'
  | 'write:vessels'
  | 'read:ports'
  | 'write:ports'
  | 'read:signals'
  | 'write:signals'
  | 'read:predictions'
  | 'write:predictions'
  | 'read:storage'
  | 'write:storage'
  | 'read:models'
  | 'write:models'
  | 'read:users'
  | 'write:users'
  | 'read:audit'
  | 'read:watchlists'
  | 'write:watchlists'
  | 'read:alerts'
  | 'write:alerts'
  | 'admin:system';

const rolePermissions: Record<Role, Permission[]> = {
  admin: [
    'read:vessels', 'write:vessels',
    'read:ports', 'write:ports',
    'read:signals', 'write:signals',
    'read:predictions', 'write:predictions',
    'read:storage', 'write:storage',
    'read:models', 'write:models',
    'read:users', 'write:users',
    'read:audit',
    'read:watchlists', 'write:watchlists',
    'read:alerts', 'write:alerts',
    'admin:system'
  ],
  analyst: [
    'read:vessels',
    'read:ports',
    'read:signals', 'write:signals',
    'read:predictions', 'write:predictions',
    'read:storage',
    'read:models', 'write:models',
    'read:watchlists', 'write:watchlists',
    'read:alerts', 'write:alerts'
  ],
  operator: [
    'read:vessels', 'write:vessels',
    'read:ports', 'write:ports',
    'read:signals',
    'read:predictions',
    'read:storage', 'write:storage',
    'read:watchlists', 'write:watchlists',
    'read:alerts', 'write:alerts'
  ],
  viewer: [
    'read:vessels',
    'read:ports',
    'read:signals',
    'read:predictions',
    'read:storage',
    'read:watchlists',
    'read:alerts'
  ]
};

export interface AuthenticatedUser {
  userId: string;
  email: string;
  role: Role;
}

declare global {
  namespace Express {
    interface Request {
      user?: AuthenticatedUser;
    }
  }
}

export function hasPermission(role: Role, permission: Permission): boolean {
  const permissions = rolePermissions[role];
  return permissions ? permissions.includes(permission) : false;
}

export function getRolePermissions(role: Role): Permission[] {
  return rolePermissions[role] || [];
}

export function authenticate(req: Request, res: Response, next: NextFunction) {
  const authHeader = req.headers.authorization;
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Authentication required' });
  }
  
  const token = authHeader.substring(7);
  
  // Try authService first (for JWT tokens from /v1/auth/login)
  const authPayload = verifyAuthToken(token);
  if (authPayload) {
    if (authPayload.type !== 'access') {
      return res.status(401).json({ error: 'Invalid token type' });
    }
    req.user = {
      userId: authPayload.userId,
      email: authPayload.email,
      role: authPayload.role as Role
    };
    return next();
  }
  
  // Fallback to sessionService (for legacy tokens)
  const sessionPayload = sessionService.verifyToken(token);
  if (sessionPayload) {
    if (sessionPayload.type !== 'access') {
      return res.status(401).json({ error: 'Invalid token type' });
    }
    req.user = {
      userId: sessionPayload.userId,
      email: sessionPayload.email,
      role: sessionPayload.role as Role
    };
    return next();
  }
  
  return res.status(401).json({ error: 'Invalid or expired token' });
}

export function optionalAuth(req: Request, res: Response, next: NextFunction) {
  const authHeader = req.headers.authorization;
  
  if (authHeader && authHeader.startsWith('Bearer ')) {
    const token = authHeader.substring(7);
    
    // Try authService first (for JWT tokens from /v1/auth/login)
    const authPayload = verifyAuthToken(token);
    if (authPayload && authPayload.type === 'access') {
      req.user = {
        userId: authPayload.userId,
        email: authPayload.email,
        role: authPayload.role as Role
      };
    } else {
      // Fallback to sessionService (for legacy tokens)
      const sessionPayload = sessionService.verifyToken(token);
      if (sessionPayload && sessionPayload.type === 'access') {
        req.user = {
          userId: sessionPayload.userId,
          email: sessionPayload.email,
          role: sessionPayload.role as Role
        };
      }
    }
  }
  
  next();
}

export function requirePermission(...permissions: Permission[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    const userRole = req.user.role;
    const hasRequiredPermission = permissions.some(permission => 
      hasPermission(userRole, permission)
    );
    
    if (!hasRequiredPermission) {
      logger.warn('Permission denied', {
        userId: req.user.userId,
        role: userRole,
        requiredPermissions: permissions,
        path: req.path,
        method: req.method
      });
      
      return res.status(403).json({ 
        error: 'Insufficient permissions',
        required: permissions
      });
    }
    
    next();
  };
}

export function requireRole(...roles: Role[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    if (!roles.includes(req.user.role)) {
      logger.warn('Role denied', {
        userId: req.user.userId,
        userRole: req.user.role,
        requiredRoles: roles,
        path: req.path,
        method: req.method
      });
      
      return res.status(403).json({ 
        error: 'Insufficient role privileges',
        required: roles
      });
    }
    
    next();
  };
}

export function requireAdmin(req: Request, res: Response, next: NextFunction) {
  return requireRole('admin')(req, res, next);
}

export function requireSelfOrAdmin(userIdParam: string = 'userId') {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    const targetUserId = req.params[userIdParam] || req.body[userIdParam];
    const isOwnResource = req.user.userId === targetUserId;
    const isAdmin = req.user.role === 'admin';
    
    if (!isOwnResource && !isAdmin) {
      logger.warn('Self or admin access denied', {
        userId: req.user.userId,
        targetUserId,
        path: req.path,
        method: req.method
      });
      
      return res.status(403).json({ 
        error: 'Access denied. You can only access your own resources.'
      });
    }
    
    next();
  };
}

export function logAccess(resourceType: string) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (req.user) {
      logger.info('Resource access', {
        userId: req.user.userId,
        role: req.user.role,
        resourceType,
        path: req.path,
        method: req.method
      });
    }
    next();
  };
}
