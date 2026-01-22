import { db } from '../db';
import { auditLogs } from '@shared/schema';
import { Request } from 'express';

export interface AuditEntry {
  userId?: string;
  action: string;
  resource?: string;
  resourceId?: string;
  status: 'success' | 'failure';
  errorMessage?: string;
  metadata?: Record<string, any>;
}

class AuditService {
  async log(entry: AuditEntry, req?: Request): Promise<void> {
    try {
      await db.insert(auditLogs).values({
        userId: entry.userId,
        action: entry.action,
        resource: entry.resource,
        resourceId: entry.resourceId,
        ipAddress: req?.ip || req?.socket?.remoteAddress,
        userAgent: req?.headers['user-agent'],
        status: entry.status,
        errorMessage: entry.errorMessage,
        metadata: entry.metadata
      });
    } catch (error) {
      console.error('Failed to write audit log:', error);
    }
  }

  async logLogin(userId: string, success: boolean, req?: Request, errorMessage?: string): Promise<void> {
    await this.log({
      userId,
      action: success ? 'login' : 'failed_login',
      status: success ? 'success' : 'failure',
      errorMessage
    }, req);
  }

  async logLogout(userId: string, req?: Request): Promise<void> {
    await this.log({
      userId,
      action: 'logout',
      status: 'success'
    }, req);
  }

  async logRegister(userId: string, success: boolean, req?: Request, errorMessage?: string): Promise<void> {
    await this.log({
      userId,
      action: 'register',
      status: success ? 'success' : 'failure',
      errorMessage
    }, req);
  }

  async logDataAccess(userId: string, resource: string, resourceId?: string, req?: Request): Promise<void> {
    await this.log({
      userId,
      action: 'data_access',
      resource,
      resourceId,
      status: 'success'
    }, req);
  }

  async logDataModify(userId: string, resource: string, resourceId: string, metadata?: Record<string, any>, req?: Request): Promise<void> {
    await this.log({
      userId,
      action: 'data_modify',
      resource,
      resourceId,
      status: 'success',
      metadata
    }, req);
  }

  async logSecurityEvent(action: string, metadata: Record<string, any>, req?: Request): Promise<void> {
    await this.log({
      action,
      status: 'failure',
      metadata
    }, req);
  }
}

export const auditService = new AuditService();
