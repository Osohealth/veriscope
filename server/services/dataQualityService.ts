import { db } from '../db';
import { eventLogs, ingestionCheckpoints, dataQualityScores } from '@shared/schema';
import { eq, desc } from 'drizzle-orm';
import { createHash } from 'crypto';

interface EventData {
  eventType: string;
  sourceId: string;
  payload: Record<string, any>;
  sequenceNumber?: number;
}

interface DeduplicationResult {
  isDuplicate: boolean;
  eventId?: string;
  existingEventId?: string;
}

interface QualityMetrics {
  value: number;
  confidenceScore: number;
  dataCompleteness: number;
  dataFreshness: number;
  outlierScore?: number;
}

class DataQualityService {
  private sortObjectKeys(obj: Record<string, any>): Record<string, any> {
    if (obj === null || typeof obj !== 'object') return obj;
    if (Array.isArray(obj)) return obj.map(item => this.sortObjectKeys(item));
    
    const sorted: Record<string, any> = {};
    Object.keys(obj).sort().forEach(key => {
      sorted[key] = this.sortObjectKeys(obj[key]);
    });
    return sorted;
  }

  private generateEventHash(eventType: string, sourceId: string, payload: Record<string, any>): string {
    const canonical = {
      eventType,
      sourceId,
      payload: this.sortObjectKeys(payload)
    };
    const normalized = JSON.stringify(canonical);
    return createHash('sha256').update(normalized).digest('hex');
  }

  async processEvent(event: EventData): Promise<DeduplicationResult> {
    const eventHash = this.generateEventHash(event.eventType, event.sourceId, event.payload);
    
    try {
      const existing = await db.select()
        .from(eventLogs)
        .where(eq(eventLogs.eventHash, eventHash))
        .limit(1);
      
      if (existing.length > 0) {
        return {
          isDuplicate: true,
          existingEventId: existing[0].id
        };
      }
      
      const [inserted] = await db.insert(eventLogs).values({
        eventType: event.eventType,
        sourceId: event.sourceId,
        eventHash,
        sequenceNumber: event.sequenceNumber,
        payload: event.payload,
        status: 'pending'
      }).returning();
      
      return {
        isDuplicate: false,
        eventId: inserted.id
      };
    } catch (error: any) {
      if (error.code === '23505') {
        return { isDuplicate: true };
      }
      throw error;
    }
  }

  async checkSequenceOrder(streamName: string, newSequence: number): Promise<{ inOrder: boolean; expectedSequence: number; gap: number }> {
    const checkpoint = await db.select()
      .from(ingestionCheckpoints)
      .where(eq(ingestionCheckpoints.streamName, streamName))
      .limit(1);
    
    const lastCount = checkpoint[0]?.messageCount || 0;
    const expectedSequence = lastCount + 1;
    const gap = newSequence - expectedSequence;
    
    return {
      inOrder: gap === 0,
      expectedSequence,
      gap
    };
  }

  async updateCheckpoint(streamName: string, messageCount: number, offset?: string): Promise<void> {
    const existing = await db.select()
      .from(ingestionCheckpoints)
      .where(eq(ingestionCheckpoints.streamName, streamName))
      .limit(1);
    
    if (existing.length > 0) {
      await db.update(ingestionCheckpoints)
        .set({
          messageCount,
          lastOffset: offset,
          lastTimestamp: new Date(),
          updatedAt: new Date()
        })
        .where(eq(ingestionCheckpoints.streamName, streamName));
    } else {
      await db.insert(ingestionCheckpoints).values({
        streamName,
        messageCount,
        lastOffset: offset,
        lastTimestamp: new Date(),
        status: 'active'
      });
    }
  }

  async recordError(streamName: string, error: string): Promise<void> {
    const existing = await db.select()
      .from(ingestionCheckpoints)
      .where(eq(ingestionCheckpoints.streamName, streamName))
      .limit(1);
    
    if (existing.length > 0) {
      await db.update(ingestionCheckpoints)
        .set({
          errorCount: (existing[0].errorCount || 0) + 1,
          lastError: error,
          status: 'error',
          updatedAt: new Date()
        })
        .where(eq(ingestionCheckpoints.streamName, streamName));
    } else {
      await db.insert(ingestionCheckpoints).values({
        streamName,
        errorCount: 1,
        lastError: error,
        status: 'error'
      });
    }
  }

  async markEventProcessed(eventId: string): Promise<void> {
    await db.update(eventLogs)
      .set({
        status: 'processed',
        processedAt: new Date()
      })
      .where(eq(eventLogs.id, eventId));
  }

  async markEventFailed(eventId: string, errorMessage: string): Promise<void> {
    await db.update(eventLogs)
      .set({
        status: 'failed',
        errorMessage,
        retryCount: 1
      })
      .where(eq(eventLogs.id, eventId));
  }

  calculateQualityScore(data: Record<string, any>, requiredFields: string[], freshnessTresholdSeconds: number = 300): QualityMetrics {
    const presentFields = requiredFields.filter(f => data[f] !== undefined && data[f] !== null);
    const completeness = presentFields.length / requiredFields.length;
    
    let freshness = freshnessTresholdSeconds;
    if (data.timestamp || data.createdAt || data.receivedAt) {
      const timestamp = new Date(data.timestamp || data.createdAt || data.receivedAt);
      freshness = Math.floor((Date.now() - timestamp.getTime()) / 1000);
    }
    
    let accuracy = 1;
    if (data.latitude !== undefined && (data.latitude < -90 || data.latitude > 90)) {
      accuracy -= 0.2;
    }
    if (data.longitude !== undefined && (data.longitude < -180 || data.longitude > 180)) {
      accuracy -= 0.2;
    }
    if (data.speed !== undefined && (data.speed < 0 || data.speed > 50)) {
      accuracy -= 0.1;
    }
    
    const confidenceScore = (completeness * 0.4) + (Math.max(0, 1 - freshness / freshnessTresholdSeconds) * 0.3) + (accuracy * 0.3);
    
    const value = data.value !== undefined ? parseFloat(data.value) : 0;
    
    return {
      value,
      confidenceScore: Math.round(confidenceScore * 10000) / 10000,
      dataCompleteness: Math.round(completeness * 10000) / 10000,
      dataFreshness: freshness,
      outlierScore: undefined
    };
  }

  async recordQualityScore(metricType: string, entityId: string, metrics: QualityMetrics, methodology?: string): Promise<void> {
    await db.insert(dataQualityScores).values({
      metricType,
      entityId,
      value: metrics.value.toString(),
      confidenceScore: metrics.confidenceScore.toString(),
      dataCompleteness: metrics.dataCompleteness.toString(),
      dataFreshness: metrics.dataFreshness,
      outlierScore: metrics.outlierScore?.toString(),
      methodology
    });
  }

  async getLatestQualityScores(entityId?: string, limit: number = 10): Promise<any[]> {
    if (entityId) {
      return db.select()
        .from(dataQualityScores)
        .where(eq(dataQualityScores.entityId, entityId))
        .orderBy(desc(dataQualityScores.timestamp))
        .limit(limit);
    }
    
    return db.select()
      .from(dataQualityScores)
      .orderBy(desc(dataQualityScores.timestamp))
      .limit(limit);
  }

  async getStreamHealth(streamName: string): Promise<{ healthy: boolean; errorRate: number; lastProcessed: Date | null }> {
    const checkpoint = await db.select()
      .from(ingestionCheckpoints)
      .where(eq(ingestionCheckpoints.streamName, streamName))
      .limit(1);
    
    if (checkpoint.length === 0) {
      return { healthy: false, errorRate: 0, lastProcessed: null };
    }
    
    const errorCount = checkpoint[0].errorCount || 0;
    const messageCount = checkpoint[0].messageCount || 1;
    const errorRate = errorCount / messageCount;
    
    return {
      healthy: checkpoint[0].status === 'active' && errorRate < 0.1,
      errorRate,
      lastProcessed: checkpoint[0].lastTimestamp
    };
  }

  async getAllStreamHealth(): Promise<any[]> {
    return db.select().from(ingestionCheckpoints).orderBy(desc(ingestionCheckpoints.updatedAt));
  }
}

export const dataQualityService = new DataQualityService();
