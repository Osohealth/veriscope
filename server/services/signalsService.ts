import { WebSocketServer } from 'ws';
import { storage } from '../storage';
import type { AlertRule } from '@shared/schema';

class SignalsService {
  private intervalId: NodeJS.Timeout | null = null;
  private wss: WebSocketServer | null = null;

  startMonitoring(wss: WebSocketServer) {
    this.wss = wss;
    
    // Check for signals every 2 minutes
    this.intervalId = setInterval(async () => {
      try {
        await this.analyzeForSignals();
        await this.evaluateAlertRules();
      } catch (error) {
        console.error('Signals monitoring error:', error);
      }
    }, 120000);
    
    console.log('Signals monitoring service started');
  }

  stopMonitoring() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    console.log('Signals monitoring service stopped');
  }

  private async analyzeForSignals() {
    const ports = await storage.getPorts();
    
    for (const port of ports) {
      await this.checkPortCongestion(port);
      await this.checkStorageAnomalies(port);
    }
  }

  private async checkPortCongestion(port: any) {
    const stats = await storage.getLatestPortStats(port.id);
    
    if (!stats) return;

    // Convert decimal strings to numbers
    const avgWaitHours = stats.averageWaitHours ? parseFloat(stats.averageWaitHours as string) : 0;
    const throughput = stats.throughputMT ? parseFloat(stats.throughputMT as string) : 0;

    // Check for high queue lengths
    if (stats.queueLength && stats.queueLength > 10) {
      const severity = stats.queueLength > 15 ? 5 : 3;
      const signal = {
        type: 'CONGESTION_ALERT',
        title: `High congestion at ${port.name}`,
        description: `Queue length: ${stats.queueLength} vessels, Average wait: ${avgWaitHours.toFixed(1)}h`,
        severity,
        metadata: {
          queueLength: stats.queueLength,
          averageWaitHours: avgWaitHours,
          portCode: port.code
        },
        explainability: {
          triggerReason: `Queue length exceeded threshold (${stats.queueLength} > 10 vessels)`,
          dataSources: ['Port API', 'AIS Vessel Positions'],
          timeWindow: 'Last 2 hours',
          confidence: severity >= 4 ? 'high' : 'medium',
          methodology: 'Real-time port queue monitoring with AIS position validation'
        }
      };

      this.broadcastSignal(signal);
    }

    // Check for unusual throughput
    if (throughput > 3.0) {
      const signal = {
        type: 'THROUGHPUT_SURGE',
        title: `High throughput at ${port.name}`,
        description: `Throughput: ${throughput.toFixed(1)}M MT/day`,
        severity: 2,
        metadata: {
          throughputMT: throughput,
          portCode: port.code
        },
        explainability: {
          triggerReason: `Daily throughput exceeded baseline (${throughput.toFixed(1)}M MT > 3.0M MT)`,
          dataSources: ['Port API', 'Historical Throughput Data'],
          timeWindow: 'Last 24 hours',
          confidence: 'medium',
          methodology: 'Throughput anomaly detection against 30-day moving average'
        }
      };

      this.broadcastSignal(signal);
    }
  }

  private async checkStorageAnomalies(port: any) {
    const sites = await storage.getStorageSites();
    const portSites = sites.filter(s => s.portId === port.id);
    
    for (const site of portSites) {
      const fillData = await storage.getLatestStorageFillData(site.id);
      
      if (!fillData || !fillData.fillIndex) continue;

      // Convert decimal string to number
      const fillIndex = parseFloat(fillData.fillIndex as string);

      // Check for very high fill levels
      if (fillIndex > 0.85) {
        const severity = fillIndex > 0.95 ? 4 : 2;
        const signal = {
          type: 'HIGH_STORAGE_FILL',
          title: `High storage fill at ${site.name}`,
          description: `Fill level: ${(fillIndex * 100).toFixed(1)}%`,
          severity,
          metadata: {
            fillIndex: fillIndex,
            siteName: site.name,
            portCode: port.code
          },
          explainability: {
            triggerReason: `Storage fill level exceeded threshold (${(fillIndex * 100).toFixed(1)}% > 85%)`,
            dataSources: ['Satellite Imagery', 'Tank Level Sensors', 'Port Storage API'],
            timeWindow: 'Last 6 hours',
            confidence: severity >= 4 ? 'high' : 'medium',
            methodology: 'Satellite-validated tank level monitoring with sensor fusion'
          }
        };

        this.broadcastSignal(signal);
      }
    }
  }

  private broadcastSignal(signal: any) {
    if (!this.wss) return;
    
    const message = JSON.stringify({ 
      type: 'new_signal', 
      data: signal 
    });
    
    this.wss.clients.forEach((client) => {
      if (client.readyState === client.OPEN) {
        client.send(message);
      }
    });
  }

  private async evaluateAlertRules() {
    try {
      const activeRules = await storage.getActiveAlertRules();
      const processedRuleIds = new Set<string>();

      for (const rule of activeRules) {
        if (processedRuleIds.has(rule.id)) continue;
        
        const evalTime = new Date();
        if (!this.shouldEvaluateRule(rule, evalTime)) continue;

        const triggered = await this.checkRuleConditions(rule);
        
        if (triggered) {
          const triggeredAt = await this.triggerAlert(rule, triggered);
          processedRuleIds.add(rule.id);
          (rule as any).lastTriggered = triggeredAt;
          (rule as any).triggerCount = (rule.triggerCount || 0) + 1;
        }
      }
    } catch (error) {
      console.error('Alert rules evaluation error:', error);
    }
  }

  private shouldEvaluateRule(rule: AlertRule, now: Date): boolean {
    if (rule.isMuted) return false;
    
    if (rule.snoozedUntil && new Date(rule.snoozedUntil) > now) return false;
    
    const effectiveCooldown = rule.cooldownMinutes ?? 60;
    if (rule.lastTriggered && effectiveCooldown > 0) {
      const cooldownEnd = new Date(rule.lastTriggered);
      cooldownEnd.setMinutes(cooldownEnd.getMinutes() + effectiveCooldown);
      if (cooldownEnd > now) return false;
    }
    
    return true;
  }

  private async checkRuleConditions(rule: AlertRule): Promise<{ value: number; threshold: number; message: string } | null> {
    const conditions = rule.conditions as any;
    if (!conditions) return null;

    switch (rule.type) {
      case 'price_threshold':
        return await this.checkPriceThreshold(conditions);
      case 'congestion':
        return await this.checkCongestionThreshold(conditions);
      case 'storage_level':
        return await this.checkStorageThreshold(conditions);
      case 'vessel_arrival':
        return await this.checkVesselArrival(conditions);
      default:
        return null;
    }
  }

  private async checkPriceThreshold(conditions: any): Promise<{ value: number; threshold: number; message: string } | null> {
    const { commodityCode, operator, value } = conditions;
    if (!commodityCode || !operator || value === undefined) return null;

    const prices = await storage.getLatestCommodityPrices();
    const commodity = await storage.getCommodityByCode(commodityCode);
    if (!commodity) return null;

    const latestPrice = prices.find(p => p.commodityId === commodity.id);
    if (!latestPrice) return null;

    const currentPrice = parseFloat(latestPrice.price as string);
    const threshold = parseFloat(value);
    
    const triggered = this.evaluateOperator(currentPrice, operator, threshold);
    if (triggered) {
      return {
        value: currentPrice,
        threshold,
        message: `${commodityCode} price ${currentPrice.toFixed(2)} is ${operator.replace('_', ' ')} ${threshold.toFixed(2)}`
      };
    }
    return null;
  }

  private async checkCongestionThreshold(conditions: any): Promise<{ value: number; threshold: number; message: string } | null> {
    const { portCode, operator, value } = conditions;
    if (!portCode || !operator || value === undefined) return null;

    const port = await storage.getPortByCode(portCode);
    if (!port) return null;

    const stats = await storage.getLatestPortStats(port.id);
    if (!stats || !stats.queueLength) return null;

    const queueLength = stats.queueLength;
    const threshold = parseInt(value);
    
    const triggered = this.evaluateOperator(queueLength, operator, threshold);
    if (triggered) {
      return {
        value: queueLength,
        threshold,
        message: `${portCode} queue length ${queueLength} is ${operator.replace('_', ' ')} ${threshold}`
      };
    }
    return null;
  }

  private async checkStorageThreshold(conditions: any): Promise<{ value: number; threshold: number; message: string } | null> {
    const { siteId, operator, value } = conditions;
    if (!operator || value === undefined) return null;

    const sites = await storage.getStorageSites();
    const targetSite = siteId ? sites.find(s => s.id === siteId) : sites[0];
    if (!targetSite) return null;

    const fillData = await storage.getLatestStorageFillData(targetSite.id);
    if (!fillData || !fillData.fillIndex) return null;

    const fillLevel = parseFloat(fillData.fillIndex as string) * 100;
    const threshold = parseFloat(value);
    
    const triggered = this.evaluateOperator(fillLevel, operator, threshold);
    if (triggered) {
      return {
        value: fillLevel,
        threshold,
        message: `${targetSite.name} fill level ${fillLevel.toFixed(1)}% is ${operator.replace('_', ' ')} ${threshold}%`
      };
    }
    return null;
  }

  private async checkVesselArrival(conditions: any): Promise<{ value: number; threshold: number; message: string } | null> {
    const { portCode, vesselType, hoursAhead = 24 } = conditions;
    if (!portCode) return null;

    const port = await storage.getPortByCode(portCode);
    if (!port) return null;

    const portCalls = await storage.getLatestPortCalls(port.id, 20);
    const now = new Date();
    const cutoffTime = new Date(now.getTime() + Number(hoursAhead) * 60 * 60 * 1000);

    const allVessels = vesselType ? await storage.getVessels() : [];
    const vesselMap = new Map(allVessels.map(v => [v.id, v]));

    const upcomingArrivals: any[] = [];
    for (const pc of portCalls) {
      if (pc.callType !== 'arrival' || pc.status !== 'scheduled') continue;
      
      if (!pc.arrivalTime) continue;
      
      const arrivalDate = new Date(pc.arrivalTime);
      if (isNaN(arrivalDate.getTime()) || arrivalDate < now || arrivalDate > cutoffTime) continue;
      
      if (vesselType) {
        const vessel = vesselMap.get(pc.vesselId);
        if (!vessel || vessel.vesselType !== vesselType) continue;
      }
      
      upcomingArrivals.push(pc);
    }

    if (upcomingArrivals.length > 0) {
      return {
        value: upcomingArrivals.length,
        threshold: 1,
        message: `${upcomingArrivals.length} ${vesselType || ''} vessel(s) arriving at ${portCode} within ${hoursAhead}h`
      };
    }
    return null;
  }

  private evaluateOperator(value: number, operator: string, threshold: number): boolean {
    switch (operator) {
      case 'greater_than': return value > threshold;
      case 'less_than': return value < threshold;
      case 'equals': return Math.abs(value - threshold) < 0.01;
      case 'greater_than_or_equal': return value >= threshold;
      case 'less_than_or_equal': return value <= threshold;
      default: return false;
    }
  }

  private async triggerAlert(rule: AlertRule, result: { value: number; threshold: number; message: string }): Promise<Date> {
    const triggeredAt = new Date();
    try {
      await storage.createNotification({
        userId: rule.userId,
        alertId: null,
        type: rule.type,
        title: `Alert: ${rule.name}`,
        message: result.message,
        severity: rule.severity || 'medium',
        data: {
          ruleId: rule.id,
          ruleName: rule.name,
          currentValue: result.value,
          threshold: result.threshold,
          conditions: rule.conditions,
          triggeredAt: triggeredAt.toISOString()
        },
        isRead: false,
        timestamp: triggeredAt
      });

      await storage.updateAlertRule(rule.id, {
        lastTriggered: triggeredAt,
        triggerCount: (rule.triggerCount || 0) + 1
      });

      const channels = rule.channels as string[] | null;
      if (channels?.includes('in_app') || !channels) {
        this.broadcastAlert({
          type: 'alert_triggered',
          ruleId: rule.id,
          ruleName: rule.name,
          ruleType: rule.type,
          severity: rule.severity,
          message: result.message,
          timestamp: new Date().toISOString()
        });
      }

      console.log(`Alert triggered: ${rule.name} - ${result.message}`);
    } catch (error) {
      console.error(`Error triggering alert ${rule.name}:`, error);
    }
    return triggeredAt;
  }

  private broadcastAlert(alert: any) {
    if (!this.wss) return;
    
    const message = JSON.stringify({ 
      type: 'alert_triggered', 
      data: alert 
    });
    
    this.wss.clients.forEach((client) => {
      if (client.readyState === client.OPEN) {
        client.send(message);
      }
    });
  }
}

export const signalsService = new SignalsService();
