import OpenAI from 'openai';
import { storage } from '../storage';
import { type InsertPrediction } from '@shared/schema';

const openai = new OpenAI({ 
  apiKey: process.env.OPENAI_API_KEY || 'demo-key'
});

class PredictionService {
  private intervalId: NodeJS.Timeout | null = null;

  startPredictionService() {
    // Generate predictions every 6 hours
    this.intervalId = setInterval(async () => {
      try {
        await this.generatePredictions();
      } catch (error) {
        console.error('Prediction service error:', error);
      }
    }, 6 * 60 * 60 * 1000);
    
    // Generate initial predictions
    this.generatePredictions();
    
    console.log('Prediction service started');
  }

  stopPredictionService() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    console.log('Prediction service stopped');
  }

  private async generatePredictions() {
    // Generate predictions for crude oil commodities
    const commodities = await storage.getCommodities();
    const crudeOilCommodities = commodities.filter(c => 
      c.code === 'BRENT' || c.code === 'WTI' || c.category === 'crude_oil'
    );

    const timeframes = ['1D', '1W'];

    for (const commodity of crudeOilCommodities) {
      for (const timeframe of timeframes) {
        await this.generatePrediction(commodity.id, timeframe);
      }
    }
  }

  private async generatePrediction(commodityId: string, timeframe: string) {
    try {
      // Collect current maritime features including delay data
      const features = await this.collectFeatures();
      
      // Get market for crude oil
      const markets = await storage.getMarkets();
      const crudeMarket = markets.find(m => m.code === 'BRENT' || m.code === 'WTI') || markets[0];
      
      // Generate AI prediction with delay-adjusted features
      const prediction = await this.aiPredict(commodityId, crudeMarket.id, timeframe, features);
      
      // Save prediction
      await storage.createPrediction(prediction);
      
      console.log(`Generated ${timeframe} prediction for commodity ${commodityId} with delay adjustments`);
      
    } catch (error) {
      console.error(`Failed to generate ${commodityId} ${timeframe} prediction:`, error);
    }
  }

  private async collectFeatures() {
    const ports = await storage.getPorts();
    const features: any = {
      totalVessels: 0,
      totalQueueLength: 0,
      averageWaitTime: 0,
      storageUtilization: 0,
      activeSignals: 0,
      delayedVessels: 0,
      totalDelayedVolume: 0,
      averageDelayHours: 0,
      delayImpactScore: 0
    };

    // Collect port statistics
    for (const port of ports) {
      const stats = await storage.getLatestPortStats(port.id);
      if (stats) {
        features.totalVessels += stats.totalVessels || 0;
        features.totalQueueLength += stats.queueLength || 0;
        features.averageWaitTime += stats.averageWaitHours || 0;
      }
    }

    // Collect delay data for Rotterdam and other key ports
    for (const port of ports) {
      const delayImpacts = await storage.getMarketDelayImpacts(port.id, undefined, 1);
      if (delayImpacts.length > 0) {
        const impact = delayImpacts[0];
        features.delayedVessels += impact.vesselCount || 0;
        features.totalDelayedVolume += impact.totalDelayedVolume || 0;
        features.averageDelayHours += parseFloat(impact.averageDelayHours || '0');
        features.delayImpactScore += parseFloat(impact.priceImpact || '0');
      }
    }

    // Collect storage data
    const sites = await storage.getStorageSites();
    let totalFill = 0;
    let siteCount = 0;
    
    for (const site of sites) {
      const fillData = await storage.getLatestStorageFillData(site.id);
      if (fillData && fillData.fillIndex) {
        totalFill += parseFloat(fillData.fillIndex.toString());
        siteCount++;
      }
    }
    
    features.storageUtilization = siteCount > 0 ? totalFill / siteCount : 0;
    features.averageWaitTime = ports.length > 0 ? features.averageWaitTime / ports.length : 0;
    features.averageDelayHours = ports.length > 0 ? features.averageDelayHours / ports.length : 0;

    // Count active signals
    const signals = await storage.getActiveSignals(10);
    features.activeSignals = signals.length;

    return features;
  }

  private async aiPredict(commodityId: string, marketId: string, timeframe: string, features: any): Promise<InsertPrediction> {
    const prompt = `
    As a maritime analytics AI, predict the ${timeframe} oil price movement based on these maritime indicators:
    
    Port & Vessel Metrics:
    - Total vessels in key ports: ${features.totalVessels}
    - Queue congestion: ${features.totalQueueLength} vessels
    - Average wait time: ${features.averageWaitTime.toFixed(1)} hours
    - Storage utilization: ${(features.storageUtilization * 100).toFixed(1)}%
    - Active alerts: ${features.activeSignals}
    
    Delay Impact Metrics (NEW):
    - Delayed vessels: ${features.delayedVessels}
    - Total delayed cargo volume: ${features.totalDelayedVolume} tons
    - Average delay: ${features.averageDelayHours.toFixed(1)} hours
    - Price impact score: ${features.delayImpactScore.toFixed(4)}
    
    Consider how vessel delays and cargo volumes impact supply disruption and price movements.
    Higher delays and larger delayed volumes typically indicate supply constraints, pushing prices up.
    
    Respond with JSON in this exact format:
    {
      "direction": "up" | "down" | "stable",
      "confidence": 0.75,
      "currentPrice": 82.50,
      "predictedPrice": 83.25,
      "reasoning": "Brief explanation including delay impact"
    }
    `;

    // the newest OpenAI model is "gpt-5" which was released August 7, 2025. do not change this unless explicitly requested by the user
    const response = await openai.chat.completions.create({
      model: "gpt-5",
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" },
      temperature: 0.3
    });

    const result = JSON.parse(response.choices[0].message.content || '{}');
    
    // Calculate valid until time based on timeframe
    const validHours = timeframe === '1D' ? 24 : timeframe === '1W' ? 168 : 24;
    
    return {
      commodityId,
      marketId,
      timeframe,
      currentPrice: (result.currentPrice || 82.50).toString(),
      predictedPrice: (result.predictedPrice || 82.50).toString(),
      confidence: (result.confidence || 0.65).toString(),
      direction: result.direction || 'stable',
      features: features,
      metadata: { reasoning: result.reasoning, modelVersion: 'gpt-5-maritime-v1' },
      validUntil: new Date(Date.now() + validHours * 60 * 60 * 1000)
    };
  }

  private generateFallbackPrediction(commodityId: string, marketId: string, timeframe: string): InsertPrediction {
    // Simple rule-based fallback
    const directions = ['up', 'down', 'stable'];
    const direction = directions[Math.floor(Math.random() * directions.length)];
    const confidence = 0.4 + Math.random() * 0.2; // 40-60% confidence for fallback
    
    const basePrice = 80 + Math.random() * 10; // Random price between 80-90
    const priceChange = (Math.random() - 0.5) * 2; // Random change +/- 1
    const predictedPrice = basePrice + priceChange;
    
    const validHours = timeframe === '1D' ? 24 : timeframe === '1W' ? 168 : 24;

    return {
      commodityId,
      marketId,
      timeframe,
      currentPrice: basePrice.toFixed(2),
      predictedPrice: predictedPrice.toFixed(2),
      confidence: confidence.toFixed(4),
      direction: direction,
      features: {},
      metadata: { modelVersion: 'fallback-v1' },
      validUntil: new Date(Date.now() + validHours * 60 * 60 * 1000)
    };
  }
}

export const predictionService = new PredictionService();
