import { db } from '../db';
import { 
  modelRegistry, 
  modelPredictions,
  ModelRegistryEntry,
  ModelPrediction
} from '@shared/schema';
import { eq, desc, and, isNotNull } from 'drizzle-orm';

interface DriftMetrics {
  modelId: string;
  predictionCount: number;
  backtestCount: number;
  meanError: number;
  meanAbsoluteError: number;
  rootMeanSquareError: number;
  accuracyWithinBounds: number;
  isDrifting: boolean;
  driftSeverity: 'none' | 'low' | 'medium' | 'high';
  recommendation: string;
}

interface BacktestResult {
  modelId: string;
  modelName: string;
  version: string;
  predictions: Array<{
    id: string;
    target: string;
    predictionDate: string;
    predictedValue: number;
    actualValue: number | null;
    error: number | null;
    confidenceLower: number | null;
    confidenceUpper: number | null;
    confidenceLevel: number;
    withinBounds: boolean;
  }>;
  summary: {
    totalPredictions: number;
    backtested: number;
    meanError: number;
    meanAbsoluteError: number;
    rootMeanSquareError: number;
    accuracyWithinBounds: number;
    maxError: number;
    minError: number;
  };
}

export class ModelRegistryService {

  async listModels(status?: string): Promise<ModelRegistryEntry[]> {
    try {
      if (status) {
        return await db.select()
          .from(modelRegistry)
          .where(eq(modelRegistry.status, status))
          .orderBy(desc(modelRegistry.createdAt));
      }
      return await db.select()
        .from(modelRegistry)
        .orderBy(desc(modelRegistry.createdAt));
    } catch (error) {
      console.error('Error listing models:', error);
      return [];
    }
  }

  async getModel(modelId: string): Promise<ModelRegistryEntry | null> {
    try {
      const [model] = await db.select()
        .from(modelRegistry)
        .where(eq(modelRegistry.id, modelId));
      return model || null;
    } catch (error) {
      console.error('Error getting model:', error);
      return null;
    }
  }

  async getActiveModelByName(modelName: string): Promise<ModelRegistryEntry | null> {
    try {
      const [model] = await db.select()
        .from(modelRegistry)
        .where(and(
          eq(modelRegistry.modelName, modelName),
          eq(modelRegistry.status, 'active'),
          eq(modelRegistry.isActive, true)
        ))
        .orderBy(desc(modelRegistry.createdAt))
        .limit(1);
      return model || null;
    } catch (error) {
      console.error('Error getting active model:', error);
      return null;
    }
  }

  async createModel(data: {
    modelName: string;
    version: string;
    modelType?: string;
    features?: string[];
    hyperparameters?: any;
    trainingMetrics?: any;
    validationMetrics?: any;
    status?: string;
  }): Promise<ModelRegistryEntry | null> {
    try {
      const finalStatus = data.status ?? 'active';
      const [model] = await db.insert(modelRegistry)
        .values({
          modelName: data.modelName,
          version: data.version,
          modelType: data.modelType ?? 'regression',
          features: data.features ?? [],
          hyperparameters: data.hyperparameters,
          trainingMetrics: data.trainingMetrics,
          validationMetrics: data.validationMetrics,
          status: finalStatus,
          isActive: finalStatus === 'active'
        })
        .returning();
      return model;
    } catch (error) {
      console.error('Error creating model:', error);
      return null;
    }
  }

  async activateModel(modelId: string): Promise<ModelRegistryEntry | null> {
    try {
      const model = await this.getModel(modelId);
      if (!model) return null;

      await db.update(modelRegistry)
        .set({ status: 'deprecated', isActive: false })
        .where(and(
          eq(modelRegistry.modelName, model.modelName),
          eq(modelRegistry.status, 'active')
        ));

      const [updated] = await db.update(modelRegistry)
        .set({ 
          status: 'active',
          isActive: true
        })
        .where(eq(modelRegistry.id, modelId))
        .returning();

      return updated;
    } catch (error) {
      console.error('Error activating model:', error);
      return null;
    }
  }

  async deprecateModel(modelId: string): Promise<ModelRegistryEntry | null> {
    try {
      const [updated] = await db.update(modelRegistry)
        .set({ status: 'deprecated', isActive: false })
        .where(eq(modelRegistry.id, modelId))
        .returning();
      return updated || null;
    } catch (error) {
      console.error('Error deprecating model:', error);
      return null;
    }
  }

  async createPrediction(data: {
    modelId: string;
    target: string;
    predictionDate: Date;
    horizon?: string;
    predictedValue: number;
    confidenceLower?: number;
    confidenceUpper?: number;
    confidenceLevel?: number;
    featuresUsed?: any;
  }): Promise<ModelPrediction | null> {
    try {
      const [prediction] = await db.insert(modelPredictions)
        .values({
          modelId: data.modelId,
          target: data.target,
          predictionDate: data.predictionDate,
          horizon: data.horizon,
          predictedValue: data.predictedValue.toString(),
          confidenceLower: data.confidenceLower?.toString(),
          confidenceUpper: data.confidenceUpper?.toString(),
          confidenceLevel: (data.confidenceLevel || 0.95).toString(),
          featuresUsed: data.featuresUsed
        })
        .returning();
      return prediction;
    } catch (error) {
      console.error('Error creating prediction:', error);
      return null;
    }
  }

  async getPredictions(modelId: string, limit = 100): Promise<ModelPrediction[]> {
    try {
      return await db.select()
        .from(modelPredictions)
        .where(eq(modelPredictions.modelId, modelId))
        .orderBy(desc(modelPredictions.predictionDate))
        .limit(limit);
    } catch (error) {
      console.error('Error getting predictions:', error);
      return [];
    }
  }

  async recordActualValue(predictionId: string, actualValue: number): Promise<ModelPrediction | null> {
    try {
      const [updated] = await db.update(modelPredictions)
        .set({ actualValue: actualValue.toString() })
        .where(eq(modelPredictions.id, predictionId))
        .returning();
      return updated || null;
    } catch (error) {
      console.error('Error recording actual value:', error);
      return null;
    }
  }

  async getBacktestResults(modelId: string): Promise<BacktestResult | null> {
    try {
      const model = await this.getModel(modelId);
      if (!model) return null;

      const predictions = await db.select()
        .from(modelPredictions)
        .where(eq(modelPredictions.modelId, modelId))
        .orderBy(desc(modelPredictions.predictionDate));

      const backtested = predictions.filter(p => p.actualValue !== null);
      
      const errors = backtested.map(p => {
        const actual = parseFloat(p.actualValue || '0');
        const predicted = parseFloat(p.predictedValue);
        return actual - predicted;
      });

      const absoluteErrors = errors.map(e => Math.abs(e));

      const meanError = errors.length > 0 
        ? errors.reduce((a, b) => a + b, 0) / errors.length 
        : 0;
      
      const meanAbsoluteError = absoluteErrors.length > 0 
        ? absoluteErrors.reduce((a, b) => a + b, 0) / absoluteErrors.length 
        : 0;

      const squaredErrors = errors.map(e => e * e);
      const rootMeanSquareError = squaredErrors.length > 0 
        ? Math.sqrt(squaredErrors.reduce((a, b) => a + b, 0) / squaredErrors.length) 
        : 0;

      // Only include predictions with valid confidence intervals for CI accuracy calculation
      const predictionsWithCI = backtested.filter(p => 
        p.confidenceLower !== null && p.confidenceUpper !== null
      );
      
      const withinBounds = predictionsWithCI.filter(p => {
        const lower = parseFloat(p.confidenceLower!);
        const upper = parseFloat(p.confidenceUpper!);
        const actual = parseFloat(p.actualValue!);
        return actual >= lower && actual <= upper;
      });

      const accuracyWithinBounds = predictionsWithCI.length > 0 
        ? (withinBounds.length / predictionsWithCI.length) * 100 
        : 0;

      return {
        modelId,
        modelName: model.modelName,
        version: model.version,
        predictions: predictions.map(p => {
          const actual = p.actualValue ? parseFloat(p.actualValue) : null;
          const predicted = parseFloat(p.predictedValue);
          const lower = p.confidenceLower ? parseFloat(p.confidenceLower) : null;
          const upper = p.confidenceUpper ? parseFloat(p.confidenceUpper) : null;
          const level = parseFloat(p.confidenceLevel || '0.95');
          
          return {
            id: p.id,
            target: p.target,
            predictionDate: p.predictionDate.toISOString(),
            predictedValue: predicted,
            actualValue: actual,
            error: actual !== null ? actual - predicted : null,
            confidenceLower: lower,
            confidenceUpper: upper,
            confidenceLevel: level,
            withinBounds: actual !== null && lower !== null && upper !== null 
              ? (actual >= lower && actual <= upper) 
              : false
          };
        }),
        summary: {
          totalPredictions: predictions.length,
          backtested: backtested.length,
          meanError: Number(meanError.toFixed(4)),
          meanAbsoluteError: Number(meanAbsoluteError.toFixed(4)),
          rootMeanSquareError: Number(rootMeanSquareError.toFixed(4)),
          accuracyWithinBounds: Number(accuracyWithinBounds.toFixed(2)),
          maxError: errors.length > 0 ? Math.max(...errors) : 0,
          minError: errors.length > 0 ? Math.min(...errors) : 0
        }
      };
    } catch (error) {
      console.error('Error getting backtest results:', error);
      return null;
    }
  }

  async getDriftMetrics(modelId: string): Promise<DriftMetrics | null> {
    try {
      const backtest = await this.getBacktestResults(modelId);
      if (!backtest) return null;

      const { summary } = backtest;
      
      let isDrifting = false;
      let driftSeverity: 'none' | 'low' | 'medium' | 'high' = 'none';
      let recommendation = 'Model performing within acceptable bounds.';

      if (summary.backtested < 5) {
        recommendation = 'Insufficient backtest data for drift detection. Record more actual values.';
      } else {
        const ciAccuracyThreshold = 90;
        const maeThreshold = 5;

        if (summary.accuracyWithinBounds < ciAccuracyThreshold - 20) {
          isDrifting = true;
          driftSeverity = 'high';
          recommendation = 'Significant model drift detected. Predictions frequently outside confidence intervals. Consider retraining.';
        } else if (summary.accuracyWithinBounds < ciAccuracyThreshold - 10) {
          isDrifting = true;
          driftSeverity = 'medium';
          recommendation = 'Moderate model drift detected. Monitor closely and prepare for retraining.';
        } else if (summary.accuracyWithinBounds < ciAccuracyThreshold) {
          isDrifting = true;
          driftSeverity = 'low';
          recommendation = 'Minor model drift detected. Continue monitoring.';
        }

        if (summary.meanAbsoluteError > maeThreshold * 2) {
          isDrifting = true;
          if (driftSeverity === 'none' || driftSeverity === 'low') {
            driftSeverity = 'high';
            recommendation = 'High prediction error detected. Immediate model review recommended.';
          }
        } else if (summary.meanAbsoluteError > maeThreshold) {
          if (!isDrifting) {
            isDrifting = true;
            driftSeverity = 'medium';
            recommendation = 'Elevated prediction error. Consider model adjustments.';
          }
        }
      }

      return {
        modelId,
        predictionCount: summary.totalPredictions,
        backtestCount: summary.backtested,
        meanError: summary.meanError,
        meanAbsoluteError: summary.meanAbsoluteError,
        rootMeanSquareError: summary.rootMeanSquareError,
        accuracyWithinBounds: summary.accuracyWithinBounds,
        isDrifting,
        driftSeverity,
        recommendation
      };
    } catch (error) {
      console.error('Error calculating drift metrics:', error);
      return null;
    }
  }

  async generatePredictionWithConfidence(
    modelId: string,
    target: string,
    predictionDate: Date,
    predictedValue: number,
    confidenceLevel = 0.95,
    horizon?: string,
    featuresUsed?: Record<string, any>
  ): Promise<ModelPrediction | null> {
    try {
      const model = await this.getModel(modelId);
      if (!model) return null;

      const metrics = model.validationMetrics as { standardError?: number } | null;
      const standardError = metrics?.standardError || predictedValue * 0.05;

      const zScore = confidenceLevel === 0.99 ? 2.576 : confidenceLevel === 0.95 ? 1.96 : 1.645;
      const margin = zScore * standardError;

      return this.createPrediction({
        modelId,
        target,
        predictionDate,
        horizon,
        predictedValue,
        confidenceLower: predictedValue - margin,
        confidenceUpper: predictedValue + margin,
        confidenceLevel,
        featuresUsed
      });
    } catch (error) {
      console.error('Error generating prediction with confidence:', error);
      return null;
    }
  }
}

export const modelRegistryService = new ModelRegistryService();
