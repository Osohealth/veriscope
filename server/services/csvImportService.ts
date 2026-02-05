import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import { db } from '../db';
import { 
  refineryUnits, 
  refineryUtilizationDaily, 
  refineryCrackSpreadsDaily,
  sdModelsDaily,
  sdForecastsWeekly,
  researchInsightsDaily 
} from '@shared/schema';

const DEFAULT_CSV_DATA_PATH = path.join(process.cwd(), 'attached_assets', 'extracted_data');
const CSV_DATA_PATH = process.env.CSV_DATA_PATH || DEFAULT_CSV_DATA_PATH;

function readCSV(filename: string): any[] {
  const filePath = path.join(CSV_DATA_PATH, filename);
  if (!fs.existsSync(filePath)) {
    console.warn(`[CSV IMPORT] Missing file: ${filePath}. Skipping.`);
    return [];
  }
  const fileContent = fs.readFileSync(filePath, 'utf-8');
  return parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
  });
}

export async function importRefineryUnits() {
  try {
    const records = readCSV('refinery_units.csv');
    
    for (const record of records) {
      try {
        await db.insert(refineryUnits).values({
          plant: record.plant,
          unit: record.unit,
          nameplateBpd: parseInt(record.nameplate_bpd),
        });
      } catch (err: any) {
        // Skip duplicates silently
        if (!err.message?.includes('duplicate')) {
          console.error('Error inserting refinery unit:', err.message);
        }
      }
    }
    
    console.log(`Imported ${records.length} refinery units`);
  } catch (error) {
    console.error('Error importing refinery units:', error);
  }
}

export async function importRefineryUtilization() {
  try {
    const records = readCSV('refinery_utilization.csv');
    
    for (const record of records) {
      try {
        await db.insert(refineryUtilizationDaily).values({
          date: record.date,
          plant: record.plant,
          utilizationPct: record.utilization_pct,
        });
      } catch (err: any) {
        if (!err.message?.includes('duplicate')) {
          console.error('Error inserting utilization:', err.message);
        }
      }
    }
    
    console.log(`Imported ${records.length} refinery utilization records`);
  } catch (error) {
    console.error('Error importing refinery utilization:', error);
  }
}

export async function importRefineryCrackSpreads() {
  try {
    const records = readCSV('refinery_crack_spreads.csv');
    
    for (const record of records) {
      try {
        await db.insert(refineryCrackSpreadsDaily).values({
          date: record.date,
          spread321Usd: record['3_2_1_spread_usd'],
          gasolineUsd: record.gasoline_usd,
          dieselUsd: record.diesel_usd,
          crudeUsd: record.crude_usd,
        });
      } catch (err: any) {
        if (!err.message?.includes('duplicate')) {
          console.error('Error inserting crack spreads:', err.message);
        }
      }
    }
    
    console.log(`Imported ${records.length} refinery crack spreads records`);
  } catch (error) {
    console.error('Error importing refinery crack spreads:', error);
  }
}

export async function importSdModelsDaily() {
  try {
    const records = readCSV('sd_models_daily.csv');
    
    for (const record of records) {
      try {
        await db.insert(sdModelsDaily).values({
          date: record.date,
          region: record.region,
          supplyMt: parseInt(record.supply_mt),
          demandMt: parseInt(record.demand_mt),
          balanceMt: parseInt(record.balance_mt),
        });
      } catch (err: any) {
        if (!err.message?.includes('duplicate')) {
          console.error('Error inserting S&D model:', err.message);
        }
      }
    }
    
    console.log(`Imported ${records.length} S&D models daily records`);
  } catch (error) {
    console.error('Error importing S&D models daily:', error);
  }
}

export async function importSdForecastsWeekly() {
  try {
    const records = readCSV('sd_forecasts_weekly.csv');
    
    for (const record of records) {
      try {
        await db.insert(sdForecastsWeekly).values({
          weekEnd: record.week_end,
          region: record.region,
          balanceForecastMt: parseInt(record.balance_forecast_mt),
        });
      } catch (err: any) {
        if (!err.message?.includes('duplicate')) {
          console.error('Error inserting S&D forecast:', err.message);
        }
      }
    }
    
    console.log(`Imported ${records.length} S&D forecasts weekly records`);
  } catch (error) {
    console.error('Error importing S&D forecasts weekly:', error);
  }
}

export async function importResearchInsightsDaily() {
  try {
    const records = readCSV('research_insights_daily.csv');
    
    for (const record of records) {
      try {
        await db.insert(researchInsightsDaily).values({
          date: record.date,
          title: record.title,
          summary: record.summary,
          impactScore: record.impact_score,
        });
      } catch (err: any) {
        if (!err.message?.includes('duplicate')) {
          console.error('Error inserting research insight:', err.message);
        }
      }
    }
    
    console.log(`Imported ${records.length} research insights daily records`);
  } catch (error) {
    console.error('Error importing research insights daily:', error);
  }
}

export async function importAllCSVData() {
  console.log('Starting CSV data import...');
  console.log(`[CSV IMPORT] Using CSV_DATA_PATH=${CSV_DATA_PATH}`);
  
  await importRefineryUnits();
  await importRefineryUtilization();
  await importRefineryCrackSpreads();
  await importSdModelsDaily();
  await importSdForecastsWeekly();
  await importResearchInsightsDaily();
  
  console.log('CSV data import completed');
}
