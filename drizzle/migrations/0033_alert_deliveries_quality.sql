-- 0033_alert_deliveries_quality.sql
-- P4.3.3.1: alert delivery quality scoring fields

ALTER TABLE alert_deliveries
  ADD COLUMN IF NOT EXISTS quality_score integer,
  ADD COLUMN IF NOT EXISTS quality_band text,
  ADD COLUMN IF NOT EXISTS quality_reasons jsonb,
  ADD COLUMN IF NOT EXISTS quality_version text;
