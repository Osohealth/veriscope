-- 0036_alert_delivery_decisions.sql
-- P4.3.4 Step 1: decision snapshot on deliveries

ALTER TABLE alert_deliveries
  ADD COLUMN IF NOT EXISTS decision jsonb;

CREATE INDEX IF NOT EXISTS alert_deliveries_decision_gin
  ON alert_deliveries
  USING gin (decision);
