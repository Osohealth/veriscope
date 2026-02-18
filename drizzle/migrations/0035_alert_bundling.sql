-- 0035_alert_bundling.sql
-- P4.3.3 Step 3: bundle deliveries

ALTER TABLE IF EXISTS "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "is_bundle" boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS "bundle_size" integer NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS "bundle_overflow" integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS "bundle_payload" jsonb;
