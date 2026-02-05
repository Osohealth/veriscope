ALTER TABLE "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "is_test" boolean NOT NULL DEFAULT false;
