-- B6: make alert_deliveries.user_id NOT NULL (back-fill nulls first)
UPDATE "alert_deliveries"
SET "user_id" = '00000000-0000-0000-0000-000000000001'
WHERE "user_id" IS NULL;

ALTER TABLE "alert_deliveries"
  ALTER COLUMN "user_id" SET NOT NULL;

-- B7: add tenant_id to watchlists and alert_rules
ALTER TABLE "watchlists"
  ADD COLUMN IF NOT EXISTS "tenant_id" uuid NOT NULL
  DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE "alert_rules"
  ADD COLUMN IF NOT EXISTS "tenant_id" uuid NOT NULL
  DEFAULT '00000000-0000-0000-0000-000000000001';

-- Composite indexes for tenant-scoped queries
CREATE INDEX IF NOT EXISTS "watchlists_tenant_user"
  ON "watchlists" ("tenant_id", "user_id");

CREATE INDEX IF NOT EXISTS "alert_rules_tenant_user"
  ON "alert_rules" ("tenant_id", "user_id");
