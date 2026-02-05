ALTER TABLE "signals"
  ADD COLUMN IF NOT EXISTS "confidence_score" double precision,
  ADD COLUMN IF NOT EXISTS "confidence_band" text,
  ADD COLUMN IF NOT EXISTS "method" text,
  ADD COLUMN IF NOT EXISTS "cluster_id" text,
  ADD COLUMN IF NOT EXISTS "cluster_type" text,
  ADD COLUMN IF NOT EXISTS "cluster_summary" text;
