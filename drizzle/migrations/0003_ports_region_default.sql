ALTER TABLE "ports" ALTER COLUMN "region" SET DEFAULT 'Unknown';
UPDATE "ports" SET "region" = 'Unknown' WHERE "region" IS NULL OR "region" = '';
