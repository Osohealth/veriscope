ALTER TABLE api_keys
ADD COLUMN IF NOT EXISTS role text NOT NULL DEFAULT 'OWNER';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'api_keys_role_check'
  ) THEN
    ALTER TABLE api_keys
    ADD CONSTRAINT api_keys_role_check
    CHECK (role IN ('OWNER','OPERATOR','VIEWER'));
  END IF;
END $$;
