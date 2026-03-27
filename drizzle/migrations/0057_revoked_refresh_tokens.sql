CREATE TABLE IF NOT EXISTS revoked_refresh_tokens (
  id VARCHAR(255) PRIMARY KEY DEFAULT gen_random_uuid()::text,
  jti VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  revoked_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS revoked_refresh_tokens_jti ON revoked_refresh_tokens (jti);
CREATE INDEX IF NOT EXISTS revoked_refresh_tokens_expires ON revoked_refresh_tokens (expires_at);
