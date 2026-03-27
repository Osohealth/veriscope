CREATE TABLE IF NOT EXISTS failed_login_attempts (
  id VARCHAR(255) PRIMARY KEY DEFAULT gen_random_uuid()::text,
  user_id VARCHAR(255),
  email_attempted VARCHAR(255) NOT NULL,
  ip_address VARCHAR(45),
  attempted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS failed_login_attempts_user_window ON failed_login_attempts (user_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS failed_login_attempts_email_window ON failed_login_attempts (email_attempted, attempted_at DESC);
