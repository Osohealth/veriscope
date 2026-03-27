UPDATE alert_subscriptions SET secret = encode(gen_random_bytes(32), 'hex') WHERE secret IS NULL;
ALTER TABLE alert_subscriptions ALTER COLUMN secret SET NOT NULL;
