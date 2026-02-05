ALTER TABLE alert_subscriptions
  ADD COLUMN scope varchar(20) NOT NULL DEFAULT 'PORT';

UPDATE alert_subscriptions
SET scope = 'PORT'
WHERE scope IS NULL;
