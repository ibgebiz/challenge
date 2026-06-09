CREATE TYPE channel AS ENUM ('sms', 'email', 'push');
CREATE TYPE priority AS ENUM ('high', 'normal', 'low');
CREATE TYPE status AS ENUM ('pending', 'queued', 'sending', 'delivered', 'failed', 'cancelled');

CREATE TABLE batches (
    id         uuid PRIMARY KEY,
    total      int NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE templates (
    id         uuid PRIMARY KEY,
    name       text NOT NULL,
    channel    channel NOT NULL,
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  uuid PRIMARY KEY,
    batch_id            uuid REFERENCES batches (id),
    channel             channel NOT NULL,
    recipient           text NOT NULL,
    content             text NOT NULL,
    template_id         uuid REFERENCES templates (id),
    variables           jsonb,
    priority            priority NOT NULL DEFAULT 'normal',
    status              status NOT NULL DEFAULT 'pending',
    idempotency_key     text UNIQUE,
    scheduled_at        timestamptz,
    attempts            int NOT NULL DEFAULT 0,
    last_error          text,
    provider_message_id text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_notifications_status ON notifications (status);
CREATE INDEX idx_notifications_channel ON notifications (channel);
CREATE INDEX idx_notifications_created_at ON notifications (created_at);
CREATE INDEX idx_notifications_batch_id ON notifications (batch_id);

CREATE TABLE delivery_attempts (
    id                uuid PRIMARY KEY,
    notification_id   uuid NOT NULL REFERENCES notifications (id),
    attempt_no        int NOT NULL,
    status            text NOT NULL,
    provider_response jsonb,
    error             text,
    attempted_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_attempts_notification ON delivery_attempts (notification_id);
