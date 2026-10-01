-- Internal bookkeeping of the context engine. Materialized tables themselves are created
-- dynamically in this schema, named after their topic.

CREATE TABLE _tables (
    topic             text PRIMARY KEY,
    mode              text NOT NULL CHECK (mode IN ('APPEND', 'UPSERT')),
    description       text NOT NULL DEFAULT '',
    key_columns       jsonb NOT NULL DEFAULT '[]',
    columns           jsonb NOT NULL DEFAULT '[]',
    status            text NOT NULL,
    status_message    text,
    last_record_ts    timestamptz,
    last_ingested_at  timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- Next offset to consume per partition, committed in the same transaction as the data.
CREATE TABLE _offsets (
    topic        text NOT NULL REFERENCES _tables (topic) ON DELETE CASCADE,
    partition    int NOT NULL,
    next_offset  bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);

-- Grants may exist before the table is enabled, so they do not reference _tables.
CREATE TABLE _grants (
    topic  text NOT NULL,
    role   text NOT NULL,
    PRIMARY KEY (topic, role)
);

CREATE TABLE _audit (
    id          bigserial PRIMARY KEY,
    at          timestamptz NOT NULL DEFAULT now(),
    principal   text NOT NULL,
    roles       text[] NOT NULL,
    channel     text NOT NULL,
    topic       text,
    query       text NOT NULL,
    outcome     text NOT NULL,
    row_count   int,
    elapsed_ms  int,
    error       text
);
CREATE INDEX _audit_at ON _audit (at);
