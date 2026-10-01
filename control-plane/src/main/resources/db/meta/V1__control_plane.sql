-- Desired state: one row per resource, stored as the canonical statement that created it.
CREATE TABLE resources (
    kind        text NOT NULL,
    name        text NOT NULL,
    statement   text NOT NULL,
    generation  bigint NOT NULL DEFAULT 1,
    deleted     boolean NOT NULL DEFAULT false,
    created_by  text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, name)
);

-- Observed state, written by the reconcilers.
CREATE TABLE resource_status (
    kind                 text NOT NULL,
    name                 text NOT NULL,
    phase                text NOT NULL,
    message              text,
    observed_generation  bigint NOT NULL DEFAULT 0,
    details              jsonb NOT NULL DEFAULT '{}',
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, name),
    FOREIGN KEY (kind, name) REFERENCES resources (kind, name) ON DELETE CASCADE
);

-- Dataset-level lineage compiled from the resources: from -> to, produced by a resource.
CREATE TABLE lineage_edges (
    source_dataset  text NOT NULL,
    target_dataset  text NOT NULL,
    via_kind        text NOT NULL,
    via_name        text NOT NULL,
    PRIMARY KEY (source_dataset, target_dataset, via_kind, via_name),
    FOREIGN KEY (via_kind, via_name) REFERENCES resources (kind, name) ON DELETE CASCADE
);
CREATE INDEX lineage_edges_target ON lineage_edges (target_dataset);

CREATE TABLE audit (
    id         bigserial PRIMARY KEY,
    at         timestamptz NOT NULL DEFAULT now(),
    principal  text NOT NULL,
    roles      text[] NOT NULL,
    statement  text NOT NULL,
    outcome    text NOT NULL,
    error      text
);
CREATE INDEX audit_at ON audit (at);
