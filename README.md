# Streamhouse OSS

A fully open source **streamhouse**: the vendor-neutral architecture described at
[streamhouse.com](https://www.streamhouse.com), built from Apache-licensed projects plus the
pieces no open source project provides yet.

> Capture, transport, transform, govern and serve the current state of the business continuously,
> so that production applications and AI agents can act on it.

You describe a pipeline in SQL. Streamhouse OSS turns it into CDC connectors, Kafka topics, Flink
jobs, Iceberg tables and real-time context that AI agents query over MCP.

```sql
CREATE SOURCE shop FROM CONNECTION shop_pg TABLES (public.customers, public.orders);

CREATE MATERIALIZED VIEW customer_360 PRIMARY KEY (customer_id) AS
SELECT c.customer_id, c.email, COUNT(o.order_id) AS orders, SUM(o.total) AS lifetime_value
FROM `shop.public.customers` c LEFT JOIN `shop.public.orders` o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.email;

ALTER TOPIC customer_360 ENABLE ICEBERG;     -- continuously maintained Iceberg table
ALTER TOPIC customer_360 ENABLE CONTEXT;     -- millisecond lookups for apps and agents
GRANT SELECT ON CONTEXT customer_360 TO ROLE support_agent;
```

## Architecture

| Stage | Technology | What this project adds |
|---|---|---|
| Capture | Debezium 3.7 on Kafka Connect | `CREATE CONNECTION` / `CREATE SOURCE` render and manage the connectors |
| Transport | Apache Kafka 4.3 (KRaft), Apicurio Registry 3.3 | Topic lifecycle; Avro everywhere in the Confluent wire format |
| Transform | Apache Flink 2.1 + SQL Gateway | `CREATE MATERIALIZED VIEW` becomes a managed Flink job writing an upsert topic |
| Serve: analytics | Apache Iceberg 1.12 on SeaweedFS (S3) | `ENABLE ICEBERG` continuously lands any topic in an Iceberg table (append or upsert) |
| Serve: real time | PostgreSQL 17 | **Context Engine**: an open counterpart of Confluent's Real-Time Context Engine. It materializes topics and answers *lightning queries* over REST and MCP |
| Govern | Apache Gravitino 1.3, Keycloak 26 | OIDC everywhere, `GRANT`s enforced on every query, an audit log, lineage (stored and emitted as OpenLineage) |

Everything is Apache 2.0, except PostgreSQL, which uses the permissive PostgreSQL License.

```
            shctl / REST                                            Claude, apps
                 │                                                  │ MCP, REST
          ┌──────▼────────┐  reconciles   ┌───────────────────────────▼──────────┐
          │ control plane ├──────────────►│ context engine (Postgres serving)    │
          └──┬────┬────┬──┘               └───────────────▲──────────────────────┘
             │    │    │                                  │ materializes topics
  Connect ◄──┘    │    └──► Gravitino (catalogs, lineage) │
     │            ▼                                       │
 Postgres ──► Debezium ──► Kafka ◄──► Flink (views) ──────┤
                             │                            │
                             └──► Flink (topic → Iceberg) ──► Iceberg on S3
```

### Modules

| Path | What it is |
|---|---|
| `sql/` | Parser for the streamhouse DDL |
| `model/` | Resource model shared by all services |
| `control-plane/` | Quarkus service. Stores desired state in Postgres; reconcilers drive Kafka, Connect, Flink, Gravitino and the context engine |
| `context-engine/` | Quarkus service. Exactly-once topic → Postgres materialization, lightning queries (a safe single-table SQL subset compiled with Apache Calcite), MCP server |
| `cli/` | `shctl`: login, `sql -f`, `get`, `describe`, `query` |
| `deploy/compose/` | The whole platform for a laptop (see its [README](deploy/compose/README.md) and [conventions](deploy/compose/CONVENTIONS.md)) |
| `examples/ecommerce/` | Demo shop database, pipeline and order generator ([walkthrough](examples/ecommerce/README.md)) |
| `e2e/` | End-to-end checks against the running platform |

## Quick start

You need Java 21, Maven, and Docker with about 12 GB of memory (Colima, OrbStack or Docker Desktop).

```bash
make demo          # build, start the platform, log in as admin, apply examples/ecommerce/pipeline.sql
make generate      # in another terminal: stream orders into the shop database
bin/shctl get      # every resource should be READY
bin/shctl query "SELECT * FROM customer_360 ORDER BY lifetime_value DESC LIMIT 5"
```

Run `make e2e` to verify the whole flow: freshness, Iceberg, lineage, MCP, grants and audit. Run `make test` for the unit and integration tests.

### Connect an AI agent

The context engine is an MCP server at `http://localhost:8082/mcp`. It exposes `listTopics`, `getMetadata` and `queryData`, the same tool names as Confluent's RTCE. Agents authenticate with Keycloak; the `support-agent` client has the `support_agent` role:

```bash
TOKEN=$(curl -s -d grant_type=client_credentials -d client_id=support-agent -d client_secret=support-agent-secret \
  http://localhost:8180/realms/streamhouse/protocol/openid-connect/token | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
claude mcp add --transport http streamhouse http://localhost:8082/mcp --header "Authorization: Bearer $TOKEN"
```

Then ask: *"What is customer 42's lifetime value, and do they have open orders?"*. Tokens last one hour.

## SQL reference

```sql
CREATE [OR REPLACE] CONNECTION name TYPE POSTGRES WITH (host = '…', port = '5432', database = '…',
    user = '…', password = SECRET 'ref' [, publication = '…'])
CREATE [OR REPLACE] SOURCE name FROM CONNECTION conn TABLES (schema.table, …)
CREATE [OR REPLACE] MATERIALIZED VIEW name PRIMARY KEY (col, …) AS <Flink SQL query>
ALTER TOPIC topic ENABLE ICEBERG [WITH (mode = 'upsert' | 'append')]
ALTER TOPIC topic ENABLE CONTEXT [WITH (mode = 'upsert' | 'append', description = '…')]
ALTER TOPIC topic DISABLE ICEBERG | CONTEXT
GRANT | REVOKE SELECT ON CONTEXT topic TO | FROM ROLE role
DROP CONNECTION | SOURCE | MATERIALIZED VIEW [IF EXISTS] name
SHOW TOPICS | CONNECTIONS | SOURCES | MATERIALIZED VIEWS | ICEBERG TABLES | CONTEXT TABLES | GRANTS
DESCRIBE CONNECTION | SOURCE | MATERIALIZED VIEW | ICEBERG | CONTEXT name
```

Rules and behaviors:
- **Topics:** a source writes one topic per table, `<source>.<schema>.<table>`; a view writes a topic named after itself. In view queries, quote dotted topic names with backticks.
- **Secrets:** `SECRET 'ref'` is resolved from the control plane's environment variable `STREAMHOUSE_SECRET_<REF>`. It is never stored.
- **Statements describe desired state.** The control plane reconciles it continuously, and `shctl sql --wait` blocks until everything is `READY` or `FAILED`. Re-running a script is safe: unchanged resources are left alone, and re-applying a failed one retries it.
- **Roles:** `admin` and `engineer` may change resources; only `admin` may grant. Everyone authenticated may `SHOW` and `DESCRIBE`.
- **Lightning queries:** a single table; `WHERE` with `= <> < <= > >= BETWEEN IN LIKE IS NULL` combined with `AND`/`OR`/`NOT`; `ORDER BY`, `LIMIT` (default 100, max 1000) and `OFFSET`; `COUNT/SUM/AVG/MIN/MAX` without `GROUP BY`. Literals are always bound as parameters. Queries run read-only, with a statement timeout.

## Status: milestone 1

What works, verified by `make e2e` on a laptop:
- CDC from Postgres.
- Materialized views as Flink jobs.
- Topics continuously materialized as Iceberg tables: append and upsert.
- The Context Engine: upsert and append modes, exactly-once offsets, schema evolution by added columns, REST and MCP.
- OIDC, grants, audit and lineage.

Measured freshness from a row committed in Postgres to the context engine is about 1 s.

Known limitations, and what comes next:
- **Kafka:** runs without authentication on the internal network. Next: SASL/OAUTHBEARER with ACLs derived from grants.
- **Grants:** enforced by the context engine. They are not yet pushed into Gravitino's RBAC.
- **Lineage:** stored by the control plane (shown by `DESCRIBE`) and sent to Gravitino as OpenLineage events. Gravitino 1.3 has no API to read lineage back.
- **Context Engine:** one instance. Next: scale out with partition assignment, and pluggable stores (RocksDB, Fluss).
- **Changing a view's query** starts a new job that reprocesses its inputs from the beginning (no savepoint migration).
- **Iceberg schema evolution:** a new column in the topic is not yet added to the Iceberg table.
- **Debezium `numeric`** columns are captured as `double`.
- **Not yet available:** Helm charts, a web UI, column masking and row filters.

## License

[Apache License 2.0](LICENSE)
