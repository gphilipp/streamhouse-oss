# Shop assistant: one streamhouse demo, two platforms

The same application runs on **Streamhouse OSS** (this repository) and on **Confluent Cloud**. Pick the platform with `--env`:

```bash
shop-assistant --env oss provision          # or: --env confluent
shop-assistant --env oss simulate --duration 60
shop-assistant --env oss analytics
shop-assistant --env oss ask "Which customers have more than one open order?"
```

It runs the e-commerce streamhouse end to end:

| Step | What happens | Same on both platforms |
|---|---|---|
| `provision` | Postgres CDC → Flink SQL (`customer_360`, `inventory_live`) → Iceberg tables → real-time context for agents | `pipeline.yaml`, including the Flink SQL text |
| `simulate` | Places, ships and cancels orders and moves stock in the source database | Everything |
| `analytics` | Reads the Iceberg tables through the Iceberg REST catalog and reports with DuckDB | Everything except the catalog address and credentials |
| `ask` | A Claude support agent answers from live context through the MCP tools | Everything except the MCP address and API key |

## How portability works

The **data plane** uses standard interfaces only:
- Flink SQL in Confluent's dialect, where Kafka topics are tables and `CREATE TABLE … AS SELECT` creates a topic. Streamhouse OSS provides the same model through its topic catalog.
- The Iceberg REST catalog, read with PyIceberg.
- MCP over streamable HTTP, authenticated with an API key and secret sent as HTTP Basic auth.

The **control plane** differs, so it sits behind a small interface (`shop_assistant/platforms/base.py`) with one implementation per platform:

| Operation | Streamhouse OSS | Confluent Cloud |
|---|---|---|
| CDC | `CREATE CONNECTION` / `CREATE SOURCE` (Debezium) | Managed `PostgresCdcSourceV2` connector (Connect API) |
| Flink statements | `CREATE STATEMENT name AS <sql>` | Flink SQL statements API |
| Iceberg tables | `ALTER TOPIC t ENABLE ICEBERG` | Iceberg tables API |
| Real-time context | `ALTER TOPIC t ENABLE CONTEXT` + `GRANT` | Context engine API |

`pipeline.yaml` follows the stricter platform's rules, so it is valid on both:
- **Context topics:** names use `[a-zA-Z][a-zA-Z0-9_]*`, with raw string keys.
- **Agent queries:** single-table filters only, no aggregates.
- **CDC topics:** read as upsert changelogs with `ALTER TABLE … SET ('changelog.mode' = 'upsert')`.

## Setup

```bash
cd demos/shop-assistant
python3 -m venv .venv && .venv/bin/pip install -e .
export ANTHROPIC_API_KEY=...        # or: ant auth login
```

**Streamhouse OSS.** Run `make up` at the repository root. `config/oss.env` already points at the local stack and its shop database, which is seeded.

**Confluent Cloud.**
1. Copy `config/confluent.env.example` to `config/confluent.env` and fill it in. The file is git-ignored.
2. The source database must be reachable from Confluent Cloud, with logical replication enabled. For example, use AWS RDS with `rds.logical_replication=1`, Neon or Supabase.
3. Create and seed the tables with `shop-assistant --env confluent init-db`.
4. Give `SOURCE_PG_CDC_USER` replication rights and `SELECT` on the shop tables. The connector creates its own publication (`publication.autocreate.mode=filtered`).

## Expected differences

- **Iceberg freshness:** Confluent commits Iceberg snapshots every few minutes; Streamhouse OSS commits on every Flink checkpoint (10 s).
- **Iceberg metadata columns:** the platforms add different metadata columns to Iceberg tables. The reports select pipeline columns only.
- **MCP tool arguments:** the three tools have the same names on both platforms. Their argument schemas come from each server, and the agent passes them to Claude unchanged.
