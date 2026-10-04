# Shop assistant: the e-commerce streamhouse

A shop database becomes a live customer 360 and inventory view. Analysts get them as Iceberg
tables, and support agents (human or AI) get them as real-time context.

The demo comes in two forms over the same data:
- **Native:** [`sql/pipeline.sql`](sql/pipeline.sql), streamhouse SQL applied with `shctl` (Streamhouse OSS only).
- **Portable:** [`pipeline.yaml`](pipeline.yaml) and the `shop-assistant` Python app, which run unchanged on **Streamhouse OSS** and on **Confluent Cloud**.

Both create topics named `customer_360` and `inventory_live`, so deploy one at a time on a stack.

## Data

[`sql/shop-schema.sql`](sql/shop-schema.sql) seeds the `shop-db` Postgres with:
- 50 customers
- 20 products
- 200 orders and 401 order items
- 20 inventory rows

It also creates the `debezium` replication user and the `streamhouse` publication.

## Native: streamhouse SQL

| Statement | Result |
|---|---|
| `CREATE CONNECTION shop_pg …` | Credentials for the shop database (the password is a secret reference) |
| `CREATE SOURCE shop …` | A Debezium connector; topics `shop.public.customers`, `…orders`, `…products`, `…inventory` |
| `CREATE MATERIALIZED VIEW customer_360` | A Flink job: customers joined with their orders, aggregated per customer |
| `CREATE MATERIALIZED VIEW inventory_live` | A Flink job: products joined with stock, with `available` and `low_stock` |
| `ALTER TOPIC … ENABLE ICEBERG` | Iceberg tables `lake.streamhouse.customer_360`, `inventory_live` (upsert) and `shop_public_orders` (append history) |
| `ALTER TOPIC … ENABLE CONTEXT` | Context tables for lightning queries and MCP |
| `GRANT SELECT ON CONTEXT … TO ROLE support_agent` | What the support agent may read |

```bash
make demo                                   # build, start the platform, apply sql/pipeline.sql (waits until READY)
make generate                               # second terminal: ~2 order events per second
bin/shctl describe view customer_360        # status, job, upstream and downstream lineage
bin/shctl query "SELECT name, orders, lifetime_value, open_orders FROM customer_360 WHERE customer_id = 42"
```

Insert an order and watch it arrive, usually within a second:

```bash
docker compose -f deploy/compose/docker-compose.yml exec shop-db \
  psql -U shop -d shop -c "INSERT INTO orders (customer_id, status, total) VALUES (42, 'open', 99.90)"
bin/shctl query "SELECT orders, open_orders, lifetime_value FROM customer_360 WHERE customer_id = 42"
```

Query the Iceberg tables from the Flink SQL client. The `lake` catalog is registered in every session:

```bash
docker compose -f deploy/compose/docker-compose.yml exec jobmanager ./bin/sql-client.sh
```

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT tier, COUNT(*), SUM(lifetime_value) FROM lake.streamhouse.customer_360 GROUP BY tier;
```

Ask an AI agent. Connect Claude Code as shown in the [main README](../../README.md#connect-an-ai-agent), then try:
- "Which customers have more than one open order?"
- "Which products are low on stock, and how many units are available?"
- "Summarize customer 42: tier, lifetime value, last order."

The `untrusted-agent` client has no grants. Over MCP it sees no tables, and its queries are refused and audited (`serving._audit` in platform-db).

## Portable: one app, two platforms

Pick the platform with `--env`:

```bash
shop-assistant --env oss provision          # or: --env confluent
shop-assistant --env oss simulate --duration 60
shop-assistant --env oss analytics
shop-assistant --env oss ask "Which customers have more than one open order?"
```

| Step | What happens | Same on both platforms |
|---|---|---|
| `provision` | Postgres CDC → Flink SQL (`customer_360`, `inventory_live`) → Iceberg tables → real-time context for agents | `pipeline.yaml`, including the Flink SQL text |
| `simulate` | Places, ships and cancels orders and moves stock in the source database | Everything |
| `analytics` | Reads the Iceberg tables through the Iceberg REST catalog and reports with DuckDB | Everything except the catalog address and credentials |
| `ask` | A Claude support agent answers from live context through the MCP tools | Everything except the MCP address and API key |

### How portability works

The **data plane** uses standard interfaces only:
- Flink SQL in Confluent's dialect, where Kafka topics are tables and `CREATE TABLE … AS SELECT` creates a topic. Streamhouse OSS provides the same model through its topic catalog.
- The Iceberg REST catalog, read with DuckDB's Iceberg extension (which handles the equality deletes of upsert tables).
- MCP over streamable HTTP, authenticated with an API key and secret sent as HTTP Basic auth.

The **control plane** differs, so it sits behind a small interface (`shop_assistant/platforms/base.py`) with one implementation per platform:

| Operation | Streamhouse OSS | Confluent Cloud |
|---|---|---|
| CDC | `CREATE CONNECTION` / `CREATE SOURCE` (Debezium) | Managed `PostgresCdcSourceV2` connector (Connect API) |
| Flink statements | ``CREATE STATEMENT `name` AS <sql>`` | Flink SQL statements API |
| Iceberg tables | `ALTER TOPIC t ENABLE ICEBERG` | Iceberg tables API |
| Real-time context | `ALTER TOPIC t ENABLE CONTEXT` + `GRANT` | Context engine API |

`pipeline.yaml` follows the stricter platform's rules, so it is valid on both:
- **Context topics:** names use `[a-zA-Z][a-zA-Z0-9_]*`, with raw string keys in a column named `key`.
- **Agent queries:** single-table filters only, no aggregates.
- **CDC topics:** read as upsert changelogs with `ALTER TABLE … SET ('changelog.mode' = 'upsert')`.

### Setup

```bash
cd demos/shop-assistant
python3 -m venv .venv && .venv/bin/pip install -e .
export ANTHROPIC_API_KEY=...        # or: ant auth login
```

**Streamhouse OSS.** Run `make build up` at the repository root. `config/oss.env` already points at the local stack and its shop database, which is seeded.

**Confluent Cloud.**
1. Copy `config/confluent.env.example` to `config/confluent.env` and fill it in. The file is git-ignored.
2. The source database must be reachable from Confluent Cloud, with logical replication enabled. For example, use AWS RDS with `rds.logical_replication=1`, Neon or Supabase.
3. Create and seed the tables with `shop-assistant --env confluent init-db`.
4. Give `SOURCE_PG_CDC_USER` replication rights and `SELECT` on the shop tables. The connector creates its own publication (`publication.autocreate.mode=filtered`).

### Expected differences

- **Iceberg freshness:** Confluent commits Iceberg snapshots every few minutes; Streamhouse OSS commits on every Flink checkpoint (10 s).
- **Iceberg metadata columns:** the platforms add different metadata columns to Iceberg tables. The reports select pipeline columns only.
- **MCP tool arguments:** the three tools have the same names on both platforms. Their argument schemas come from each server, and the agent passes them to Claude unchanged.
