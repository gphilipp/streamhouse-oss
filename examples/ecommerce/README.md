# E-commerce demo

A shop database becomes a live customer 360 and inventory view. Analysts get them as Iceberg
tables, and support agents (human or AI) get them as real-time context.

## Data

`shop-schema.sql` seeds the `shop-db` Postgres with:
- 50 customers
- 20 products
- 200 orders and 401 order items
- 20 inventory rows

It also creates the `debezium` replication user and the `streamhouse` publication.

## Pipeline

[`pipeline.sql`](pipeline.sql) declares:

| Statement | Result |
|---|---|
| `CREATE CONNECTION shop_pg …` | Credentials for the shop database (the password is a secret reference) |
| `CREATE SOURCE shop …` | A Debezium connector; topics `shop.public.customers`, `…orders`, `…products`, `…inventory` |
| `CREATE MATERIALIZED VIEW customer_360` | A Flink job: customers joined with their orders, aggregated per customer |
| `CREATE MATERIALIZED VIEW inventory_live` | A Flink job: products joined with stock, with `available` and `low_stock` |
| `ALTER TOPIC … ENABLE ICEBERG` | Iceberg tables `lake.streamhouse.customer_360`, `inventory_live` (upsert) and `shop_public_orders` (append history) |
| `ALTER TOPIC … ENABLE CONTEXT` | Context tables for lightning queries and MCP |
| `GRANT SELECT ON CONTEXT … TO ROLE support_agent` | What the support agent may read |

## Walkthrough

```bash
make demo                         # platform up + pipeline applied (waits until READY)
make generate                     # second terminal: ~2 order events per second
bin/shctl describe view customer_360        # status, job, upstream and downstream lineage
bin/shctl query "SELECT name, orders, lifetime_value, open_orders FROM customer_360 WHERE customer_id = 42"
```

Insert an order and watch it arrive, usually within a second:

```bash
docker compose -f deploy/compose/docker-compose.yml exec shop-db \
  psql -U shop -d shop -c "INSERT INTO orders (customer_id, status, total) VALUES (42, 'open', 99.90)"
bin/shctl query "SELECT orders, open_orders, lifetime_value FROM customer_360 WHERE customer_id = 42"
```

Query the Iceberg tables from the Flink SQL client:

```bash
docker compose -f deploy/compose/docker-compose.yml exec jobmanager ./bin/sql-client.sh
```

Then, in the SQL client, register the catalog and query in batch mode:

```sql
CREATE CATALOG IF NOT EXISTS lake WITH ('type'='iceberg', 'catalog-type'='rest', 'uri'='http://gravitino:9001/iceberg/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', 's3.endpoint'='http://s3:8333', 's3.path-style-access'='true',
  's3.access-key-id'='streamhouse', 's3.secret-access-key'='streamhouse-secret', 'client.region'='us-east-1');
SET 'execution.runtime-mode' = 'batch';
SELECT tier, COUNT(*), SUM(lifetime_value) FROM lake.streamhouse.customer_360 GROUP BY tier;
```

Ask an AI agent. Connect Claude Code as shown in the [main README](../../README.md#connect-an-ai-agent), then try:
- "Which customers have more than one open order?"
- "Which products are low on stock, and how many units are available?"
- "Summarize customer 42: tier, lifetime value, last order."

The `untrusted-agent` client has no grants. Over MCP it sees no tables, and its queries are refused and audited (`serving._audit` in platform-db).
