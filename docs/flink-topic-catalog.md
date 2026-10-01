# Flink topic catalog

`flink-catalog/` is a Flink 2.1 catalog in which **every Kafka topic with a registered value schema is a table**, and in which `CREATE TABLE` creates a topic. It is how topics-as-tables Flink SQL (the dialect used by Confluent Cloud for Apache Flink) runs unchanged on the Apache Flink SQL Gateway of this stack.

| Concept | Here |
|---|---|
| Catalog (Confluent: environment) | `streamhouse` |
| Database (Confluent: Kafka cluster) | `local` |
| Table | Kafka topic + `<topic>-key` / `<topic>-value` Avro subjects |

## Setup

The jar (shaded, with relocated Kafka clients) is baked into the Flink image (`deploy/compose/flink/Dockerfile`). Flink's file catalog store registers the catalog in every session, so no DDL is needed:

```
table.catalog-store.kind: file
table.catalog-store.file.path: /opt/flink/catalogs          # contains streamhouse.yaml
table.column-expansion-strategy: EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS   # SELECT * skips $rowtime
```

`deploy/compose/flink/catalogs/streamhouse.yaml`:

```yaml
type: streamhouse-kafka
bootstrap.servers: kafka:9092
schema-registry.url: http://apicurio:8080/apis/ccompat/v7
default-database: local
default.partitions: "6"
default.replication-factor: "1"
```

Options with the `properties.` prefix are passed to the Kafka clients (e.g. SASL settings).

**Session defaults.** Flink 2.1 cannot make a store-registered catalog the *current* catalog, so new sessions still start in `default_catalog`. Clients must run `USE CATALOG streamhouse; USE `local`;` first. That is what Confluent's `sql.current-catalog` / `sql.current-database` statement properties mean, so the statement facade maps them to these two statements. Fully qualified names (`` `streamhouse`.`local`.`orders` ``) also work.

## Inference: existing topics

| Topic | Table |
|---|---|
| `<topic>-value` Avro record | value columns |
| `<topic>-key` Avro record | key columns; if a key field name collides with a value field, all key columns get the `key_` prefix (`key.fields-prefix`) |
| No key subject, or a primitive key schema | one `key BYTES` column with `key.format = raw` |
| Value with `before`/`after`/`op` (Debezium envelope) | the `after` columns, read with `debezium-avro-confluent` as a retract changelog |
| `cleanup.policy` includes `compact` | `upsert` (connector `upsert-kafka`, `PRIMARY KEY` = key columns) |
| Otherwise | `append` (connector `kafka`, `scan.startup.mode = earliest-offset`) |
| Every table | `` `$rowtime` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL `` and `WATERMARK FOR $rowtime AS $rowtime - INTERVAL '0.180' SECOND` |

Example: the Debezium topic `shop.public.orders` (key `{order_id}`, flattened value with `order_id`) becomes `key_order_id INT NOT NULL PRIMARY KEY, order_id, customer_id, status, total, created_at, updated_at`.

## CREATE TABLE and CTAS

Supported `WITH` options:
- `changelog.mode`: `append` or `upsert`
- `key.format` and `value.format`: `avro-registry`, or `raw` for keys
- `value.fields-include`: `all` or `except-key`
- `key.fields-prefix`
- `scan.startup.mode`
- `kafka.cleanup-policy`
- `kafka.retention.time`
- `connector = 'confluent'`, which is accepted and ignored

Any other option is rejected with the list of supported ones.

| Rule | Behaviour |
|---|---|
| Partitions | `DISTRIBUTED BY ... INTO n BUCKETS`, default 6 |
| Key | the `PRIMARY KEY`, else the `DISTRIBUTED BY` columns |
| Changelog mode | `upsert` with a primary key, else `append` |
| Cleanup policy | `compact` for upsert, else `delete`; `kafka.cleanup-policy` overrides (`delete-compact` → `compact,delete`) |
| Value | `except-key` by default (key columns are not repeated in the value) |
| Schemas | registered immediately (`<topic>-value`, and `<topic>-key` for Avro keys), so the table is listable before the first write |
| Raw keys | exactly one `STRING`/`BYTES` key column |

What the topic and registry cannot express is stored in the compacted topic `_streamhouse.flink-tables`, keyed by table name: raw key columns, key prefix, value inclusion and startup mode. This is how a raw-keyed upsert table round-trips with its key column name, type and primary key.

`DROP TABLE` deletes the topic, both subjects and the stored definition.

`ALTER TABLE t SET ('changelog.mode' = 'upsert' | 'append')` sets the topic's `cleanup.policy` to `compact` or `delete`. The mode is always derived from the topic, so the change is visible to every session. Upsert needs a key, and Debezium-envelope topics cannot be upsert. Any other option change is rejected.

## Verified statements

All of these ran on the compose stack in fresh SQL Gateway sessions, through the REST API, with no other DDL.

```sql
USE CATALOG streamhouse;
USE `local`;
SHOW TABLES;              -- customer_360, inventory_live, shop.public.customers, shop.public.orders, ...
SELECT order_id, status, total FROM `shop.public.orders` LIMIT 3;

CREATE TABLE open_orders_by_customer (PRIMARY KEY (customer_key) NOT ENFORCED)
  DISTRIBUTED BY HASH(customer_key) INTO 1 BUCKETS
  WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw')
AS SELECT CAST(customer_id AS STRING) AS customer_key, COUNT(*) AS open_orders
   FROM `shop.public.orders` WHERE status = 'open' GROUP BY customer_id;
-- topic: 1 partition, cleanup.policy=compact; subject open_orders_by_customer-value only (raw key)
-- a second session: DESCRIBE shows customer_key STRING NOT NULL PRI, open_orders BIGINT; SELECT returns rows

CREATE TABLE alter_test (id INT, s STRING) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS;   -- append, Avro key id
ALTER TABLE alter_test SET ('changelog.mode' = 'upsert');   -- cleanup.policy=compact, PRIMARY KEY (id)
ALTER TABLE alter_test SET ('kafka.retention.time' = '1 d'); -- rejected: only 'changelog.mode' can be changed
ALTER TABLE `shop.public.customers` SET ('changelog.mode' = 'append');  -- also works on inferred tables
DROP TABLE open_orders_by_customer;                      -- topic and subjects removed
```

## Differences from Confluent Cloud

| Topic | Confluent Cloud | Here | Why |
|---|---|---|---|
| Current catalog | Set per statement (`sql.current-catalog`) | `USE CATALOG` needed in each session | Flink 2.1 has no default for store-registered catalogs |
| Retract tables | `changelog.mode = 'retract'` writes an `op` header | Rejected for new tables; Debezium topics are read as retract | Apache Flink's Kafka connector has no header-encoded changelog |
| Debezium topic, compacted | `upsert` | Still read as a retract changelog | `upsert-kafka` cannot decode Debezium envelopes |
| Atomic Avro keys (`"int"`) | Column `key` with `avro-registry` | Raw `key BYTES` | Flink's Avro format only handles record keys |
| Formats | `json-registry`, `proto-registry` | Not supported | Not needed by the demo |
| `$rowtime` | System column, `SOURCE_WATERMARK()` | Virtual metadata column with a fixed 180 ms watermark; `SELECT *` hides it through the column-expansion strategy | Same behaviour for queries |
| Unsupported options | `error-handling.*`, `kafka.compaction.time`, `scan.bounded.*`, ... | Rejected | Add as needed |
| Key schema compatibility | Key subjects require FULL compatibility | Not enforced | |

### What portable Flink SQL should avoid

- Retract-mode tables. Declare a `PRIMARY KEY` and use `'changelog.mode' = 'upsert'`.
- Atomic Avro keys. Use record keys or `'key.format' = 'raw'` with one `STRING` column.
- Options other than the supported list above.
- Relying on the current catalog. Always send `USE CATALOG` / `USE` (the statement facade does this) or qualify names fully.
