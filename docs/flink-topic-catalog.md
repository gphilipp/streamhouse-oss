# Flink topic catalog

`flink-catalog/` is a Flink 2.1 catalog in which **every Kafka topic with a registered value schema is a table**, and in which `CREATE TABLE` creates a topic. It is how topics-as-tables Flink SQL (the dialect used by Confluent Cloud for Apache Flink) runs unchanged on the Apache Flink SQL Gateway of this stack.

| Concept | Here |
|---|---|
| Catalog (Confluent: environment) | `streamhouse` |
| Database (Confluent: Kafka cluster) | `local` |
| Table | Kafka topic + `<topic>-key` / `<topic>-value` schema subjects |

Nothing else is stored: the topic, its `cleanup.policy` and its schemas are the table.

## Setup

The jar (shaded, with relocated Kafka clients) is baked into the Flink image ([deploy/compose/flink/Dockerfile](../deploy/compose/flink/Dockerfile)). Flink's file catalog store registers it in every session from [deploy/compose/flink/catalogs/streamhouse.yaml](../deploy/compose/flink/catalogs/streamhouse.yaml); the store settings are in the `FLINK_PROPERTIES` of [docker-compose.yml](../deploy/compose/docker-compose.yml). Catalog options with the `properties.` prefix are passed to the Kafka clients (e.g. SASL settings).

**Session defaults.** Flink 2.1 cannot make a store-registered catalog the *current* catalog, so new sessions still start in `default_catalog`. Clients must run ``USE CATALOG streamhouse; USE `local`;`` first; that is what Confluent's `sql.current-catalog` / `sql.current-database` statement properties mean. Fully qualified names (`` `streamhouse`.`local`.`orders` ``) also work.

## Inference: existing topics

| Topic | Table |
|---|---|
| `<topic>-value` Avro record | value columns |
| `<topic>-key` Avro record | key columns; if a key field name collides with a value field, all key columns get the `key_` prefix (`key.fields-prefix`) |
| `<topic>-key` is the primitive `"string"` | one `key STRING` column with `key.format = raw` |
| No key subject, or another primitive key schema | one `key BYTES` column with `key.format = raw` |
| Value with `before`/`after`/`op` (Debezium envelope) | the `after` columns, read with `debezium-avro-confluent` as a retract changelog |
| `cleanup.policy` includes `compact` | `upsert` (connector `upsert-kafka`, `PRIMARY KEY` = key columns) |
| Otherwise | `append` (connector `kafka`, `scan.startup.mode = earliest-offset`) |
| Every table | `` `$rowtime` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL `` and `WATERMARK FOR $rowtime AS $rowtime - INTERVAL '0.180' SECOND`; `SELECT *` hides it |

Example: the Debezium topic `shop.public.orders` (key `{order_id}`, flattened value with `order_id`) becomes `key_order_id INT NOT NULL PRIMARY KEY, order_id, customer_id, status, total, created_at, updated_at`.

## CREATE TABLE and CTAS

Supported `WITH` options:
- `changelog.mode`: `append` or `upsert`
- `key.format` and `value.format`: `avro-registry`, or `raw` for keys
- `kafka.cleanup-policy`: `delete`, `compact` or `delete-compact`
- `connector = 'confluent'`, which is accepted and ignored

Any other option is rejected with the list of supported ones.

| Rule | Behaviour |
|---|---|
| Partitions | `DISTRIBUTED BY ... INTO n BUCKETS`, default 6 |
| Key | the `PRIMARY KEY`, else the `DISTRIBUTED BY` columns |
| Changelog mode | `upsert` with a primary key, else `append` |
| Cleanup policy | `compact` for upsert, else `delete`; `kafka.cleanup-policy` overrides (`delete-compact` → `compact,delete`) |
| Value | never repeats the key columns (Confluent's `except-key` default) |
| Raw keys | exactly one key column, named `key`, of type `STRING` or `BYTES` |
| Schemas | registered immediately, so the table is listable before the first write: `<topic>-value`, and `<topic>-key` (an Avro record, or `"string"`/`"bytes"` for a raw key) |

Because a raw key's type is recorded under `<topic>-key` and the column is always named `key`, a raw-keyed upsert table round-trips with its key column, type and primary key, for example:

```sql
CREATE TABLE open_orders_by_customer (PRIMARY KEY (`key`) NOT ENFORCED)
  DISTRIBUTED BY HASH(`key`) INTO 1 BUCKETS
  WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw')
AS SELECT CAST(customer_id AS STRING) AS `key`, COUNT(*) AS open_orders
   FROM `shop.public.orders` WHERE status = 'open' GROUP BY customer_id;
```

`DROP TABLE` deletes the topic and both subjects.

`ALTER TABLE t SET ('changelog.mode' = 'upsert' | 'append')` sets the topic's `cleanup.policy` to `compact` or `delete`. The mode is always derived from the topic, so the change is visible to every session. Upsert needs a key, and Debezium-envelope topics cannot be upsert. Any other option change is rejected.

## Differences from Confluent Cloud

| Topic | Confluent Cloud | Here | Why |
|---|---|---|---|
| Current catalog | Set per statement (`sql.current-catalog`) | `USE CATALOG` needed in each session | Flink 2.1 has no default for store-registered catalogs |
| Retract tables | `changelog.mode = 'retract'` writes an `op` header | Rejected for new tables; Debezium topics are read as retract | Apache Flink's Kafka connector has no header-encoded changelog |
| Debezium topic, compacted | `upsert` | Still read as a retract changelog | `upsert-kafka` cannot decode Debezium envelopes |
| Raw keys | Any column name | The column is named `key` | It is how a raw key's column is found again in later sessions |
| Atomic Avro keys (`"int"`) | Column `key` with `avro-registry` | Raw `key BYTES` | Flink's Avro format only handles record keys |
| Formats | `json-registry`, `proto-registry` | Not supported | Not needed by the demo |
| `$rowtime` | System column, `SOURCE_WATERMARK()` | Virtual metadata column with a fixed 180 ms watermark | Same behaviour for queries |
| Options | `value.fields-include`, `scan.startup.mode`, `kafka.retention.time`, `error-handling.*`, ... | Rejected | Add as needed |
| Key schema compatibility | Key subjects require FULL compatibility | Not enforced | |

### What portable Flink SQL should avoid

- Retract-mode tables. Declare a `PRIMARY KEY` and use `'changelog.mode' = 'upsert'`.
- Raw keys under another name than `key`, and atomic Avro keys. Use record keys or `'key.format' = 'raw'` with one `STRING` column named `key`.
- Options other than the supported list above.
- Relying on the current catalog. Always send `USE CATALOG` / `USE` or qualify names fully.
