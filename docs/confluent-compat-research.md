# Confluent Cloud dialect: compatibility research (snapshot 2026-10-01)

Goal: one demo application runs against Confluent Cloud and the OSS streamhouse (Kafka + Apicurio ccompat SR + Flink 2.1 + Iceberg REST + Postgres "context engine" + MCP) with only endpoint/credential changes.

Sources: docs.confluent.io pages (most also published as `.md` by swapping `.html` for `.md`) and the aggregate OpenAPI spec `https://docs.confluent.io/cloud/current/openapi.yaml` (fetched 2026-10-01; cited as "OpenAPI" with schema/operation names). Anything not stated in those sources is marked **UNVERIFIED**.

---

## 1. Confluent Cloud for Apache Flink SQL dialect

### 1.1 Topics as tables (metadata mapping)

Source: https://docs.confluent.io/cloud/current/flink/overview.html#metadata-mapping-between-kafka-cluster-topics-schemas-and-flink

| Kafka | Flink | Notes (quoted) |
|---|---|---|
| Environment | Catalog | "Flink can query and join data that are in any environments/catalogs" |
| Cluster | Database | "Flink can query and join data that are in different clusters/databases" |
| Topic + Schema | Table | "You never need to declare tables manually for existing topics. Creating a table in Flink creates a topic and the associated schema." |

- "Any topic created in Kafka is visible directly as a table in Flink, and any table created in Flink is visible as a topic in Kafka."
- DDL "act[s] on physical objects and not only on metadata" (CREATE TABLE creates topic + SR subjects immediately).
- Catalog name: `USE CATALOG catalog_name;` "The `catalog_name` parameter is case-sensitive." "The default current catalog is named `default`." USE CATALOG / USE are "client-side setting statement[s]" that set request properties; "By itself, a USE CATALOG statement is a no-op." (https://docs.confluent.io/cloud/current/flink/reference/statements/use-catalog.html, .../use-database.html)
- Names vs IDs: INFORMATION_SCHEMA exposes `CATALOG_ID` (e.g. `env-xmzdkk`) and `CATALOG_NAME` (e.g. `default`), `SCHEMA_ID` (e.g. `lkc-kgjwwv`) and schema name (https://docs.confluent.io/cloud/current/flink/reference/flink-sql-information-schema.html). Docs examples use display names (`USE CATALOG my_env; USE cluster_0;`) and the Tableflow doc says a cluster name or ID works. Whether both env ID and display name are accepted in `USE CATALOG` / fully-qualified identifiers: **UNVERIFIED** (only display names shown in USE examples; `sql.current-database` described as "Semantically equivalent with USE [database_id]").
- Fully-qualified form used in docs: `` `<your-environment>`.`<your-kafka-cluster>`.`users` `` (deduplicate-rows how-to) and SHOW CREATE TABLE prints `` CREATE TABLE `catalog`.`database`.`orders` ``.
- Identifiers (https://docs.confluent.io/cloud/current/flink/reference/sql-syntax.html): case-sensitive by default; unquoted identifiers may contain only letters, digits, underscore; anything else (e.g. topic `table-with-dashes`, keywords like `value`, `count`) must be backticked: ``SELECT * FROM `table-with-dashes`;``. Unquoted dash gives `SQL parse failed. Encountered "-" ...`.

### 1.2 Inferred tables (existing topics)

Source: https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#inferred-tables

- No SR key/value: columns `` `key` VARBINARY(2147483647), `val` VARBINARY(2147483647) ``, `'key.format'='raw'`, `'value.format'='raw'`, `'changelog.mode'='append'`.
- No SR key, SR value record: `` `key` VARBINARY `` + value fields; `key.format=raw`, `value.format=avro-registry`; `DISTRIBUTED BY HASH(`key`) INTO <partitions> BUCKETS`.
- Atomic SR key (`"int"`): column named `key` (SR provides no name), `key.format=avro-registry`.
- Name collision (value has a field `key`): raw key column becomes `key_key`, `'key.fields-prefix'='key_'`. "The prefix for an inferred table is `key_`, for non-atomic Schema Registry types and fields that have a name."
- Inferred changelog mode: `append` (uncompacted, not Debezium), `upsert` (if compacted), `retract` (Debezium envelope detected and uncompacted).
- Inferred key formats: `raw` (no SR entry), `avro-registry`, `json-registry`, `proto-registry`. Inferred value formats additionally include `avro-debezium-registry`, `json-debezium-registry`, `proto-debezium-registry`.

### 1.3 CREATE TABLE

Source: https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html

- "creates a table backed by an Apache Kafka® topic, along with the corresponding key and value schemas in Schema Registry." Default: "registered as append-only, uses AVRO serializers, and reads from the earliest offset."
- Partitions: no `kafka.partitions` option exists. Partition count comes from `DISTRIBUTED BY (cols) INTO n BUCKETS`; "Kafka partitions map 1:1 to SQL buckets... If n is not defined, the default is 6." `PARTITIONED BY` is deprecated in favour of `DISTRIBUTED BY`.
- Default distribution when no bucket key: "round robin for append, and hash-by-row for retract." "For upsert mode, the bucket key must be equal to primary key."
- PRIMARY KEY: only `NOT ENFORCED`; makes columns NOT NULL; "distributes the table implicitly by the key column"; creates a key schema. Example `CREATE TABLE t_pk (k INT PRIMARY KEY NOT ENFORCED, s STRING);` has properties "Upsert changelog mode... `k` is the Schema Registry key... 6 Kafka partitions" (https://docs.confluent.io/cloud/current/flink/reference/sql-examples.html).
- Minimal table `CREATE TABLE t_minimal (s STRING);`: "Append changelog mode. No Schema Registry key. Round-robin distribution. 6 Kafka partitions. The `$rowtime` column and system watermark are added implicitly."
- An append table rejects updating queries: `INSERT INTO t_changelog_modes SELECT COUNT(*) ...` "does not work because the query is updating, causing an error".
- Schema Registry subject naming: TopicNameStrategy, e.g. "share the Schema Registry subjects `t_shared_schema-key` and `t_shared_schema-value`". Key-format SR compatibility: "The Schema Registry subject compatibility mode must be FULL or FULL_TRANSITIVE."
- Wire format: `key|value.<format>.id-encoding` default `payload` = "5-byte Confluent wire-format prefix (magic byte + 4-byte schema ID)"; `header` puts the ID in a record header. On read Flink checks header, then payload, then falls back to the registered schema.

WITH options (complete list on the page) and documented defaults:

| Option | Values | Default |
|---|---|---|
| `changelog.mode` | `append`/`upsert`/`retract` | append (no PK); upsert with PK (per sql-examples) |
| `connector` | `confluent`, `faker`, external (`confluent-jdbc`, `mongodb`, `elastic`, `pinecone`, ...) | `confluent` |
| `error-handling.mode` | `fail`/`ignore`/`log` | `fail` |
| `error-handling.log.target` | DLQ table | `error_log` |
| `kafka.cleanup-policy` | `delete`/`compact`/`delete-compact` | "compact for upsert tables, delete for all other tables" |
| `kafka.compaction.time` | duration | 7 days for upsert tables |
| `kafka.consumer.isolation-level` | `read-committed`/`read-uncommitted` | `read-committed` |
| `kafka.max-message-size` | MemorySize | 2097164 bytes |
| `kafka.message-timestamp-type` | `CreateTime`/`LogAppendTime` | `CreateTime` |
| `kafka.producer.compression.type` | none/gzip/snappy/lz4/zstd | none |
| `kafka.retention.size` | MemorySize | 0 |
| `kafka.retention.time` | duration (`0` = infinite) | Option table says "0 (infinite retention)"; the same page's example says "By default, the retention time is 7 days" and SHOW CREATE TABLE output shows `'kafka.retention.time' = '604800000 ms'`. Treat 7 days as observed default (**doc conflict**). |
| `key.fields-prefix` | string | `""` |
| `key.format` | `avro-registry`/`json-registry`/`proto-registry` (`raw` for inferred) | `avro-registry` ("only if a primary or distribution key is defined") |
| `key.<format>.id-encoding` / `value.<format>.id-encoding` | `payload`/`header` | `payload` |
| `key.<format>.schema-context` / `value.<format>.schema-context` | `.ctx` | none |
| `late-handling.mode` | `pass-through`/`filter` | `pass-through` |
| `scan.bounded.mode` | `latest-offset`/`timestamp`/`unbounded` | `unbounded` |
| `scan.bounded.timestamp-millis` | long | none |
| `scan.startup.mode` | `earliest-offset`/`latest-offset`/`timestamp`/`specific-offsets`/`group-offsets` | `earliest-offset` ("differs from the default in Apache Flink, which is group-offsets") |
| `scan.startup.specific-offsets` | `'partition:0,offset:42;partition:1,offset:300'` | none |
| `scan.startup.timestamp-millis` | long | none |
| `value.fields-include` | `all`/`except-key` | `except-key` ("By default, the key is never included in the value in Schema Registry") |
| `value.format` | `avro-registry`/`json-registry`/`proto-registry` (+ `*-debezium-registry` for inferred) | `avro-registry` |

Note: a docs SHOW CREATE TABLE example shows an upsert table with `'kafka.cleanup-policy' = 'delete'` (sql-examples.html, schema-context example), so cleanup policy is not forced by upsert mode.

Changelog encoding on Kafka (create-table.html "Primary key interaction" table):
- append: "Each value is an insertion (+I)."
- retract: "A special op header represents the change (+I, -U, +U, -D). The header is omitted for insertions." Header values: `0`=+I, `1`=-U, `2`=+U, `3`=-D; default 0.
- upsert: "If value is null, it represents a deletion (-D). Other values are +U"; PK mandatory, hash by PK.

### 1.4 CREATE TABLE AS SELECT (CTAS)

Source: create-table.html#create-table-as-select-ctas

- `CREATE TABLE my_ctas_table AS SELECT id, name, age FROM source_table WHERE mod(id, 10) = 0;` is documented as equivalent to `CREATE TABLE my_ctas_table (id BIGINT, name STRING, age INT);` + `INSERT INTO ... SELECT ...`.
- Options in WITH before AS: `CREATE TABLE t WITH ('scan.startup.mode' = 'latest-offset') AS SELECT * FROM b;`
- Explicit columns in the CREATE part come first, then SELECT columns; types can be overridden; computed columns and WATERMARK allowed in the CREATE part.
- PK/distribution: "Primary keys work only on NOT NULL columns. Currently, primary keys only allow you to define columns from the SELECT part". Documented example:
  ```sql
  CREATE TABLE my_ctas_table (
      PRIMARY KEY (id) NOT ENFORCED
  ) DISTRIBUTED BY HASH(id) INTO 4 BUCKETS
  AS SELECT id, name FROM source_table;
  ```
- Console-generated dedup CTAS sets mode explicitly (https://docs.confluent.io/cloud/current/flink/how-to-guides/deduplicate-rows.html):
  ```sql
  CREATE TABLE `<env>`.`<cluster>`.`users_deduplicate` (
         PRIMARY KEY (`user_id`) NOT ENFORCED
  ) DISTRIBUTED BY HASH(`user_id`) WITH (
         'changelog.mode' = 'upsert',
         'value.format'='avro-registry',
         'key.format'='avro-registry'
  ) AS SELECT ...
  ```
- Whether CTAS over an *updating* query without explicit `changelog.mode` auto-selects upsert/retract ("leverages the planner to choose... the changelog mode"): **UNVERIFIED** (seen only in a search-engine summary, not in fetched page text). Safe portable form: always declare PRIMARY KEY + `'changelog.mode'='upsert'` explicitly.

### 1.5 Reading Debezium CDC topics

Source: https://docs.confluent.io/cloud/current/flink/reference/serialization.html#debezium-format and create-table.html

- Formats: `avro-debezium-registry`, `json-debezium-registry`, `proto-debezium-registry`. Read-only (Flink cannot write Debezium). Requires SR schema with `after`, `before`, `op`.
- Auto-detection "For schemas created after May 19, 2025 at 09:00 UTC": `value.format` defaults to `*-debezium-registry`, `changelog.mode` defaults to `retract`; "If the Kafka topic has `cleanup.policy` set to `compact`, the `changelog.mode` is set to `upsert` instead." Table schema = fields of `after`.
- Example inferred table: `` `key` VARBINARY ``, `id`, `name`, `email`; `DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS`; `'changelog.mode'='retract'`, `'key.format'='raw'`, `'value.format'='avro-debezium-registry'`.
- Modes: append = "Handles all create, read, and update events as INSERT operations. Delete events are ignored." retract = c/r -> INSERT, u -> UPDATE_BEFORE+UPDATE_AFTER, d -> DELETE; "Retract mode is not compatible with the `after.state.only` option"; Postgres needs `REPLICA IDENTITY FULL`. upsert = c/r/u -> UPDATE_AFTER, d -> DELETE by PK.
- Manual override: `ALTER TABLE t SET ('value.format'='avro-debezium-registry','changelog.mode'='retract');`

### 1.6 System columns and watermarks

- "`$rowtime TIMESTAMP_LTZ(3) NOT NULL` is provided as a system column" = Kafka record timestamp. Not in `SELECT *`, DESCRIBE, SHOW CREATE TABLE; shown by DESCRIBE EXTENDED. Read-only; to write the timestamp use `METADATA FROM 'timestamp'`.
- Default watermark on every table: `WATERMARK FOR $rowtime AS SOURCE_WATERMARK()`, per-partition, "fixed out-of-orderness tolerance of 180 milliseconds".
- Progressive idleness: starts at 10 s, grows to 5 min; `sql.tables.scan.idle-timeout` (0 disables).
- Metadata columns: `headers` (MAP, rw), `leader-epoch`, `offset`, `partition`, `raw-key`, `raw-value`, `timestamp` (rw), `timestamp-type`, `topic`.

### 1.7 Statement properties (SET)

Source: https://docs.confluent.io/cloud/current/flink/reference/statements/set.html
- `sql.current-catalog`, `sql.current-database` ("Required if object identifiers are not fully qualified"), `sql.local-time-zone` (default "UTC"), `sql.state-ttl` (0 = never), `sql.tables.scan.startup.mode` and friends (override table defaults for new queries), `sql.tables.scan.idle-timeout`, `client.statement-name`, `sql.snapshot.mode` (`'now'` for snapshot queries).

### 1.8 Statements REST API (sql/v1, GA)

Sources: https://docs.confluent.io/cloud/current/flink/operate-and-deploy/flink-rest-api.html ; OpenAPI `sql.v1.Statement`, `sql.v1.StatementSpec`, `sql.v1.StatementStatus`, `sql.v1.StatementResult`.

- Host: `https://flink.${CLOUD_REGION}.${CLOUD_PROVIDER}.confluent.cloud` (private: `https://flink.${CLOUD_REGION}.${CLOUD_PROVIDER}.private.confluent.cloud`), provider lower-case in examples (`aws`).
- Paths (prefix `/sql/v1/organizations/{organization_id}/environments/{environment_id}`):
  - `POST /statements` -> 201 (create)
  - `GET /statements` (list; `page_size`, `page_token`, label filter)
  - `GET /statements/{name}` -> 200; deleted -> 404
  - `PUT /statements/{name}` -> 202; `PATCH /statements/{name}` (RFC 6902 JSON Patch, e.g. `[{"path":"/spec/stopped","op":"replace","value":true}]`) -> 200
  - `DELETE /statements/{name}` -> 202
  - `GET /statements/{name}/results?page_token=...` -> 200
  - `GET /statements/{name}/exceptions`
- Auth: `Authorization: Basic base64(FLINK_API_KEY:FLINK_API_SECRET)` (Flink API key, or global API key per OpenAPI security `resource-api-key`/`global-api-key`), or OAuth bearer token (identity pool as principal). Rate limits per IP: 100 concurrent, 1000/min, 50/s.
- Statement name: max 100 chars, regex `[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*`; doc says "The underscore character (`_`) and period character (`.`) are not supported." Unique within environment.
- Create request (doc example):
  ```json
  {
    "name": "${STATEMENT_NAME}",
    "organization_id": "${ORG_ID}",
    "environment_id": "${ENV_ID}",
    "spec": {
      "statement": "${SQL_CODE}",
      "properties": {"sql.current-catalog": "my_environment", "sql.current-database": "my_kafka_cluster"},
      "compute_pool_id": "${COMPUTE_POOL_ID}",
      "principal": "${PRINCIPAL_ID}",
      "stopped": false
    }
  }
  ```
  `compute_pool_id` optional ("uses the default compute pool"); `principal` optional ("inferred from the provided Flink API key"), values `u-...`, `sa-...`, `pool-...`; `spec.statement` max 131072 chars; `properties` is map<string,string> (example values above are from OpenAPI `StatementSpec`).
- Response (doc example, GET):
  ```json
  {
    "api_version": "sql/v1",
    "kind": "Statement",
    "metadata": {"self": "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/<org>/environments/env-z3y2x1/statements/demo-statement-1",
                 "created_at": "2023-12-16T16:08:36.650591Z", "updated_at": "...", "uid": "5387a4a4-...", "resource_version": "13"},
    "name": "demo-statement-1",
    "organization_id": "b0b21724-4586-4a07-b787-d0bb5aacbf87",
    "environment_id": "env-z3y2x1",
    "spec": {"statement": "select 1;", "properties": {}, "compute_pool_id": "lfcp-8m03rm", "principal": "u-aq1dr2", "stopped": false, "execution_mode": "streaming"},
    "status": {
      "phase": "COMPLETED",
      "scaling_status": {"scaling_state": "OK", "last_updated": "2023-12-16T16:10:05Z"},
      "detail": "Statement completed successfully",
      "traits": {"sql_kind": "SELECT", "is_bounded": true, "is_append_only": true,
                 "schema": {"columns": [{"name": "EXPR$0", "type": {"type": "INTEGER", "nullable": false}}]}},
      "network_kind": "PUBLIC"
    }
  }
  ```
  Create response returns `"status": {"phase": "PENDING", "detail": "Statement is being submitted"}`.
- `status.phase` (OpenAPI x-extensible-enum): `PENDING, RUNNING, COMPLETED, DELETING, FAILING, FAILED, STOPPING, STOPPED, DEGRADED`. Other status fields: `detail`, `warnings[]`, `scaling_status`, `state_limit_status`, `traits`.
- Results (OpenAPI `sql.v1.StatementResult`): `{"api_version":"sql/v1","kind":"StatementResult","metadata":{"self":"...","next":"<url with page_token>"},"results":{"data":[{"op":0,"row":["101","Jay",...]}]}}`; `op` 0=+I,1=-U,2=+U,3=-D, default 0. (Shape from schema; a full doc response example was not found.)
- Lifecycle facts (https://docs.confluent.io/cloud/current/flink/concepts/statements.html): SQL text immutable after submit; terminal statements retained 30 days; foreground statement with no result consumer for 5 min -> STOPPED.

**Implications for emulation (Flink)**
- Provide a catalog per "environment" and a database per "cluster" whose tables are auto-derived from Kafka topics + SR subjects (`<topic>-key`/`<topic>-value`), honoring Confluent inference rules (`key`/`val` raw columns, `key_` prefix on collision, Debezium detection -> retract/upsert). In OSS Flink 2.1 this means a custom Catalog implementation backed by Kafka AdminClient + SR (Apicurio ccompat).
- Default table options must be Confluent's, not Apache Flink's: `scan.startup.mode=earliest-offset`, formats `avro-registry` (map to Flink `avro-confluent`), 6 partitions, PK => upsert + compacted topic + hash-by-PK key schema, `value.fields-include=except-key`.
- `CREATE TABLE`/CTAS must physically create the topic (partitions from `INTO n BUCKETS`, default 6; cleanup policy; retention) and register SR subjects; `DROP TABLE` should delete them.
- Translate Confluent-only syntax/options: `DISTRIBUTED BY ... INTO n BUCKETS` (Flink 2.x supports DISTRIBUTED BY grammar; mapping to partitions is ours), `kafka.*`, `changelog.mode`, `*-debezium-registry`, `error-handling.*`, `$rowtime` system column + default `SOURCE_WATERMARK()` (180 ms bounded out-of-orderness).
- Retract-mode tables need the `op` header encoding (0..3) on Kafka records; upsert needs null-value tombstones.
- Accept backticked 3-part names and `USE CATALOG`/`USE` client-side semantics; accept both IDs and display names if possible.
- Expose a REST facade with the exact sql/v1 paths, JSON field names, phase strings, results paging (`metadata.next`, `op`/`row`) and Basic auth; ignore/echo `compute_pool_id` and `principal`. Enforce statement-name regex.

---

## 2. Tableflow

### 2.1 Enable/disable via REST (tableflow/v1, GA)

Source: OpenAPI paths `/tableflow/v1/tableflow-topics`, schema `tableflow.v1.TableflowTopicSpec`; https://docs.confluent.io/cloud/current/topics/tableflow/operate/configure-tableflow.html

- `POST https://api.confluent.cloud/tableflow/v1/tableflow-topics` -> 202 (409 if exists). Required in spec: `display_name`, `storage`, `environment`, `kafka_cluster`.
- `GET|PATCH|DELETE /tableflow/v1/tableflow-topics/{display_name}?environment=<env>&spec.kafka_cluster=<lkc>` (GET 200, PATCH 200, DELETE 204). List: `GET /tableflow/v1/tableflow-topics?environment=..&spec.kafka_cluster=..[&spec.table_formats=ICEBERG]`.
- Auth: Basic with Cloud API key / global API key (security `resource-api-key`, `global-api-key`).
- Spec fields:
  - `display_name` (string, immutable) = "The name of the Kafka topic for which Tableflow is enabled."
  - `storage` (oneOf by `kind`): `{"kind":"Managed"}` (Confluent Managed Storage); `{"kind":"ByobAws","bucket_name":"...","provider_integration_id":"cspi-..."}`; also `AzureDataLakeStorageGen2`, `GoogleCloudStorage`. Read-only `table_path` (e.g. `s3://.../org-1/env-2/lkc-3/v1/tableId`), `bucket_region`.
  - `table_formats`: array of `ICEBERG`|`DELTA`, default `["ICEBERG"]`.
  - `config`: `retention_ms` (snapshot expiration, default "604800000", min "86400000"), `data_retention_ms` (LA; "0" disables; min non-zero 30 days), `error_handling` (`{"mode":"SUSPEND"}` | `{"mode":"SKIP"}` | `{"mode":"LOG","target":"error_log"}`), `record_failure_strategy` (deprecated), `metadata_column_naming_scheme` (`DEFAULT`|`PORTABLE`; default PORTABLE on GCP, DEFAULT on AWS/Azure per API; Console selects PORTABLE), deprecated read-only `enable_compaction`, `enable_partitioning`.
  - `suspended` (only settable to false = resume).
  - `environment: {"id": "env-..."}`, `kafka_cluster: {"id": "lkc-..."}`.
- Status: `phase` `PENDING|RUNNING|FAILED`, `error_message`, `catalog_sync_statuses[]`, `failing_table_formats[]`, `write_mode` (deprecated, `APPEND|UPSERT|UPSERT_HISTORY`).
- Request example derived from the schema (no full curl example found in docs):
  ```json
  {
    "spec": {
      "display_name": "orders",
      "storage": {"kind": "Managed"},
      "table_formats": ["ICEBERG"],
      "config": {"retention_ms": "604800000", "error_handling": {"mode": "SUSPEND"}},
      "environment": {"id": "env-abc123"},
      "kafka_cluster": {"id": "lkc-abc123"}
    }
  }
  ```
  Doc PATCH-style example (configure-tableflow.html): `{"spec":{"display_name":"high-volume-events","config":{"metadata_column_naming_scheme":"PORTABLE"},"table_formats":["ICEBERG"],"environment":{"id":"env-xxxxx"},"kafka_cluster":{"id":"lkc-xxxxx"}}}`.

### 2.2 CLI

Source: https://docs.confluent.io/confluent-cli/current/command-reference/tableflow/topic/confluent_tableflow_topic_enable.html
```
confluent tableflow topic enable <name> --cluster lkc-123456 --storage-type MANAGED \
  [--retention-ms 604800000] [--table-formats ICEBERG] [--error-handling SUSPEND|SKIP|LOG --log-target <topic>] \
  [--metadata-column-naming-scheme DEFAULT|PORTABLE] [--environment env-...]
# BYOS: --storage-type BYOS --provider-integration cspi-... --bucket-name bucket_1
```
Defaults: `--storage-type MANAGED`, `--table-formats ICEBERG`, `--retention-ms 604800000`. Terraform: `confluent_tableflow_topic`.

### 2.3 Iceberg REST catalog (IRC)

Sources: https://docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-managed-storage.html ; .../how-to-guides/query-engines/query-with-duckdb.html ; .../query-with-trino.html ; .../how-to-guides/catalog-integration/integrate-with-snowflake-horizon-catalog.html ; .../catalog-integration/user-defined-namespaces.html

- Endpoint (catalog URI): `https://tableflow.<cloud_region>.aws.confluent.cloud/iceberg/catalog/organizations/<org_id>/environments/<env_id>` (GCP: `.gcp.confluent.cloud`). One catalog per environment.
- Auth: Tableflow-scoped API key or Global API key, used as OAuth2 client credentials: client_id = API key, client_secret = API secret, scope `catalog`. Token endpoint: "`OAUTH_TOKEN_URI` is that same endpoint with `/v1/oauth/tokens` appended" (i.e. `.../environments/<env_id>/v1/oauth/tokens`). Clients configure it as Iceberg `credential=<api_key>:<secret>` (Spark), `iceberg.rest-catalog.oauth2.credential=<api-key>:<api-secret>` + `security=OAUTH2` (Trino), DuckDB `CLIENT_ID/CLIENT_SECRET/OAUTH2_SCOPE 'catalog'`.
- Namespace = Kafka cluster ID; table = topic name: Spark `SELECT * FROM `<lkc-id>`.`stock-trades``; DuckDB `iceberg_catalog."lkc-abc123"."test-topic-123"`. "Query with the cluster ID, not the cluster name" (Spark quick start; Trino page says the cluster name also works: conflicting). "The Tableflow Iceberg REST Catalog (IRC) namespace continues to use the cluster ID; user-configurable IRC namespaces are planned" (user-defined namespaces apply only to external catalog sync).
- Warehouse: DuckDB `ATTACH 'warehouse' AS iceberg_catalog (TYPE iceberg, SECRET ..., ENDPOINT ...)`; "`warehouse` is a required logical name for the driver." Spark/Trino examples set no warehouse. Server-side meaning of the warehouse parameter / `/v1/config` prefix: **UNVERIFIED**.
- Storage access: Managed storage "can only be accessed with credentials vended by the Confluent Iceberg REST Catalog" (Spark uses `s3.remote-signing-enabled=true`; Trino `iceberg.rest-catalog.vended-credentials-enabled=true`). No credential vending for BYOS buckets.
- Tables are read-only for external engines.
- Table shape: key columns + value columns (append: "key schemas act as additional fields"; raw/no-key shown as `key binary`), plus headers as `MAP<VARCHAR, VARBINARY>`, plus metadata columns `$$topic`, `$$partition`, `$$offset`, `$$leader-epoch` (PORTABLE: `cflt_metadata_topic`, `cflt_metadata_partition`, `cflt_metadata_offset`, `cflt_metadata_leader_epoch`). Full metadata-column list and the header column name: **UNVERIFIED**.
- Requirements/limits: key and value must both have SR schemas (no schemaless); "Retract tables (Flink retract changelog mode) are not supported"; CC Flink cannot query Iceberg tables (only snapshot queries on Tableflow-enabled topics via `SET 'sql.snapshot.mode' = 'now';`); 10..100 snapshots kept.

### 2.4 Append vs upsert

Source: https://docs.confluent.io/cloud/current/topics/tableflow/concepts/write-modes.html

| kafka.cleanup-policy | changelog.mode | Tableflow write mode |
|---|---|---|
| delete | append | APPEND |
| compact | upsert | UPSERT |
| delete, compact | upsert | UPSERT |

- Mode is fixed while enabled (disable + re-enable creates a new table).
- UPSERT: "the Kafka message key and partition number form the composite primary key"; last version per key kept; tombstone (key + empty value) deletes the row; if key fields exist in both key and value, value wins; key schema evolution not supported; 30 B key limit; keys >256 bytes hashed.
- CDC (https://docs.confluent.io/cloud/current/topics/tableflow/concepts/materialize-cdc.html): Tableflow does NOT natively read `*-debezium-registry`. Either (a) Flink decoding: copy SHOW CREATE TABLE, set `changelog.mode='upsert'`, `value.format='avro-registry'`, create new (compacted) table, `INSERT INTO <new> SELECT * FROM <source>`, enable Tableflow on the new topic; or (b) connector `after.state.only=true` with `tombstones.on.delete=false` (append, or error-handling skip) / `true` (upsert).

### 2.5 Freshness

"Tableflow publishes new data to a table approximately every 5 minutes. Higher-throughput topics often publish sooner because Tableflow also commits when enough data has accumulated." Quick start: "can take around 10 minutes to materialize a newly created topic". (https://docs.confluent.io/cloud/current/topics/tableflow/overview.html#table-freshness)

**Implications for emulation (Tableflow)**
- Implement `POST/GET/PATCH/DELETE /tableflow/v1/tableflow-topics[/{display_name}]` with `environment` and `spec.kafka_cluster` query params, accept `storage.kind=Managed` (map BYOS to our object store), `table_formats` default `["ICEBERG"]`, return `status.phase` PENDING->RUNNING.
- Decide write mode exactly like Confluent: topic `cleanup.policy` + (if Flink-created) table `changelog.mode`; reject retract tables and Debezium-envelope topics (or emulate the Confluent limitation).
- Iceberg REST catalog must be served at `/iceberg/catalog/organizations/{org}/environments/{env}` (Iceberg spec paths `/v1/config`, `/v1/namespaces/...` beneath it), support `POST .../v1/oauth/tokens` client_credentials with `client_id=<key>`, `client_secret=<secret>`, `scope=catalog`, and expose namespace `<cluster_id>` with table names equal to topic names (including dashes). Accept any warehouse value.
- Add metadata columns `$$topic/$$partition/$$offset/$$leader-epoch` (or `cflt_metadata_*`), key columns, headers map. Commit roughly every 5 min or on size threshold; the demo must tolerate minutes of lag (OSS can be faster).
- Upsert tables: key = Kafka key + partition; tombstones delete (Iceberg equality deletes or MoR).

---

## 3. Real-Time Context Engine (RTCE) and lightning queries

### 3.1 Enable/manage API (rtce/v1, GA)

Sources: https://docs.confluent.io/cloud/current/ai/real-time-context-engine/get-started.html ; .../manage-topics.html ; OpenAPI `rtce.v1.RtceTopic`, https://docs.confluent.io/cloud/current/ccloud/get-rtce-v-1-rtce-topic/

- `POST https://api.confluent.cloud/rtce/v1/rtce-topics` (Basic `-u "<api_key>:<api_secret>"`, Cloud or Global API key) -> 202:
  ```json
  {
    "spec": {
      "cloud": "AWS",
      "description": "Customer orders table",
      "environment": {"id": "env-abc123"},
      "kafka_cluster": {"id": "lkc-abc123"},
      "region": "us-west-2",
      "topic_name": "orders_topic"
    }
  }
  ```
  `cloud` enum `AWS` only; `topic_name` pattern `^[a-zA-Z][a-zA-Z0-9_]*$` (no dashes or dots), immutable; `description` 1..2048 chars ("model-readable"); doc says optional, but CLI flag and OpenAPI response schema mark it required (**conflict**; always send it).
- `GET /rtce/v1/rtce-topics?environment=<env>&spec.kafka_cluster=<lkc>` (list), `GET /rtce/v1/rtce-topics/{topic_name}?environment=..&spec.kafka_cluster=..`, `PATCH /rtce/v1/rtce-topics/{topic_name}?environment=..&spec.cloud=AWS&spec.kafka_cluster=..` with `{"spec":{"environment":{"id":..},"kafka_cluster":{"id":..},"description":"..."}}`, `DELETE /rtce/v1/rtce-topics/{topic_name}?environment=..&spec.kafka_cluster=..` -> 204. Also `GET /rtce/v1/regions`.
- Response object: `api_version: "rtce/v1"`, `kind: "RtceTopic"`, `metadata {self, resource_name (crn://...topic=...), created_at, updated_at, deleted_at}`, `spec {cloud, region, topic_name, description, environment{id,related,resource_name}, kafka_cluster{id,related,resource_name}}`, `status {phase, error_message}`; `phase` in `PENDING, PROVISIONING, ACTIVE, DELETING, FAILED, UNAVAILABLE`.
- Errors: `{"errors":[{"id","status","code","title","detail","source":{"pointer","parameter"}}]}`; `X-Request-Id`, `X-RateLimit-*` headers.
- CLI: `confluent rtce rtce-topic create --cloud aws --region us-west-2 --topic-name orders_topic --description "..." [--environment] [--cluster] [--wait --timeout 1h]`; `confluent rtce rtce-topic delete <topic>`; `... list`. Terraform `confluent_rtce_topic`.
- Prereqs: AWS Basic/Standard/Enterprise/Dedicated cluster in supported region; topic must have Avro/Protobuf/JSON Schema value schema.

### 3.2 Append vs upsert ingestion

Source: https://docs.confluent.io/cloud/current/ai/real-time-context-engine/limitations.html#upsert-mode
- Append (default, `cleanup.policy=delete`): every record queryable. Upsert: automatically when `cleanup.policy` includes `compact`; Kafka record key is the PK, last-write-wins, null value deletes. "The key must use raw byte format; structured key schemas (Avro, Protobuf) are not supported."
- Materialized table outlives topic retention (lightning-queries.html). Unsupported types: INTERVAL*, MULTISET, TIMESTAMP WITH TIME ZONE, RAW; TIMESTAMP(_LTZ) precision 7-9 unsupported in Avro/JSON. Schema evolution: backward-compatible adds (nullable, default null), widening int->long/float->double, relaxing nullability; otherwise materialization suspends.

### 3.3 MCP endpoint, transport, auth

Sources: get-started.html#rtce-get-mcp-url ; https://docs.confluent.io/cloud/current/ai/real-time-context-engine/access-control.html
- URL: `https://mcp.<region>.aws.confluent.cloud/mcp/v1/context-engine/organizations/<org_id>/environments/<env_id>/kafka-clusters/<lkc_id>` (one MCP server per cluster; private-networking format differs).
- Transport: MCP Streamable HTTP ("Any MCP client that supports streamable HTTP transport can connect"; `claude mcp add --transport http`).
- Auth: `Authorization: Basic <base64(GLOBAL_API_KEY:SECRET)>` (Global API key required for querying; Cloud API key is enough for topic management only). OAuth alternative: client sends header `Confluent-Identity-Pool-Id: <pool-id>`; server replies 401 with `WWW-Authenticate` naming the authorization server; client does client_credentials and sends `Authorization: Bearer <token>`.
- Client config:
  ```json
  {"mcpServers":{"confluent-rtce":{"url":"https://mcp.<region>.aws.confluent.cloud/mcp/v1/context-engine/organizations/<org_id>/environments/<env_id>/kafka-clusters/<lkc_id>","headers":{"Authorization":"Basic <token>"}}}}
  ```

### 3.4 MCP tools

Source: https://docs.confluent.io/cloud/current/ai/real-time-context-engine/query-data.html
- `listTopics`: "takes no arguments"; returns RTCE-enabled topics visible to the caller "and their descriptions".
- `getMetadata`: "The agent passes the name of the topic"; returns "column names, data types, and primary key information".
- `queryData`: "The agent passes the name of the topic to query and an SQL fragment that specifies the SELECT, WHERE, ORDER BY, and LIMIT clauses" plus `max_result_rows`, which "is required on every queryData call. Setting it to 0 or omitting it returns an error." Errors return "an error code and message" (e.g. `RATE_LIMIT_EXCEEDED`).
- Exact input property names other than `max_result_rows` (topic argument name, SQL argument name), JSON Schema types, and output JSON shape: **UNVERIFIED** (not published; obtain via MCP `tools/list` against a live endpoint).
- Documented example query: `SELECT ORDER_ID, STATUS, TOTAL FROM orders_topic WHERE STATUS = 'completed' AND TOTAL > 100 ORDER BY TOTAL DESC LIMIT 10`.

### 3.5 Lightning query SQL subset and limits

- Supported: key lookups by PK; `=`, `!=`, `>`, `<`, `>=`, `<=`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`, `BETWEEN`; `AND`/`OR`; column projection or `*`; `ORDER BY ... ASC|DESC`; `LIMIT`.
- Rejected without execution: aggregates (COUNT/SUM/AVG/MIN/MAX), GROUP BY, joins, subqueries, UNION/EXCEPT/INTERSECT, DDL, DML.
- Limits: max 200 rows; 20 s timeout; rate limits -> `RATE_LIMIT_EXCEEDED`. "Confluent refreshes that table within seconds of new data arriving."
- Direct (non-agent) lightning-query API is Early Access only (https://docs.confluent.io/cloud/current/ai/real-time-context-engine/lightning-queries.html).

**Implications for emulation (RTCE)**
- Implement `/rtce/v1/rtce-topics` CRUD with the same query params, body, 202/204 codes, phases (PENDING->PROVISIONING->ACTIVE), and validate `topic_name` against `^[a-zA-Z][a-zA-Z0-9_]*$` (so demo topics fed to RTCE must use underscores, no dashes or dots). Accept `cloud: "AWS"` and any region string.
- Materialize into Postgres: append mode = insert every record; upsert mode when topic is compacted, PK = raw key bytes, tombstone delete. Note Confluent requires a raw-bytes key in upsert mode, so a Flink CTAS feeding RTCE needs `'key.format'='raw'` (single STRING/BYTES key column) or the topic must be non-compacted. Keep rows beyond Kafka retention.
- Serve MCP over Streamable HTTP at `/mcp/v1/context-engine/organizations/{org}/environments/{env}/kafka-clusters/{lkc}`, accept `Authorization: Basic` (and optionally Bearer), expose exactly `listTopics`, `getMetadata`, `queryData`; require `max_result_rows` (1..200); enforce the SQL subset (parse and reject aggregates/joins/subqueries/DML), 20 s timeout, max 200 rows; column names as in schema (the doc example uses upper-case names).
- Because argument names are unpublished, capture the real `tools/list` once against Confluent and copy the JSON Schemas verbatim.

---

## 4. Managed Postgres CDC source connector (PostgresCdcSourceV2)

Source: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html ; Connect API: https://docs.confluent.io/cloud/current/connectors/connect-api-section.html ; OpenAPI `connect.v1.Connector`

### 4.1 Connect REST API

- `POST https://api.confluent.cloud/connect/v1/environments/{env_id}/clusters/{lkc_id}/connectors` (201) with `{"name": "...", "config": {...all string values...}}`.
- `PUT .../connectors/{connector_name}/config` (create-or-update, body is the flat config map).
- `GET .../connectors` (list names), `GET .../connectors?expand=info,status,id`, `GET .../connectors/{name}`, `GET .../connectors/{name}/status`, `PUT .../pause`, `PUT .../resume`, `POST .../restart`, `DELETE .../connectors/{name}`, offsets under `.../offsets`.
- Auth: `Authorization: Basic base64(<cloud API key>:<secret>)` with a key for `--resource cloud` ("Using the API key created for your Confluent Cloud cluster ... results in an authentication error"). Kafka credentials go inside config (`kafka.auth.mode` = `KAFKA_API_KEY` (default) with `kafka.api.key`/`kafka.api.secret`, or `SERVICE_ACCOUNT` with `kafka.service.account.id`).
- Name: max 64 chars. Server adds `cloud.environment`, `cloud.provider`, `kafka.endpoint` (`SASL_SSL://pkc-....confluent.cloud:9092`), `kafka.region`, `schema.registry.url`; secrets masked as `****************` in responses.
- CLI equivalent: `confluent connect cluster create --config-file <file>.json`.

### 4.2 Documented config example
```json
{
  "connector.class": "PostgresCdcSourceV2",
  "name": "PostgresCdcSourceV2Connector_0",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "****",
  "kafka.api.secret": "****",
  "database.hostname": "debezium-1.<host-id>.us-east-2.rds.amazonaws.com",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "****",
  "database.dbname": "postgres",
  "topic.prefix": "cdc",
  "slot.name": "dbz_slot",
  "publication.name": "dbz_publication",
  "table.include.list": "public.passengers",
  "output.data.format": "JSON",
  "tasks.max": "1"
}
```

### 4.3 Key properties and defaults

| Property | Default | Notes |
|---|---|---|
| `output.data.format` | `JSON` | `AVRO`, `JSON_SR`, `PROTOBUF`, `JSON` |
| `output.key.format` | `JSON` | `AVRO`, `JSON`, `JSON_SR`, `PROTOBUF`, `STRING` |
| `after.state.only` | `false` (V2; legacy V1 defaulted to `true`) | "only the state of the row after the event occurred" |
| `after.state.only.replace.null.with.default` | `true` | |
| `tombstones.on.delete` | `true` | delete event followed by tombstone |
| `topic.prefix` | (required) | topics `<topic.prefix>.<schemaName>.<tableName>` |
| `table.include.list` / `table.exclude.list` | all non-system tables | anchored regexes `schema.table`, mutually exclusive |
| `column.exclude.list`, `message.key.columns` | | key defaults to table PK |
| `snapshot.mode` | `initial` | `initial`, `initial_only`, `no_data`, `when_needed`, `never` (deprecated) |
| `decimal.handling.mode` | `precise` | `precise`, `double`, `string` |
| `time.precision.mode` | `adaptive` | `adaptive`, `adaptive_time_microseconds`, `connect` |
| `binary.handling.mode` | `bytes` | `base64`, `base64-url-safe`, `hex` |
| `hstore.handling.mode` / `interval.handling.mode` | `json` / `numeric` | |
| `plugin` | `pgoutput` only | |
| `publication.autocreate.mode` | (`all_tables`/`disabled`/`filtered`) | default not shown in fetched text: **UNVERIFIED** |
| `database.sslmode` | | `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `topic.creation.topic_prefix_match.partitions` | `1` | replication 3 |
| `schema.context.name` | `default` | |
| `key/value.converter.*.subject.name.strategy` | `TopicNameStrategy` | |
| `value.converter.decimal.format` | `BASE64` | JSON/JSON_SR only; `NUMERIC` alternative |
| `heartbeat.interval.ms` | `0` | |
| `tasks.max` | `1` (only 1 allowed) | |

### 4.4 Record shape

- `after.state.only=false`: standard Debezium envelope `{"before":...,"after":{...},"source":{"version","connector":"postgresql","name","ts_ms","snapshot","db","sequence","schema","table","txId","lsn","xmin"},"op":"c|r|u|d","ts_ms":...,"transaction":null}`; `before` is null/partial unless `REPLICA IDENTITY FULL`.
- `after.state.only=true`: record "contain[s] only the state of the row after the event" ("Debezium producing Kafka messages that closely resemble the schema of your source CDC table" per Tableflow CDC page). Internally implemented with an `unwrap` transform alias (reserved; SMT page says user ExtractNewRecordState requires `after.state.only=false`). Whether flattened records carry `__deleted`, `__op`, `__table`, `__source_ts_ms` fields, and whether delete events are dropped or emitted (vs only tombstones): **UNVERIFIED** (not documented).
- Key: table PK columns (or `message.key.columns`), in `output.key.format`. SR subjects TopicNameStrategy: `<prefix>.<schema>.<table>-key|-value`.

**Implications for emulation (CDC)**
- Expose `/connect/v1/environments/{env}/clusters/{lkc}/connectors` (POST/PUT config/GET/DELETE/status) that accepts `connector.class=PostgresCdcSourceV2` and translates Confluent property names to OSS Debezium 2.x/3.x Postgres connector config: `output.data.format=AVRO` -> AvroConverter (Apicurio ccompat URL), `output.key.format`, `after.state.only=true` -> ExtractNewRecordState SMT, `kafka.api.*`/`kafka.auth.mode` ignored or mapped, `database.*`, `slot.name`, `publication.name`, `publication.autocreate.mode`, `snapshot.mode`, `decimal.handling.mode`, `time.precision.mode`, `tombstones.on.delete`, `topic.prefix`, `table.include.list`.
- Reproduce defaults where OSS Debezium differs (OSS `decimal.handling.mode` default is also `precise`; ensure `output.*` default JSON; topics 1 partition, RF per local cluster).
- Topic names `<prefix>.<schema>.<table>` contain dots: Flink must backtick them, and they are invalid RTCE `topic_name`s; route through a Flink CTAS into an underscore-named topic before RTCE.
- Debezium envelope topics read in Flink as retract (or upsert if compacted) via `*-debezium-registry` detection; with `after.state.only=true` they read as plain `avro-registry` append tables (deletes invisible unless tombstones + upsert).

---

## 5. Client connection settings

Sources: https://docs.confluent.io/cloud/current/client-apps/config-client.html ; https://docs.confluent.io/cloud/current/client-apps/client-configs.html ; https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html

Confluent Cloud (Java properties, from docs `creds.config`):
```properties
bootstrap.servers=<BOOTSTRAP_SERVER>            # e.g. pkc-xxxxx.<region>.<cloud>.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<CLUSTER_API_KEY>" password="<CLUSTER_API_SECRET>";
client.dns.lookup=use_all_dns_ips
session.timeout.ms=45000
acks=all
# Schema Registry
schema.registry.url=https://psrc-xxxxx.<region>.<cloud>.confluent.cloud
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<SR_API_KEY>:<SR_API_SECRET>
```
- Docs: "All clients must support TLS 1.2 encryption and either SASL_PLAIN or SASL_OAUTHBEARER authentication." librdkafka-based clients use `sasl.username`/`sasl.password` ("Use the same `sasl.username` and `sasl.password` configuration fields" for global keys) and `sasl.mechanisms=PLAIN`. Global API keys are not accepted by Basic/Standard Kafka clusters (use cluster-scoped keys there).
- Console tools pass SR auth as `schema.registry.basic.auth.user.info` with `basic.auth.credentials.source=USER_INFO` (prefixed form for consumer/producer CLI). Exact `confluent kafka client-config create java` output not fetched: **UNVERIFIED** beyond the lines above.
- Generate configs: `confluent kafka client-config create <language-id> --environment <env> --cluster <lkc> --api-key .. --api-secret .. --schema-registry-api-key .. --schema-registry-api-secret ..`.

Plain local cluster equivalent:
```properties
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT
schema.registry.url=http://localhost:8080/apis/ccompat/v7   # Apicurio ccompat
```

**Implications for emulation (clients)**
- To keep "only endpoint/credential changes", run the OSS broker with a SASL_SSL (or at least SASL_PLAINTEXT) PLAIN listener that accepts API-key-style username/password, and front Apicurio with HTTP Basic auth so the same property keys (`basic.auth.credentials.source=USER_INFO`, `basic.auth.user.info`) work; otherwise the app must branch on `security.protocol`.
- Apicurio ccompat must serve the Confluent SR REST paths at the configured base URL (`/subjects/{subject}/versions`, `/schemas/ids/{id}`, `/config/{subject}` with `FULL` compatibility for Flink key subjects) and produce 4-byte global IDs compatible with the 5-byte wire prefix.
- Keep all secrets in one env file: Kafka key/secret, SR key/secret, Flink key/secret, Cloud/Global key/secret (Connect, Tableflow, RTCE), Tableflow key (IRC OAuth client credentials), plus IDs `org`, `env`, `lkc`, `region`, `compute_pool_id`.

---

## UNVERIFIED / conflicting points (summary)
1. RTCE MCP tool input property names (except `max_result_rows`), JSON Schema types, and output JSON shape.
2. Whether `USE CATALOG`/fully-qualified names accept env/cluster IDs as well as display names in Flink.
3. CTAS over updating queries auto-choosing upsert/retract without an explicit `changelog.mode`.
4. `kafka.retention.time` default (option table "0 infinite" vs example "7 days" / `604800000 ms`).
5. Tableflow IRC `warehouse` semantics and `/v1/config` prefix; whether cluster name works as namespace (Trino says yes, Spark quick start says use ID).
6. Full Tableflow metadata column list and headers column name.
7. PostgresCdcSourceV2 `after.state.only=true` record shape on deletes and presence of `__deleted`/`__op` fields; `publication.autocreate.mode` default.
8. RTCE `description`: docs say optional, CLI/OpenAPI say required.
9. Exact `confluent kafka client-config create java` output.
