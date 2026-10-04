# Local stack: verified recipes

Research notes from building the local stack (2026-10-01): the exact calls that worked against each component. The control plane, the Flink topic catalog and `deploy/compose/flink/catalogs/` now do all of this; keep these notes for debugging a component by hand.

Each snippet below ran successfully against this stack on 2026-10-01. Inside the network, use the internal hostnames; from the host, use the mapped ports.

### 1. Debezium Postgres source (Kafka Connect REST)

`PUT http://connect:8083/connectors/<name>/config` (idempotent create-or-update). Check with `GET /connectors/<name>/status`. Delete with `DELETE /connectors/<name>`, and then also drop the replication slot (see cleanup below).

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "shop-db",
  "database.port": "5432",
  "database.user": "debezium",
  "database.password": "debezium",
  "database.dbname": "shop",
  "topic.prefix": "shop",
  "plugin.name": "pgoutput",
  "publication.name": "streamhouse",
  "publication.autocreate.mode": "disabled",
  "slot.name": "streamhouse_shop",
  "table.include.list": "public.customers,public.orders",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "double",
  "time.precision.mode": "connect",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.tombstone.handling.mode": "tombstone",
  "key.converter": "io.apicurio.registry.utils.converter.AvroConverter",
  "key.converter.apicurio.registry.url": "http://apicurio:8080/apis/registry/v3",
  "key.converter.apicurio.registry.auto-register": "true",
  "key.converter.apicurio.registry.find-latest": "true",
  "key.converter.apicurio.registry.headers.enabled": "false",
  "key.converter.apicurio.registry.id-handler": "io.apicurio.registry.serde.Default4ByteIdHandler",
  "key.converter.apicurio.registry.use-id": "contentId",
  "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
  "value.converter.apicurio.registry.url": "http://apicurio:8080/apis/registry/v3",
  "value.converter.apicurio.registry.auto-register": "true",
  "value.converter.apicurio.registry.find-latest": "true",
  "value.converter.apicurio.registry.headers.enabled": "false",
  "value.converter.apicurio.registry.id-handler": "io.apicurio.registry.serde.Default4ByteIdHandler",
  "value.converter.apicurio.registry.use-id": "contentId",
  "topic.creation.default.replication.factor": "1",
  "topic.creation.default.partitions": "1",
  "topic.creation.default.cleanup.policy": "compact"
}
```

Observed behavior:
- **Topics:** `shop.public.customers` and `shop.public.orders` are created by Connect (`topic.creation.*`) with `cleanup.policy=compact`. The snapshot delivered 50 and 200 records.
- **Deletes:** a delete produces **only a tombstone** (key + null value), because `delete.tombstone.handling.mode=tombstone`.
- **Subjects:** `<topic>-key` and `<topic>-value` (Apicurio group `default`, artifactId = subject).
- **Key schema:** an Avro record `Key` with the PK fields, e.g. `{"name":"order_id","type":"int"}`.
- **Value schema:** an Avro record `Value` with the row columns. NOT NULL columns are plain types; nullable ones are `["null", T]`.
- **Type mapping with these settings:**
  - `integer` → `int`
  - `numeric` → `double` (`decimal.handling.mode=double`)
  - `text` → `string`
  - `timestamptz` → `string` with `connect.name=io.debezium.time.ZonedTimestamp`, ISO-8601 like `2026-01-01T01:00:00.000000Z`
  - `timestamp` (no tz) → `long` with `connect.name=org.apache.kafka.connect.data.Timestamp` (millis), because `time.precision.mode=connect`

#### Wire format: Confluent-compatible, contentId

Every key and value is **`0x00` magic byte + 4-byte big-endian schema id + Avro binary** (no Kafka headers). The id is the Apicurio **contentId**. Apicurio 3's ccompat API also exposes contentIds as schema ids (legacy-id mode is off), so the id on the wire resolves directly:

```
GET http://apicurio:8080/apis/ccompat/v7/schemas/ids/{id}            -> {"schema": "<avro json string>"}
GET http://apicurio:8080/apis/ccompat/v7/subjects/{topic}-value/versions/latest -> {"id":4,"version":1,...}
```

Verified: the first `shop.public.orders` value starts with `00 00 00 00 04`, `schemas/ids/4` returns the `shop.public.orders.Value` schema, and `subjects/shop.public.orders-value/versions/latest` has `id: 4`. In this fresh registry, globalId happened to equal contentId. Do not rely on that: the setting that matters is `use-id=contentId`.

### 2. Flink SQL Gateway REST call sequence (v3)

```
POST /v3/sessions                                   body {"sessionName":"..."}  -> {"sessionHandle": S}
POST /v3/sessions/S/statements                      body {"statement":"<one SQL statement>"} -> {"operationHandle": O}
GET  /v3/sessions/S/operations/O/result/0?rowFormat=JSON
     -> {"resultType":"NOT_READY"|"PAYLOAD"|"EOS", "results":{"columns":[...],"data":[{"kind":"INSERT","fields":[...]}]},
         "nextResultUri":"/v3/sessions/S/operations/O/result/1?rowFormat=JSON", "jobID": "..."}
     poll while NOT_READY; follow nextResultUri until EOS
DELETE /v3/sessions/S/operations/O/close            (closing a streaming SELECT cancels its job)
POST /v3/sessions/S/heartbeat                       (sessions idle out after 30 min; sql-gateway.session.idle-timeout)
DELETE /v3/sessions/S
```

- **DDL/SET:** returns one row `["OK"]` (column `result`).
- **INSERT INTO:** returns one row with the Flink job id (column `job id`), and the response also has `jobID`. The job keeps running after the operation is closed.
- **Statement scope:** session-scoped state (`CREATE CATALOG`, temporary/`default_catalog` tables, `SET`) lives only in that session. The control plane should re-issue `CREATE CATALOG lake ...` and the source table DDL in every new session before submitting an INSERT.
- **Job names:** `SET 'pipeline.name' = '<deterministic-name>'` before an INSERT names the job. Track or cancel jobs through the JobManager REST API: `GET http://jobmanager:8081/jobs/overview` → `jobs[].{jid,name,state}`, `PATCH /jobs/<jid>?mode=cancel`, or `POST /jobs/<jid>/stop` with `{"drain":false}` for stop-with-savepoint.

### 3. Flink DDL over Debezium topics (avro-confluent via Apicurio ccompat)

**Changelog / current-state view** (`upsert-kafka`: a tombstone becomes a DELETE):
```sql
CREATE TABLE orders_src (
  order_id INT NOT NULL,
  customer_id INT,
  status STRING,
  total DOUBLE,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'shop.public.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'smoke',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7',
  'value.fields-include' = 'ALL'
)
```
Streaming `SELECT * FROM orders_src` returned the snapshot rows. upsert-kafka always reads from the earliest offset.

**Append / event-log view** (`kafka` connector; tombstones are skipped without failing the job):
```sql
CREATE TABLE orders_log (
  order_id INT NOT NULL,
  customer_id INT,
  status STRING,
  total DOUBLE,
  created_at STRING,
  updated_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'shop.public.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'smoke-log',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
)
```

Notes:
- Flink resolves the writer schema by id. Debezium's record names (`shop.public.orders.Value`) differ from Flink's derived reader schema, but Avro resolution works.
- Column names and types must be compatible with the Avro fields: `int`→INT, `double`→DOUBLE, `string`→STRING, ZonedTimestamp→STRING (cast with `TO_TIMESTAMP_LTZ` / `CAST` in SQL if needed).
- A Flink-produced `upsert-kafka` sink with `avro-confluent` auto-registers `<topic>-key` / `<topic>-value` through the same ccompat URL.

### 4. Iceberg REST catalog `lake` (served by Gravitino) and topic-to-Iceberg jobs

```sql
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://gravitino:9001/iceberg/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://s3:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'streamhouse',
  's3.secret-access-key' = 'streamhouse-secret',
  'client.region' = 'us-east-1'
);
CREATE DATABASE IF NOT EXISTS lake.streamhouse;

-- append table (full history)
CREATE TABLE IF NOT EXISTS lake.streamhouse.shop_public_orders (
  order_id INT NOT NULL, customer_id INT, status STRING, total DOUBLE, created_at STRING, updated_at STRING
);
SET 'pipeline.name' = 'iceberg-shop.public.orders-append';
INSERT INTO lake.streamhouse.shop_public_orders SELECT * FROM orders_log;

-- upsert table (current state; equality deletes)
CREATE TABLE IF NOT EXISTS lake.streamhouse.shop_public_orders_upsert (
  order_id INT NOT NULL, customer_id INT, status STRING, total DOUBLE, created_at STRING, updated_at STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');
SET 'pipeline.name' = 'iceberg-shop.public.orders-upsert';
INSERT INTO lake.streamhouse.shop_public_orders_upsert SELECT * FROM orders_src;
```

Verified, after an UPDATE (order 3 open→shipped), an INSERT (order 1001) and a DELETE (order 1001) in shop-db:
- the append table had 202 rows, with order 3 appearing twice (open, shipped);
- the upsert table had 201 rows, then 200 after the delete, with order 3 = shipped and 1001 gone.

The batch read used `SET 'execution.runtime-mode' = 'batch'; SELECT ...`. Data commits on every checkpoint (10 s), so expect ≤ ~15 s from source change to a visible Iceberg snapshot. Files land under `s3://warehouse/streamhouse/<table>/{data,metadata}`. Metadata pointers are in `platform-db/iceberg`, table `iceberg_tables` (catalog_name `lake`).

### 5. Gravitino REST (metalake, catalogs)

All calls use the headers `Accept: application/vnd.gravitino.v1+json` and `Content-Type: application/json`. Authorization is disabled (user `anonymous`).

```bash
GET  http://gravitino:8090/api/version
POST http://gravitino:8090/api/metalakes
     {"name":"streamhouse","comment":"Streamhouse OSS metalake","properties":{}}
POST http://gravitino:8090/api/metalakes/streamhouse/catalogs        # Kafka topics
     {"name":"kafka","type":"MESSAGING","provider":"kafka","comment":"Kafka cluster",
      "properties":{"bootstrap.servers":"kafka:9092"}}
GET  .../metalakes/streamhouse/catalogs/kafka/schemas/default/topics # lists live Kafka topics (schema is always "default")
POST http://gravitino:8090/api/metalakes/streamhouse/catalogs        # the same Iceberg tables Flink writes
     {"name":"lake","type":"RELATIONAL","provider":"lakehouse-iceberg","comment":"Iceberg tables materialized from topics",
      "properties":{"catalog-backend":"jdbc","catalog-backend-name":"lake",
        "uri":"jdbc:postgresql://platform-db:5432/iceberg","jdbc-driver":"org.postgresql.Driver",
        "jdbc-user":"streamhouse","jdbc-password":"streamhouse","jdbc-initialize":"true",
        "warehouse":"s3://warehouse/","io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
        "s3-endpoint":"http://s3:8333","s3-region":"us-east-1","s3-access-key-id":"streamhouse",
        "s3-secret-access-key":"streamhouse-secret","s3-path-style-access":"true"}}
GET  .../metalakes/streamhouse/catalogs/lake/schemas/streamhouse/tables/<table>   # columns, properties, snapshot
```

The `catalog-backend-name` must be `lake` (the JDBC catalog name used by the Iceberg REST service). Otherwise the Gravitino catalog sees an empty namespace list.

Creates return HTTP 409 if the object already exists. Dropping requires `force=true` (or disabling first): `DELETE .../catalogs/<name>?force=true` and `DELETE /api/metalakes/streamhouse?force=true`.

### 6. Lineage (Gravitino 1.3.1): ingest only, no query API

- Gravitino's HTTP lineage source is enabled by default: `POST http://gravitino:8090/api/lineage` takes an OpenLineage RunEvent and returns **201**.
- Events go to the configured sinks. The default `log` sink writes to `/opt/gravitino/logs/gravitino_lineage.log` (verified). The other built-in sink is `http` (OpenLineage REST, e.g. Marquez):
  ```
  gravitino.lineage.sinks = http
  gravitino.lineage.http.sinkClass = org.apache.gravitino.lineage.sink.LineageHttpSink
  gravitino.lineage.http.url = http://marquez:5000
  gravitino.lineage.http.authType = none
  ```
- **There is no API to read lineage back from Gravitino** (`GET /api/lineage` → 405). A queryable lineage graph has to live elsewhere: either the control plane persists the edges it compiles, or Marquez is added as the http sink. The Marquez image `marquezproject/marquez:0.51.1` is **amd64-only** and runs emulated on Apple Silicon, so it is not part of this stack.

Verified event body:
```json
{"eventType":"COMPLETE","eventTime":"2026-10-01T10:00:00Z",
 "run":{"runId":"0196a8c2-fe01-7439-87e6-56a1a1b4029f"},
 "job":{"namespace":"streamhouse","name":"iceberg-shop.public.orders-upsert"},
 "inputs":[{"namespace":"kafka://kafka:9092","name":"shop.public.orders"}],
 "outputs":[{"namespace":"iceberg://lake","name":"streamhouse.shop_public_orders_upsert"}],
 "producer":"https://github.com/streamhouse-oss","schemaURL":"https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"}
```

### 7. Keycloak tokens

```bash
# agent (client credentials), works from the host and from inside the network
curl -s -d grant_type=client_credentials -d client_id=support-agent -d client_secret=support-agent-secret \
  http://localhost:8180/realms/streamhouse/protocol/openid-connect/token
# user (password grant via the public shctl client)
curl -s -d grant_type=password -d client_id=shctl -d username=engineer -d password=engineer \
  http://localhost:8180/realms/streamhouse/protocol/openid-connect/token
# device flow (shctl login)
curl -s -d client_id=shctl http://localhost:8180/realms/streamhouse/protocol/openid-connect/auth/device
```

Verified claims:
- `iss` = `http://localhost:8180/realms/streamhouse`, whether the token is requested via `localhost:8180` or `keycloak:8180`.
- `aud` = `account`.
- `azp` = the client id.
- `realm_access.roles`:
  - support-agent: `support_agent`
  - control-plane: `admin`
  - untrusted-agent: none of the streamhouse roles
  - engineer: `engineer`
- `preferred_username` = `service-account-<client>` for client-credentials tokens.

The discovery document fetched **inside** the network (`http://keycloak:8180/realms/streamhouse/.well-known/openid-configuration`) has issuer `http://localhost:8180/realms/streamhouse`, but its `jwks_uri` and `token_endpoint` point at `http://keycloak:8180/...` (backchannel dynamic). Quarkus services can therefore use `quarkus.oidc.auth-server-url=http://keycloak:8180/realms/streamhouse`, and the discovered issuer matches the tokens.

### 8. Cleanup of a CDC source

```bash
curl -X DELETE http://connect:8083/connectors/<name>
psql shop: SELECT pg_drop_replication_slot('<slot.name>');   # otherwise WAL is retained forever
kafka-topics --delete --topic '<prefix>.<schema>.<table>'
DELETE http://apicurio:8080/apis/registry/v3/groups/default/artifacts/<topic>-key   (and -value)
```
