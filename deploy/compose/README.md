# Local streamhouse stack

Everything runs from this directory with docker compose. Addresses, ports and credentials are in
[CONVENTIONS.md](CONVENTIONS.md). From the repository root, `make build up` does the same as below.

```bash
cd deploy/compose
docker compose up -d --build --wait                 # infrastructure only (~1 min after images are pulled)
docker compose --profile apps up -d --build --wait  # + control-plane and context-engine
docker compose down                                 # stop (keeps volumes)
docker compose down -v                              # stop and wipe all data
```

Images are built from packaged jars, so run `make build` (`mvn package`) first: the Flink image takes
the topic catalog jar from `flink-catalog/target`, and the two services use
[quarkus.Dockerfile](quarkus.Dockerfile) with their `target/quarkus-app` as build context.

Requirements: Docker with ≥ 6 CPUs / 12 GB RAM (e.g. `colima start --cpu 6 --memory 16`) and BuildKit: the Flink image uses a named build context. Docker Desktop and OrbStack include the buildx plugin; with Colima and the Homebrew Docker CLI, run `brew install docker-buildx`.

## What runs

- **kafka**: single KRaft node, `auto.create.topics.enable=false` (Connect/Debezium and the control plane create topics explicitly).
- **apicurio**: Apicurio Registry 3.3.3, SQL storage in `platform-db/apicurio`.
- **connect**: Debezium 3.7 Connect image, Apicurio converters enabled (`ENABLE_APICURIO_CONVERTERS=true`).
- **s3** + **s3-init**: SeaweedFS 4.48 S3 gateway; `s3-init` creates the `warehouse` bucket and exits.
- **jobmanager**, **taskmanager**, **sql-gateway**: custom image `streamhouse/flink:2.1.3` ([flink/Dockerfile](flink/Dockerfile)).
  - Adds the Kafka connector, avro-confluent, the Iceberg runtime and AWS bundle, the Hadoop client and the s3-fs-hadoop plugin.
  - Registers two catalogs in every SQL Gateway session through the file catalog store ([flink/catalogs](flink/catalogs)): `streamhouse` (every Kafka topic is a table, see [docs/flink-topic-catalog.md](../../docs/flink-topic-catalog.md)) and `lake` (Iceberg, through Gravitino).
  - Checkpoints go every 10 s to `s3://warehouse/_flink/checkpoints`, which Iceberg sinks need to commit. The taskmanager has 8 slots.
- **gravitino**: Gravitino 1.3.1 with its config in [gravitino/gravitino.conf](gravitino/gravitino.conf) (`SKIP_CONFIG_REWRITE=true`).
  - Entity store: PostgreSQL `platform-db/gravitino`. The schema is applied by the platform-db init script.
  - Iceberg REST auxiliary service on :9001: JDBC backend `platform-db/iceberg`, catalog name `lake`, S3FileIO to SeaweedFS.
- **keycloak**: `start-dev --import-realm` with [keycloak/streamhouse-realm.json](keycloak/streamhouse-realm.json). The realm is re-imported on every start because there is no persistent volume.
- **shop-db**: Postgres 17 with `wal_level=logical`, seeded from [demos/shop-assistant/sql/shop-schema.sql](../../demos/shop-assistant/sql/shop-schema.sql): 50 customers, 20 products, 200 orders, 401 order items and 20 inventory rows. It includes the `debezium` user and the publication `streamhouse` FOR ALL TABLES.
- **platform-db**: Postgres 17, database `streamhouse` with schemas `streamhouse_meta` and `serving`, plus the databases `gravitino`, `iceberg` and `apicurio`.
- **control-plane**, **context-engine** (profile `apps`): the streamhouse services.

## Smoke checks

```bash
docker compose ps                                   # every service (healthy); s3-init exited 0
curl -s localhost:8084/v3/info                      # {"productName":"Apache Flink","version":"2.1.3"}
curl -s localhost:8083/connector-plugins | grep -o 'PostgresConnector'
curl -s localhost:9001/iceberg/v1/config
curl -s -d grant_type=client_credentials -d client_id=support-agent -d client_secret=support-agent-secret \
  localhost:8180/realms/streamhouse/protocol/openid-connect/token | head -c 80
```

How each component was configured and called by hand while building the stack is in
[docs/research/compose-recipes.md](../../docs/research/compose-recipes.md).
