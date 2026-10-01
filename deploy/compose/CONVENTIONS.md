# Local stack conventions

Services, internal endpoints and host ports used by docker compose and by the Java services.
Anything that talks to the stack (control plane, context engine, CLI, e2e) relies on these.

| Service | Internal address | Host port | Notes |
|---|---|---|---|
| Kafka (KRaft, single node) | `kafka:9092` | `19092` | auto topic creation **off** |
| Apicurio Registry 3 | `http://apicurio:8080` | `8085` | Confluent-compatible API at `/apis/ccompat/v7` |
| Kafka Connect + Debezium 3.7 | `http://connect:8083` | `8083` | Apicurio Avro converter, Confluent wire format |
| Flink JobManager (REST/UI) | `http://jobmanager:8081` | `8081` | Flink 2.1.3, Java 21 |
| Flink SQL Gateway | `http://sql-gateway:8083` | `8084` | REST API v3 |
| SeaweedFS S3 | `http://s3:8333` | `8333` | bucket `warehouse`, key `streamhouse` / secret `streamhouse-secret`, region `us-east-1` |
| Gravitino server | `http://gravitino:8090` | `8090` | metalake `streamhouse` |
| Gravitino Iceberg REST | `http://gravitino:9001/iceberg/` | `9001` | warehouse `s3://warehouse/`, catalog name used by Flink: `lake` |
| Keycloak 26 | `http://keycloak:8180` | `8180` | realm `streamhouse`; issuer `http://localhost:8180/realms/streamhouse` |
| shop-db (Postgres 17, `wal_level=logical`) | `shop-db:5432` | `5433` | db `shop`, owner `shop`/`shop`, CDC user `debezium`/`debezium` |
| platform-db (Postgres 17) | `platform-db:5432` | `5432` | db `streamhouse`, user `streamhouse`/`streamhouse`; schemas `streamhouse_meta`, `serving` |
| control-plane | `http://control-plane:8080` | `8080` | |
| context-engine | `http://context-engine:8082` | `8082` | REST `/v1/*`, MCP at `/mcp` |

## Keycloak realm `streamhouse`

- Realm roles: `admin`, `engineer`, `support_agent`.
- Users: `admin`/`admin` (admin), `engineer`/`engineer` (engineer).
- Clients:
  - `shctl`: public, device authorization grant + direct access grants (for scripts/tests).
  - `support-agent`: confidential, secret `support-agent-secret`, client-credentials only; its service account has the realm role `support_agent`.
  - `untrusted-agent`: confidential, secret `untrusted-agent-secret`, client-credentials, no roles (used to test denials).
  - `control-plane`: confidential, secret `control-plane-secret`, client-credentials only; its service account has the realm role `admin` (used by the control plane to call the context engine admin API).
- Roles appear in the token under `realm_access.roles`.
- Tokens are issued with issuer `http://localhost:8180/realms/streamhouse` both from inside and outside the network (`KC_HOSTNAME=http://localhost:8180`, backchannel dynamic). Services validate JWTs with JWKS fetched from `http://keycloak:8180`.

## Kafka topic naming

- Debezium source `<source>` on table `<schema>.<table>` → topic `<source>.<schema>.<table>`, key = PK fields, value = flattened row (`ExtractNewRecordState`, `drop.tombstones=false`, `delete.tombstone.handling.mode=tombstone`), `cleanup.policy=compact`.
- Materialized view `<name>` → topic `<name>` (`upsert-kafka`, compacted).
- Iceberg tables: `lake.<namespace>.<topic with dots replaced by _>`, namespace `streamhouse`.
- Subjects: `<topic>-key`, `<topic>-value` (TopicNameStrategy).
- Wire format of every Avro key/value (Debezium, Flink `avro-confluent`): Confluent framing — byte `0x00` + 4-byte big-endian schema id + Avro binary, no headers. The id is Apicurio's **contentId**, resolvable at `http://apicurio:8080/apis/ccompat/v7/schemas/ids/{id}`. Debezium converters must set `apicurio.registry.headers.enabled=false`, `apicurio.registry.id-handler=io.apicurio.registry.serde.Default4ByteIdHandler`, `apicurio.registry.use-id=contentId` (see README "Verified recipes").
- Debezium value types: `numeric` → Avro `double` (`decimal.handling.mode=double`), `timestamptz` → ISO-8601 `string` (`io.debezium.time.ZonedTimestamp`), `timestamp` → `long` millis (`time.precision.mode=connect`). Deletes produce only a tombstone.
- shop-db publication `streamhouse` (FOR ALL TABLES) is pre-created; connectors use `publication.autocreate.mode=disabled`.

## Lineage

Gravitino 1.3.1 accepts OpenLineage RunEvents at `POST http://gravitino:8090/api/lineage` (201) and writes them to a sink (default: `logs/gravitino_lineage.log`). It has **no read API**, so a queryable lineage graph must be persisted by the control plane (or by Marquez configured as Gravitino's `http` sink — not in this stack; its image is amd64-only).
