"""Confluent Cloud: managed CDC connector, Flink statements, Iceberg tables and the context engine."""

from __future__ import annotations

import httpx2

from ..settings import ContextTopic, Pipeline, Statement
from .base import IcebergAccess, McpEndpoint, Platform, PlatformError, basic_auth, literal

API = "https://api.confluent.cloud"


class ConfluentPlatform(Platform):
    name = "confluent"

    def __init__(self, settings, log=print):
        super().__init__(settings, log)
        s = settings
        self.org = s["CONFLUENT_ORG_ID"]
        self.env = s["CONFLUENT_ENV_ID"]
        self.cluster = s["CONFLUENT_KAFKA_CLUSTER_ID"]
        self.region = s["CONFLUENT_REGION"]
        self.cloud = s.get("CONFLUENT_CLOUD", "AWS")
        auth = (s["CONFLUENT_GLOBAL_API_KEY"], s["CONFLUENT_GLOBAL_API_SECRET"])
        self.api = httpx2.Client(base_url=API, auth=auth, timeout=30)
        self.flink = httpx2.Client(
            base_url=f"https://flink.{self.region}.{self.cloud.lower()}.confluent.cloud"
                     f"/sql/v1/organizations/{self.org}/environments/{self.env}",
            auth=auth, timeout=30)
        self.registry = httpx2.Client(
            base_url=s["CONFLUENT_SCHEMA_REGISTRY_URL"],
            auth=(s["CONFLUENT_SCHEMA_REGISTRY_API_KEY"], s["CONFLUENT_SCHEMA_REGISTRY_API_SECRET"]), timeout=30)

    @property
    def _scope(self) -> dict[str, str]:
        return {"environment": self.env, "spec.kafka_cluster": self.cluster}

    # ---- CDC ------------------------------------------------------------------------------

    def connector_name(self, pipeline: Pipeline) -> str:
        return f"{pipeline.source_name}-postgres-cdc"

    def create_source(self, pipeline: Pipeline) -> None:
        s = self.settings
        name = self.connector_name(pipeline)
        config = {
            "connector.class": "PostgresCdcSourceV2",
            "name": name,
            "kafka.auth.mode": "KAFKA_API_KEY",
            "kafka.api.key": s["CONFLUENT_KAFKA_API_KEY"],
            "kafka.api.secret": s["CONFLUENT_KAFKA_API_SECRET"],
            "database.hostname": s["SOURCE_PG_HOST"],
            "database.port": s.get("SOURCE_PG_PORT", "5432"),
            "database.user": s.get("SOURCE_PG_CDC_USER") or s["SOURCE_PG_USER"],
            "database.password": s.get("SOURCE_PG_CDC_PASSWORD") or s["SOURCE_PG_PASSWORD"],
            "database.dbname": s["SOURCE_PG_DATABASE"],
            "database.sslmode": s.get("SOURCE_PG_SSLMODE", "require"),
            "topic.prefix": pipeline.source_name,
            "table.include.list": ",".join(pipeline.tables),
            "slot.name": f"{pipeline.source_name}_cdc",
            "publication.name": f"{pipeline.source_name}_cdc",
            "publication.autocreate.mode": "filtered",
            "snapshot.mode": "initial",
            # Same record shape as the OSS stack: flattened rows keyed by primary key, deletes as tombstones.
            "output.data.format": "AVRO",
            "output.key.format": "AVRO",
            "after.state.only": "true",
            "tombstones.on.delete": "true",
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            "tasks.max": "1",
        }
        path = f"/connect/v1/environments/{self.env}/clusters/{self.cluster}/connectors"
        response = self.api.put(f"{path}/{name}/config", json=config)
        self._check(response, f"configuring connector {name}")

        def running() -> bool:
            status = self.api.get(f"{path}/{name}/status")
            if status.status_code == 404:
                return False
            body = self._check(status, f"reading connector {name}").json()
            state = body.get("connector", {}).get("state")
            failed = [t for t in body.get("tasks", []) if t.get("state") == "FAILED"]
            if state == "FAILED" or failed:
                trace = (failed[0] if failed else body["connector"]).get("trace", "")
                raise PlatformError(f"connector {name} failed: {trace.splitlines()[0] if trace else state}")
            return state == "RUNNING"

        self.poll(running, f"connector {name} to run")

    # ---- Flink ----------------------------------------------------------------------------

    def run_statement(self, statement: Statement) -> None:
        existing = self.flink.get(f"/statements/{statement.name}")
        if existing.status_code == 200:
            phase = existing.json()["status"]["phase"]
            if phase in ("RUNNING", "COMPLETED", "PENDING"):
                self.log(f"  already {phase.lower()}")
                return self._await_statement(statement.name)
            self.flink.delete(f"/statements/{statement.name}")  # failed/stopped: resubmit
            self.poll(lambda: self.flink.get(f"/statements/{statement.name}").status_code == 404,
                      f"statement {statement.name} to be deleted", timeout_s=120)
        spec = {
            "statement": statement.sql,
            "properties": {
                "sql.current-catalog": self.settings["CONFLUENT_ENV_NAME"],
                "sql.current-database": self.settings["CONFLUENT_KAFKA_CLUSTER_NAME"],
            },
            "compute_pool_id": self.settings["CONFLUENT_FLINK_COMPUTE_POOL_ID"],
        }
        if principal := self.settings.get("CONFLUENT_FLINK_PRINCIPAL"):
            spec["principal"] = principal
        response = self.flink.post("/statements", json={"name": statement.name, "spec": spec})
        self._check(response, f"submitting statement {statement.name}")
        self._await_statement(statement.name)

    def _await_statement(self, name: str) -> None:
        def settled() -> bool:
            status = self._check(self.flink.get(f"/statements/{name}"), f"reading statement {name}").json()["status"]
            if status["phase"] in ("FAILED", "FAILING", "STOPPED"):
                raise PlatformError(f"statement {name} {status['phase'].lower()}: {status.get('detail', '')}")
            return status["phase"] in ("RUNNING", "COMPLETED")

        self.poll(settled, f"statement {name}")

    # ---- Iceberg and context --------------------------------------------------------------

    def enable_iceberg(self, topic: str) -> None:
        if self.api.get(f"/tableflow/v1/tableflow-topics/{topic}", params=self._scope).status_code == 200:
            return
        body = {"spec": {
            "display_name": topic,
            "storage": {"kind": "Managed"},
            "table_formats": ["ICEBERG"],
            "environment": {"id": self.env},
            "kafka_cluster": {"id": self.cluster},
        }}
        self._check(self.api.post("/tableflow/v1/tableflow-topics", json=body), f"enabling Iceberg for {topic}")

    def enable_context(self, ctx: ContextTopic) -> None:
        if self.api.get(f"/rtce/v1/rtce-topics/{ctx.topic}", params=self._scope).status_code == 200:
            return
        body = {"spec": {
            "cloud": self.cloud,
            "region": self.region,
            "topic_name": ctx.topic,
            "description": ctx.description,
            "environment": {"id": self.env},
            "kafka_cluster": {"id": self.cluster},
        }}
        self._check(self.api.post("/rtce/v1/rtce-topics", json=body), f"enabling context for {ctx.topic}")

    def wait_until_ready(self, pipeline: Pipeline) -> None:
        def phase(path: str) -> str:
            response = self._check(self.api.get(path, params=self._scope), f"reading {path}")
            status = response.json().get("status", {})
            if status.get("phase") == "FAILED":
                raise PlatformError(f"{path} failed: {status.get('error_message', '')}")
            return status.get("phase", "")

        for topic in pipeline.iceberg:
            self.poll(lambda: phase(f"/tableflow/v1/tableflow-topics/{topic}") == "RUNNING", f"Iceberg table {topic}")
        for ctx in pipeline.context:
            self.poll(lambda: phase(f"/rtce/v1/rtce-topics/{ctx.topic}") == "ACTIVE", f"context table {ctx.topic}")

    def teardown(self, pipeline: Pipeline) -> None:
        for ctx in pipeline.context:
            self.api.delete(f"/rtce/v1/rtce-topics/{ctx.topic}", params=self._scope)
        for topic in pipeline.iceberg:
            self.api.delete(f"/tableflow/v1/tableflow-topics/{topic}", params=self._scope)
        for statement in reversed(pipeline.statements):
            self.flink.delete(f"/statements/{statement.name}")
        name = self.connector_name(pipeline)
        self.api.delete(f"/connect/v1/environments/{self.env}/clusters/{self.cluster}/connectors/{name}")
        self.log(f"[{self.name}] removed the context tables, Iceberg tables, statements and connector "
                 "(topics created by the pipeline are kept)")

    # ---- data access ----------------------------------------------------------------------

    def iceberg(self) -> IcebergAccess:
        s = self.settings
        uri = (f"https://tableflow.{self.region}.{self.cloud.lower()}.confluent.cloud"
               f"/iceberg/catalog/organizations/{self.org}/environments/{self.env}")
        return IcebergAccess(
            # OAuth2 client credentials with the API key; storage credentials are vended by the catalog.
            setup=[f"""CREATE OR REPLACE SECRET lake_catalog (TYPE iceberg,
                       CLIENT_ID {literal(s['CONFLUENT_GLOBAL_API_KEY'])}, CLIENT_SECRET {literal(s['CONFLUENT_GLOBAL_API_SECRET'])},
                       OAUTH2_SERVER_URI {literal(uri + '/v1/oauth/tokens')}, OAUTH2_SCOPE 'catalog')"""],
            attach=f"ATTACH {literal(s.get('CONFLUENT_ICEBERG_WAREHOUSE', ''))} AS lake "
                   f"(TYPE iceberg, ENDPOINT {literal(uri)}, SECRET lake_catalog)",
            namespace=self.cluster)

    def mcp(self) -> McpEndpoint:
        s = self.settings
        url = (f"https://mcp.{self.region}.{self.cloud.lower()}.confluent.cloud/mcp/v1/context-engine"
               f"/organizations/{self.org}/environments/{self.env}/kafka-clusters/{self.cluster}")
        return McpEndpoint(url, basic_auth(s["CONFLUENT_GLOBAL_API_KEY"], s["CONFLUENT_GLOBAL_API_SECRET"]))

    @staticmethod
    def _check(response: httpx2.Response, what: str) -> httpx2.Response:
        if response.status_code >= 400:
            try:
                errors = response.json().get("errors") or response.json()
            except ValueError:
                errors = response.text
            raise PlatformError(f"{what}: HTTP {response.status_code} {errors}")
        return response
