"""Streamhouse OSS: everything is declared through the control plane's SQL API."""

from __future__ import annotations

import base64

import httpx

from ..settings import ContextTopic, Pipeline, Statement
from .base import IcebergAccess, McpEndpoint, Platform, PlatformError


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class OssPlatform(Platform):
    name = "oss"

    def __init__(self, settings, log=print):
        super().__init__(settings, log)
        s = settings
        self.control_plane = httpx.Client(base_url=s["OSS_CONTROL_PLANE_URL"], timeout=60)
        self.registry = httpx.Client(base_url=s["OSS_SCHEMA_REGISTRY_URL"], timeout=30)
        self._token: str | None = None

    # ---- control plane access -------------------------------------------------------------

    def _bearer(self) -> dict[str, str]:
        if self._token is None:
            s = self.settings
            response = httpx.post(f"{s['OSS_OIDC_ISSUER']}/protocol/openid-connect/token", data={
                "grant_type": "password", "client_id": "shctl",
                "username": s["OSS_ADMIN_USER"], "password": s["OSS_ADMIN_PASSWORD"]}, timeout=30)
            if response.status_code != 200:
                raise PlatformError(f"cannot log in to the control plane: {response.text}")
            self._token = response.json()["access_token"]
        return {"Authorization": f"Bearer {self._token}"}

    def sql(self, script: str) -> list[dict]:
        response = self.control_plane.post("/v1/sql", json={"sql": script}, headers=self._bearer())
        body = response.json()
        if response.status_code != 200 or not body.get("ok"):
            failed = [r for r in body.get("results", []) if r.get("status") == "ERROR"]
            detail = failed[0]["message"] if failed else body.get("error", response.text)
            raise PlatformError(f"control plane rejected the statement: {detail}")
        return body["results"]

    def resources(self) -> list[dict]:
        response = self.control_plane.get("/v1/resources", headers=self._bearer())
        response.raise_for_status()
        return response.json()

    def _await(self, kind: str, name: str) -> None:
        def ready() -> bool:
            for r in self.resources():
                if r["kind"] == kind and r["name"] == name and r["settled"]:
                    if r["phase"] == "FAILED":
                        raise PlatformError(f"{kind.lower()} {name} failed: {r['message']}")
                    return r["phase"] == "READY"
            return False

        self.poll(ready, f"{kind.lower()} {name}")

    # ---- provisioning ---------------------------------------------------------------------

    def create_source(self, pipeline: Pipeline) -> None:
        s = self.settings
        connection = f"{pipeline.source_name}_pg"
        tables = ", ".join(pipeline.tables)
        self.sql(f"""
            CREATE CONNECTION {connection} TYPE POSTGRES WITH (
              host = {_literal(s['OSS_SOURCE_PG_HOST'])},
              port = {_literal(s.get('OSS_SOURCE_PG_PORT', '5432'))},
              database = {_literal(s['SOURCE_PG_DATABASE'])},
              user = {_literal(s['SOURCE_PG_CDC_USER'])},
              password = SECRET {_literal(s['OSS_SOURCE_PASSWORD_SECRET'])},
              publication = {_literal(s.get('OSS_SOURCE_PUBLICATION', 'streamhouse'))}
            );
            CREATE SOURCE {pipeline.source_name} FROM CONNECTION {connection} TABLES ({tables});""")
        self._await("SOURCE", pipeline.source_name)

    def topics_ready(self, topics: list[str]) -> list[str]:
        response = self.registry.get("/subjects")
        response.raise_for_status()
        subjects = set(response.json())
        return [t for t in topics if f"{t}-value" not in subjects]

    def run_statement(self, statement: Statement) -> None:
        self.sql(f"CREATE STATEMENT {_ident(statement.name)} AS {statement.sql}")
        self._await("STATEMENT", statement.name)

    def enable_iceberg(self, topic: str) -> None:
        self.sql(f"ALTER TOPIC {topic} ENABLE ICEBERG")

    def enable_context(self, ctx: ContextTopic) -> None:
        role = self.settings["OSS_AGENT_ROLE"]
        self.sql(f"ALTER TOPIC {ctx.topic} ENABLE CONTEXT WITH (description = {_literal(ctx.description)});\n"
                 f"GRANT SELECT ON CONTEXT {ctx.topic} TO ROLE {role};")

    def wait_until_ready(self, pipeline: Pipeline) -> None:
        for topic in pipeline.iceberg:
            self._await("ICEBERG_TABLE", topic)
        for ctx in pipeline.context:
            self._await("CONTEXT_TABLE", ctx.topic)

    def teardown(self, pipeline: Pipeline) -> None:
        role = self.settings["OSS_AGENT_ROLE"]
        script = []
        for ctx in pipeline.context:
            script.append(f"REVOKE SELECT ON CONTEXT {ctx.topic} FROM ROLE {role}")
            script.append(f"ALTER TOPIC {ctx.topic} DISABLE CONTEXT")
        script += [f"ALTER TOPIC {t} DISABLE ICEBERG" for t in pipeline.iceberg]
        script += [f"DROP STATEMENT IF EXISTS {_ident(st.name)}" for st in reversed(pipeline.statements)]
        script.append(f"DROP SOURCE IF EXISTS {pipeline.source_name}")
        script.append(f"DROP CONNECTION IF EXISTS {pipeline.source_name}_pg")
        for statement in script:
            try:
                self.sql(statement)
            except PlatformError as e:
                self.log(f"  {e}")
        self.log(f"[{self.name}] teardown requested; the control plane removes resources in dependency order")

    # ---- data access ----------------------------------------------------------------------

    def iceberg(self) -> IcebergAccess:
        s = self.settings
        endpoint = s["OSS_S3_ENDPOINT"].removeprefix("http://").removeprefix("https://")
        return IcebergAccess(
            setup=[f"""CREATE OR REPLACE SECRET lake_storage (TYPE s3, KEY_ID {_literal(s['OSS_S3_ACCESS_KEY'])},
                       SECRET {_literal(s['OSS_S3_SECRET_KEY'])}, REGION {_literal(s.get('OSS_S3_REGION', 'us-east-1'))},
                       ENDPOINT {_literal(endpoint)}, URL_STYLE 'path',
                       USE_SSL {str(s['OSS_S3_ENDPOINT'].startswith('https')).lower()})"""],
            # The local catalog needs no authentication and does not vend storage credentials.
            attach=f"ATTACH '' AS lake (TYPE iceberg, ENDPOINT {_literal(s['OSS_ICEBERG_REST_URL'])}, "
                   "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
            namespace=s["OSS_ICEBERG_NAMESPACE"])

    def mcp(self) -> McpEndpoint:
        s = self.settings
        token = base64.b64encode(f"{s['OSS_AGENT_API_KEY']}:{s['OSS_AGENT_API_SECRET']}".encode()).decode()
        return McpEndpoint(s["OSS_MCP_URL"], {"Authorization": f"Basic {token}"})
