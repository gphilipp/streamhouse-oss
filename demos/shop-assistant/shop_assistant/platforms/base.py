"""The platform interface. Everything else in the demo is platform-independent."""

from __future__ import annotations

import base64
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

import httpx2

from ..settings import ContextTopic, Pipeline, Settings, Statement

Log = Callable[[str], None]


def literal(value: str) -> str:
    """A SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def basic_auth(key: str, secret: str) -> dict[str, str]:
    """HTTP Basic credentials for an API key and secret."""
    return {"Authorization": "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode()}


class PlatformError(Exception):
    pass


@dataclass(frozen=True)
class McpEndpoint:
    url: str
    headers: dict[str, str]


@dataclass(frozen=True)
class IcebergAccess:
    """How DuckDB reaches the platform's Iceberg REST catalog: setup statements (secrets) and the
    ATTACH options. Tables are then `lake.<namespace>.<table>`."""
    setup: list[str]
    attach: str
    namespace: str


class Platform(ABC):
    """Provisioning (control plane) differs per platform; data access goes through standard
    interfaces: Flink SQL text, the Iceberg REST catalog and MCP."""

    name: str
    #: The platform's Confluent-compatible schema registry; a topic is ready once it has a value schema.
    registry: httpx2.Client

    def __init__(self, settings: Settings, log: Log = print):
        self.settings = settings
        self.log = log

    # ---- provisioning ---------------------------------------------------------------------

    def provision(self, pipeline: Pipeline) -> None:
        self.log(f"[{self.name}] capturing {', '.join(pipeline.tables)}")
        self.create_source(pipeline)
        self.wait_for_topics(pipeline.source_topics())
        for statement in pipeline.statements:
            self.log(f"[{self.name}] statement {statement.name}")
            self.run_statement(statement)
        derived = [t for t in {*pipeline.iceberg, *(c.topic for c in pipeline.context)}]
        self.wait_for_topics(derived)
        for topic in pipeline.iceberg:
            self.log(f"[{self.name}] iceberg table for {topic}")
            self.enable_iceberg(topic)
        for ctx in pipeline.context:
            self.log(f"[{self.name}] real-time context for {ctx.topic}")
            self.enable_context(ctx)
        self.wait_until_ready(pipeline)
        self.log(f"[{self.name}] pipeline is running")

    @abstractmethod
    def create_source(self, pipeline: Pipeline) -> None: ...

    @abstractmethod
    def run_statement(self, statement: Statement) -> None:
        """Submits a Flink SQL statement (idempotent by name) and waits until it runs or completes."""

    @abstractmethod
    def enable_iceberg(self, topic: str) -> None: ...

    @abstractmethod
    def enable_context(self, ctx: ContextTopic) -> None: ...

    @abstractmethod
    def wait_until_ready(self, pipeline: Pipeline) -> None: ...

    @abstractmethod
    def teardown(self, pipeline: Pipeline) -> None: ...

    # ---- data access ----------------------------------------------------------------------

    @abstractmethod
    def iceberg(self) -> IcebergAccess: ...

    @abstractmethod
    def mcp(self) -> McpEndpoint: ...

    # ---- helpers --------------------------------------------------------------------------

    def topics_ready(self, topics: list[str]) -> list[str]:
        """Returns the topics that have no registered value schema yet."""
        response = self.registry.get("/subjects")
        if response.status_code >= 400:
            raise PlatformError(f"listing schema subjects: HTTP {response.status_code} {response.text}")
        subjects = set(response.json())
        return [t for t in topics if f"{t}-value" not in subjects]

    def wait_for_topics(self, topics: list[str], timeout_s: float = 600) -> None:
        deadline = time.monotonic() + timeout_s
        missing = self.topics_ready(topics)
        if missing:
            self.log(f"[{self.name}] waiting for topics: {', '.join(missing)}")
        while missing:
            if time.monotonic() > deadline:
                raise PlatformError(f"topics never appeared: {', '.join(missing)}")
            time.sleep(5)
            missing = self.topics_ready(topics)

    @staticmethod
    def poll(check: Callable[[], bool], what: str, timeout_s: float = 600, interval_s: float = 3) -> None:
        deadline = time.monotonic() + timeout_s
        while not check():
            if time.monotonic() > deadline:
                raise PlatformError(f"timed out waiting for {what}")
            time.sleep(interval_s)
