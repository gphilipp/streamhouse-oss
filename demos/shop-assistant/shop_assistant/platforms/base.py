"""The platform interface. Everything else in the demo is platform-independent."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from ..settings import ContextTopic, Pipeline, Settings, Statement

Log = Callable[[str], None]


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
    def topics_ready(self, topics: list[str]) -> list[str]:
        """Returns the topics that do not exist yet (or have no schema yet)."""

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
