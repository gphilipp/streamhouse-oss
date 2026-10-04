"""Loads a platform profile (config/<env>.env) and the neutral pipeline definition."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    """Settings read from the environment, after loading config/<env>.env into it."""
    env: str

    def __getitem__(self, key: str) -> str:
        if not (value := os.environ.get(key)):
            raise ConfigError(f"{key} is not set in config/{self.env}.env")
        return value

    def get(self, key: str, default: str | None = None) -> str | None:
        return os.environ.get(key) or default

    @property
    def platform(self) -> str:
        return self["PLATFORM"]


def load_settings(env: str) -> Settings:
    path = ROOT / "config" / f"{env}.env"
    if not path.exists():
        hint = f" (copy config/{env}.env.example)" if (ROOT / "config" / f"{env}.env.example").exists() else ""
        raise ConfigError(f"missing {path}{hint}")
    load_dotenv(path, override=False)  # variables already in the environment win
    return Settings(env)


@dataclass(frozen=True)
class Statement:
    name: str
    sql: str


@dataclass(frozen=True)
class ContextTopic:
    topic: str
    description: str


@dataclass(frozen=True)
class Pipeline:
    source_name: str
    tables: list[str]
    statements: list[Statement]
    iceberg: list[str]
    context: list[ContextTopic]

    def source_topics(self) -> list[str]:
        return [f"{self.source_name}.{t}" for t in self.tables]


def load_pipeline(path: Path = ROOT / "pipeline.yaml") -> Pipeline:
    raw = yaml.safe_load(path.read_text())
    return Pipeline(
        source_name=raw["source"]["name"],
        tables=list(raw["source"]["tables"]),
        statements=[Statement(s["name"], s["sql"].strip()) for s in raw.get("statements", [])],
        iceberg=list(raw.get("iceberg", [])),
        context=[ContextTopic(c["topic"], " ".join(c["description"].split())) for c in raw.get("context", [])],
    )
