"""Loads a platform profile (config/<env>.env) and the neutral pipeline definition."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    env: str
    values: dict[str, str]

    def __getitem__(self, key: str) -> str:
        value = self.values.get(key)
        if value is None or value == "":
            raise ConfigError(f"{key} is not set in config/{self.env}.env")
        return value

    def get(self, key: str, default: str | None = None) -> str | None:
        value = self.values.get(key)
        return default if value in (None, "") else value

    @property
    def platform(self) -> str:
        return self["PLATFORM"]


def load_settings(env: str) -> Settings:
    path = ROOT / "config" / f"{env}.env"
    if not path.exists():
        hint = f" (copy config/{env}.env.example)" if (ROOT / "config" / f"{env}.env.example").exists() else ""
        raise ConfigError(f"missing {path}{hint}")
    values = {k: v for k, v in dotenv_values(path).items() if v is not None}
    # Environment variables override the file.
    values.update({k: v for k, v in os.environ.items() if k in values})
    return Settings(env=env, values=values)


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
