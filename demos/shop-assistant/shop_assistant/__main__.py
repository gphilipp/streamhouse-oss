"""shop-assistant: the same streamhouse demo on Streamhouse OSS or Confluent Cloud.

    shop-assistant --env oss provision
    shop-assistant --env confluent ask "Which customers have more than one open order?"
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from . import agent, analytics, simulate
from .platforms.base import Platform, PlatformError
from .settings import ConfigError, Settings, load_pipeline, load_settings

DEFAULT_QUESTIONS = [
    "Summarize customer 42: tier, number of orders, lifetime value and open orders.",
    "Which products are low on stock, and how many units of each are available?",
]


def platform_for(settings: Settings) -> Platform:
    if settings.platform == "oss":
        from .platforms.oss import OssPlatform
        return OssPlatform(settings)
    if settings.platform == "confluent":
        from .platforms.confluent import ConfluentPlatform
        return ConfluentPlatform(settings)
    raise ConfigError(f"unknown PLATFORM {settings.platform!r}; use oss or confluent")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shop-assistant", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", default="oss", help="profile in config/<env>.env (default: oss)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="create and seed the shop tables in the source database")
    commands.add_parser("provision", help="create the CDC source, Flink statements, Iceberg tables and context")
    sim = commands.add_parser("simulate", help="write orders and stock movements into the source database")
    sim.add_argument("--rate", type=float, default=2.0, help="events per second (default 2)")
    sim.add_argument("--duration", type=float, help="seconds to run (default: until Ctrl-C)")
    commands.add_parser("analytics", help="report on the Iceberg tables with DuckDB")
    ask = commands.add_parser("ask", help="ask the support agent (Claude over the MCP context tools)")
    ask.add_argument("question", nargs="*", help="question (default: two sample questions)")
    commands.add_parser("teardown", help="remove what provision created")
    args = parser.parse_args(argv)

    try:
        settings = load_settings(args.env)
        pipeline = load_pipeline()
        match args.command:
            case "init-db":
                simulate.init_db(settings)
            case "provision":
                platform_for(settings).provision(pipeline)
            case "simulate":
                simulate.simulate(settings, args.rate, args.duration)
            case "analytics":
                analytics.run(platform_for(settings))
            case "ask":
                platform = platform_for(settings)
                questions = [" ".join(args.question)] if args.question else DEFAULT_QUESTIONS
                for question in questions:
                    print(f"\nQ: {question}")
                    print(f"A: {asyncio.run(agent.ask(platform, question))}")
            case "teardown":
                platform_for(settings).teardown(pipeline)
    except (ConfigError, PlatformError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
