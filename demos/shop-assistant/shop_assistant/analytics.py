"""Analytics on the Iceberg tables, read with DuckDB through the platform's Iceberg REST catalog.

DuckDB's Iceberg extension reads upsert tables correctly (including equality deletes)."""

from __future__ import annotations

import duckdb

from .platforms.base import Platform

# Only pipeline columns are selected: platforms add their own metadata columns to Iceberg tables.
REPORTS = {
    "Revenue by customer tier": """
        SELECT tier, count(*) AS customers, sum(orders) AS orders, round(sum(lifetime_value), 2) AS revenue
        FROM customer_360 GROUP BY tier ORDER BY revenue DESC""",
    "Top 5 customers": """
        SELECT customer_id, name, country, orders, round(lifetime_value, 2) AS lifetime_value
        FROM customer_360 ORDER BY lifetime_value DESC, customer_id LIMIT 5""",
    "Products to restock": """
        SELECT product_id, name, category, available FROM inventory_live
        WHERE low_stock ORDER BY available, product_id""",
}


def run(platform: Platform, log=print) -> dict[str, list[tuple]]:
    access = platform.iceberg()
    db = duckdb.connect()
    db.sql("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    for statement in access.setup:
        db.sql(statement)
    db.sql(access.attach)
    namespace = '"' + access.namespace.replace('"', '""') + '"'
    for table in ("customer_360", "inventory_live"):
        db.sql(f"CREATE VIEW {table} AS SELECT * FROM lake.{namespace}.{table}")
        log(f"{access.namespace}.{table}: {db.sql(f'SELECT count(*) FROM {table}').fetchone()[0]} rows")
    results = {}
    for title, sql in REPORTS.items():
        relation = db.sql(sql)
        rows = relation.fetchall()
        results[title] = rows
        log(f"\n{title}")
        log(_table(relation.columns, rows))
    return results


def _table(columns: list[str], rows: list[tuple]) -> str:
    cells = [[str(c) for c in columns]] + [["NULL" if v is None else str(v) for v in row] for row in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(columns))]
    lines = ["  ".join(v.ljust(w) for v, w in zip(r, widths)).rstrip() for r in cells]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines) if rows else lines[0] + "\n(no rows)"
