"""The shop's operational database: schema setup and a live order simulator."""

from __future__ import annotations

import random
import time
from pathlib import Path

import psycopg

from .settings import ROOT, Settings

SCHEMA = ROOT / "sql" / "shop-schema.sql"
CDC_SECTION = "-- CDC access for Debezium"


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings["SOURCE_PG_HOST"], port=int(settings.get("SOURCE_PG_PORT", "5432")),
        dbname=settings["SOURCE_PG_DATABASE"], user=settings["SOURCE_PG_USER"],
        password=settings["SOURCE_PG_PASSWORD"], sslmode=settings.get("SOURCE_PG_SSLMODE", "prefer"),
        autocommit=True)


def init_db(settings: Settings, log=print) -> None:
    """Creates and seeds the shop tables unless they exist. CDC users and publications are
    platform-specific and left to the database administrator (see the README)."""
    with connect(settings) as conn:
        exists = conn.execute("SELECT to_regclass('public.orders') IS NOT NULL").fetchone()[0]
        if exists:
            log("shop tables already exist; nothing to do")
            return
        script = Path(SCHEMA).read_text()
        tables_and_seed = script.split(CDC_SECTION)[0].rsplit("-- ----", 1)[0]
        conn.execute(tables_and_seed)
        counts = conn.execute("SELECT (SELECT count(*) FROM customers), (SELECT count(*) FROM orders)").fetchone()
        log(f"created the shop schema: {counts[0]} customers, {counts[1]} orders")


def simulate(settings: Settings, rate: float, duration_s: float | None, log=print) -> None:
    """Places, ships and cancels orders and moves stock, about `rate` events per second."""
    pause = 1.0 / rate
    started = time.monotonic()
    events = 0
    with connect(settings) as conn:
        customers = [r[0] for r in conn.execute("SELECT customer_id FROM customers")]
        products = [r[0] for r in conn.execute("SELECT product_id FROM products")]
        log(f"simulating ~{rate:g} event(s)/s{'' if duration_s is None else f' for {duration_s:g}s'} (Ctrl-C to stop)")
        try:
            while duration_s is None or time.monotonic() - started < duration_s:
                roll = random.random()
                if roll < 0.6:
                    conn.execute("INSERT INTO orders (customer_id, status, total) VALUES (%s, 'open', %s)",
                                 (random.choice(customers), round(random.uniform(10, 500), 2)))
                elif roll < 0.85:
                    conn.execute("""UPDATE orders SET status = 'shipped', updated_at = now() WHERE order_id =
                                    (SELECT order_id FROM orders WHERE status = 'open' ORDER BY random() LIMIT 1)""")
                elif roll < 0.92:
                    conn.execute("""UPDATE orders SET status = 'cancelled', updated_at = now() WHERE order_id =
                                    (SELECT order_id FROM orders WHERE status = 'open' ORDER BY random() LIMIT 1)""")
                else:
                    conn.execute("""UPDATE inventory SET on_hand = greatest(0, on_hand + %s), updated_at = now()
                                    WHERE product_id = %s""", (random.randint(-10, 10), random.choice(products)))
                events += 1
                time.sleep(pause)
        except KeyboardInterrupt:
            pass
    log(f"{events} event(s) written")
