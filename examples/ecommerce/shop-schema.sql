-- E-commerce shop database: the operational (OLTP) source of the streamhouse demo.
-- Runs once on first start of shop-db (as the superuser `shop`).

CREATE TABLE customers (
    customer_id  INTEGER PRIMARY KEY,
    email        TEXT NOT NULL UNIQUE,
    first_name   TEXT NOT NULL,
    last_name    TEXT NOT NULL,
    country      TEXT NOT NULL,
    tier         TEXT NOT NULL DEFAULT 'standard' CHECK (tier IN ('standard', 'gold', 'platinum')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   INTEGER PRIMARY KEY,
    sku          TEXT NOT NULL UNIQUE,
    name         TEXT NOT NULL,
    category     TEXT NOT NULL,
    price        NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
);

CREATE TABLE orders (
    order_id     INTEGER PRIMARY KEY,
    customer_id  INTEGER NOT NULL REFERENCES customers (customer_id),
    status       TEXT NOT NULL CHECK (status IN ('open', 'shipped', 'cancelled')),
    total        NUMERIC(12, 2) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id     INTEGER NOT NULL REFERENCES orders (order_id),
    line_no      INTEGER NOT NULL,
    product_id   INTEGER NOT NULL REFERENCES products (product_id),
    quantity     INTEGER NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE inventory (
    product_id   INTEGER PRIMARY KEY REFERENCES products (product_id),
    warehouse    TEXT NOT NULL DEFAULT 'main',
    on_hand      INTEGER NOT NULL CHECK (on_hand >= 0),
    reserved     INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_customer_idx ON orders (customer_id);

-- Sequences for the order generator (seed rows use explicit ids below these starts)
CREATE SEQUENCE orders_order_id_seq START 1001 OWNED BY orders.order_id;
ALTER TABLE orders ALTER COLUMN order_id SET DEFAULT nextval('orders_order_id_seq');

-- ---------------------------------------------------------------------------
-- Seed data (deterministic)
-- ---------------------------------------------------------------------------

INSERT INTO customers (customer_id, email, first_name, last_name, country, tier, created_at)
SELECT i,
       format('customer%s@example.com', i),
       (ARRAY['Ada','Alan','Grace','Linus','Margaret','Edsger','Barbara','Ken','Frances','Dennis'])[1 + i % 10],
       (ARRAY['Lovelace','Turing','Hopper','Torvalds','Hamilton','Dijkstra','Liskov','Thompson','Allen','Ritchie'])[1 + (i * 7) % 10],
       (ARRAY['FR','US','DE','GB','NL','ES','IT','BE','CA','JP'])[1 + (i * 3) % 10],
       CASE WHEN i % 10 = 0 THEN 'platinum' WHEN i % 4 = 0 THEN 'gold' ELSE 'standard' END,
       timestamptz '2025-01-01 00:00:00+00' + (i || ' days')::interval
FROM generate_series(1, 50) AS i;

INSERT INTO products (product_id, sku, name, category, price)
SELECT i,
       format('SKU-%s', lpad(i::text, 4, '0')),
       (ARRAY['Espresso Machine','Coffee Grinder','Milk Frother','Kettle','French Press',
              'Pour-Over Kit','Tea Infuser','Travel Mug','Ceramic Cup','Bean Canister',
              'Scale','Tamper','Knock Box','Descaler','Cleaning Brush',
              'Filter Papers','Moka Pot','Cold Brew Jar','Barista Apron','Latte Art Pen'])[i],
       (ARRAY['machines','accessories','cups','maintenance'])[1 + i % 4],
       round((5 + (i * 37) % 300)::numeric + 0.99, 2)
FROM generate_series(1, 20) AS i;

INSERT INTO inventory (product_id, on_hand, reserved)
SELECT i, 20 + (i * 13) % 200, 0
FROM generate_series(1, 20) AS i;

INSERT INTO orders (order_id, customer_id, status, created_at, updated_at)
SELECT i,
       1 + (i * 17) % 50,
       CASE WHEN i % 7 = 0 THEN 'cancelled' WHEN i % 3 = 0 THEN 'open' ELSE 'shipped' END,
       timestamptz '2026-01-01 00:00:00+00' + (i || ' hours')::interval,
       timestamptz '2026-01-01 00:00:00+00' + (i || ' hours')::interval
FROM generate_series(1, 200) AS i;

INSERT INTO order_items (order_id, line_no, product_id, quantity, unit_price)
SELECT o.order_id, l.line_no, p.product_id, 1 + (o.order_id + l.line_no) % 3, p.price
FROM orders o
CROSS JOIN generate_series(1, 3) AS l(line_no)
JOIN products p ON p.product_id = 1 + (o.order_id * 5 + l.line_no * 3) % 20
WHERE l.line_no <= 1 + o.order_id % 3;

UPDATE orders o
SET total = s.total
FROM (SELECT order_id, sum(quantity * unit_price) AS total FROM order_items GROUP BY order_id) s
WHERE s.order_id = o.order_id;

-- ---------------------------------------------------------------------------
-- CDC access for Debezium
-- ---------------------------------------------------------------------------

CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Publications FOR ALL TABLES need a superuser, so it is created here once;
-- connectors use publication.name=streamhouse with publication.autocreate.mode=disabled
-- and select tables with table.include.list.
CREATE PUBLICATION streamhouse FOR ALL TABLES;
