-- E-commerce streamhouse: shop database -> live customer 360 and inventory, as Iceberg tables
-- for analytics and as real-time context for support agents.
--
--   bin/shctl sql -f demos/shop-assistant/sql/pipeline.sql --wait

-- Capture ------------------------------------------------------------------------------------

CREATE CONNECTION shop_pg TYPE POSTGRES WITH (
  host = 'shop-db',
  port = '5432',
  database = 'shop',
  user = 'debezium',
  password = SECRET 'shop_pg_pwd',
  publication = 'streamhouse'
);

-- One compacted topic per table: shop.public.customers, shop.public.orders, ...
CREATE SOURCE shop FROM CONNECTION shop_pg
  TABLES (public.customers, public.orders, public.products, public.inventory);

-- Transform ----------------------------------------------------------------------------------

-- Everything support needs to know about a customer, kept up to date on every order change.
CREATE MATERIALIZED VIEW customer_360 PRIMARY KEY (customer_id) AS
SELECT
  c.customer_id,
  c.first_name || ' ' || c.last_name AS name,
  c.email,
  c.country,
  c.tier,
  COUNT(o.order_id) AS orders,
  COALESCE(SUM(CASE WHEN o.status <> 'cancelled' THEN o.total END), 0) AS lifetime_value,
  SUM(CASE WHEN o.status = 'open' THEN 1 ELSE 0 END) AS open_orders,
  MAX(o.created_at) AS last_order_at
FROM `shop.public.customers` AS c
LEFT JOIN `shop.public.orders` AS o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.country, c.tier;

-- Sellable stock per product.
CREATE MATERIALIZED VIEW inventory_live PRIMARY KEY (product_id) AS
SELECT
  p.product_id,
  p.sku,
  p.name,
  p.category,
  p.price,
  i.on_hand,
  i.reserved,
  i.on_hand - i.reserved AS available,
  i.on_hand - i.reserved <= 5 AS low_stock,
  i.updated_at
FROM `shop.public.products` AS p
JOIN `shop.public.inventory` AS i ON i.product_id = p.product_id;

-- Serve: analytics (Iceberg) -----------------------------------------------------------------

ALTER TOPIC customer_360 ENABLE ICEBERG;
ALTER TOPIC inventory_live ENABLE ICEBERG;
-- Every order change, as history.
ALTER TOPIC shop.public.orders ENABLE ICEBERG WITH (mode = 'append');

-- Serve: real-time context for apps and agents ------------------------------------------------

ALTER TOPIC customer_360 ENABLE CONTEXT WITH (
  description = 'One row per customer: contact details, tier, number of orders, lifetime value (excluding cancelled orders), open orders and time of the last order.'
);
ALTER TOPIC inventory_live ENABLE CONTEXT WITH (
  description = 'One row per product: price, stock on hand, reserved and available units, and a low_stock flag (5 units or fewer available).'
);
ALTER TOPIC shop.public.orders ENABLE CONTEXT WITH (
  description = 'Current state of every order: customer, status (open, shipped, cancelled), total and timestamps.'
);

-- Govern -------------------------------------------------------------------------------------

GRANT SELECT ON CONTEXT customer_360 TO ROLE support_agent;
GRANT SELECT ON CONTEXT inventory_live TO ROLE support_agent;
GRANT SELECT ON CONTEXT shop.public.orders TO ROLE support_agent;
