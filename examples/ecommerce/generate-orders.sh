#!/usr/bin/env sh
# Streams shop activity: new orders, shipments, cancellations and stock movements.
# Usage: generate-orders.sh [orders-per-second] (default 2)
set -e
RATE="${1:-2}"
COMPOSE="docker compose -f $(cd "$(dirname "$0")/../.." && pwd)/deploy/compose/docker-compose.yml"
echo "Generating ~$RATE order event(s)/s into shop-db (Ctrl-C to stop)"
$COMPOSE exec -T shop-db psql -U shop -d shop -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$
DECLARE
  pause double precision := 1.0 / $RATE;
  roll double precision;
BEGIN
  LOOP
    roll := random();
    IF roll < 0.6 THEN
      -- a new open order for a random customer
      INSERT INTO orders (customer_id, status, total)
      VALUES (1 + floor(random() * 50)::int, 'open', round((10 + random() * 490)::numeric, 2));
    ELSIF roll < 0.85 THEN
      UPDATE orders SET status = 'shipped', updated_at = now()
      WHERE order_id = (SELECT order_id FROM orders WHERE status = 'open' ORDER BY random() LIMIT 1);
    ELSIF roll < 0.92 THEN
      UPDATE orders SET status = 'cancelled', updated_at = now()
      WHERE order_id = (SELECT order_id FROM orders WHERE status = 'open' ORDER BY random() LIMIT 1);
    ELSE
      UPDATE inventory SET on_hand = greatest(0, on_hand + (floor(random() * 21) - 10)::int), updated_at = now()
      WHERE product_id = 1 + floor(random() * 20)::int;
    END IF;
    COMMIT;
    PERFORM pg_sleep(pause);
  END LOOP;
END
\$\$;
SQL
