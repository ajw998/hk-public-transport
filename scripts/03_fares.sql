-- Fares for CTB bus route (default short_name = '9'), including origin/destination sequences and fare products.
-- Parameters:
--   {route_short_name} (default '9')
WITH bus_route AS (
  SELECT route_id, route_key, route_short_name
  FROM routes
  WHERE mode = 'bus'
    AND operator_id = 'operator:CTB'
    AND route_short_name = '{route_short_name}'
    AND is_active = 1
)
SELECT
  r.route_key,
  fr.rule_key,
  fr.origin_seq,
  fr.destination_seq,
  fa.amount_cents,
  fa.is_default
FROM fare_rules fr
JOIN bus_route r ON r.route_id = fr.route_id
JOIN fare_amounts fa ON fa.fare_rule_id = fr.fare_rule_id
ORDER BY fr.rule_key, fa.fare_product_id
LIMIT 100;
