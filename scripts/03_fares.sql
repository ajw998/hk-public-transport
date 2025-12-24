-- Fare segments for CTB bus route (compressed representation).
WITH bus_route AS (
  SELECT r.route_id
  FROM routes r
  JOIN operators o ON o.operator_pk = r.operator_pk
  WHERE o.operator_code = 'CTB'
    AND r.route_short_name = '{route_short_name}'
)
SELECT
  fs.route_id,
  fs.fare_product_id,
  fs.origin_seq,
  fs.dest_from_seq,
  fs.dest_to_seq,
  fs.amount_cents
FROM fare_segments fs
JOIN bus_route r ON r.route_id = fs.route_id
ORDER BY fs.route_id, fs.fare_product_id, fs.origin_seq, fs.dest_from_seq
LIMIT 200;
