-- List stops for CTB bus route and all patterns and sequences.
WITH bus_route AS (
  SELECT r.route_id
  FROM routes r
  JOIN operators o ON o.operator_pk = r.operator_pk
  WHERE o.operator_code = 'CTB'
    AND r.route_short_name = '{route_short_name}'
),
patterns AS (
  SELECT rp.pattern_id, rp.route_id, rp.route_seq, rp.direction_id
  FROM route_patterns rp
  JOIN bus_route r ON r.route_id = rp.route_id
)
SELECT
  p.pattern_id,
  p.route_seq,
  p.direction_id,
  ps.seq AS stop_seq,
  pl.place_id,
  pl.name_tc,
FROM patterns p
JOIN pattern_stops ps ON ps.pattern_id = p.pattern_id
JOIN places pl ON pl.place_id = ps.place_id
ORDER BY p.pattern_id, ps.seq;
