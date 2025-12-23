-- List stops bus route and all patterns and sequences (CTB only for now).
WITH bus_route AS (
  SELECT route_id, route_key, route_short_name
  FROM routes
  WHERE mode = 'bus'
    AND operator_id = 'operator:CTB'
    AND route_short_name = '{route_short_name}'
),
patterns AS (
  SELECT rp.pattern_id, rp.pattern_key, rp.route_id, rp.route_seq, rp.direction_id
  FROM route_patterns rp
  JOIN bus_route r ON r.route_id = rp.route_id
)
SELECT
  r.route_key,
  p.pattern_key,
  p.route_seq,
  p.direction_id,
  ps.seq AS stop_seq,
  pl.place_key,
  pl.name_tc,
  pl.display_name_tc
FROM patterns p
JOIN pattern_stops ps ON ps.pattern_id = p.pattern_id
JOIN places pl ON pl.place_id = ps.place_id
JOIN bus_route r ON r.route_id = p.route_id
ORDER BY p.pattern_key, ps.seq;
