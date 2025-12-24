SELECT
  o.operator_code,
  r.route_short_name,
  r.origin_text_tc,
  r.destination_text_tc,
  r.journey_time_minutes
FROM routes r
JOIN operators o ON o.operator_id = r.operator_id
WHERE r.route_short_name = '{route_short_name}'
ORDER BY o.operator_code, r.route_id;
