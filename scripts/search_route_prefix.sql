-- Route-number prefix search (B-tree).
-- Inputs:
--   :q           (TEXT) required
--   :mode_id     (INTEGER) optional
--   :operator_id (INTEGER) optional (routes.operator_id / operators.operator_id)
SELECT route_id, route_short_name, mode_id, operator_id
FROM routes
WHERE route_short_name LIKE :q || '%'
  AND (:mode_id IS NULL OR mode_id = :mode_id)
  AND (:operator_id IS NULL OR operator_id = :operator_id)
ORDER BY
  (route_short_name = :q) DESC,
  LENGTH(route_short_name) ASC,
  route_short_name ASC
LIMIT 50;

