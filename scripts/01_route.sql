select 
  operator_id,
  route_short_name,
  origin_text_sc,
  destination_text_sc,
from routes where route_short_name = '{route_short_name}';
