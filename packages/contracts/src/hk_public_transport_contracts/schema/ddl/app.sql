-- Serving schema for shippable app database (e.g., iOS).
-- Canonical stable TEXT keys are intentionally excluded.

CREATE TABLE meta (
  meta_id         INTEGER PRIMARY KEY CHECK (meta_id = 1),
  schema_version  INTEGER NOT NULL,
  bundle_version  TEXT    NOT NULL,
  generated_at    TEXT    NOT NULL,
  notes           TEXT
);

CREATE TABLE operators (
  operator_id       INTEGER PRIMARY KEY,
  operator_code     TEXT NOT NULL UNIQUE,
  operator_name_en  TEXT NOT NULL,
  operator_name_tc  TEXT,
  operator_name_sc  TEXT
);

CREATE TABLE places (
  place_id          INTEGER PRIMARY KEY,
  place_type_id     INTEGER NOT NULL,
  primary_mode_id   INTEGER NOT NULL,

  name_en           TEXT,
  name_tc           TEXT,
  name_sc           TEXT,

  lat_e7            INTEGER,
  lon_e7            INTEGER,

  parent_place_id   INTEGER REFERENCES places(place_id)
);

CREATE INDEX idx_places_parent ON places(parent_place_id);
CREATE INDEX idx_places_type_mode ON places(place_type_id, primary_mode_id);

CREATE TABLE routes (
  route_id             INTEGER PRIMARY KEY,
  operator_id          INTEGER NOT NULL REFERENCES operators(operator_id),
  mode_id              INTEGER NOT NULL,

  route_short_name     TEXT,
  origin_text_en       TEXT,
  origin_text_tc       TEXT,
  origin_text_sc       TEXT,
  destination_text_en  TEXT,
  destination_text_tc  TEXT,
  destination_text_sc  TEXT,

  journey_time_minutes INTEGER,
  upstream_route_id    TEXT
);

CREATE INDEX idx_routes_op_short ON routes(operator_id, route_short_name);
CREATE INDEX idx_routes_mode_short ON routes(mode_id, route_short_name);

CREATE TABLE route_patterns (
  pattern_id          INTEGER PRIMARY KEY,
  route_id            INTEGER NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
  route_seq           INTEGER,
  direction_id        INTEGER NOT NULL,
  service_type_id     INTEGER NOT NULL,
  sequence_incomplete INTEGER NOT NULL DEFAULT 0 CHECK (sequence_incomplete IN (0,1)),
  is_circular         INTEGER NOT NULL DEFAULT 0 CHECK (is_circular IN (0,1))
);

CREATE INDEX idx_route_patterns_route ON route_patterns(route_id, route_seq, direction_id, service_type_id);

CREATE TABLE pattern_stops (
  pattern_id    INTEGER NOT NULL REFERENCES route_patterns(pattern_id) ON DELETE CASCADE,
  seq           INTEGER NOT NULL CHECK (seq >= 1),
  place_id      INTEGER NOT NULL REFERENCES places(place_id),
  allow_repeat  INTEGER NOT NULL DEFAULT 0 CHECK (allow_repeat IN (0,1)),
  PRIMARY KEY (pattern_id, seq)
) WITHOUT ROWID;

CREATE UNIQUE INDEX idx_pattern_stops_no_repeat
  ON pattern_stops(pattern_id, place_id)
  WHERE allow_repeat = 0;

CREATE TABLE fare_products (
  fare_product_id  INTEGER PRIMARY KEY,
  mode_id          INTEGER NOT NULL
);

CREATE TABLE fare_segments (
  route_id        INTEGER NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
  fare_product_id INTEGER NOT NULL REFERENCES fare_products(fare_product_id) ON DELETE CASCADE,
  origin_seq      INTEGER NOT NULL,
  dest_from_seq   INTEGER NOT NULL,
  dest_to_seq     INTEGER NOT NULL,
  amount_cents    INTEGER NOT NULL,
  is_default      INTEGER NOT NULL DEFAULT 1 CHECK (is_default IN (0,1)),
  CHECK (dest_to_seq >= dest_from_seq),
  PRIMARY KEY (route_id, fare_product_id, origin_seq, dest_from_seq)
) WITHOUT ROWID;

-- Contentless FTS index for app search.
-- unicode61 does not segment Chinese. 
-- We pre-segment TC/SC into space-separated characters
-- to allow short queries like "機場".
CREATE TABLE search_docs (
  doc_id      INTEGER PRIMARY KEY,
  kind        TEXT    NOT NULL CHECK (kind IN ('p','r')),
  ref_id      INTEGER NOT NULL,
  mode_id     INTEGER NOT NULL,
  operator_id INTEGER,
  code        TEXT    NOT NULL
) WITHOUT ROWID;

CREATE INDEX idx_search_docs_kind_ref ON search_docs(kind, ref_id);

CREATE VIRTUAL TABLE search_fts USING fts5(
  kind        UNINDEXED,   -- 'p' (places) or 'r' (routes)
  ref_id      UNINDEXED,
  mode_id     UNINDEXED,
  operator_id UNINDEXED, 
  code,
  en,
  tc,
  sc,
  tokenize = "unicode61 remove_diacritics 2",
  content=''
);
