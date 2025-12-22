-- Metadata
CREATE TABLE meta (
  meta_id               INTEGER PRIMARY KEY CHECK (meta_id = 1),
  schema_version        INTEGER NOT NULL,
  bundle_version        TEXT    NOT NULL,
  generated_at          TEXT    NOT NULL,
  source_versions_json  TEXT    NOT NULL,
  notes                TEXT
);

-- Operators
CREATE TABLE operators (
  operator_id            TEXT PRIMARY KEY, -- canonical enum-like string
  operator_name_en       TEXT NOT NULL,
  operator_name_tc       TEXT,
  operator_name_sc       TEXT,
  is_active              INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))
) WITHOUT ROWID;

-- Places
CREATE TABLE places (
  place_id               INTEGER PRIMARY KEY,
  place_key              TEXT NOT NULL UNIQUE,
  place_type             TEXT NOT NULL CHECK (
    place_type IN (
      'stop',            -- generic boarding/alighting point (bus/tram/lr/etc)
      'station',         -- rail station (node; can have exits/platforms)
      'station_complex', -- complex/interchange grouping entity
      'pier',            -- ferry terminal/pier
      'platform',        -- platform (child of station)
      'entrance_exit',   -- station exit/entrance (child of station)
      'interchange',     -- virtual interchange node
      'other'
    )
  ),
  primary_mode           TEXT NOT NULL DEFAULT 'unknown' CHECK (
    primary_mode IN ('bus','gmb','mtr','lightrail','mtr_bus','ferry','tram','peak_tram','unknown')
  ),

  -- Names
  name_en                TEXT,
  name_tc                TEXT,
  name_sc                TEXT,
  display_name_en        TEXT,
  display_name_tc        TEXT,
  display_name_sc        TEXT,

  -- Geometry
  lat                    REAL,
  lon                    REAL,
  hk80_x                 INTEGER,
  hk80_y                 INTEGER,

  -- Parent/containment (reserved for stations [station complex > station > entrance] )
  parent_place_id        INTEGER REFERENCES places(place_id),

  is_active              INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))
);

CREATE INDEX idx_places_parent       ON places(parent_place_id);
CREATE INDEX idx_places_type_mode    ON places(place_type, primary_mode, is_active);

-- Routes
CREATE TABLE routes (
  -- IMPORTANT: `route_id` is the internal id, not the upstream id. To perform upstream route_id correlation,
  -- use the `route_key` instead.
  route_id               INTEGER PRIMARY KEY,
  -- This is the canonical stable key.
  route_key              TEXT NOT NULL UNIQUE,
  upstream_route_id      TEXT NOT NULL,

  mode                   TEXT NOT NULL CHECK (
    mode IN ('bus','gmb','mtr','lightrail','mtr_bus','ferry','tram','peak_tram')
  ),
  operator_id            TEXT NOT NULL REFERENCES operators(operator_id),

  -- Public-facing identifiers
  route_short_name       TEXT,
  route_long_name_en     TEXT,
  route_long_name_tc     TEXT,
  route_long_name_sc     TEXT,
  origin_text_en         TEXT,
  origin_text_tc         TEXT,
  origin_text_sc         TEXT,
  destination_text_en    TEXT,
  destination_text_tc    TEXT,
  destination_text_sc    TEXT,

  -- Region/district code
  service_area_code      TEXT,

  journey_time_minutes   INTEGER,

  is_active              INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))
);

CREATE INDEX idx_routes_mode_op_short ON routes(mode, operator_id, route_short_name);
CREATE INDEX idx_routes_operator_mode ON routes(operator_id, mode, is_active);
CREATE INDEX idx_routes_mode_upstream ON routes(mode, upstream_route_id);

-- Route patterns
CREATE TABLE route_patterns (
  -- IMPORTANT: This is the internal pattern ID
  pattern_id             INTEGER PRIMARY KEY,
  -- Just like `route_key` this is the canonical stable key
  pattern_key            TEXT NOT NULL UNIQUE,
  route_id               INTEGER NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
  route_seq              INTEGER,

  -- Direction semantics:
  -- 0 unknown; 1 outbound; 2 inbound; 3 clockwise; 4 counterclockwise
  direction_id           INTEGER NOT NULL DEFAULT 0 CHECK (direction_id IN (0,1,2,3,4)),

  -- Destination/headsign text (not necessarily identical to route.destination_text_*)
  headsign_en            TEXT,
  headsign_tc            TEXT,
  headsign_sc            TEXT,

  -- Service type / variant (must be mode-agnostic)
  service_type           TEXT NOT NULL DEFAULT 'regular' CHECK (
    service_type IN ('regular','night','express','holiday','limited','special','unknown')
  ),

  -- Sequence quality flags (e.g., HKTD notes GMB may only have termini in RSTOP) 
  sequence_incomplete    INTEGER NOT NULL DEFAULT 0 CHECK (sequence_incomplete IN (0,1)),
  is_circular            INTEGER NOT NULL DEFAULT 0 CHECK (is_circular IN (0,1)),

  is_active              INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))
);

CREATE INDEX idx_route_patterns_route_dir ON route_patterns(route_id, direction_id, service_type, is_active);

-- Ordered stop sequence for each pattern
CREATE TABLE pattern_stops (
  pattern_id             INTEGER NOT NULL REFERENCES route_patterns(pattern_id) ON DELETE CASCADE,
  seq                    INTEGER NOT NULL CHECK (seq >= 1),
  place_id               INTEGER NOT NULL REFERENCES places(place_id),

  -- If 0, we expect place_id to be unique within the pattern
  -- if 1, allow repeated stops
  allow_repeat           INTEGER NOT NULL DEFAULT 0 CHECK (allow_repeat IN (0,1)),

  PRIMARY KEY (pattern_id, seq)
) WITHOUT ROWID;

CREATE UNIQUE INDEX ux_pattern_stops_no_repeat
  ON pattern_stops(pattern_id, place_id)
  WHERE allow_repeat = 0;

CREATE INDEX idx_pattern_stops_place       ON pattern_stops(place_id, pattern_id);
CREATE INDEX idx_pattern_stops_pattern_seq ON pattern_stops(pattern_id, seq);

-- Fares
CREATE TABLE fare_products (
  fare_product_id        INTEGER PRIMARY KEY,
  product_key            TEXT NOT NULL UNIQUE,
  mode                   TEXT CHECK (mode IN ('bus','gmb','mtr','lightrail','mtr_bus','ferry','tram','peak_tram')),

  is_active              INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))
);

-- Fare rules
CREATE TABLE fare_rules (
  fare_rule_id           INTEGER PRIMARY KEY,
  -- Canonical stable fare_rule key
  rule_key               TEXT NOT NULL UNIQUE,

  operator_id            TEXT NOT NULL REFERENCES operators(operator_id),
  mode                   TEXT NOT NULL CHECK (mode IN ('bus','gmb','mtr','lightrail','mtr_bus','ferry','tram','peak_tram')),

  route_id               INTEGER REFERENCES routes(route_id),
  pattern_id             INTEGER REFERENCES route_patterns(pattern_id),

  origin_seq             INTEGER CHECK (origin_seq IS NULL OR origin_seq >= 1),
  destination_seq        INTEGER CHECK (destination_seq IS NULL OR destination_seq >= 1),

  is_active              INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))

  CHECK (
    route_id IS NOT NULL
    OR pattern_id IS NOT NULL
  )
);

CREATE INDEX idx_fare_rules_route    ON fare_rules(route_id, pattern_id, is_active);

CREATE TABLE fare_amounts (
  fare_rule_id           INTEGER NOT NULL REFERENCES fare_rules(fare_rule_id) ON DELETE CASCADE,
  fare_product_id        INTEGER NOT NULL REFERENCES fare_products(fare_product_id),
  amount_cents           INTEGER NOT NULL CHECK (amount_cents >= 0),
  is_default             INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0,1)),
  PRIMARY KEY (fare_rule_id, fare_product_id)
) WITHOUT ROWID;

CREATE TABLE service_calendars (
  service_id     INTEGER PRIMARY KEY,
  start_date     INTEGER NOT NULL,
  end_date       INTEGER NOT NULL,
  monday         INTEGER NOT NULL CHECK (monday IN (0,1)),
  tuesday         INTEGER NOT NULL CHECK (tuesday IN (0,1)),
  wednesday         INTEGER NOT NULL CHECK (wednesday IN (0,1)),
  thursday         INTEGER NOT NULL CHECK (thursday IN (0,1)),
  friday         INTEGER NOT NULL CHECK (friday IN (0,1)),
  saturday         INTEGER NOT NULL CHECK (saturday IN (0,1)),
  sunday         INTEGER NOT NULL CHECK (sunday IN (0,1))
) WITHOUT ROWID;

CREATE TABLE headway_trips (
  trip_id            TEXT PRIMARY KEY,
  upstream_route_id  INTEGER NOT NULL,
  route_seq          INTEGER,
  service_id         INTEGER NOT NULL REFERENCES service_calendars(service_id),
  departure_time     TEXT
) WITHOUT ROWID;

CREATE INDEX idx_headway_trips_route_service
  ON headway_trips(upstream_route_id, route_seq, service_id);

CREATE TABLE headway_frequencies (
  upstream_route_id  INTEGER NOT NULL,
  -- TODO: This should correlate with route key
  route_seq          INTEGER,
  service_id         INTEGER NOT NULL REFERENCES service_calendars(service_id),
  start_time         TEXT NOT NULL,
  end_time           TEXT NOT NULL,
  headway_secs       INTEGER NOT NULL CHECK (headway_secs > 0),
  sample_trip_id     TEXT,
  PRIMARY KEY (upstream_route_id, route_seq, service_id, start_time, end_time)
) WITHOUT ROWID;

CREATE INDEX idx_headway_freq_service
  ON headway_frequencies(service_id, upstream_route_id, route_seq);

CREATE TABLE headway_stop_times (
  trip_id           TEXT NOT NULL REFERENCES headway_trips(trip_id) ON DELETE CASCADE,
  stop_sequence     INTEGER NOT NULL CHECK (stop_sequence >= 0),
  stop_id           INTEGER NOT NULL, -- upstream stop_id
  arrival_time      TEXT,
  departure_time    TEXT,
  pickup_type       INTEGER,
  drop_off_type     INTEGER,
  timepoint         INTEGER,
  PRIMARY KEY (trip_id, stop_sequence)
) WITHOUT ROWID;

CREATE INDEX idx_headway_stop_times_stop
  ON headway_stop_times(stop_id, departure_time);

CREATE TABLE service_exceptions (
  service_id     INTEGER NOT NULL REFERENCES service_calendars(service_id) ON DELETE CASCADE,
  date           INTEGER NOT NULL,
  exception_type INTEGER NOT NULL CHECK (exception_type IN (1,2)),
  PRIMARY KEY (service_id, date)
) WITHOUT ROWID;

CREATE INDEX idx_service_exceptions_date ON service_exceptions(date, exception_type);

CREATE TABLE pattern_headways (
  pattern_id      INTEGER NOT NULL REFERENCES route_patterns(pattern_id) ON DELETE CASCADE,
  service_id      INTEGER NOT NULL REFERENCES service_calendars(service_id),
  start_time      TEXT    NOT NULL,  -- "HH:MM:SS"
  end_time        TEXT    NOT NULL,  -- "HH:MM:SS"
  headway_secs    INTEGER NOT NULL CHECK (headway_secs > 0),
  sample_trip_id  TEXT,
  PRIMARY KEY (pattern_id, service_id, start_time, end_time)
) WITHOUT ROWID;

CREATE INDEX idx_pattern_headways_pattern
  ON pattern_headways(pattern_id, service_id);
