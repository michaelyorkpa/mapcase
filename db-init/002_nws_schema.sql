-- NWS gridpoint metadata (from /points)
CREATE TABLE IF NOT EXISTS nws_gridpoints (
  id BIGSERIAL PRIMARY KEY,
  grid_id TEXT NOT NULL,
  grid_x INTEGER NOT NULL,
  grid_y INTEGER NOT NULL,

  forecast_url TEXT,
  forecast_hourly_url TEXT,
  forecast_griddata_url TEXT,
  observation_stations_url TEXT,

  time_zone TEXT,
  radar_station TEXT,

  raw_json JSONB NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (grid_id, grid_x, grid_y)
);

-- Cache mapping: a queried lat/lon -> nearest cached gridpoint
CREATE TABLE IF NOT EXISTS nws_point_cache (
  id BIGSERIAL PRIMARY KEY,
  query_geog GEOGRAPHY(Point, 4326) NOT NULL,
  gridpoint_id BIGINT NOT NULL REFERENCES nws_gridpoints(id) ON DELETE CASCADE,

  distance_m DOUBLE PRECISION,

  raw_json JSONB NOT NULL,
  etag TEXT,
  last_modified TIMESTAMPTZ,

  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nws_point_cache_geog ON nws_point_cache USING GIST (query_geog);
CREATE INDEX IF NOT EXISTS idx_nws_point_cache_expires ON nws_point_cache (expires_at);

-- Observation stations
CREATE TABLE IF NOT EXISTS nws_stations (
  id BIGSERIAL PRIMARY KEY,
  station_identifier TEXT NOT NULL UNIQUE,
  name TEXT,
  station_geog GEOGRAPHY(Point, 4326),
  raw_json JSONB NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Link gridpoint -> stations (priority order from API)
CREATE TABLE IF NOT EXISTS nws_gridpoint_stations (
  gridpoint_id BIGINT NOT NULL REFERENCES nws_gridpoints(id) ON DELETE CASCADE,
  station_id BIGINT NOT NULL REFERENCES nws_stations(id) ON DELETE CASCADE,
  priority INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (gridpoint_id, station_id)
);

-- Forecast caches per gridpoint
CREATE TABLE IF NOT EXISTS nws_forecasts (
  id BIGSERIAL PRIMARY KEY,
  gridpoint_id BIGINT NOT NULL REFERENCES nws_gridpoints(id) ON DELETE CASCADE,
  forecast_type TEXT NOT NULL CHECK (forecast_type IN ('forecast','hourly','griddata')),
  url TEXT NOT NULL,

  data_json JSONB NOT NULL,
  status_code INTEGER,
  error TEXT,

  etag TEXT,
  last_modified TIMESTAMPTZ,

  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (gridpoint_id, forecast_type)
);

CREATE INDEX IF NOT EXISTS idx_nws_forecasts_expires ON nws_forecasts (expires_at);