-- Mapcase: NWS caching schema (idempotent)
-- Safe to run repeatedly: uses IF NOT EXISTS and does not drop/overwrite tables.

-- 1) Gridpoints (forecast key = grid_id + grid_x + grid_y)
CREATE TABLE IF NOT EXISTS nws_gridpoints (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    grid_id              TEXT NOT NULL,          -- e.g., "CTP" (WFO/CWA grid id)
    grid_x               INTEGER NOT NULL,
    grid_y               INTEGER NOT NULL,

    -- URLs returned by /points/{lat},{lon}
    forecast_url         TEXT,
    forecast_hourly_url  TEXT,
    forecast_griddata_url TEXT,
    observation_stations_url TEXT,

    -- useful metadata (also from /points payload)
    time_zone            TEXT,
    radar_station        TEXT,
    county_url           TEXT,
    fire_weather_zone_url TEXT,

    -- bookkeeping
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT nws_gridpoints_uq UNIQUE (grid_id, grid_x, grid_y)
);

CREATE INDEX IF NOT EXISTS idx_nws_gridpoints_grid
    ON nws_gridpoints (grid_id, grid_x, grid_y);


-- 2) Cache of /points lookups by lat/lon for “nearby” reuse
CREATE TABLE IF NOT EXISTS nws_point_lookup_cache (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    query_lat           DOUBLE PRECISION NOT NULL CHECK (query_lat BETWEEN -90 AND 90),
    query_lon           DOUBLE PRECISION NOT NULL CHECK (query_lon BETWEEN -180 AND 180),

    -- Geography point for fast nearest-neighbor / within-radius searches
    query_geog          GEOGRAPHY(Point, 4326)
        GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(query_lon, query_lat), 4326)::geography) STORED,

    gridpoint_id        BIGINT NOT NULL REFERENCES nws_gridpoints(id) ON DELETE CASCADE,

    -- Raw /points JSON (keep it for debugging + future fields)
    raw_points_json     JSONB NOT NULL,

    -- caching headers (optional but useful)
    etag                TEXT,
    last_modified       TIMESTAMPTZ,

    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT nws_point_lookup_cache_gridpoint_fetched_uq
        UNIQUE (gridpoint_id, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_nws_point_lookup_cache_geog
    ON nws_point_lookup_cache
    USING GIST (query_geog);

CREATE INDEX IF NOT EXISTS idx_nws_point_lookup_cache_expires
    ON nws_point_lookup_cache (expires_at);

CREATE INDEX IF NOT EXISTS idx_nws_point_lookup_cache_gridpoint
    ON nws_point_lookup_cache (gridpoint_id);


-- 3) Stations (for observations, and “is station the same?” questions later)
CREATE TABLE IF NOT EXISTS nws_stations (
    station_id          TEXT PRIMARY KEY,        -- e.g., "KMDT"
    name                TEXT,

    station_geog        GEOGRAPHY(Point, 4326),

    elevation_m         NUMERIC,
    time_zone           TEXT,

    raw_station_json    JSONB,

    first_seen          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nws_stations_geog
    ON nws_stations
    USING GIST (station_geog);


-- 4) Gridpoint <-> Stations mapping (rank 1 = closest / preferred)
CREATE TABLE IF NOT EXISTS nws_gridpoint_stations (
    gridpoint_id        BIGINT NOT NULL REFERENCES nws_gridpoints(id) ON DELETE CASCADE,
    station_id          TEXT NOT NULL REFERENCES nws_stations(station_id) ON DELETE CASCADE,

    rank                SMALLINT,                -- 1,2,3... (your code decides ordering)
    is_primary          BOOLEAN NOT NULL DEFAULT FALSE,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (gridpoint_id, station_id)
);

CREATE INDEX IF NOT EXISTS idx_nws_gridpoint_stations_station
    ON nws_gridpoint_stations (station_id);

CREATE INDEX IF NOT EXISTS idx_nws_gridpoint_stations_primary
    ON nws_gridpoint_stations (gridpoint_id, is_primary);


-- 5) Forecast cache (latest per gridpoint + type)
-- forecast_type: 'forecast' | 'hourly' | 'griddata'
CREATE TABLE IF NOT EXISTS nws_forecast_cache (
    gridpoint_id        BIGINT NOT NULL REFERENCES nws_gridpoints(id) ON DELETE CASCADE,
    forecast_type       TEXT NOT NULL CHECK (forecast_type IN ('forecast','hourly','griddata')),

    source_url          TEXT,
    data_json           JSONB NOT NULL,

    -- caching headers
    etag                TEXT,
    last_modified       TIMESTAMPTZ,

    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,

    -- optional diagnostics
    status_code         INTEGER,
    error               TEXT,

    PRIMARY KEY (gridpoint_id, forecast_type)
);

CREATE INDEX IF NOT EXISTS idx_nws_forecast_cache_expires
    ON nws_forecast_cache (expires_at);

CREATE INDEX IF NOT EXISTS idx_nws_forecast_cache_gridpoint
    ON nws_forecast_cache (gridpoint_id);