from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.http_cache import utcnow


async def get_nearest_cached_gridpoint(
    engine: AsyncEngine,
    *,
    lat: float,
    lon: float,
    radius_m: float,
) -> Optional[dict[str, Any]]:
    """
    Returns {gridpoint_id, distance_m} if a non-expired nearby cached point exists.
    """
    sql = text(
        """
        SELECT
          pc.gridpoint_id,
          ST_Distance(
            pc.query_geog,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
          ) AS distance_m
        FROM nws_point_cache pc
        WHERE pc.expires_at > now()
          AND ST_DWithin(
            pc.query_geog,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
            :radius_m
          )
        ORDER BY distance_m ASC
        LIMIT 1;
        """
    )
    async with engine.connect() as conn:
        r = await conn.execute(sql, {"lat": lat, "lon": lon, "radius_m": radius_m})
        row = r.mappings().first()
        return dict(row) if row else None


async def get_gridpoint(engine: AsyncEngine, gridpoint_id: int) -> Optional[dict[str, Any]]:
    sql = text("SELECT * FROM nws_gridpoints WHERE id = :id;")
    async with engine.connect() as conn:
        r = await conn.execute(sql, {"id": gridpoint_id})
        row = r.mappings().first()
        return dict(row) if row else None


async def upsert_gridpoint_from_points(
    engine: AsyncEngine,
    *,
    points_json: dict[str, Any],
) -> int:
    props = points_json.get("properties", {}) or {}

    grid_id = props.get("gridId")
    grid_x = props.get("gridX")
    grid_y = props.get("gridY")

    if not (grid_id and grid_x is not None and grid_y is not None):
        raise ValueError("NWS /points response missing gridId/gridX/gridY")

    sql = text(
        """
        INSERT INTO nws_gridpoints (
          grid_id, grid_x, grid_y,
          forecast_url, forecast_hourly_url, forecast_griddata_url, observation_stations_url,
          time_zone, radar_station,
          raw_json, updated_at
        )
        VALUES (
          :grid_id, :grid_x, :grid_y,
          :forecast_url, :forecast_hourly_url, :forecast_griddata_url, :observation_stations_url,
          :time_zone, :radar_station,
          :raw_json::jsonb, now()
        )
        ON CONFLICT (grid_id, grid_x, grid_y)
        DO UPDATE SET
          forecast_url = EXCLUDED.forecast_url,
          forecast_hourly_url = EXCLUDED.forecast_hourly_url,
          forecast_griddata_url = EXCLUDED.forecast_griddata_url,
          observation_stations_url = EXCLUDED.observation_stations_url,
          time_zone = EXCLUDED.time_zone,
          radar_station = EXCLUDED.radar_station,
          raw_json = EXCLUDED.raw_json,
          updated_at = now()
        RETURNING id;
        """
    )

    payload = {
        "grid_id": grid_id,
        "grid_x": int(grid_x),
        "grid_y": int(grid_y),
        "forecast_url": props.get("forecast"),
        "forecast_hourly_url": props.get("forecastHourly"),
        "forecast_griddata_url": props.get("forecastGridData"),
        "observation_stations_url": props.get("observationStations"),
        "time_zone": props.get("timeZone"),
        "radar_station": props.get("radarStation"),
        "raw_json": json.dumps(points_json),
    }

    async with engine.begin() as conn:
        r = await conn.execute(sql, payload)
        return int(r.scalar_one())


async def insert_point_cache(
    engine: AsyncEngine,
    *,
    lat: float,
    lon: float,
    gridpoint_id: int,
    distance_m: Optional[float],
    points_json: dict[str, Any],
    etag: Optional[str],
    last_modified: Optional[datetime],
    expires_at: datetime,
) -> None:
    sql = text(
        """
        INSERT INTO nws_point_cache (
          query_geog, gridpoint_id, distance_m,
          raw_json, etag, last_modified,
          fetched_at, expires_at
        )
        VALUES (
          ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
          :gridpoint_id,
          :distance_m,
          :raw_json::jsonb,
          :etag,
          :last_modified,
          now(),
          :expires_at
        );
        """
    )
    async with engine.begin() as conn:
        await conn.execute(
            sql,
            {
                "lat": lat,
                "lon": lon,
                "gridpoint_id": gridpoint_id,
                "distance_m": distance_m,
                "raw_json": json.dumps(points_json),
                "etag": etag,
                "last_modified": last_modified,
                "expires_at": expires_at,
            },
        )


async def get_forecast_cache(
    engine: AsyncEngine, *, gridpoint_id: int, forecast_type: str
) -> Optional[dict[str, Any]]:
    sql = text(
        """
        SELECT *
        FROM nws_forecasts
        WHERE gridpoint_id = :gridpoint_id
          AND forecast_type = :forecast_type
        LIMIT 1;
        """
    )
    async with engine.connect() as conn:
        r = await conn.execute(sql, {"gridpoint_id": gridpoint_id, "forecast_type": forecast_type})
        row = r.mappings().first()
        return dict(row) if row else None


async def upsert_forecast_cache(
    engine: AsyncEngine,
    *,
    gridpoint_id: int,
    forecast_type: str,
    url: str,
    data_json: dict[str, Any],
    status_code: int,
    error: Optional[str],
    etag: Optional[str],
    last_modified: Optional[datetime],
    expires_at: datetime,
) -> None:
    sql = text(
        """
        INSERT INTO nws_forecasts (
          gridpoint_id, forecast_type, url,
          data_json, status_code, error,
          etag, last_modified,
          fetched_at, expires_at,
          updated_at
        )
        VALUES (
          :gridpoint_id, :forecast_type, :url,
          :data_json::jsonb, :status_code, :error,
          :etag, :last_modified,
          now(), :expires_at,
          now()
        )
        ON CONFLICT (gridpoint_id, forecast_type)
        DO UPDATE SET
          url = EXCLUDED.url,
          data_json = EXCLUDED.data_json,
          status_code = EXCLUDED.status_code,
          error = EXCLUDED.error,
          etag = EXCLUDED.etag,
          last_modified = EXCLUDED.last_modified,
          fetched_at = now(),
          expires_at = EXCLUDED.expires_at,
          updated_at = now();
        """
    )
    async with engine.begin() as conn:
        await conn.execute(
            sql,
            {
                "gridpoint_id": gridpoint_id,
                "forecast_type": forecast_type,
                "url": url,
                "data_json": json.dumps(data_json),
                "status_code": status_code,
                "error": error,
                "etag": etag,
                "last_modified": last_modified,
                "expires_at": expires_at,
            },
        )


async def upsert_stations_for_gridpoint(
    engine: AsyncEngine,
    *,
    gridpoint_id: int,
    stations_geojson: dict[str, Any],
) -> int:
    """
    Stores station records + links them to the gridpoint.
    Returns count linked.
    """
    features = stations_geojson.get("features") or []
    if not isinstance(features, list):
        return 0

    # Insert/update stations and linking in one transaction
    async with engine.begin() as conn:
        linked = 0
        for idx, feature in enumerate(features):
            props = (feature or {}).get("properties") or {}
            geom = (feature or {}).get("geometry") or {}

            station_identifier = props.get("stationIdentifier") or props.get("identifier")
            if not station_identifier:
                continue

            name = props.get("name")

            # geometry is GeoJSON Point: {"type":"Point","coordinates":[lon,lat]}
            station_geog_expr = "NULL"
            params: dict[str, Any] = {
                "station_identifier": station_identifier,
                "name": name,
                "raw_json": json.dumps(feature),
            }

            coords = geom.get("coordinates")
            if isinstance(coords, list) and len(coords) == 2:
                lon, lat = coords
                station_geog_expr = "ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography"
                params["lon"] = float(lon)
                params["lat"] = float(lat)

            station_sql = text(
                f"""
                INSERT INTO nws_stations (station_identifier, name, station_geog, raw_json, updated_at)
                VALUES (:station_identifier, :name, {station_geog_expr}, :raw_json::jsonb, now())
                ON CONFLICT (station_identifier)
                DO UPDATE SET
                  name = EXCLUDED.name,
                  station_geog = EXCLUDED.station_geog,
                  raw_json = EXCLUDED.raw_json,
                  updated_at = now()
                RETURNING id;
                """
            )
            r = await conn.execute(station_sql, params)
            station_id = int(r.scalar_one())

            link_sql = text(
                """
                INSERT INTO nws_gridpoint_stations (gridpoint_id, station_id, priority)
                VALUES (:gridpoint_id, :station_id, :priority)
                ON CONFLICT (gridpoint_id, station_id)
                DO UPDATE SET priority = EXCLUDED.priority;
                """
            )
            await conn.execute(
                link_sql,
                {"gridpoint_id": gridpoint_id, "station_id": station_id, "priority": idx},
            )
            linked += 1

        return linked