from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from app.clients.nws_client import NwsClient
from app.core.geo import validate_lat_lon, assert_in_pa_bounds
from app.core.http_cache import compute_expires_at, utcnow
from app.db.nws_repo import (
    get_nearest_cached_gridpoint,
    get_gridpoint,
    upsert_gridpoint_from_points,
    insert_point_cache,
    get_forecast_cache,
    upsert_forecast_cache,
    upsert_stations_for_gridpoint,
)


class NwsWeatherService:
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        cache_radius_m: float = 2500.0,
        points_default_ttl_s: int = 86400,
        forecast_default_ttl_s: int = 600,
        stations_default_ttl_s: int = 86400,
    ):
        self.engine = engine
        self.cache_radius_m = cache_radius_m
        self.points_default_ttl_s = points_default_ttl_s
        self.forecast_default_ttl_s = forecast_default_ttl_s
        self.stations_default_ttl_s = stations_default_ttl_s
        self.nws = NwsClient()

    async def get_forecast_bundle(self, *, lat: float, lon: float) -> dict[str, Any]:
        debug: list[str] = []
        validate_lat_lon(lat, lon)
        assert_in_pa_bounds(lat, lon)
        debug.append(f"Received lat/lon: {lat},{lon}")
        debug.append("Validated lat/lon within PA bounds")

        # 1) Try cached point->gridpoint
        nearest = await get_nearest_cached_gridpoint(
            self.engine, lat=lat, lon=lon, radius_m=self.cache_radius_m
        )
        gridpoint_id = None
        gridpoint = None

        if nearest:
            gridpoint_id = int(nearest["gridpoint_id"])
            gridpoint = await get_gridpoint(self.engine, gridpoint_id)
            debug.append(
                f"Point cache hit: gridpoint_id={gridpoint_id} (distance≈{nearest['distance_m']:.1f}m)"
            )

        # 2) If no gridpoint, call NWS /points
        if not gridpoint:
            debug.append("Point cache miss -> calling NWS /points")
            points_res = await self.nws.points(lat, lon)

            if points_res.status_code != 200 or not points_res.json_data:
                return {
                    "ok": False,
                    "error": f"NWS /points failed ({points_res.status_code})",
                    "debug": debug,
                }

            points_expires_at = compute_expires_at(points_res.headers, self.points_default_ttl_s)
            gridpoint_id = await upsert_gridpoint_from_points(self.engine, points_json=points_res.json_data)
            await insert_point_cache(
                self.engine,
                lat=lat,
                lon=lon,
                gridpoint_id=gridpoint_id,
                distance_m=None,
                points_json=points_res.json_data,
                etag=points_res.etag,
                last_modified=points_res.last_modified,
                expires_at=points_expires_at,
            )
            debug.append(f"NWS /points OK -> gridpoint_id={gridpoint_id}, cached until {points_expires_at.isoformat()}")

            gridpoint = await get_gridpoint(self.engine, gridpoint_id)

        if not gridpoint:
            return {"ok": False, "error": "Unable to resolve gridpoint", "debug": debug}

        # 3) Stations list (store everything for now)
        stations_url = gridpoint.get("observation_stations_url")
        stations_linked = 0
        if stations_url:
            debug.append("Fetching observation stations list")
            stations_res = await self.nws.fetch_json(stations_url)
            if stations_res.status_code == 200 and stations_res.json_data:
                stations_linked = await upsert_stations_for_gridpoint(
                    self.engine,
                    gridpoint_id=int(gridpoint["id"]),
                    stations_geojson=stations_res.json_data,
                )
                debug.append(f"Stored/linked {stations_linked} stations for gridpoint")
            else:
                debug.append(f"Stations fetch skipped/failed ({stations_res.status_code})")
        else:
            debug.append("No observationStations URL on gridpoint metadata")

        # 4) Forecast bundle (forecast + hourly + griddata)
        urls = {
            "forecast": gridpoint.get("forecast_url"),
            "hourly": gridpoint.get("forecast_hourly_url"),
            "griddata": gridpoint.get("forecast_griddata_url"),
        }

        out: dict[str, Any] = {
            "ok": True,
            "query": {"lat": lat, "lon": lon},
            "gridpoint": {
                "id": int(gridpoint["id"]),
                "grid_id": gridpoint["grid_id"],
                "grid_x": gridpoint["grid_x"],
                "grid_y": gridpoint["grid_y"],
                "time_zone": gridpoint.get("time_zone"),
            },
            "stations_linked": stations_linked,
            "debug": debug,
            "data": {},
        }

        for ftype, url in urls.items():
            if not url:
                debug.append(f"No URL available for {ftype}, skipping")
                continue

            cached = await get_forecast_cache(self.engine, gridpoint_id=int(gridpoint["id"]), forecast_type=ftype)
            now = utcnow()

            if cached and cached.get("expires_at") and cached["expires_at"] > now:
                debug.append(f"{ftype}: cache hit (expires {cached['expires_at'].isoformat()})")
                out["data"][ftype] = cached["data_json"]
                continue

            debug.append(f"{ftype}: cache miss/expired -> calling NWS")
            if_none_match = cached.get("etag") if cached else None
            if_modified_since = None
            # NOTE: last-modified may be absent for some endpoints; we handle that.
            if cached and cached.get("last_modified"):
                if_modified_since = cached["last_modified"].strftime("%a, %d %b %Y %H:%M:%S GMT")

            res = await self.nws.fetch_json(url, if_none_match=if_none_match, if_modified_since=if_modified_since)
            expires_at = compute_expires_at(res.headers, self.forecast_default_ttl_s)

            # Handle 304 properly: keep cached JSON but refresh expiry/validators
            if res.status_code == 304 and cached and cached.get("data_json"):
                debug.append(f"{ftype}: NWS 304 Not Modified -> extending cache until {expires_at.isoformat()}")
                await upsert_forecast_cache(
                    self.engine,
                    gridpoint_id=int(gridpoint["id"]),
                    forecast_type=ftype,
                    url=url,
                    data_json=cached["data_json"],
                    status_code=304,
                    error=None,
                    etag=res.etag or cached.get("etag"),
                    last_modified=res.last_modified or cached.get("last_modified"),
                    expires_at=expires_at,
                )
                out["data"][ftype] = cached["data_json"]
                continue

            # Any non-200 or missing JSON -> stale-if-error
            if res.status_code != 200 or not res.json_data:
                debug.append(f"{ftype}: NWS failed ({res.status_code})")
                if res.error:
                    debug.append(f"{ftype}: NWS error detail: {res.error}")
                if res.body_preview:
                    debug.append(f"{ftype}: body preview: {res.body_preview}")

                if cached and cached.get("data_json"):
                    debug.append(f"{ftype}: returning STALE cached data due to upstream failure")
                    out["data"][ftype] = cached["data_json"]
                    continue

                out["data"][ftype] = {"error": "fetch failed", "status": res.status_code}
                continue

            debug.append(f"{ftype}: NWS OK -> cached until {expires_at.isoformat()}")
            await upsert_forecast_cache(
                self.engine,
                gridpoint_id=int(gridpoint["id"]),
                forecast_type=ftype,
                url=url,
                data_json=res.json_data,
                status_code=200,
                error=None,
                etag=res.etag,
                last_modified=res.last_modified,
                expires_at=expires_at,
            )
            out["data"][ftype] = res.json_data

        return out