from __future__ import annotations

# Pennsylvania bounding box (approx)
PA_MIN_LAT = 39.7199
PA_MAX_LAT = 42.5167
PA_MIN_LON = -80.5243
PA_MAX_LON = -74.707


def validate_lat_lon(lat: float, lon: float) -> None:
    if lat != lat or lon != lon:  # NaN check
        raise ValueError("lat/lon must be real numbers")
    if not (-90.0 <= lat <= 90.0):
        raise ValueError("lat out of range (-90..90)")
    if not (-180.0 <= lon <= 180.0):
        raise ValueError("lon out of range (-180..180)")


def assert_in_pa_bounds(lat: float, lon: float) -> None:
    if not (PA_MIN_LAT <= lat <= PA_MAX_LAT and PA_MIN_LON <= lon <= PA_MAX_LON):
        raise ValueError("lat/lon is outside PA bounds (MVP restriction)")