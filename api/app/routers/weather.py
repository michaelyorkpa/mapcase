from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.services.nws_weather_service import NwsWeatherService

router = APIRouter(prefix="/weather", tags=["weather"])


@router.get("/forecast")
async def weather_forecast(
    request: Request,
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
):
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status_code=500, detail="Database engine not initialized")

    svc = NwsWeatherService(engine=engine)

    try:
        result = await svc.get_forecast_bundle(lat=lat, lon=lon)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Weather service error: {e}")

    if not result.get("ok", False):
        raise HTTPException(status_code=502, detail=result)

    return result