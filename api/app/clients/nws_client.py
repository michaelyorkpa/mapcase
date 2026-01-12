from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Mapping

import httpx

from app.core.http_cache import get_etag, get_last_modified


USER_AGENT = "(Mapcase by Adventure Adjacent; adventureadjacent.com; contact: mapcase@adventureadjacent.com)"
DEFAULT_ACCEPT = "application/geo+json"


@dataclass
class NwsFetchResult:
    url: str
    status_code: int
    json_data: Optional[dict[str, Any]]
    headers: Mapping[str, str]
    etag: Optional[str]
    last_modified: Optional[Any]  # datetime or None


class NwsClient:
    def __init__(self, timeout_seconds: float = 15.0):
        self._timeout = timeout_seconds

    async def fetch_json(
        self,
        url: str,
        *,
        if_none_match: Optional[str] = None,
        if_modified_since: Optional[str] = None,
    ) -> NwsFetchResult:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": DEFAULT_ACCEPT,
        }
        if if_none_match:
            headers["If-None-Match"] = if_none_match
        if if_modified_since:
            headers["If-Modified-Since"] = if_modified_since

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(url, headers=headers)

        data = None
        if resp.status_code != 304:
            # NWS returns JSON-LD/GeoJSON
            data = resp.json()

        return NwsFetchResult(
            url=str(resp.url),
            status_code=resp.status_code,
            json_data=data,
            headers=resp.headers,
            etag=get_etag(resp.headers),
            last_modified=get_last_modified(resp.headers),
        )

    async def points(self, lat: float, lon: float, **kwargs) -> NwsFetchResult:
        return await self.fetch_json(f"https://api.weather.gov/points/{lat},{lon}", **kwargs)