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
    error: Optional[str] = None
    body_preview: Optional[str] = None


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

        try:
            async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
                resp = await client.get(url, headers=headers)
        except Exception as e:
            return NwsFetchResult(
                url=url,
                status_code=0,
                json_data=None,
                headers={},
                etag=None,
                last_modified=None,
                error=f"request error: {type(e).__name__}: {e}",
            )

        # 304 has no body by design
        if resp.status_code == 304:
            return NwsFetchResult(
                url=str(resp.url),
                status_code=304,
                json_data=None,
                headers=resp.headers,
                etag=get_etag(resp.headers),
                last_modified=get_last_modified(resp.headers),
            )

        content = resp.content or b""
        if len(content) == 0:
            return NwsFetchResult(
                url=str(resp.url),
                status_code=resp.status_code,
                json_data=None,
                headers=resp.headers,
                etag=get_etag(resp.headers),
                last_modified=get_last_modified(resp.headers),
                error="empty response body",
            )

        try:
            data = resp.json()
            if not isinstance(data, dict):
                return NwsFetchResult(
                    url=str(resp.url),
                    status_code=resp.status_code,
                    json_data=None,
                    headers=resp.headers,
                    etag=get_etag(resp.headers),
                    last_modified=get_last_modified(resp.headers),
                    error=f"unexpected JSON type: {type(data).__name__}",
                    body_preview=resp.text[:300],
                )
        except Exception as e:
            return NwsFetchResult(
                url=str(resp.url),
                status_code=resp.status_code,
                json_data=None,
                headers=resp.headers,
                etag=get_etag(resp.headers),
                last_modified=get_last_modified(resp.headers),
                error=f"json parse error: {type(e).__name__}: {e}",
                body_preview=resp.text[:300],
            )

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