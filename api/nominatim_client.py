"""Nominatim / OpenStreetMap geocoding client.

Respects the Nominatim usage policy:
  - 1 request per second max
  - Custom User-Agent header
"""

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://nominatim.openstreetmap.org"
_USER_AGENT = "FindDataNearby/1.0"
_RATE_LIMIT_SECONDS = 1.0


class NominatimClient:
    """Geocoding and POI search via the Nominatim API."""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        """Enforce at most 1 request per second."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _RATE_LIMIT_SECONDS:
            time.sleep(_RATE_LIMIT_SECONDS - elapsed)
        self._last_request_time = time.monotonic()

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        """Issue a throttled GET and return parsed JSON."""
        self._throttle()
        params["format"] = "jsonv2"
        url = f"{_BASE_URL}{path}"
        logger.debug("GET %s params=%s", url, params)
        resp = self._session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ── Public methods ─────────────────────────────────────────────────

    def geocode(self, address: str) -> dict[str, Any] | None:
        """Forward-geocode an address string.

        Returns {"lat": float, "lon": float, "display_name": str} or None.
        """
        results = self._get("/search", {"q": address, "limit": 1})
        if not results:
            return None
        hit = results[0]
        return {
            "lat": float(hit["lat"]),
            "lon": float(hit["lon"]),
            "display_name": hit.get("display_name", ""),
        }

    def reverse_geocode(self, lat: float, lon: float) -> dict[str, Any] | None:
        """Reverse-geocode coordinates to an address.

        Returns address details dict or None.
        """
        result = self._get("/reverse", {"lat": str(lat), "lon": str(lon)})
        if not result or "error" in result:
            return None
        return {
            "lat": float(result.get("lat", lat)),
            "lon": float(result.get("lon", lon)),
            "display_name": result.get("display_name", ""),
            "address": result.get("address", {}),
        }

    def search_nearby_pois(
        self,
        lat: float,
        lon: float,
        radius_km: float = 1.0,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search for points of interest near a coordinate.

        Uses the Nominatim /search endpoint with viewbox filtering.
        *category* maps to an amenity= or tourism= filter if provided.
        """
        # Convert radius_km to approximate degree offset for bounding box
        deg_offset = radius_km / 111.0  # ~111 km per degree latitude
        viewbox = (
            f"{lon - deg_offset},{lat + deg_offset},"
            f"{lon + deg_offset},{lat - deg_offset}"
        )

        params: dict[str, Any] = {
            "viewbox": viewbox,
            "bounded": 1,
            "limit": 50,
        }

        if category:
            # Use free-form query with the category term
            params["q"] = category
        else:
            params["q"] = "*"

        results = self._get("/search", params)
        return [
            {
                "name": r.get("display_name", ""),
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "type": r.get("type", ""),
                "category": r.get("category", ""),
            }
            for r in results
        ]
