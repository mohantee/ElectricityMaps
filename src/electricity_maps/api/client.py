"""HTTP client for the Electricity Maps API v4.

Features:
- Automatic retries with exponential backoff on 429 / 5xx / network errors
- User-Agent header (required by Cloudflare)
- Type-safe response parsing via Pydantic models
- Raw-dict methods for Bronze layer (no parsing overhead)
"""

from __future__ import annotations

import logging
from datetime import datetime

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from electricity_maps.api.models import (
    ElectricityFlowsResponse,
    ElectricityMixResponse,
)
from electricity_maps.config import Settings, get_settings

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Custom exceptions                                                  #
# ------------------------------------------------------------------ #

class RateLimitError(Exception):
    """Raised when the API returns 429 Too Many Requests."""


class APIError(Exception):
    """Raised for non-retriable API errors (4xx except 429)."""


# ------------------------------------------------------------------ #
#  Retry predicate                                                    #
# ------------------------------------------------------------------ #

def _is_retriable(exc: BaseException) -> bool:
    """Return True for errors that warrant a retry."""
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException)):
        return True
    return False


# ------------------------------------------------------------------ #
#  Client                                                             #
# ------------------------------------------------------------------ #

class ElectricityMapsClient:
    """Client for the Electricity Maps API v4.

    Usage::

        with ElectricityMapsClient() as client:
            mix = client.get_mix_range("FR", start, end)
            flows = client.get_flows_range("FR", start, end)
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = httpx.Client(
            base_url=self._settings.api_base_url,
            headers={
                "auth-token": self._settings.api_key,
                "User-Agent": "ElectricityMapsETL/0.1.0",
            },
            timeout=30.0,
        )

    # -- context manager --------------------------------------------------

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> ElectricityMapsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # -- internal request --------------------------------------------------

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception(_is_retriable),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _request(self, endpoint: str, params: dict) -> dict:
        """Make an authenticated GET request with retry logic.

        Raises:
            RateLimitError: On HTTP 429.
            APIError: On non-retriable 4xx errors.
            httpx.HTTPStatusError: On 5xx (will be retried).
        """
        response = self._client.get(endpoint, params=params)

        if response.status_code == 429:
            raise RateLimitError(
                f"Rate limited on {endpoint} "
                f"(Retry-After: {response.headers.get('Retry-After', '?')})"
            )

        if 400 <= response.status_code < 500:
            raise APIError(
                f"Client error {response.status_code} on {endpoint}: "
                f"{response.text[:200]}"
            )

        response.raise_for_status()
        return response.json()

    # -- parsed (typed) endpoints ------------------------------------------

    def get_mix_range(
        self,
        zone: str,
        start: datetime,
        end: datetime,
    ) -> ElectricityMixResponse:
        """Fetch electricity mix for a zone over a time range.

        Args:
            zone: Zone code (e.g. ``"FR"``).
            start: Range start (UTC).
            end: Range end (UTC).

        Returns:
            Parsed ``ElectricityMixResponse``.
        """
        raw = self._request(
            "/electricity-mix/past-range",
            params={
                "zone": zone,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )
        logger.info(
            "Fetched mix range for %s: %s → %s (%d records)",
            zone, start, end, len(raw.get("data", [])),
        )
        return ElectricityMixResponse.model_validate(raw)

    def get_flows_range(
        self,
        zone: str,
        start: datetime,
        end: datetime,
    ) -> ElectricityFlowsResponse:
        """Fetch electricity flows for a zone over a time range.

        Args:
            zone: Zone code (e.g. ``"FR"``).
            start: Range start (UTC).
            end: Range end (UTC).

        Returns:
            Parsed ``ElectricityFlowsResponse``.
        """
        raw = self._request(
            "/electricity-flows/past-range",
            params={
                "zone": zone,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )
        logger.info(
            "Fetched flows range for %s: %s → %s (%d records)",
            zone, start, end, len(raw.get("data", [])),
        )
        return ElectricityFlowsResponse.model_validate(raw)

    # -- raw (untyped) endpoints for Bronze --------------------------------

    def get_raw_mix_range(
        self,
        zone: str,
        start: datetime,
        end: datetime,
    ) -> dict:
        """Fetch raw electricity mix JSON (no Pydantic parsing)."""
        return self._request(
            "/electricity-mix/past-range",
            params={
                "zone": zone,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )

    def get_raw_flows_range(
        self,
        zone: str,
        start: datetime,
        end: datetime,
    ) -> dict:
        """Fetch raw electricity flows JSON (no Pydantic parsing)."""
        return self._request(
            "/electricity-flows/past-range",
            params={
                "zone": zone,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )
