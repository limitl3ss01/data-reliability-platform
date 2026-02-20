from typing import Any

import requests

from drp.config.settings import Settings
from drp.core.exceptions import DataSourceError


class OrdersApiClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def fetch_orders(self, limit: int) -> list[dict[str, Any]]:
        url = f"{self._settings.api_base_url}{self._settings.api_orders_endpoint}"
        try:
            response = requests.get(url, params={"limit": limit}, timeout=20)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise DataSourceError(f"Orders API request failed: {exc}") from exc

        records = payload.get("records")
        if not isinstance(records, list):
            raise DataSourceError("Orders API returned invalid payload: missing list 'records'.")
        return records
