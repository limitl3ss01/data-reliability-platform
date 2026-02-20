from typing import Any
from uuid import UUID

from drp.config.settings import Settings
from drp.ingestion.connectors.orders_api_client import OrdersApiClient
from drp.storage.postgres.raw_orders_repository import RawOrdersRepository


class OrdersIngestionService:
    def __init__(
        self,
        settings: Settings,
        client: OrdersApiClient,
        repository: RawOrdersRepository,
    ) -> None:
        self._settings = settings
        self._client = client
        self._repository = repository

    def extract(self, limit: int | None = None) -> list[dict[str, Any]]:
        batch_size = limit if limit is not None else self._settings.ingest_batch_size
        return self._client.fetch_orders(limit=batch_size)

    def load_raw(self, records: list[dict[str, Any]], batch_id: UUID) -> int:
        self._repository.ensure_table()
        return self._repository.insert_raw_orders(records=records, batch_id=batch_id)
