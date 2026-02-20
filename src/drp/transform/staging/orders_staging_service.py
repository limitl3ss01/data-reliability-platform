from typing import Any

from drp.storage.duckdb.warehouse_repository import DuckDbWarehouseRepository


class OrdersStagingService:
    def __init__(self, warehouse: DuckDbWarehouseRepository) -> None:
        self._warehouse = warehouse

    def build_staging(self, raw_records: list[dict[str, Any]]) -> int:
        # Keep latest version of each source order ID.
        deduped: dict[str, dict[str, Any]] = {}
        for row in raw_records:
            key = str(row["source_order_id"])
            existing = deduped.get(key)
            if existing is None or str(row["ingested_at"]) > str(existing["ingested_at"]):
                deduped[key] = row

        cleaned = [record for record in deduped.values() if float(record["amount"]) >= 0]
        return self._warehouse.replace_staging_orders(cleaned)
