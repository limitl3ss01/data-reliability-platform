from drp.storage.duckdb.warehouse_repository import DuckDbWarehouseRepository


class OrdersAnalyticsService:
    def __init__(self, warehouse: DuckDbWarehouseRepository) -> None:
        self._warehouse = warehouse

    def refresh_daily_metrics(self) -> int:
        return self._warehouse.refresh_daily_metrics()
