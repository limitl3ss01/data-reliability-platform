from pathlib import Path

import duckdb

from drp.quality.great_expectations.orders_quality_validator import OrdersQualityValidator
from drp.storage.duckdb.warehouse_repository import DuckDbWarehouseRepository
from drp.transform.analytics.orders_analytics_service import OrdersAnalyticsService
from drp.transform.staging.orders_staging_service import OrdersStagingService


class DummySettings:
    def __init__(self, duckdb_path: str) -> None:
        self.duckdb_path = duckdb_path


def test_stage_analytics_quality_integration(tmp_path: Path) -> None:
    settings = DummySettings(str(tmp_path / "warehouse.duckdb"))
    warehouse = DuckDbWarehouseRepository(settings=settings)
    staging_service = OrdersStagingService(warehouse=warehouse)
    analytics_service = OrdersAnalyticsService(warehouse=warehouse)
    quality_validator = OrdersQualityValidator(settings=settings)

    raw_records = [
        {
            "source_order_id": "ord_001",
            "customer_id": "cus_001",
            "amount": 100.0,
            "order_created_at": "2026-02-20T08:00:00+00:00",
            "ingested_at": "2026-02-20T08:01:00+00:00",
            "batch_id": "11111111-1111-1111-1111-111111111111",
            "source_system": "test-source",
        },
        {
            "source_order_id": "ord_002",
            "customer_id": "cus_002",
            "amount": 75.5,
            "order_created_at": "2026-02-20T09:00:00+00:00",
            "ingested_at": "2026-02-20T09:01:00+00:00",
            "batch_id": "11111111-1111-1111-1111-111111111111",
            "source_system": "test-source",
        },
    ]

    staged_rows = staging_service.build_staging(raw_records=raw_records)
    analytics_rows = analytics_service.refresh_daily_metrics()
    quality = quality_validator.validate_staging_orders()

    assert staged_rows == 2
    assert analytics_rows == 1
    assert quality.success is True
    assert quality.failed_expectations == 0

    with duckdb.connect(settings.duckdb_path) as conn:
        row = conn.execute(
            "SELECT total_orders, total_amount FROM analytics.daily_order_metrics LIMIT 1"
        ).fetchone()
    assert row is not None
    assert int(row[0]) == 2
