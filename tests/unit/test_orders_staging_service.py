from drp.transform.staging.orders_staging_service import OrdersStagingService


class FakeWarehouse:
    def __init__(self) -> None:
        self.last_records: list[dict] = []

    def replace_staging_orders(self, records: list[dict]) -> int:
        self.last_records = records
        return len(records)


def test_build_staging_deduplicates_and_filters_negative_amounts() -> None:
    warehouse = FakeWarehouse()
    service = OrdersStagingService(warehouse=warehouse)

    raw_records = [
        {
            "source_order_id": "ord_001",
            "customer_id": "cus_001",
            "amount": 100.0,
            "ingested_at": "2026-02-20T00:00:00+00:00",
        },
        {
            "source_order_id": "ord_001",
            "customer_id": "cus_001",
            "amount": 130.0,
            "ingested_at": "2026-02-20T00:05:00+00:00",
        },
        {
            "source_order_id": "ord_002",
            "customer_id": "cus_002",
            "amount": -10.0,
            "ingested_at": "2026-02-20T00:02:00+00:00",
        },
    ]

    inserted = service.build_staging(raw_records=raw_records)

    assert inserted == 1
    assert len(warehouse.last_records) == 1
    assert warehouse.last_records[0]["source_order_id"] == "ord_001"
    assert float(warehouse.last_records[0]["amount"]) == 130.0
