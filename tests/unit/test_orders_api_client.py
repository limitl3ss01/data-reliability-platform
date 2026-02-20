import pytest

from drp.core.exceptions import DataSourceError
from drp.ingestion.connectors.orders_api_client import OrdersApiClient


class DummySettings:
    api_base_url = "http://test-api:8000"
    api_orders_endpoint = "/v1/orders"


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_fetch_orders_returns_records(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        return FakeResponse({"records": [{"order_id": "ord_1"}]})

    monkeypatch.setattr("drp.ingestion.connectors.orders_api_client.requests.get", fake_get)
    client = OrdersApiClient(settings=DummySettings())

    records = client.fetch_orders(limit=10)

    assert len(records) == 1
    assert records[0]["order_id"] == "ord_1"


def test_fetch_orders_raises_on_invalid_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        return FakeResponse({"unexpected": []})

    monkeypatch.setattr("drp.ingestion.connectors.orders_api_client.requests.get", fake_get)
    client = OrdersApiClient(settings=DummySettings())

    with pytest.raises(DataSourceError):
        client.fetch_orders(limit=10)
