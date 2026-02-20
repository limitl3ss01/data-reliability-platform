import pytest

from drp.observability.alerting import AlertNotifier


class DummySettings:
    def __init__(self, alert_on_failure: bool, alert_webhook_url: str | None) -> None:
        self.alert_on_failure = alert_on_failure
        self.alert_webhook_url = alert_webhook_url


def test_notify_failure_skips_without_webhook() -> None:
    notifier = AlertNotifier(settings=DummySettings(alert_on_failure=True, alert_webhook_url=None))
    notifier.notify_failure(
        flow_name="flow-a",
        flow_run_id="run-1",
        error_message="boom",
        metadata={"k": "v"},
    )


def test_notify_failure_posts_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, json: dict, timeout: int):  # type: ignore[no-untyped-def]
        calls.append({"url": url, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("drp.observability.alerting.requests.post", fake_post)
    notifier = AlertNotifier(settings=DummySettings(alert_on_failure=True, alert_webhook_url="https://example.test/hook"))
    notifier.notify_failure(
        flow_name="flow-a",
        flow_run_id="run-1",
        error_message="boom",
        metadata={"k": "v"},
    )

    assert len(calls) == 1
    assert calls[0]["url"] == "https://example.test/hook"
    assert calls[0]["json"]["event_type"] == "pipeline_failure"
