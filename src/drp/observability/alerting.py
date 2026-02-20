import logging
from typing import Any

import requests

from drp.config.settings import Settings


class AlertNotifier:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = logging.getLogger(__name__)

    def notify_failure(
        self,
        flow_name: str,
        flow_run_id: str,
        error_message: str,
        metadata: dict[str, Any],
    ) -> None:
        if not self._settings.alert_on_failure:
            return
        if not self._settings.alert_webhook_url:
            self._logger.warning(
                "Failure alert skipped; ALERT_WEBHOOK_URL is not configured flow=%s run=%s",
                flow_name,
                flow_run_id,
            )
            return

        payload = {
            "event_type": "pipeline_failure",
            "flow_name": flow_name,
            "flow_run_id": flow_run_id,
            "error_message": error_message,
            "metadata": metadata,
        }
        try:
            response = requests.post(self._settings.alert_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
        except requests.RequestException as exc:
            self._logger.exception("Failed to send failure alert flow=%s run=%s error=%s", flow_name, flow_run_id, exc)
