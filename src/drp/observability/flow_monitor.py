from dataclasses import dataclass
from datetime import datetime
from typing import Any

from prefect.runtime import flow_run

from drp.config.settings import Settings
from drp.observability.alerting import AlertNotifier
from drp.observability.run_audit_repository import FlowAuditRepository, utc_now


@dataclass(frozen=True)
class FlowExecutionContext:
    flow_name: str
    flow_run_id: str
    started_at: str


class FlowMonitor:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._audit_repo = FlowAuditRepository(settings)
        self._notifier = AlertNotifier(settings)

    def start(self, flow_name: str) -> FlowExecutionContext:
        run_id = getattr(flow_run, "id", None)
        flow_run_id = str(run_id or "local-manual-run")
        started_at = utc_now()
        return FlowExecutionContext(flow_name=flow_name, flow_run_id=flow_run_id, started_at=started_at.isoformat())

    def success(self, ctx: FlowExecutionContext, records_processed: int | None, metadata: dict[str, Any]) -> None:
        started = _parse_dt(ctx.started_at)
        ended = utc_now()
        self._audit_repo.insert_audit_event(
            flow_name=ctx.flow_name,
            flow_run_id=ctx.flow_run_id,
            status="success",
            started_at=started,
            ended_at=ended,
            records_processed=records_processed,
            metadata=metadata,
        )

    def failure(self, ctx: FlowExecutionContext, error: Exception, metadata: dict[str, Any]) -> None:
        started = _parse_dt(ctx.started_at)
        ended = utc_now()
        error_message = str(error)
        self._audit_repo.insert_audit_event(
            flow_name=ctx.flow_name,
            flow_run_id=ctx.flow_run_id,
            status="failed",
            started_at=started,
            ended_at=ended,
            records_processed=None,
            metadata=metadata,
            error_message=error_message,
        )
        self._notifier.notify_failure(
            flow_name=ctx.flow_name,
            flow_run_id=ctx.flow_run_id,
            error_message=error_message,
            metadata=metadata,
        )


def _parse_dt(iso_value: str) -> datetime:
    return datetime.fromisoformat(iso_value)
