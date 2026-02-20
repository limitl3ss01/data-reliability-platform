from typing import Any

from prefect import flow, get_run_logger, task

from drp.config.settings import get_settings
from drp.core.logging import configure_logging
from drp.observability.flow_monitor import FlowMonitor
from drp.quality.great_expectations.orders_quality_validator import OrdersQualityValidator
from drp.storage.duckdb.warehouse_repository import DuckDbWarehouseRepository
from drp.storage.object_store.archive_service import ObjectStoreArchiveService
from drp.storage.postgres.raw_orders_repository import RawOrdersRepository
from drp.transform.analytics.orders_analytics_service import OrdersAnalyticsService
from drp.transform.staging.orders_staging_service import OrdersStagingService


@task(name="extract-raw-orders")
def extract_raw_orders(limit: int) -> list[dict[str, Any]]:
    settings = get_settings()
    repo = RawOrdersRepository(settings)
    return repo.fetch_recent_raw_orders(limit=limit)


@task(name="build-staging-orders")
def build_staging_orders(raw_records: list[dict[str, Any]]) -> int:
    settings = get_settings()
    warehouse = DuckDbWarehouseRepository(settings)
    service = OrdersStagingService(warehouse=warehouse)
    return service.build_staging(raw_records=raw_records)


@task(name="refresh-analytics-metrics")
def refresh_analytics_metrics() -> int:
    settings = get_settings()
    warehouse = DuckDbWarehouseRepository(settings)
    service = OrdersAnalyticsService(warehouse=warehouse)
    return service.refresh_daily_metrics()


@task(name="run-quality-checks")
def run_quality_checks() -> dict[str, int | bool]:
    settings = get_settings()
    validator = OrdersQualityValidator(settings=settings)
    result = validator.validate_staging_orders()
    payload = {
        "success": result.success,
        "checked_rows": result.checked_rows,
        "failed_expectations": result.failed_expectations,
    }
    if not result.success:
        raise RuntimeError(
            f"Data quality gate failed: failed_expectations={result.failed_expectations}, checked_rows={result.checked_rows}"
        )
    return payload


@task(name="archive-analytics-snapshot")
def archive_analytics_snapshot() -> str | None:
    settings = get_settings()
    warehouse = DuckDbWarehouseRepository(settings)
    archive = ObjectStoreArchiveService(settings)
    return archive.archive_analytics_snapshot(warehouse=warehouse)


@flow(name="stage-and-validate-orders")
def stage_and_validate_orders_flow(limit: int | None = None) -> dict[str, int | bool | str | None]:
    configure_logging(service_name="drp-pipeline")
    settings = get_settings()
    logger = get_run_logger()
    monitor = FlowMonitor(settings)
    ctx = monitor.start(flow_name="stage-and-validate-orders")

    source_limit = limit if limit is not None else settings.transform_source_limit
    logger.info("Starting staging and quality flow limit=%s", source_limit)

    try:
        raw_records = extract_raw_orders(limit=source_limit)
        staged_rows = build_staging_orders(raw_records=raw_records)
        analytics_rows = refresh_analytics_metrics()
        quality = run_quality_checks()
        analytics_archive_uri = archive_analytics_snapshot()
        result = {
            "staged_rows": staged_rows,
            "analytics_rows": analytics_rows,
            "quality_success": bool(quality["success"]),
            "quality_failed_expectations": int(quality["failed_expectations"]),
            "analytics_archive_uri": analytics_archive_uri,
        }
        monitor.success(
            ctx=ctx,
            records_processed=staged_rows,
            metadata={
                "source_limit": source_limit,
                "raw_records": len(raw_records),
                **result,
            },
        )
        logger.info(
            "Finished stage/validate flow staged_rows=%s analytics_rows=%s quality_success=%s",
            staged_rows,
            analytics_rows,
            quality["success"],
        )
    except Exception as exc:  # noqa: BLE001
        monitor.failure(
            ctx=ctx,
            error=exc,
            metadata={"source_limit": source_limit},
        )
        logger.exception("Stage and validate flow failed source_limit=%s", source_limit)
        raise

    return result


if __name__ == "__main__":
    stage_and_validate_orders_flow()
