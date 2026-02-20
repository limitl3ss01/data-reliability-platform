from uuid import UUID, uuid4

from prefect import flow, get_run_logger, task

from drp.config.settings import get_settings
from drp.core.logging import configure_logging
from drp.ingestion.connectors.orders_api_client import OrdersApiClient
from drp.ingestion.services.orders_ingestion_service import OrdersIngestionService
from drp.observability.flow_monitor import FlowMonitor
from drp.storage.object_store.archive_service import ObjectStoreArchiveService
from drp.storage.postgres.raw_orders_repository import RawOrdersRepository


@task(name="extract-orders", retries=2, retry_delay_seconds=10)
def extract_orders(limit: int | None = None) -> list[dict]:
    settings = get_settings()
    service = OrdersIngestionService(
        settings=settings,
        client=OrdersApiClient(settings),
        repository=RawOrdersRepository(settings),
    )
    return service.extract(limit=limit)


@task(name="load-raw-orders")
def load_raw_orders(records: list[dict], batch_id: str) -> int:
    settings = get_settings()
    service = OrdersIngestionService(
        settings=settings,
        client=OrdersApiClient(settings),
        repository=RawOrdersRepository(settings),
    )
    return service.load_raw(records=records, batch_id=UUID(batch_id))


@task(name="archive-raw-batch")
def archive_raw_batch(batch_id: str, records: list[dict]) -> str | None:
    settings = get_settings()
    archive = ObjectStoreArchiveService(settings=settings)
    return archive.archive_raw_batch(batch_id=batch_id, records=records)


@flow(name="ingest-orders-to-raw")
def ingest_orders_to_raw_flow(limit: int | None = None) -> dict[str, str | int | None]:
    configure_logging(service_name="drp-pipeline")
    settings = get_settings()
    logger = get_run_logger()
    monitor = FlowMonitor(settings)
    ctx = monitor.start(flow_name="ingest-orders-to-raw")
    batch_id = str(uuid4())
    source_limit = limit if limit is not None else settings.ingest_batch_size

    logger.info("Starting ingestion batch batch_id=%s", batch_id)
    try:
        records = extract_orders(limit=limit)
        inserted_count = load_raw_orders(records=records, batch_id=batch_id)
        archive_uri = archive_raw_batch(batch_id=batch_id, records=records)
        monitor.success(
            ctx=ctx,
            records_processed=inserted_count,
            metadata={
                "batch_id": batch_id,
                "source_limit": source_limit,
                "records_extracted": len(records),
                "raw_archive_uri": archive_uri,
            },
        )
        logger.info("Completed ingestion batch batch_id=%s inserted=%s", batch_id, inserted_count)
    except Exception as exc:  # noqa: BLE001
        monitor.failure(
            ctx=ctx,
            error=exc,
            metadata={
                "batch_id": batch_id,
                "source_limit": source_limit,
            },
        )
        logger.exception("Ingestion flow failed batch_id=%s", batch_id)
        raise

    return {"batch_id": batch_id, "inserted_count": inserted_count, "raw_archive_uri": archive_uri}


if __name__ == "__main__":
    ingest_orders_to_raw_flow()
