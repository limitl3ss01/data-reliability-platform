from datetime import UTC, datetime
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from drp.config.settings import Settings
from drp.core.exceptions import StorageError
from drp.storage.duckdb.warehouse_repository import DuckDbWarehouseRepository
from drp.storage.object_store.s3_repository import S3Repository


class ObjectStoreArchiveService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._repo = S3Repository(settings)
        self._logger = logging.getLogger(__name__)

    def archive_raw_batch(self, batch_id: str, records: list[dict[str, Any]]) -> str | None:
        if not self._settings.object_store_enabled:
            return None

        key = (
            f"{self._settings.object_store_raw_prefix}/"
            f"ingest_date={datetime.now(UTC).date().isoformat()}/batch_id={batch_id}.json"
        )
        payload = {"batch_id": batch_id, "record_count": len(records), "records": records}
        return self._put_json_safe(key=key, payload=payload)

    def archive_analytics_snapshot(self, warehouse: DuckDbWarehouseRepository) -> str | None:
        if not self._settings.object_store_enabled:
            return None

        snapshot_date = datetime.now(UTC).date().isoformat()
        key = (
            f"{self._settings.object_store_analytics_prefix}/"
            f"snapshot_date={snapshot_date}/daily_order_metrics.parquet"
        )

        with TemporaryDirectory(prefix="drp-analytics-") as tmp_dir:
            output_path = str(Path(tmp_dir) / "daily_order_metrics.parquet")
            warehouse.export_daily_metrics_to_parquet(output_path)
            return self._upload_file_safe(local_path=output_path, key=key)

    def _put_json_safe(self, key: str, payload: dict[str, Any]) -> str | None:
        try:
            return self._repo.put_json(key=key, payload=payload)
        except StorageError as exc:
            if self._settings.object_store_required:
                raise
            self._logger.warning("Skipping raw archive upload key=%s error=%s", key, exc)
            return None

    def _upload_file_safe(self, local_path: str, key: str) -> str | None:
        try:
            return self._repo.upload_file(local_path=local_path, key=key)
        except StorageError as exc:
            if self._settings.object_store_required:
                raise
            self._logger.warning("Skipping analytics archive upload key=%s error=%s", key, exc)
            return None
