from drp.core.exceptions import StorageError
from drp.storage.object_store.archive_service import ObjectStoreArchiveService


class DummySettings:
    object_store_enabled = True
    object_store_required = False
    object_store_endpoint_url = "http://minio:9000"
    object_store_region = "us-east-1"
    object_store_bucket = "drp-lakehouse"
    object_store_access_key_id = "minioadmin"
    object_store_secret_access_key = "minioadmin"
    object_store_secure = False
    object_store_raw_prefix = "raw/orders"
    object_store_analytics_prefix = "analytics/orders"


class FakeRepo:
    def __init__(self, raise_error: bool = False) -> None:
        self.raise_error = raise_error

    def put_json(self, key: str, payload: dict) -> str:
        if self.raise_error:
            raise StorageError("upload failed")
        return f"s3://bucket/{key}"

    def upload_file(self, local_path: str, key: str) -> str:
        if self.raise_error:
            raise StorageError("upload failed")
        return f"s3://bucket/{key}"


def test_archive_raw_batch_returns_none_when_disabled() -> None:
    settings = DummySettings()
    settings.object_store_enabled = False
    service = ObjectStoreArchiveService(settings=settings)
    service._repo = FakeRepo()  # type: ignore[assignment]

    uri = service.archive_raw_batch(batch_id="b-1", records=[{"id": 1}])

    assert uri is None


def test_archive_raw_batch_swallow_errors_when_not_required() -> None:
    settings = DummySettings()
    settings.object_store_required = False
    service = ObjectStoreArchiveService(settings=settings)
    service._repo = FakeRepo(raise_error=True)  # type: ignore[assignment]

    uri = service.archive_raw_batch(batch_id="b-1", records=[{"id": 1}])

    assert uri is None
