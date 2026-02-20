from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = Field(default="local", alias="APP_ENV")

    api_base_url: str = Field(default="http://api-generator:8000", alias="API_BASE_URL")
    api_orders_endpoint: str = Field(default="/v1/orders", alias="API_ORDERS_ENDPOINT")
    ingest_batch_size: int = Field(default=100, alias="INGEST_BATCH_SIZE")
    duckdb_path: str = Field(default="/app/data/analytics/warehouse.duckdb", alias="DUCKDB_PATH")

    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="drp_platform", alias="POSTGRES_DB")
    postgres_user: str = Field(default="drp_user", alias="POSTGRES_USER")
    postgres_password: str = Field(default="drp_password", alias="POSTGRES_PASSWORD")

    raw_schema: str = Field(default="raw", alias="RAW_SCHEMA")
    raw_orders_table: str = Field(default="orders_raw", alias="RAW_ORDERS_TABLE")

    source_system: str = Field(default="fastapi-orders-api", alias="SOURCE_SYSTEM")
    transform_source_limit: int = Field(default=5000, alias="TRANSFORM_SOURCE_LIMIT")
    observability_schema: str = Field(default="ops", alias="OBSERVABILITY_SCHEMA")
    flow_audit_table: str = Field(default="pipeline_flow_audit", alias="FLOW_AUDIT_TABLE")
    alert_on_failure: bool = Field(default=True, alias="ALERT_ON_FAILURE")
    alert_webhook_url: Optional[str] = Field(default=None, alias="ALERT_WEBHOOK_URL")
    object_store_enabled: bool = Field(default=True, alias="OBJECT_STORE_ENABLED")
    object_store_required: bool = Field(default=False, alias="OBJECT_STORE_REQUIRED")
    object_store_endpoint_url: Optional[str] = Field(default="http://minio:9000", alias="OBJECT_STORE_ENDPOINT_URL")
    object_store_region: str = Field(default="us-east-1", alias="OBJECT_STORE_REGION")
    object_store_bucket: str = Field(default="drp-lakehouse", alias="OBJECT_STORE_BUCKET")
    object_store_access_key_id: Optional[str] = Field(default="minioadmin", alias="OBJECT_STORE_ACCESS_KEY_ID")
    object_store_secret_access_key: Optional[str] = Field(default="minioadmin", alias="OBJECT_STORE_SECRET_ACCESS_KEY")
    object_store_secure: bool = Field(default=False, alias="OBJECT_STORE_SECURE")
    object_store_raw_prefix: str = Field(default="raw/orders", alias="OBJECT_STORE_RAW_PREFIX")
    object_store_analytics_prefix: str = Field(default="analytics/orders", alias="OBJECT_STORE_ANALYTICS_PREFIX")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
