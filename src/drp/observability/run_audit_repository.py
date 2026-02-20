from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.types.json import Json

from drp.config.settings import Settings
from drp.core.exceptions import StorageError


class FlowAuditRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def ensure_table(self) -> None:
        schema = self._settings.observability_schema
        table = self._settings.flow_audit_table
        statement = f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id BIGSERIAL PRIMARY KEY,
            flow_name TEXT NOT NULL,
            flow_run_id TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            ended_at TIMESTAMPTZ NOT NULL,
            duration_seconds DOUBLE PRECISION NOT NULL,
            records_processed BIGINT,
            metadata JSONB NOT NULL,
            error_message TEXT
        );
        """
        try:
            with psycopg.connect(self._settings.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(statement)
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed ensuring flow audit table: {exc}") from exc

    def insert_audit_event(
        self,
        flow_name: str,
        flow_run_id: str,
        status: str,
        started_at: datetime,
        ended_at: datetime,
        records_processed: int | None,
        metadata: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        self.ensure_table()

        schema = self._settings.observability_schema
        table = self._settings.flow_audit_table
        duration_seconds = max((ended_at - started_at).total_seconds(), 0.0)
        statement = f"""
        INSERT INTO {schema}.{table} (
            flow_name,
            flow_run_id,
            status,
            started_at,
            ended_at,
            duration_seconds,
            records_processed,
            metadata,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with psycopg.connect(self._settings.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        statement,
                        (
                            flow_name,
                            flow_run_id,
                            status,
                            started_at,
                            ended_at,
                            duration_seconds,
                            records_processed,
                            Json(metadata),
                            error_message,
                        ),
                    )
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed writing flow audit event: {exc}") from exc


def utc_now() -> datetime:
    return datetime.now(UTC)
