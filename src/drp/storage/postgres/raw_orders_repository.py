from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from drp.config.settings import Settings
from drp.core.exceptions import StorageError


class RawOrdersRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def ensure_table(self) -> None:
        schema = self._settings.raw_schema
        table = self._settings.raw_orders_table

        statement = f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id BIGSERIAL PRIMARY KEY,
            source_order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            order_created_at TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            batch_id UUID NOT NULL,
            source_system TEXT NOT NULL,
            raw_payload JSONB NOT NULL
        );
        """

        try:
            with psycopg.connect(self._settings.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(statement)
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed creating raw orders table: {exc}") from exc

    def insert_raw_orders(self, records: list[dict[str, Any]], batch_id: UUID) -> int:
        if not records:
            return 0

        now = datetime.now(UTC)
        schema = self._settings.raw_schema
        table = self._settings.raw_orders_table
        statement = f"""
            INSERT INTO {schema}.{table} (
                source_order_id,
                customer_id,
                amount,
                order_created_at,
                ingested_at,
                batch_id,
                source_system,
                raw_payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        rows: list[tuple[Any, ...]] = []
        try:
            for record in records:
                rows.append(
                    (
                        record["order_id"],
                        record["customer_id"],
                        Decimal(str(record["amount"])),
                        datetime.fromisoformat(str(record["created_at"])),
                        now,
                        batch_id,
                        self._settings.source_system,
                        Json(record),
                    )
                )
        except (KeyError, ValueError, TypeError) as exc:
            raise StorageError(f"Invalid order payload shape for raw load: {exc}") from exc

        try:
            with psycopg.connect(self._settings.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany(statement, rows)
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed inserting raw orders: {exc}") from exc

        return len(rows)

    def fetch_recent_raw_orders(self, limit: int) -> list[dict[str, Any]]:
        schema = self._settings.raw_schema
        table = self._settings.raw_orders_table
        statement = f"""
            SELECT
                source_order_id,
                customer_id,
                amount,
                order_created_at,
                ingested_at,
                batch_id,
                source_system
            FROM {schema}.{table}
            ORDER BY ingested_at DESC
            LIMIT %s
        """

        try:
            with psycopg.connect(self._settings.postgres_dsn, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(statement, (limit,))
                    rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed reading raw orders: {exc}") from exc

        return list(reversed(rows))
