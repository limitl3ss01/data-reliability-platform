from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import duckdb

from drp.config.settings import Settings
from drp.core.exceptions import StorageError


class DuckDbWarehouseRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _connect(self) -> duckdb.DuckDBPyConnection:
        db_path = Path(self._settings.duckdb_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(db_path))

    def ensure_tables(self) -> None:
        statement = """
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS staging.orders (
            source_order_id VARCHAR,
            customer_id VARCHAR,
            amount DOUBLE,
            order_created_at TIMESTAMP,
            ingested_at TIMESTAMP,
            batch_id VARCHAR,
            source_system VARCHAR
        );

        CREATE TABLE IF NOT EXISTS analytics.daily_order_metrics (
            order_date DATE,
            total_orders BIGINT,
            total_amount DOUBLE,
            avg_amount DOUBLE,
            last_refreshed_at TIMESTAMP
        );
        """
        try:
            with self._connect() as conn:
                conn.execute(statement)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed ensuring DuckDB tables: {exc}") from exc

    def replace_staging_orders(self, records: Sequence[dict[str, Any]]) -> int:
        self.ensure_tables()

        if not records:
            with self._connect() as conn:
                conn.execute("DELETE FROM staging.orders")
            return 0

        insert_rows: list[tuple[Any, ...]] = []
        for record in records:
            insert_rows.append(
                (
                    str(record["source_order_id"]),
                    str(record["customer_id"]),
                    float(record["amount"]),
                    _to_datetime(record["order_created_at"]),
                    _to_datetime(record["ingested_at"]),
                    _to_str(record["batch_id"]),
                    str(record["source_system"]),
                )
            )

        try:
            with self._connect() as conn:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("DELETE FROM staging.orders")
                conn.executemany(
                    """
                    INSERT INTO staging.orders (
                        source_order_id,
                        customer_id,
                        amount,
                        order_created_at,
                        ingested_at,
                        batch_id,
                        source_system
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_rows,
                )
                conn.execute("COMMIT")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed writing staging orders in DuckDB: {exc}") from exc

        return len(insert_rows)

    def refresh_daily_metrics(self) -> int:
        self.ensure_tables()
        statement = """
        INSERT INTO analytics.daily_order_metrics
        SELECT
            CAST(order_created_at AS DATE) AS order_date,
            COUNT(*) AS total_orders,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            NOW() AS last_refreshed_at
        FROM staging.orders
        GROUP BY 1
        ORDER BY 1;
        """
        count_statement = "SELECT COUNT(*) AS cnt FROM analytics.daily_order_metrics"
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM analytics.daily_order_metrics")
                conn.execute(statement)
                row = conn.execute(count_statement).fetchone()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed refreshing analytics metrics: {exc}") from exc

        return int(row[0] if row else 0)

    def export_daily_metrics_to_parquet(self, output_path: str) -> None:
        self.ensure_tables()
        statement = """
        COPY (
            SELECT
                order_date,
                total_orders,
                total_amount,
                avg_amount,
                last_refreshed_at
            FROM analytics.daily_order_metrics
            ORDER BY order_date
        )
        TO ? (FORMAT PARQUET);
        """
        try:
            with self._connect() as conn:
                conn.execute(statement, [output_path])
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed exporting analytics parquet snapshot: {exc}") from exc


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _to_str(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    return str(value)
