from dataclasses import dataclass

import duckdb
import great_expectations as gx

from drp.config.settings import Settings


@dataclass(frozen=True)
class QualityResult:
    success: bool
    checked_rows: int
    failed_expectations: int


class OrdersQualityValidator:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def validate_staging_orders(self) -> QualityResult:
        checked_rows = self._count_rows()
        dataframe = self._read_staging_dataframe()
        validator = gx.from_pandas(dataframe)

        validator.expect_table_row_count_to_be_between(min_value=1)
        validator.expect_column_values_to_not_be_null("source_order_id")
        validator.expect_column_values_to_not_be_null("customer_id")
        validator.expect_column_values_to_be_unique("source_order_id")
        validator.expect_column_values_to_be_between("amount", min_value=0)
        validator.expect_column_values_to_not_be_null("order_created_at")

        results = validator.validate()
        failed_expectations = len([item for item in results.results if not item.success])
        return QualityResult(
            success=bool(results.success),
            checked_rows=checked_rows,
            failed_expectations=failed_expectations,
        )

    def _count_rows(self) -> int:
        with duckdb.connect(self._settings.duckdb_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM staging.orders").fetchone()
        return int(row[0] if row else 0)

    def _read_staging_dataframe(self):  # type: ignore[no-untyped-def]
        with duckdb.connect(self._settings.duckdb_path) as conn:
            return conn.execute(
                """
                SELECT
                    source_order_id,
                    customer_id,
                    amount,
                    order_created_at
                FROM staging.orders
                """
            ).fetchdf()
