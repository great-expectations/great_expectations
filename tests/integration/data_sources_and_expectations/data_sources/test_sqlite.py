import pathlib
from datetime import datetime, timezone

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import SqliteDatasourceTestConfig

DATE_COL = "date"
VALUE_COL = "value"

LAST_YEAR = "last year"
FIRST_DAY_OF_THE_YEAR = "first day of the year"
FIRST_DAY_OF_THE_MONTH = "first day of the month"
SECOND_DAY_OF_THE_MONTH = "second day of the month"

DATA = pd.DataFrame(
    {
        DATE_COL: [
            datetime(year=2023, month=1, day=1, tzinfo=timezone.utc).date(),
            datetime(year=2024, month=1, day=1, tzinfo=timezone.utc).date(),
            datetime(year=2024, month=2, day=1, tzinfo=timezone.utc).date(),
            datetime(year=2024, month=2, day=2, tzinfo=timezone.utc).date(),
        ],
        VALUE_COL: [
            LAST_YEAR,
            FIRST_DAY_OF_THE_YEAR,
            FIRST_DAY_OF_THE_MONTH,
            SECOND_DAY_OF_THE_MONTH,
        ],
    }
)

JUST_SQLITE = [SqliteDatasourceTestConfig()]


class TestPartitioning:
    """Tests to show that we partition sqlite data sourdces correctly.

    All tests use ExpectColumnDistinctValuesToEqualSet to detect that we are just seeing the
    appropriate rows.
    """

    @parameterize_batch_for_data_sources(
        data_source_configs=JUST_SQLITE,
        data=pd.DataFrame(DATA),
    )
    def test_yearly_partitioning(self, asset_for_datasource: TableAsset) -> None:
        batch_def = asset_for_datasource.add_batch_definition_yearly("yearly", column=DATE_COL)
        batch = batch_def.get_batch()

        result = batch.validate(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=VALUE_COL,
                value_set=[
                    # NOT LAST_YEAR
                    FIRST_DAY_OF_THE_YEAR,
                    FIRST_DAY_OF_THE_MONTH,
                    SECOND_DAY_OF_THE_MONTH,
                ],
            )
        )
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=JUST_SQLITE,
        data=pd.DataFrame(DATA),
    )
    def test_monthly_partitioning(self, asset_for_datasource: TableAsset) -> None:
        batch_def = asset_for_datasource.add_batch_definition_monthly("monthly", column=DATE_COL)
        batch = batch_def.get_batch()

        result = batch.validate(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=VALUE_COL,
                value_set=[
                    # NOT LAST_YEAR
                    # NOT FIRST_DAY_OF_THE_YEAR,
                    FIRST_DAY_OF_THE_MONTH,
                    SECOND_DAY_OF_THE_MONTH,
                ],
            )
        )
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=JUST_SQLITE,
        data=pd.DataFrame(DATA),
    )
    def test_daily_partitioning(self, asset_for_datasource: TableAsset) -> None:
        batch_def = asset_for_datasource.add_batch_definition_daily("daily", column=DATE_COL)
        batch = batch_def.get_batch()

        result = batch.validate(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=VALUE_COL,
                value_set=[
                    # NOT LAST_YEAR
                    # NOT FIRST_DAY_OF_THE_YEAR,
                    # NOT FIRST_DAY_OF_THE_MONTH,
                    SECOND_DAY_OF_THE_MONTH,
                ],
            )
        )
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=JUST_SQLITE,
        data=pd.DataFrame(DATA),
    )
    def test_order_ascending__true(self, asset_for_datasource: TableAsset) -> None:
        batch_def = asset_for_datasource.add_batch_definition_daily(
            "daily_ascending", column=DATE_COL, sort_ascending=True
        )
        batch = batch_def.get_batch()

        result = batch.validate(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=VALUE_COL,
                value_set=[
                    SECOND_DAY_OF_THE_MONTH,
                ],
            )
        )
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=JUST_SQLITE,
        data=pd.DataFrame(DATA),
    )
    def test_order_ascending__false(self, asset_for_datasource: TableAsset) -> None:
        batch_def = asset_for_datasource.add_batch_definition_daily(
            "daily_descending", column=DATE_COL, sort_ascending=False
        )
        batch = batch_def.get_batch()

        result = batch.validate(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=VALUE_COL,
                value_set=[
                    LAST_YEAR,
                ],
            )
        )
        assert result.success


@pytest.mark.sqlite
def test_cached_execution_engine_sees_schema_changes(
    ephemeral_context_with_defaults: AbstractDataContext,
    tmp_path: pathlib.Path,
) -> None:
    """Reusing the execution engine must not reuse stale table metadata.

    The engine's SQLAlchemy inspector caches what it reflects. With the execution engine
    cached across validations, a column added between two validations has to show up in
    the second one, exactly as it did when every validation built a fresh engine. Only a
    real database reflected twice shows this, so it belongs here rather than in a unit test.
    """
    db_path = tmp_path / "schema_changes.db"
    raw_engine = sa.create_engine(f"sqlite:///{db_path}")
    with raw_engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (a INTEGER, b INTEGER)"))
        conn.execute(sa.text("INSERT INTO t VALUES (1, 2)"))

    ds = ephemeral_context_with_defaults.data_sources.add_sqlite(
        name="schema_changes", connection_string=f"sqlite:///{db_path}"
    )
    batch = (
        ds.add_table_asset(name="t", table_name="t")
        .add_batch_definition_whole_table(name="whole")
        .get_batch()
    )

    before = batch.validate(gxe.ExpectTableColumnCountToEqual(value=2))
    assert ds.get_execution_engine() is ds.get_execution_engine()
    with raw_engine.begin() as conn:
        conn.execute(sa.text("ALTER TABLE t ADD COLUMN c INTEGER"))
    after = batch.validate(gxe.ExpectTableColumnCountToEqual(value=2))

    assert before.success is True
    assert after.success is False
    assert after.result["observed_value"] == 3
