import pandas as pd
import pytest

from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.metrics.column_pair.values_in_set import (
    ColumnPairValuesInSetUnexpectedCount,
    ColumnPairValuesInSetUnexpectedCountResult,
)
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import SparkFilesystemCsvDatasourceTestConfig
from tests.metrics.conftest import PANDAS_DATA_SOURCES, SPARK_DATA_SOURCES, SQL_DATA_SOURCES

COL_A = "A"
COL_B = "B"
COL_A_WITH_NULLS = "A_WITH_NULLS"
COL_B_WITH_NULLS = "B_WITH_NULLS"
DATA_FRAME = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        COL_A: [1, 2, 3, 4],
        COL_B: ["a", "b", "c", "d"],
    },
)
DATA_FRAME_WITH_NULLS = pd.DataFrame(
    {
        COL_A_WITH_NULLS: [None, 2, None, 4, None, None],
        COL_B_WITH_NULLS: ["a", "b", None, None, None, None],
    }
)
SUCCESS_VALUE_PAIR_SET = {(1, "a"), (2, "b"), (3, "c"), (4, "d")}
FAILURE_VALUE_PAIR_SET = {(1, "a"), (2, "b"), (3, "c")}
FAILURE_RESULT_COUNT = 1
NO_MATCH_PAIR_SET = {(5, "e")}


class TestColumnPairValuesInSetUnexpectedValues:
    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES + SQL_DATA_SOURCES,
        data=DATA_FRAME,
    )
    def test_success(self, batch_for_datasource: Batch) -> None:
        batch = batch_for_datasource
        metric = ColumnPairValuesInSetUnexpectedCount(
            value_pairs_set=SUCCESS_VALUE_PAIR_SET,
            column_A=COL_A,
            column_B=COL_B,
        )
        result = batch.compute_metrics(metric)
        assert isinstance(result, ColumnPairValuesInSetUnexpectedCountResult)
        assert result.value == 0

    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES + SQL_DATA_SOURCES,
        data=DATA_FRAME,
    )
    def test_failure(self, batch_for_datasource: Batch) -> None:
        batch = batch_for_datasource
        metric = ColumnPairValuesInSetUnexpectedCount(
            value_pairs_set=FAILURE_VALUE_PAIR_SET,
            column_A=COL_A,
            column_B=COL_B,
        )
        result = batch.compute_metrics(metric)
        assert isinstance(result, ColumnPairValuesInSetUnexpectedCountResult)
        assert result.value == FAILURE_RESULT_COUNT

    @pytest.mark.parametrize(
        "ignore_row_if,unexpected_count",
        [
            ("either_value_is_missing", 1),
            ("both_values_are_missing", 3),
            ("neither", 6),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES,
        data=DATA_FRAME_WITH_NULLS,
    )
    def test_ignore_row_if__pandas(
        self, batch_for_datasource: Batch, ignore_row_if, unexpected_count
    ) -> None:
        batch = batch_for_datasource
        metric = ColumnPairValuesInSetUnexpectedCount(
            value_pairs_set=NO_MATCH_PAIR_SET,
            column_A=COL_A_WITH_NULLS,
            column_B=COL_B_WITH_NULLS,
            ignore_row_if=ignore_row_if,
        )
        result = batch.compute_metrics(metric)
        assert result.value == unexpected_count

    @pytest.mark.parametrize(
        "ignore_row_if,unexpected_count",
        [
            pytest.param("either_value_is_missing", 1),
            pytest.param("both_values_are_missing", 3),
            pytest.param("neither", 6),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=[SparkFilesystemCsvDatasourceTestConfig()],
        data=DATA_FRAME_WITH_NULLS,
    )
    def test_ignore_row_if__spark(
        self, batch_for_datasource: Batch, ignore_row_if, unexpected_count
    ) -> None:
        """This test captures unexpected behavior for Spark FileSystem data sources."""
        batch = batch_for_datasource
        metric = ColumnPairValuesInSetUnexpectedCount(
            value_pairs_set=NO_MATCH_PAIR_SET,
            column_A=COL_A_WITH_NULLS,
            column_B=COL_B_WITH_NULLS,
            ignore_row_if=ignore_row_if,
        )
        result = batch.compute_metrics(metric)
        assert result.value == unexpected_count

    @pytest.mark.parametrize(
        "ignore_row_if,unexpected_count",
        [
            pytest.param("either_value_is_missing", 1),
            pytest.param("both_values_are_missing", 3),
            pytest.param("neither", 6),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=SQL_DATA_SOURCES,
        data=DATA_FRAME_WITH_NULLS,
    )
    def test_ignore_row_if__sql(
        self, batch_for_datasource: Batch, ignore_row_if, unexpected_count
    ) -> None:
        """SQL data sources count the same rows as pandas and Spark: pairs where both values
        are null are no longer dropped when ignore_row_if is "neither" (#12252).
        """
        batch = batch_for_datasource
        metric = ColumnPairValuesInSetUnexpectedCount(
            value_pairs_set=NO_MATCH_PAIR_SET,
            column_A=COL_A_WITH_NULLS,
            column_B=COL_B_WITH_NULLS,
            ignore_row_if=ignore_row_if,
        )
        result = batch.compute_metrics(metric)
        assert result.value == unexpected_count
