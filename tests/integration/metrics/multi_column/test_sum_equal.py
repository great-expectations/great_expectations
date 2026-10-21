import pandas as pd
import pytest

from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.metrics.multi_column.sum_equal import (
    MultiColumnSumEqualUnexpectedCount,
    MultiColumnSumEqualUnexpectedCountResult,
)
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.metrics.conftest import (
    PANDAS_DATA_SOURCES,
    SPARK_DATA_SOURCES,
    SQL_DATA_SOURCES,
)

COL_A = "A"
COL_B = "B"
COL_A_WITH_NULLS = "A_WITH_NULLS"
COL_B_WITH_NULLS = "B_WITH_NULLS"
DATA_FRAME = pd.DataFrame(
    {
        COL_A: [1, 2, 3, 4],
        COL_B: [4, 3, 2, 1],
    },
)
SUCCESS_SUM_TOTAL = 5
SUCCESS_COUNT = 0
FAILURE_SUM_TOTAL = 0
FAILURE_COUNT = 4
DATA_FRAME_WITH_NULLS = pd.DataFrame(
    {
        COL_A_WITH_NULLS: [None, 2, None, 4, None, None],
        COL_B_WITH_NULLS: [4, 3, None, None, None, None],
    }
)


class TestMultiColumnSumEqualsUnexpectedCount:
    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES + SQL_DATA_SOURCES,
        data=DATA_FRAME,
    )
    def test_success(self, batch_for_datasource: Batch) -> None:
        batch = batch_for_datasource
        metric = MultiColumnSumEqualUnexpectedCount(
            sum_total=SUCCESS_SUM_TOTAL,
            column_list=[COL_A, COL_B],
        )
        result = batch.compute_metrics(metric)
        assert isinstance(result, MultiColumnSumEqualUnexpectedCountResult)
        assert result.value == SUCCESS_COUNT

    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES + SQL_DATA_SOURCES,
        data=DATA_FRAME,
    )
    def test_failure(self, batch_for_datasource: Batch) -> None:
        batch = batch_for_datasource
        metric = MultiColumnSumEqualUnexpectedCount(
            sum_total=FAILURE_SUM_TOTAL,
            column_list=[COL_A, COL_B],
        )
        result = batch.compute_metrics(metric)
        assert isinstance(result, MultiColumnSumEqualUnexpectedCountResult)
        assert result.value == FAILURE_COUNT

    @pytest.mark.parametrize(
        "ignore_row_if,unexpected_count",
        [
            ("any_value_is_missing", 1),
            ("all_values_are_missing", 3),
            ("never", 6),
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
        metric = MultiColumnSumEqualUnexpectedCount(
            sum_total=FAILURE_SUM_TOTAL,
            column_list=[COL_A_WITH_NULLS, COL_B_WITH_NULLS],
            ignore_row_if=ignore_row_if,
        )
        result = batch.compute_metrics(metric)
        assert result.value == unexpected_count

    @pytest.mark.parametrize(
        "ignore_row_if,unexpected_count",
        [
            pytest.param("any_value_is_missing", 1),
            pytest.param("all_values_are_missing", 3),
            pytest.param("never", 3),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=SPARK_DATA_SOURCES,
        data=DATA_FRAME_WITH_NULLS,
    )
    def test_ignore_row_if__spark(
        self, batch_for_datasource: Batch, ignore_row_if, unexpected_count
    ) -> None:
        batch = batch_for_datasource
        metric = MultiColumnSumEqualUnexpectedCount(
            sum_total=FAILURE_SUM_TOTAL,
            column_list=[COL_A_WITH_NULLS, COL_B_WITH_NULLS],
            ignore_row_if=ignore_row_if,
        )
        result = batch.compute_metrics(metric)
        assert result.value == unexpected_count

    @pytest.mark.parametrize(
        "ignore_row_if,unexpected_count",
        [
            pytest.param("any_value_is_missing", 1),
            pytest.param("all_values_are_missing", 3),
            pytest.param("never", 6),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=SQL_DATA_SOURCES,
        data=DATA_FRAME_WITH_NULLS,
    )
    def test_ignore_row_if__sql(
        self, batch_for_datasource: Batch, ignore_row_if, unexpected_count
    ) -> None:
        batch = batch_for_datasource
        metric = MultiColumnSumEqualUnexpectedCount(
            sum_total=FAILURE_SUM_TOTAL,
            column_list=[COL_A_WITH_NULLS, COL_B_WITH_NULLS],
            ignore_row_if=ignore_row_if,
        )
        result = batch.compute_metrics(metric)
        assert result.value == unexpected_count
