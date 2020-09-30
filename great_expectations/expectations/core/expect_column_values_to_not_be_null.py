from typing import Dict

import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapDatasetExpectation


class ExpectColumnValuesToNotBeNull(ColumnMapDatasetExpectation):
    """Expect column values to not be null.

    To be counted as an exception, values must be explicitly null or missing, such as a NULL in PostgreSQL or an
    np.NaN in pandas. Empty strings don't count as null unless they have been coerced to a null type.

    expect_column_values_to_not_be_null is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_be_null \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_values_to_be_null>`

    """

    map_metric = "column_values.nonnull"
    metric_dependencies = "column_values.nonnull.count"

    @PandasExecutionEngine.column_map_metric(
        metric_name=map_metric,
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        filter_column_isnull=False,
    )
    def _pandas_nonnull_count(
        self,
        series: pd.Series,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        return ~series.isnull()

    @SqlAlchemyExecutionEngine.column_map_metric(
        metric_name=map_metric,
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        filter_column_isnull=False,
    )
    def _sqlalchemy_nonnull_map_metric(
        self,
        column,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        import sqlalchemy as sa

        return sa.not_(column.is_(None))

    @SparkDFExecutionEngine.column_map_metric(
        metric_name=map_metric,
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
    )
    def _spark_null_map_metric(
        self,
        data: "pyspark.sql.DataFrame",
        column: str,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        import pyspark.sql.functions as F

        return data.withColumn(column + "__success", F.col(column).isNotNull())
