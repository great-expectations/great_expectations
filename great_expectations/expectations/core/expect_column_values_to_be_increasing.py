import logging
from typing import Optional, Union

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

from ...data_asset.util import parse_result_format
from ..expectation import ColumnMapDatasetExpectation, Expectation, _format_map_output
from ..registry import extract_metrics, get_metric_kwargs

logger = logging.getLogger(__name__)

try:
    import pyspark.sql.functions as F
    import pyspark.sql.types as sparktypes
    from pyspark.ml.feature import Bucketizer
    from pyspark.sql import DataFrame, SQLContext, Window
    from pyspark.sql.functions import (
        array,
        col,
        count,
        countDistinct,
        datediff,
        desc,
        expr,
        isnan,
        lag,
    )
    from pyspark.sql.functions import length as length_
    from pyspark.sql.functions import (
        lit,
        monotonically_increasing_id,
        stddev_samp,
        udf,
        when,
        year,
    )
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )


class ExpectColumnValuesToBeIncreasing(ColumnMapDatasetExpectation):
    map_metric = "column_values.increasing"
    metric_dependencies = (
        "column_values.increasing.count",
        "column_values.nonnull.count",
    )
    success_keys = ("strictly", "mostly", "parse_strings_as_datetimes")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "strictly": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "parse_strings_as_datetimes": None,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        return super().validate_configuration(configuration)

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.increasing",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("strictly",),
        metric_dependencies=tuple(),
        filter_column_isnull=True,
    )
    def _pandas_column_values_increasing(
        self,
        series: pd.Series,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
    ):
        strictly = metric_value_kwargs["strictly"]

        series_diff = series.diff()
        # The first element is null, so it gets a bye and is always treated as True
        series_diff[series_diff.isnull()] = 1

        if strictly:
            return pd.DataFrame({"column_values.increasing": series_diff > 0})
        else:
            return pd.DataFrame({"column_values.increasing": series_diff >= 0})

    @SparkDFExecutionEngine.column_map_metric(
        metric_name="column_values.increasing",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("strictly",),
        metric_dependencies=tuple(),
    )
    def _spark_column_values_increasing(
        self,
        data: "pyspark.sql.DataFrame",
        column: str,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
    ):
        strictly = metric_value_kwargs["strictly"]

        # string column name
        column_name = data.schema.names[0]
        # check if column is any type that could have na (numeric types)
        na_types = [
            isinstance(data.schema[column_name].dataType, typ)
            for typ in [
                sparktypes.LongType,
                sparktypes.DoubleType,
                sparktypes.IntegerType,
            ]
        ]

        # if column is any type that could have NA values, remove them (not filtered by .isNotNull())
        if any(na_types):
            data = data.filter(~isnan(data[0]))

        data = (
            data.withColumn("constant", lit("constant"))
            .withColumn("lag", lag(data[0]).over(Window.orderBy(col("constant"))))
            .withColumn("diff", data[0] - col("lag"))
        )

        # replace lag first row null with 1 so that it is not flagged as fail
        data = data.withColumn(
            "diff", when(col("diff").isNull(), 1).otherwise(col("diff"))
        )

        if strictly:
            return data.withColumn(
                column + "__success",
                when(col("diff") >= 1, lit(True)).otherwise(lit(False)),
            )

        else:
            return data.withColumn(
                column + "__success",
                when(col("diff") >= 0, lit(True)).otherwise(lit(False)),
            )

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        metric_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        metric_vals = extract_metrics(
            metric_dependencies, metrics, configuration, runtime_configuration
        )
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        else:
            result_format = self.default_kwarg_values.get("result_format")
        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=(
                metric_vals.get("column_values.increasing.count")
                / metric_vals.get("column_values.nonnull.count")
            )
            >= mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.nonnull.count")
            - metric_vals.get("column_values.increasing.count"),
            unexpected_list=metric_vals.get(
                "column_values.increasing.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.increasing.unexpected_index_list"
            ),
        )
