import json
import logging
import traceback
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import scipy.stats as stats

from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnExpectation,
    Expectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

logger = logging.getLogger(__name__)

try:
    from sqlalchemy.exc import ProgrammingError
    from sqlalchemy.sql import Select
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    ProgrammingError = None
    Select = None

try:
    from pyspark.sql.functions import mean as pyspark_mean_
    from pyspark.sql.functions import stddev as pyspark_std_
except ImportError:
    logger.debug(
        "Unable to load PySpark functions; install optional PySpark dependency for support"
    )
    pyspark_mean_ = None
    pyspark_std_ = None

try:
    from sqlalchemy.engine.row import Row
except ImportError:
    try:
        from sqlalchemy.engine.row import RowProxy

        Row = RowProxy
    except ImportError:
        logger.debug(
            "Unable to load SqlAlchemy Row class; please upgrade you sqlalchemy installation to the latest version."
        )
        RowProxy = None
        Row = None


class ColumnSkew(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.custom.skew"
    value_keys = ("abs",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, abs=False, **kwargs):
        if abs:
            return np.abs(stats.skew(column))
        return stats.skew(column)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)
        sqlalchemy_engine = execution_engine.engine
        dialect = sqlalchemy_engine.dialect

        column_mean = _get_query_result(
            func=sa.func.avg(column * 1.0),
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        column_count = _get_query_result(
            func=sa.func.count(column),
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        if dialect.name.lower() == "mssql":
            standard_deviation = sa.func.stdev(column)
        else:
            standard_deviation = sa.func.stddev_samp(column)

        column_std = _get_query_result(
            func=standard_deviation,
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        column_third_moment = _get_query_result(
            func=sa.func.sum(sa.func.pow(column - column_mean, 3)),
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        column_skew = column_third_moment / (column_std ** 3) / (column_count - 1)
        if metric_value_kwargs["abs"]:
            return np.abs(column_skew)
        else:
            return column_skew

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        abs_flag = metric_value_kwargs.get("abs", False)
        column = accessor_domain_kwargs["column"]

        column_avg = df.select(pyspark_mean_(column)).collect()[0][0]
        column_std = df.select(pyspark_std_(column)).collect()[0][0]

        count = df.count()

        # udf is possibly preferred here
        column_third_moment = df.rdd.map(lambda x: (x[column] - column_avg) ** 3).sum()

        column_skew = column_third_moment / (column_std ** 3) / (count - 1)
        if abs_flag:
            return np.abs(column_skew)
        else:
            return column_skew


def _get_query_result(func, selectable, sqlalchemy_engine):
    simple_query: Select = sa.select(func).select_from(selectable)

    try:
        result: Row = sqlalchemy_engine.execute(simple_query).fetchone()[0]
        return result
    except ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe()


class ExpectColumnSkewToBeBetween(ColumnExpectation):
    """Expect the column skew to be between a minimum value and a maximum value (inclusive). \

            The skewness is a measure of the asymmetry of a probability distribution about its mean. A distribution with negative skewness has a tail to the left (negative) side, and a distribution with positive skewness has a tail to the right (positive) side. The column skew is defined as \\Sum(X - \\mu)^3) / ((N-1) * \\sigma^3), where \\mu and \\sigma are the column mean and standard deviation. \

            expect_column_skew_to_be_between is a \
            :func:`column_aggregate_expectation \
            <great_expectations.execution_engine.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.
            Args:
                column (str): \
                    The column name.
                min_value (float or None): \
                    The minimum value for the column skew.
                max_value (float or None): \
                    The maximum value for the column skew.
                strict_min (boolean):
                    If True, the column skew must be strictly larger than min_value, default=False
                strict_max (boolean):
                    If True, the column skew must be strictly smaller than max_value, default=False
                abs (boolean):
                    If True, expectations will be applied to the absolute value of the column skew.
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
            Notes:
                These fields in the result object are customized for this expectation:
                ::
                    {
                        "observed_value": (float) The true mean for the column
                    }
                * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
                * If min_value is None, then max_value is treated as an upper bound.
                * If max_value is None, then min_value is treated as a lower bound.
                * Test values are drawn from various distributions (gamma, beta)
            """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a": [
                    5.27071512,
                    7.05981507,
                    8.46671693,
                    10.20629973,
                    6.15519149,
                    7.11709362,
                    5.31915535,
                    6.56441299,
                    5.69143401,
                    5.0389317,
                    6.48222587,
                    5.62433534,
                    5.46219467,
                    5.74686441,
                    6.05413964,
                    7.09435276,
                    6.43876861,
                    6.05301145,
                    6.12727457,
                    6.80603351,
                ],  # sampled from Gamma(1, 5)
                "b": [
                    81.11265955,
                    76.7836479,
                    85.25019592,
                    93.93285666,
                    83.63587009,
                    81.88712944,
                    80.37321975,
                    86.786491,
                    80.05277435,
                    70.36302516,
                    79.4907302,
                    84.1288281,
                    87.79298488,
                    78.02771047,
                    80.63975023,
                    88.59461893,
                    84.05632481,
                    84.54128192,
                    78.74152549,
                    83.60684806,
                ],  # sampled from Beta(50, 10)
                "c": [
                    95.74648827,
                    80.4031074,
                    85.41863916,
                    93.98001949,
                    97.84607818,
                    89.01205412,
                    89.55045229,
                    97.32734707,
                    93.94199505,
                    88.19992377,
                    98.3336087,
                    97.66984436,
                    97.39464709,
                    95.55637873,
                    96.10980996,
                    90.18004343,
                    96.2019293,
                    89.19519753,
                    94.01807868,
                    93.23978285,
                ],  # sampled from Beta(20, 2)
            },
            "tests": [
                {
                    "title": "positive_test_positive_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {"column": "a", "min_value": 0.25, "max_value": 10},
                    "out": {"success": True, "observed_value": 1.6974323016687487},
                },
                {
                    "title": "negative_test_no_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {"column": "b", "min_value": 0.25, "max_value": 10},
                    "out": {"success": False, "observed_value": -0.07638895580386174},
                },
                {
                    "title": "positive_test_negative_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {"column": "c", "min_value": -10, "max_value": -0.5},
                    "out": {"success": True, "observed_value": -0.9979514313860596},
                },
                {
                    "title": "negative_test_abs_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {
                        "column": "c",
                        "abs": True,
                        "min_value": 0,
                        "max_value": 0.5,
                    },
                    "out": {"success": False, "observed_value": 0.9979514313860596},
                },
                {
                    "title": "positive_test_abs_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {
                        "column": "c",
                        "abs": True,
                        "min_value": 0.5,
                        "max_value": 10,
                    },
                    "out": {"success": True, "observed_value": 0.9979514313860596},
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                {
                    "backend": "sqlalchemy",
                    "dialects": ["mysql", "postgresql"],
                },
                {
                    "backend": "spark",
                    "dialects": None,
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@lodeous",
            "@rexboyce",
            "@bragleg",
            "@edjoesu",
        ],
        "package": "experimental_expectations",
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.skew",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max", "abs")

    # Default values
    default_kwarg_values = {
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "abs": False,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "skew may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"skew must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"skew must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"skew must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    @renderer(renderer_type="renderer.descriptive.stats_table.skew_row")
    def _descriptive_stats_table_skew_row_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Skew",
                    "tooltip": {"content": "expect_column_skew_to_be_between"},
                },
            },
            "{:.2f}".format(result.result["observed_value"]),
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.custom.skew",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )


if __name__ == "__main__":
    self_check_report = ExpectColumnSkewToBeBetween().run_diagnostics()

    print(json.dumps(self_check_report, indent=2))
