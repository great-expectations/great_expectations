import re
from copy import deepcopy

# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
from typing import Any, Dict, Optional, Tuple

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    TableExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import substitute_none_for_missing
from great_expectations.validator.validation_graph import MetricConfiguration

# DEBUG=True
DEBUG = False

import logging

logger = logging.getLogger(__name__)

# This class defines the Metric, a class used by the Expectation to compute important data for validating itself
class TableChecksum(TableMetricProvider):

    # This is a built in metric - you do not have to implement it yourself. If you would like to use
    # a metric that does not yet exist, you can use the template below to implement it!
    metric_name = "table.checksum"

    # Below are metric computations for different dialects (Pandas, SqlAlchemy, Spark)
    # They can be used to compute the table data you will need to validate your Expectation
    # @metric_value(engine=PandasExecutionEngine)
    # def _pandas(
    #     cls,
    #     execution_engine: PandasExecutionEngine,
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict
    # ):
    #
    #     columnslist = metrics.get("table.columns")
    #
    #     return len(columnslist)

    @metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type="value")
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
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
            metric_domain_kwargs, MetricDomainTypes.TABLE
        )

        # get the all column names of table using existing metric.
        columns = metrics.get("table.columns")
        ignore_columns = runtime_configuration["ignore_columns"]
        dialect_name = execution_engine.engine.dialect.name

        selectcolumns = select_column_list(columns, ignore_columns)

        if execution_engine.engine.dialect.name == "sqlite":
            cksumquery = get_sqlite_checksum_query(
                selectable.name, selectcolumns, ignore_columns
            )
        elif dialect_name == "bigquery":
            cksumquery = get_bigquery_checksum_query(
                selectable.name, selectcolumns, ignore_columns
            )
        else:
            logger.error("sql dialect is not supported: " + dialect_name)
            return 0

        if DEBUG:
            logger.error("\n***********cksumquery***********\n" + cksumquery)

        return int(execution_engine.engine.execute(cksumquery).scalar())

    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine: SparkDFExecutionEngine,
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     columnslist = metrics.get("table.columns")
    #
    #     return len(columnslist)

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }


# function to form bigquery query as some function will be different in each dialect.
def get_bigquery_checksum_query(table_name, selectcolumns, ignore_columns):
    return (
        "select sum(cast(FARM_FINGERPRINT(concat('', "
        + ",".join(
            map(lambda x: " IFNULL(cast(" + x + " as string), '')", selectcolumns)
        )
        + "))as NUMERIC)) as cksum from "
        + str(table_name)
    )


# function to form sqlite query as some functions will be different in each dialect.
def get_sqlite_checksum_query(table_name, selectcolumns, ignore_columns):

    logger.warning(
        "“Warning: get_sqlite_checksum_query is experimental. The checksum or similar hashing function is not there in sqlite so using length function for testing purposes"
    )
    # checksum or similar hashing function is not there in sqlite so using length function for testing purposes.
    return (
        "select sum(length('' || "
        + " || ".join(
            map(lambda x: "coalesce(cast(" + x + " as varchar), '')", selectcolumns)
        )
        + ")) as hash from "
        + str(table_name)
    )
    # + ')'


def select_column_list(columns, ignore_columns):
    return filter(
        lambda x: x not in [x.strip() for x in ignore_columns.split(",")], columns
    )


class TableChecksumValues(TableMetricProvider):

    # This is a built in metric - you do not have to implement it yourself. If you would like to use
    # a metric that does not yet exist, you can use the template below to implement it!
    metric_name = "table.checksum.values"

    # Below are metric computations for different dialects (Pandas, SqlAlchemy, Spark)
    # They can be used to compute the table data you will need to validate your Expectation
    # @metric_value(engine=PandasExecutionEngine)
    # def _pandas(
    #     cls,
    #     execution_engine: PandasExecutionEngine,
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict
    # ):
    #
    #     cksum_value_self = metrics.get("table.checksum.self")
    #     cksum_value_other = metrics.get("table.checksum.other")
    #
    #     return cksum_value_self, cksum_value_other

    @metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type="value")
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):

        cksum_value_self = metrics.get("table.checksum.self")
        cksum_value_other = metrics.get("table.checksum.other")

        return cksum_value_self, cksum_value_other

    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine: SparkDFExecutionEngine,
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #
    # cksum_value_self = metrics.get("table.checksum.self")
    # cksum_value_other = metrics.get("table.checksum.other")
    #
    # return cksum_value_self, cksum_value_other

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):

        # set ignore_columns to '' if it is not provided.
        if "ignore_columns" in configuration.kwargs:
            runtime_configuration["ignore_columns"] = configuration.kwargs[
                "ignore_columns"
            ]
        else:
            runtime_configuration["ignore_columns"] = ""

        # deep copy the configuration to create new configuration for other_table_name.
        table_row_count_metric_config_other = deepcopy(metric.metric_domain_kwargs)

        # set table value to other_table_name in metric configuration.
        table_row_count_metric_config_other["table"] = configuration.kwargs[
            "other_table_name"
        ]

        return {
            "table.checksum.self": MetricConfiguration(
                "table.checksum", metric.metric_domain_kwargs
            ),
            "table.checksum.other": MetricConfiguration(
                "table.checksum", table_row_count_metric_config_other
            ),
        }


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectTableChecksumToEqualOtherTable(TableExpectation):
    """Expect the checksum table to equal the checksum of another table.

    expect_table_checksum_to_equal_other_table is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        other_table_name (str): \
            The name of the other table.
        ignore_columns (str): \
            optional: Comma seperated list of columns which should be ignored from checksum calculation. e.g. "created_date, userid"

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    See Also:
        [expect_table_row_count_to_equal_other_table](https://greatexpectations.io/expectations/xpect_table_row_count_to_equal_other_table)
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": [
                {
                    "dataset_name": "table_data_1",
                    "data": {
                        "columnone": [3, 5, 7],
                        "columntwo": [True, False, True],
                        "columnthree": ["a", "b", "c"],
                        "columnfour": [None, 2, None],
                    },
                },
                {
                    "dataset_name": "table_data_2",
                    "data": {
                        "columnone": [3, 5, 7],
                        "columntwo": [True, False, True],
                        "columnthree": ["a", "b", "c"],
                        "columnfour": [None, 2, None],
                    },
                },
                {
                    "dataset_name": "table_data_3",
                    "data": {
                        "columnone": [3, 5, 7, 8],
                        "columntwo": [True, False, True, False],
                        "columnthree": ["a", "b", "c", "d"],
                        "columnfour": [None, 2, None, None],
                    },
                },
            ],
            "tests": [
                {
                    "title": "for sqlite - positive_test_with_checksum_equal_with_ignore_columns",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "other_table_name": "table_data_2",
                        "ignore_columns": "columnone, columntwo",
                    },
                    # "in": {"other_table_name": "table_data_2",},
                    "out": {
                        "success": True,
                        "observed_value": {"self": 6, "other": 6},
                    },
                    "only_for": ["sqlite"],
                },
                {
                    "title": "for sqlite - negative_test_with_checksum_not_equal_with_ignore_columns",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "other_table_name": "table_data_3",
                        "ignore_columns": "columnone, columntwo",
                    },
                    # "in": {"other_table_name": "table_data_3",},
                    "out": {
                        "success": False,
                        "observed_value": {"self": 6, "other": 7},
                    },
                    "only_for": ["sqlite"],
                },
                {
                    "title": "for sqlite - positive_test_with_checksum_equal_without_ignore_columns",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "other_table_name": "table_data_2",
                    },
                    "out": {
                        "success": True,
                        "observed_value": {"self": 12, "other": 12},
                    },
                    "only_for": ["sqlite"],
                },
                {
                    "title": "for sqlite - negative_test_with_checksum_not_equal_without_ignore_columns",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "other_table_name": "table_data_3",
                    },
                    "out": {
                        "success": False,
                        "observed_value": {"self": 12, "other": 15},
                    },
                    "only_for": ["sqlite"],
                },
                {
                    "title": "for bigquery - positive_test_with_checksum_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "other_table_name": "table_data_2",
                        "ignore_columns": "columnone, columntwo",
                    },
                    # "in": {"other_table_name": "table_data_2",},
                    "out": {
                        "success": True,
                        "observed_value": {
                            "self": -7127504315667345025,
                            "other": -7127504315667345025,
                        },
                    },
                    "suppress_test_for": ["sqlite"],
                },
                {
                    "title": "for bigquery - negative_test_with_checksum_not_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "other_table_name": "table_data_3",
                        "ignore_columns": "columnone, columntwo",
                    },
                    # "in": {"other_table_name": "table_data_3",},
                    "out": {
                        "success": False,
                        "observed_value": {
                            "self": -7127504315667345025,
                            "other": -2656867619187774560,
                        },
                    },
                    "suppress_test_for": ["sqlite"],
                },
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    # "dialects": ['sqlite', 'bigquery',],
                    "dialects": [
                        "sqlite",
                    ],
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["table expectation", "multi-table expectation", "checksum"],
        "contributors": ["Yashavant Dudhe", "@yashgithub"],
    }

    metric_dependencies = ("table.checksum.values",)
    success_keys = ("other_table_name",)
    map_metric = "table.checksum.values"

    default_kwarg_values = {
        "other_table_name": None,
        "ignore_columns": "",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        try:
            assert (
                "other_table_name" in configuration.kwargs
            ), "other_table_name is required"
            assert isinstance(
                configuration.kwargs["other_table_name"], str
            ), "other_table_name must be a string"

            if "ignore_columns" in configuration.kwargs:
                pattern = re.compile(r"^(\w+)(,\s*\w+)*$")
                assert (
                    True
                    if (pattern.match(configuration.kwargs["ignore_columns"]))
                    else False
                ), "ignore_columns input is not valid. Please provide comma seperated columns list"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        super().validate_configuration(configuration)
        return True

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["value"])
        template_str = "Checksum values must match"
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

    # This method will utilize the computed metric to validate that your Expectation about the Table is true
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):

        checksum_self, checksum_other = metrics.get("table.checksum.values")

        if DEBUG:
            logger.error(
                "\nChecksum_values: "
                + "\nchecksum_self: "
                + str(checksum_self)
                + "\nchecksum_other: "
                + str(checksum_other)
            )

        return {
            "success": checksum_self == checksum_other,
            "result": {
                "observed_value": {
                    "self": checksum_self,
                    "other": checksum_other,
                }
            },
        }


if __name__ == "__main__":
    ExpectTableChecksumToEqualOtherTable().print_diagnostic_checklist()
