from typing import Dict, Optional

from scipy import stats

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)


class ColumnWassersteinDistance(ColumnAggregateMetricProvider):
    """MetricProvider Class for Wasserstein Distance MetricProvider"""

    metric_name = "column.custom.wasserstein"
    value_keys = ("raw_values", "partition")

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, raw_values=None, partition=None, **kwargs):
        if raw_values is not None:
            w_value = stats.wasserstein_distance(
                raw_values,
                column,
            )
        elif partition is not None:
            w_value = stats.wasserstein_distance(
                partition["values"],
                column,
                partition["weights"],
            )
        else:
            raise ValueError("raw_values and partition object cannot both be None!")

        return w_value

    # @metric_value(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(
    #     cls,
    #     execution_engine: "SqlAlchemyExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     (
    #         selectable,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, MetricDomainTypes.COLUMN
    #     )
    #     column_name = accessor_domain_kwargs["column"]
    #     column = sa.column(column_name)
    #     sqlalchemy_engine = execution_engine.engine
    #     dialect = sqlalchemy_engine.dialect
    #
    #     column_median = None
    #
    #     # TODO: compute the value and return it
    #
    #     return column_median
    #
    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine: "SqlAlchemyExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     (
    #         df,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, MetricDomainTypes.COLUMN
    #     )
    #     column = accessor_domain_kwargs["column"]
    #
    #     column_median = None
    #
    #     # TODO: compute the value and return it
    #
    #     return column_median
    #
    # @classmethod
    # def _get_evaluation_dependencies(
    #     cls,
    #     metric: MetricConfiguration,
    #     configuration: Optional[ExpectationConfiguration] = None,
    #     execution_engine: Optional[ExecutionEngine] = None,
    #     runtime_configuration: Optional[dict] = None,
    # ):
    #     """This should return a dictionary:
    #
    #     {
    #       "dependency_name": MetricConfiguration,
    #       ...
    #     }
    #     """
    #
    #     dependencies = super()._get_evaluation_dependencies(
    #         metric=metric,
    #         configuration=configuration,
    #         execution_engine=execution_engine,
    #         runtime_configuration=runtime_configuration,
    #     )
    #
    #     table_domain_kwargs = {
    #         k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
    #     }
    #
    #     dependencies.update(
    #         {
    #             "table.row_count": MetricConfiguration(
    #                 "table.row_count", table_domain_kwargs
    #             )
    #         }
    #     )
    #
    #     if isinstance(execution_engine, SqlAlchemyExecutionEngine):
    #         dependencies["column_values.nonnull.count"] = MetricConfiguration(
    #             "column_values.nonnull.count", metric.metric_domain_kwargs
    #         )
    #
    #     return dependencies


class ExpectColumnWassersteinDistanceToBeLessThan(ColumnAggregateExpectation):
    """Expect that the Wasserstein Distance of the specified column with respect to an optional partition object to be lower than the provided value.

    See Also:
        [partition objects for distributional Expectations](https://docs.greatexpectations.io/docs/reference/expectations/distributional_expectations/#partition-objects)
        [Wasserstein Metric on Wikipedia](https://en.wikipedia.org/wiki/Wasserstein_metric)
    """

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.wasserstein",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
        "raw_values",
        "partition",
    )

    library_metadata = {
        "maturity": "experimental",
        "tags": [],
        "contributors": [
            "rexboyce",
            "abegong",
            "lodeous",
        ],
    }

    # Default values
    default_kwarg_values = {
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "raw_values": None,
        "partition": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    examples = [
        {
            "data": {"a": [0, 1, 3], "b": [3, 4, 5]},
            "tests": [
                {
                    "title": "test_raw_values",
                    "exact_match_out": False,
                    "in": {"column": "a", "raw_values": [5, 6, 8], "max_value": 6},
                    "out": {"success": True, "observed_value": 5},
                    "include_in_gallery": True,
                },
                {
                    "title": "test_raw_values_strict",
                    "exact_match_out": False,
                    "in": {
                        "column": "a",
                        "raw_values": [5, 6, 8],
                        "max_value": 5,
                        "strict_max": True,
                    },
                    "out": {"success": False, "observed_value": 5},
                    "include_in_gallery": True,
                },
                {
                    "title": "test_partition",
                    "exact_match_out": False,
                    "in": {
                        "column": "b",
                        "partition": {
                            "values": [1, 2, 4],
                            "weights": [0.5, 0.25, 0.25],
                        },
                        "max_value": 5,
                        "strict_max": True,
                    },
                    "out": {"success": True, "observed_value": 2},
                    "include_in_gallery": True,
                },
            ],
        },
    ]

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    # @classmethod
    # @renderer(renderer_type="renderer.prescriptive")
    # @render_evaluation_parameter_string
    # def _prescriptive_renderer(
    #     cls,
    #     configuration=None,
    #     result=None,
    #     runtime_configuration=None,
    #     **kwargs,
    # ):
    #     runtime_configuration = runtime_configuration or {}
    #     include_column_name = False if runtime_configuration.get("include_column_name") is False else True
    #     styling = runtime_configuration.get("styling")
    #     params = substitute_none_for_missing(
    #         configuration.kwargs,
    #         [
    #             "column",
    #             "min_value",
    #             "max_value",
    #             "row_condition",
    #             "condition_parser",
    #             "strict_min",
    #             "strict_max",
    #         ],
    #     )
    #
    #     if (params["min_value"] is None) and (params["max_value"] is None):
    #         template_str = "median may have any numerical value."
    #     else:
    #         at_least_str, at_most_str = handle_strict_min_max(params)
    #         if params["min_value"] is not None and params["max_value"] is not None:
    #             template_str = f"median must be {at_least_str} $min_value and {at_most_str} $max_value."
    #         elif params["min_value"] is None:
    #             template_str = f"median must be {at_most_str} $max_value."
    #         elif params["max_value"] is None:
    #             template_str = f"median must be {at_least_str} $min_value."
    #
    #     if include_column_name:
    #         template_str = "$column " + template_str
    #
    #     if params["row_condition"] is not None:
    #         (
    #             conditional_template_str,
    #             conditional_params,
    #         ) = parse_row_condition_string_pandas_engine(params["row_condition"])
    #         template_str = conditional_template_str + ", then " + template_str
    #         params.update(conditional_params)
    #
    #     return [
    #         RenderedStringTemplateContent(
    #             **{
    #                 "content_block_type": "string_template",
    #                 "string_template": {
    #                     "template": template_str,
    #                     "params": params,
    #                     "styling": styling,
    #                 },
    #             }
    #         )
    #     ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.custom.wasserstein",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )


if __name__ == "__main__":
    ExpectColumnWassersteinDistanceToBeLessThan().print_diagnostic_checklist()
