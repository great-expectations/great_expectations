from typing import Any, Callable, Dict, KeysView, List, Optional, Union

import altair as alt
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import BatchRequestBase
from great_expectations.data_context import BaseDataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.visualization.plot_utils import (
    display,
    get_attributed_metrics_by_domain,
    get_expect_domain_values_to_be_between_chart,
    get_line_chart,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
    MetricValues,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    DataAssistantResult,
    Domain,
    ParameterNode,
)


# TODO: <Alex>ALEX Rename this "VolumeDataAssistant" to be more precise.</Alex>
class VolumeDataAssistant(DataAssistant):
    """
    VolumeDataAssistant provides exploration and validation of "Data Volume" aspects of specified data Batch objects.

    Self-Initializing Expectations relevant for assessing "Data Volume" include:
        - "expect_table_row_count_to_be_between";
        # TODO: <Alex>ALEX Implement Self-Initializing Capability for "expect_column_unique_value_count_to_be_between".</Alex>
        - "expect_column_unique_value_count_to_be_between";
        - Others in the future.
    """

    def __init__(
        self,
        name: str,
        batch_request: Union[BatchRequestBase, dict],
        data_context: BaseDataContext = None,
    ):
        super().__init__(
            name=name,
            batch_request=batch_request,
            data_context=data_context,
        )

    @property
    def expectation_kwargs_by_expectation_type(self) -> Dict[str, Dict[str, Any]]:
        return {
            "expect_table_row_count_to_be_between": {
                "auto": True,
                "profiler_config": None,
            },
        }

    @property
    def metrics_parameter_builders_by_domain_type(
        self,
    ) -> Dict[MetricDomainTypes, List[ParameterBuilder]]:
        table_row_count_metric_multi_batch_parameter_builder: MetricMultiBatchParameterBuilder = MetricMultiBatchParameterBuilder(
            name="table_row_count",
            metric_name="table.row_count",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            enforce_numeric_metric=True,
            replace_nan_with_zero=True,
            reduce_scalar_metric=True,
            evaluation_parameter_builder_configs=None,
            json_serialize=True,
            batch_list=None,
            batch_request=None,
            data_context=None,
        )
        return {
            MetricDomainTypes.TABLE: [
                table_row_count_metric_multi_batch_parameter_builder,
            ],
        }

    @property
    def variables(self) -> Optional[Dict[str, Any]]:
        return None

    @property
    def rules(self) -> Optional[List[Rule]]:
        return None

    def plot(
        self,
        result: DataAssistantResult,
        prescriptive: bool = False,
    ) -> None:
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "display()" for presentation.
        """
        metrics_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = result.metrics_by_domain

        # TODO: <Alex>ALEX Currently, only one Domain key (with domain_type of MetricDomainTypes.TABLE) is utilized; enhancements may require additional Domain key(s) with different domain_type value(s) to be incorporated.</Alex>
        # noinspection PyTypeChecker
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]] = dict(
            filter(
                lambda element: element[0].domain_type == MetricDomainTypes.TABLE,
                get_attributed_metrics_by_domain(
                    metrics_by_domain=metrics_by_domain
                ).items(),
            )
        )

        # TODO: <Alex>ALEX Currently, only one Domain key (with domain_type of MetricDomainTypes.TABLE) is utilized; enhancements may require additional Domain key(s) with different domain_type value(s) to be incorporated.</Alex>
        attributed_values_by_metric_name: Dict[str, ParameterNode] = list(
            attributed_metrics_by_domain.values()
        )[0]

        # Altair does not accept periods.
        # TODO: <Alex>ALEX Currently, only one Domain key (with domain_type of MetricDomainTypes.TABLE) is utilized; enhancements may require additional Domain key(s) with different domain_type value(s) to be incorporated.</Alex>
        metric_name: str = list(attributed_values_by_metric_name.keys())[0].replace(
            ".", "_"
        )
        x_axis: str = "batch"

        # available data types: https://altair-viz.github.io/user_guide/encoding.html#encoding-data-types
        x_axis_type: str = "ordinal"
        metric_type: str = "quantitative"

        batch_ids: KeysView[str]
        metric_values: MetricValues
        batch_ids, metric_values = list(attributed_values_by_metric_name.values())[
            0
        ].keys(), sum(list(attributed_values_by_metric_name.values())[0].values(), [])

        idx: int
        batch_numbers: List[int] = [idx + 1 for idx in range(len(batch_ids))]

        df: pd.DataFrame = pd.DataFrame(batch_numbers, columns=[x_axis])
        df["batch_id"] = batch_ids
        df[metric_name] = metric_values

        plot_impl: Callable
        charts: List[alt.Chart] = []

        expectation_configurations: List[
            ExpectationConfiguration
        ] = result.expectation_suite.expectations
        expectation_configuration: ExpectationConfiguration
        if prescriptive:
            for expectation_configuration in expectation_configurations:
                if (
                    expectation_configuration.expectation_type
                    == "expect_table_row_count_to_be_between"
                ):
                    df["min_value"] = expectation_configuration.kwargs["min_value"]
                    df["max_value"] = expectation_configuration.kwargs["max_value"]

            plot_impl = self._plot_prescriptive
        else:
            plot_impl = self._plot_descriptive

        table_row_count_chart: alt.Chart = plot_impl(
            df=df,
            metric=metric_name,
            metric_type=metric_type,
            x_axis=x_axis,
            x_axis_type=x_axis_type,
        )

        charts.append(table_row_count_chart)

        display(charts=charts)

    @staticmethod
    def _plot_descriptive(
        df: pd.DataFrame,
        metric: str,
        metric_type: str,
        x_axis: str,
        x_axis_type: str,
    ) -> alt.Chart:
        descriptive_chart: alt.Chart = get_line_chart(
            df=df,
            metric=metric,
            metric_type=metric_type,
            x_axis=x_axis,
            x_axis_type=x_axis_type,
        )
        return descriptive_chart

    @staticmethod
    def _plot_prescriptive(
        df: pd.DataFrame,
        metric: str,
        metric_type: str,
        x_axis: str,
        x_axis_type: str,
    ) -> alt.Chart:
        prescriptive_chart: alt.Chart = get_expect_domain_values_to_be_between_chart(
            df=df,
            metric=metric,
            metric_type=metric_type,
            x_axis=x_axis,
            x_axis_type=x_axis_type,
        )
        return prescriptive_chart
