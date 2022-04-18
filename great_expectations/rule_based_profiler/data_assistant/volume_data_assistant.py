from typing import Any, Dict, List, Optional, Union

import altair as alt
import numpy as np
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import BatchRequestBase
from great_expectations.data_context import BaseDataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
)
from great_expectations.types import Colors


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

    def _plot_descriptive(self, line: alt.Chart):
        descriptive_chart: alt.Chart = line
        return descriptive_chart

    def _plot_prescriptive(
        self,
        df: pd.DataFrame,
        metric: str,
        metric_type: str,
        x_axis: str,
        x_axis_type: str,
        line: alt.Chart,
    ):
        prescriptive_chart: alt.Chart = (
            self.get_expect_domain_values_to_be_between_chart(
                self,
                df=df,
                metric=metric,
                metric_type=metric_type,
                x_axis=x_axis,
                x_axis_type=x_axis_type,
                line=line,
            )
        )
        return prescriptive_chart

    def _plot(
        self,
        metric_names: List[str],
        data: np.ndarray,
        prescriptive: bool,
        expectation_configurations: List[ExpectationConfiguration],
    ):
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "super()._plot()" for display.
        """
        # altair doesn't like periods
        metric: str = metric_names[0].replace(".", "_")
        x_axis: str = "batch"

        # available data types: https://altair-viz.github.io/user_guide/encoding.html#encoding-data-types
        x_axis_type: str = "ordinal"
        metric_type: str = "quantitative"

        df: pd.DataFrame = pd.DataFrame(data, columns=[metric])
        df[x_axis] = df.index + 1

        charts: List[alt.Chart] = []

        line: alt.Chart = self.get_line_chart(
            self,
            df=df,
            metric=metric,
            metric_type=metric_type,
            x_axis=x_axis,
            x_axis_type=x_axis_type,
        )

        if prescriptive:
            for expectation_configuration in expectation_configurations:
                if (
                    expectation_configuration.expectation_type
                    == "expect_table_row_count_to_be_between"
                ):
                    df["min_value"] = expectation_configuration.kwargs["min_value"]
                    df["max_value"] = expectation_configuration.kwargs["max_value"]

            predicate = (
                (alt.datum.min_value > alt.datum.table_row_count)
                & (alt.datum.max_value > alt.datum.table_row_count)
            ) | (
                (alt.datum.min_value < alt.datum.table_row_count)
                & (alt.datum.max_value < alt.datum.table_row_count)
            )
            point_color_condition: alt.condition = alt.condition(
                predicate=predicate,
                if_false=alt.value(Colors.GREEN.value),
                if_true=alt.value(Colors.PINK.value),
            )
            anomaly_coded_line: alt.Chart = self.get_line_chart(
                self,
                df=df,
                metric=metric,
                metric_type=metric_type,
                x_axis=x_axis,
                x_axis_type=x_axis_type,
                point_color_condition=point_color_condition,
            )

            table_row_count_chart = self._plot_prescriptive(
                self,
                df=df,
                metric=metric,
                metric_type=metric_type,
                x_axis=x_axis,
                x_axis_type=x_axis_type,
                line=anomaly_coded_line,
            )
        else:
            table_row_count_chart = self._plot_descriptive(self, line=line)

        charts.append(table_row_count_chart)

        super()._plot(self, charts=charts)

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
