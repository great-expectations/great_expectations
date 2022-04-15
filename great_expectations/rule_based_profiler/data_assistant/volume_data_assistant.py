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
        chart_title: str,
        metric_label: str,
        metric_type: str,
        x_axis_label: str,
        x_axis_type: str,
        line: alt.Chart,
        expectation_configurations: List[ExpectationConfiguration],
    ):
        for expectation_configuration in expectation_configurations:
            if (
                expectation_configuration.expectation_type
                == "expect_table_row_count_to_be_between"
            ):
                min_value: float = expectation_configuration.kwargs["min_value"]
                max_value: float = expectation_configuration.kwargs["max_value"]
                prescriptive_chart: alt.Chart = (
                    self.get_expect_domain_values_to_be_between_chart(
                        self,
                        df=df,
                        chart_title=chart_title,
                        metric_label=metric_label,
                        metric_type=metric_type,
                        x_axis_label=x_axis_label,
                        x_axis_type=x_axis_type,
                        line=line,
                        min_value=min_value,
                        max_value=max_value,
                    )
                )
                return prescriptive_chart

    def _plot(
        self,
        metric_names: List[str],
        data: np.ndarray,
        prescriptive: bool,
        expectation_configurations: list[ExpectationConfiguration],
    ):
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "super()._plot()" for display.
        """
        metric_label = metric_names[0].replace(".", " ").replace("_", " ").title()
        x_axis_label: str = "Batch"

        # available data types: https://altair-viz.github.io/user_guide/encoding.html#encoding-data-types
        x_axis_type: str = "ordinal"
        metric_type: str = "quantitative"

        df: pd.DataFrame = pd.DataFrame(data, columns=[metric_label])
        df[x_axis_label] = df.index + 1

        charts: List[alt.Chart] = []

        line_chart_title: str = f"{metric_label} per {x_axis_label}"
        line: alt.Chart = self.get_line_chart(
            self,
            df=df,
            title=line_chart_title,
            metric_label=metric_label,
            metric_type=metric_type,
            x_axis_label=x_axis_label,
            x_axis_type=x_axis_type,
        )

        if prescriptive:
            table_row_count_chart = self._plot_prescriptive(
                self,
                df=df,
                chart_title=line_chart_title,
                metric_label=metric_label,
                metric_type=metric_type,
                x_axis_label=x_axis_label,
                x_axis_type=x_axis_type,
                line=line,
                expectation_configurations=expectation_configurations,
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
