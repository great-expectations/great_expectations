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
from great_expectations.types import ColorPalettes, Colors


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

    def _plot_descriptive(self):
        pass

    def _plot_prescriptive(self):
        pass

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

        # all available encodings https://altair-viz.github.io/user_guide/encoding.html
        charts: List[alt.Chart] = []

        line_chart_title: str = f"{metric_label} per {x_axis_label}"
        line: alt.Chart = (
            alt.Chart(df, title=line_chart_title)
            .mark_line(color=Colors.BLUE_2.value)
            .encode(
                x=alt.X(x_axis_label, type=x_axis_type, title=x_axis_label),
                y=alt.Y(metric_label, type=metric_type, title=metric_label),
            )
        )

        if prescriptive:
            for expectation_configuration in expectation_configurations:
                if (
                    expectation_configuration.expectation_type
                    == "expect_table_row_count_to_be_between"
                ):
                    min_label: str = "min_value"
                    max_label: str = "max_value"
                    min_value: float = expectation_configuration.kwargs["min_value"]
                    max_value: float = expectation_configuration.kwargs["max_value"]

                    df[min_label] = min_value
                    df[max_label] = max_value

                    lower_limit: alt.Chart = (
                        alt.Chart(df, title=line_chart_title)
                        .mark_line(color=ColorPalettes.HEATMAP.value[4], opacity=0.9)
                        .encode(
                            x=alt.X(x_axis_label, type=x_axis_type, title=x_axis_label),
                            y=alt.Y(min_label, type=metric_type, title=metric_label),
                        )
                    )

                    upper_limit: alt.Chart = (
                        alt.Chart(df, title=line_chart_title)
                        .mark_line(color=ColorPalettes.HEATMAP.value[4], opacity=0.9)
                        .encode(
                            x=alt.X(x_axis_label, type=x_axis_type, title=x_axis_label),
                            y=alt.Y(max_label, type=metric_type, title=metric_label),
                        )
                    )

                    band = (
                        alt.Chart(df)
                        .mark_area()
                        .encode(
                            x=alt.X(x_axis_label, type=x_axis_type, title=x_axis_label),
                            y=alt.Y(min_label, title=metric_label, type=metric_type),
                            y2=alt.Y2(max_label, title=metric_label),
                        )
                    )

                    line_chart = band + lower_limit + upper_limit + line
        else:
            line_chart = line

        charts.append(line_chart)

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
