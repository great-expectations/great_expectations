from abc import abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import altair as alt
import pandas as pd

from great_expectations.core import ExpectationSuite
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    Domain,
    ParameterNode,
)
from great_expectations.rule_based_profiler.types.altair import (
    ALTAIR_DEFAULT_CONFIGURATION,
    AltairDataTypes,
)
from great_expectations.types import ColorPalettes, Colors, SerializableDictDot


@dataclass
class DataAssistantResult(SerializableDictDot):
    """
    DataAssistantResult is a "dataclass" object, designed to hold results of executing "DataAssistant.run()" method.
    Available properties ("metrics_by_domain", "expectation_suite", and configuration object ("RuleBasedProfilerConfig")
    of effective Rule-Based Profiler, which embodies given "DataAssistant".
    """

    profiler_config: Optional["RuleBasedProfilerConfig"] = None  # noqa: F821
    metrics_by_domain: Optional[Dict[Domain, Dict[str, Any]]] = None
    # Obtain "expectation_configurations" using "expectation_configurations = expectation_suite.expectations".
    # Obtain "meta/details" using "meta = expectation_suite.meta".
    expectation_suite: Optional[ExpectationSuite] = None
    execution_time: Optional[float] = None  # Execution time (in seconds).

    def to_dict(self) -> dict:
        """Returns: this DataAssistantResult as a dictionary"""
        return asdict(self)

    def to_json_dict(self) -> dict:
        """Returns: this DataAssistantResult as a json dictionary"""
        return convert_to_json_serializable(data=self.to_dict())

    def get_attributed_metrics_by_domain(
        self,
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        domain: Domain
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        fully_qualified_parameter_name: str
        parameter_value: ParameterNode
        metrics_attributed_values_by_domain: Dict[Domain, Dict[str, ParameterNode]] = {
            domain: {
                parameter_value[
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                ].metric_configuration.metric_name: parameter_value[
                    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                ]
                for fully_qualified_parameter_name, parameter_value in parameter_values_for_fully_qualified_parameter_names.items()
            }
            for domain, parameter_values_for_fully_qualified_parameter_names in self.metrics_by_domain.items()
        }
        return metrics_attributed_values_by_domain

    @staticmethod
    def display(charts: List[alt.Chart]) -> None:
        """
        Display each chart passed by DataAssistantResult.plot()

        Args:
            charts: A list of altair chart objects to display
        """
        chart: alt.Chart
        altair_configuration: Dict[str, Any] = ALTAIR_DEFAULT_CONFIGURATION
        for chart in charts:
            chart.configure(**altair_configuration).display()

    @staticmethod
    def get_line_chart(
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
        line_color: Optional[str] = Colors.BLUE_2.value,
        point_color: Optional[str] = Colors.GREEN.value,
        point_color_condition: Optional[alt.condition] = None,
        tooltip: Optional[List[alt.Tooltip]] = None,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            domain_name: The name of the domain as it exists in the pandas dataframe
            domain_type: The altair data type for the domain being plotted
            line_color: Hex code for the line color
            point_color: Hex code for the point color
            point_color_condition: Altair condition for changing the point color
            tooltip: Altair tooltip for displaying relevant information on the chart

        Returns:
            An altair line chart
        """
        metric_title: str = metric_name.replace("_", " ").title()
        domain_title: str = domain_name.title()
        title: str = f"{metric_title} per {domain_title}"

        batch_id: str = "batch_id"
        batch_id_type: alt.StandardType = AltairDataTypes.NOMINAL.value

        if tooltip is None:
            tooltip: List[alt.Tooltip] = [
                alt.Tooltip(field=batch_id, type=batch_id_type),
                alt.Tooltip(field=metric_name, type=metric_type, format=","),
            ]

        line: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(metric_name, type=metric_type, title=metric_title),
                tooltip=tooltip,
            )
        )

        if point_color_condition is not None:
            points: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_point(opacity=1.0)
                .encode(
                    x=alt.X(
                        domain_name,
                        type=domain_type,
                        title=domain_title,
                    ),
                    y=alt.Y(metric_name, type=metric_type, title=metric_title),
                    stroke=point_color_condition,
                    fill=point_color_condition,
                    tooltip=tooltip,
                )
            )
        else:
            points: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_point(stroke=point_color, fill=point_color, opacity=1.0)
                .encode(
                    x=alt.X(
                        domain_name,
                        type=domain_type,
                        title=domain_title,
                    ),
                    y=alt.Y(metric_name, type=metric_type, title=metric_title),
                    tooltip=tooltip,
                )
            )

        return line + points

    @staticmethod
    def get_expect_domain_values_to_be_between_chart(
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            domain_name: The name of the domain as it exists in the pandas dataframe
            domain_type: The altair data type for the domain being plotted

        Returns:
            An altair line chart with confidence intervals corresponding to "between" expectations
        """
        line_opacity: float = 0.9
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP.value[4])
        fill_opacity: float = 0.5
        fill_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP.value[5])

        metric_title: str = metric_name.replace("_", " ").title()
        domain_title: str = domain_name.title()

        batch_id: str = "batch_id"
        batch_id_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        min_value: str = "min_value"
        min_value_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        max_value: str = "max_value"
        max_value_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value

        tooltip: list[alt.Tooltip] = [
            alt.Tooltip(field=batch_id, type=batch_id_type),
            alt.Tooltip(field=metric_name, type=metric_type, format=","),
            alt.Tooltip(field=min_value, type=min_value_type, format=","),
            alt.Tooltip(field=max_value, type=max_value_type, format=","),
        ]

        lower_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color, opacity=line_opacity)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(min_value, type=metric_type, title=metric_title),
                tooltip=tooltip,
            )
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color, opacity=line_opacity)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(max_value, type=metric_type, title=metric_title),
                tooltip=tooltip,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area(fill=fill_color, fillOpacity=fill_opacity)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(min_value, title=metric_title, type=metric_type),
                y2=alt.Y2(max_value, title=metric_title),
            )
        )

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
        anomaly_coded_line: alt.Chart = DataAssistantResult.get_line_chart(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
            point_color_condition=point_color_condition,
            tooltip=tooltip,
        )

        return band + lower_limit + upper_limit + anomaly_coded_line

    @abstractmethod
    def plot(
        self,
        prescriptive: bool = False,
    ) -> None:
        """
        Use contents of "DataAssistantResult" object to display mentrics and other detail for visualization purposes.

        Args:
            prescriptive: Type of plot to generate.
        """
        pass
