import copy
from abc import abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import pandas as pd

from great_expectations.core import ExpectationSuite
from great_expectations.core.util import convert_to_json_serializable, nested_update
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    Domain,
    ParameterNode,
)
from great_expectations.rule_based_profiler.types.altair import (
    AltairDataTypes,
    AltairThemes,
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
    def display(
        charts: List[alt.Chart],
        theme: Optional[Dict[str, Any]],
    ) -> None:
        """
        Display each chart passed by DataAssistantResult.plot()

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            charts: A list of altair chart objects to display
            theme: An Optional Altair top-level chart configuration dictionary to apply over the base_theme
        """
        altair_theme: Dict[str, Any] = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)
        if theme is not None:
            nested_update(altair_theme, theme)

        chart: alt.Chart
        for chart in charts:
            chart.configure(**altair_theme).display()

    @staticmethod
    def get_line_chart(
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
            An altair line chart
        """
        metric_title: str = metric_name.replace("_", " ").title()
        domain_title: str = domain_name.title()
        title: str = f"{metric_title} per {domain_title}"

        batch_id: str = "batch_id"
        batch_id_type: alt.StandardType = AltairDataTypes.NOMINAL.value

        tooltip: List[alt.Tooltip] = [
            alt.Tooltip(field=batch_id, type=batch_id_type),
            alt.Tooltip(field=metric_name, type=metric_type, format=","),
        ]

        line: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_line()
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

        points: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_point()
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
    def get_expect_table_values_to_be_between_chart(
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
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP.value[4])

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
            .mark_line(color=line_color)
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
            .mark_line(color=line_color)
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
            .mark_area()
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

        line: alt.Chart = DataAssistantResult.get_line_chart(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
        )

        anomaly_coded_line: alt.Chart = (
            DataAssistantResult._determine_anomaly_coded_line(
                line, tooltip, metric_name
            )
        )

        return band + lower_limit + upper_limit + anomaly_coded_line

    @staticmethod
    def get_vertically_concatenated_line_chart(
        column_dfs: List[Tuple[str, pd.DataFrame]],
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
    ) -> alt.VConcatChart:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            domain_name: The name of the domain as it exists in the pandas dataframe
            domain_type: The altair data type for the domain being plotted

        Returns:
            A vertically concatenated (vconcat) altair line chart
        """
        charts: List[alt.Chart] = []

        i: int
        column_name: str
        df: pd.DataFrame
        for i, (column_name, df) in enumerate(column_dfs):
            include_title: bool = i == 0
            chart: alt.Chart = (
                DataAssistantResult._get_vertically_concatenated_line_chart(
                    df=df,
                    column_name=column_name,
                    metric_name=metric_name,
                    metric_type=metric_type,
                    domain_name=domain_name,
                    domain_type=domain_type,
                    include_title=include_title,
                )
            )
            charts.append(chart)

        return alt.vconcat(*charts)

    @staticmethod
    def _get_vertically_concatenated_line_chart(
        df: pd.DataFrame,
        column_name: str,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
        include_title: bool,
    ) -> alt.Chart:
        metric_title: str = metric_name.replace("_", " ").title()
        domain_title: str = domain_name.title()

        title: str = ""
        if include_title:
            title = f"{metric_title} per {domain_title}"

        batch_id: str = "batch_id"
        batch_id_type: alt.StandardType = AltairDataTypes.NOMINAL.value

        tooltip: List[alt.Tooltip] = [
            alt.Tooltip(field=batch_id, type=batch_id_type),
            alt.Tooltip(field=metric_name, type=metric_type, format=","),
        ]

        column_label: str = column_name
        chart_height: int = 150

        line: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_line()
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(metric_name, type=metric_type, title=column_label),
                tooltip=tooltip,
            )
            .properties(height=chart_height)
        )

        points: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_point()
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(metric_name, type=metric_type, title=column_label),
                tooltip=tooltip,
            )
            .properties(height=chart_height)
        )

        return line + points

    @staticmethod
    def get_expect_column_values_to_be_between_chart(
        column_dfs: List[Tuple[str, pd.DataFrame]],
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
    ) -> alt.VConcatChart:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            domain_name: The name of the domain as it exists in the pandas dataframe
            domain_type: The altair data type for the domain being plotted

        Returns:
            A vertically concatenated (vconcat) altair line chart with confidence intervals corresponding to "between" expectations
        """
        charts: List[alt.Chart] = []

        i: int
        column_name: str
        df: pd.DataFrame
        for i, (column_name, df) in enumerate(column_dfs):
            include_title: bool = i == 0
            chart: alt.Chart = (
                DataAssistantResult._get_expect_column_values_to_be_between_chart(
                    df=df,
                    column_name=column_name,
                    metric_name=metric_name,
                    metric_type=metric_type,
                    domain_name=domain_name,
                    domain_type=domain_type,
                    include_title=include_title,
                )
            )
            charts.append(chart)

        return alt.vconcat(*charts)

    @staticmethod
    def _get_expect_column_values_to_be_between_chart(
        df: pd.DataFrame,
        column_name: str,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
        include_title: bool,
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP.value[4])

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

        column_label: str = column_name
        chart_height: int = 150

        lower_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(min_value, type=metric_type, title=column_label),
                tooltip=tooltip,
            )
            .properties(height=chart_height)
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(max_value, type=metric_type, title=column_label),
                tooltip=tooltip,
            )
            .properties(height=chart_height)
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(min_value, type=metric_type, title=column_label),
                y2=alt.Y2(max_value, title=column_label),
            )
            .properties(height=chart_height)
        )

        line: alt.Chart = DataAssistantResult._get_vertically_concatenated_line_chart(
            df=df,
            column_name=column_name,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
            include_title=include_title,
        )

        anomaly_coded_line: alt.Chart = (
            DataAssistantResult._determine_anomaly_coded_line(
                line, tooltip, metric_name
            )
        )

        return band + lower_limit + upper_limit + anomaly_coded_line

    @staticmethod
    def _determine_anomaly_coded_line(
        line: alt.Chart, tooltip: List[alt.Tooltip], metric_name: str
    ) -> alt.Chart:
        predicate: alt.expr.core.BinaryExpression = (
            (alt.datum.min_value > alt.datum[metric_name])
            & (alt.datum.max_value > alt.datum[metric_name])
        ) | (
            (alt.datum.min_value < alt.datum[metric_name])
            & (alt.datum.max_value < alt.datum[metric_name])
        )
        point_color_condition: alt.condition = alt.condition(
            predicate=predicate,
            if_false=alt.value(Colors.GREEN.value),
            if_true=alt.value(Colors.PINK.value),
        )

        anomaly_coded_points = line.layer[1].encode(
            color=point_color_condition, tooltip=tooltip
        )
        anomaly_coded_line = alt.layer(line.layer[0], anomaly_coded_points)
        return anomaly_coded_line

    @abstractmethod
    def plot(
        self,
        prescriptive: bool = False,
        theme: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Use contents of "DataAssistantResult" object to display mentrics and other detail for visualization purposes.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            prescriptive: Type of plot to generate, prescriptive if True, descriptive if False
            theme: Altair top-level chart configuration dictionary
        """
        pass
