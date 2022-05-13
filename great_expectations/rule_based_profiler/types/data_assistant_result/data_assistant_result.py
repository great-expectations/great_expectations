import copy
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import altair as alt
import pandas as pd
from IPython.display import HTML, display

from great_expectations.core import ExpectationConfiguration
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
from great_expectations.rule_based_profiler.types.data_assistant_result.plot_components import (
    BatchPlotComponent,
    DomainPlotComponent,
    ExpectationKwargPlotComponent,
    MetricPlotComponent,
    PlotComponent,
    determine_plot_title,
)
from great_expectations.rule_based_profiler.types.data_assistant_result.plot_result import (
    PlotResult,
)
from great_expectations.types import ColorPalettes, Colors, SerializableDictDot


@dataclass
class DataAssistantResult(SerializableDictDot):
    """
    DataAssistantResult is a "dataclass" object, designed to hold results of executing "DataAssistant.run()" method.
    Available properties are: "metrics_by_domain", "expectation_configurations", and configuration object
    ("RuleBasedProfilerConfig") of effective Rule-Based Profiler, which embodies given "DataAssistant".
    Use "batch_id_to_batch_identifier_display_name_map" to translate "batch_id" values to display ("friendly") names.
    """

    ALLOWED_KEYS = {
        "batch_id_to_batch_identifier_display_name_map",
        "profiler_config",
        "metrics_by_domain",
        "expectation_configurations",
        "execution_time",
    }

    batch_id_to_batch_identifier_display_name_map: Optional[
        Dict[str, Set[Tuple[str, Any]]]
    ] = None
    profiler_config: Optional["RuleBasedProfilerConfig"] = None  # noqa: F821
    metrics_by_domain: Optional[Dict[Domain, Dict[str, ParameterNode]]] = None
    expectation_configurations: Optional[List[ExpectationConfiguration]] = None
    citation: Optional[dict] = None
    execution_time: Optional[float] = None  # Execution time (in seconds).

    def to_dict(self) -> dict:
        """
        Returns: This DataAssistantResult as dictionary (JSON-serializable dictionary for DataAssistantResult objects).
        """
        domain: Domain
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        expectation_configuration: ExpectationConfiguration
        return {
            "batch_id_to_batch_identifier_display_name_map": convert_to_json_serializable(
                data=self.batch_id_to_batch_identifier_display_name_map
            ),
            "profiler_config": self.profiler_config.to_json_dict(),
            "metrics_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "parameter_values_for_fully_qualified_parameter_names": convert_to_json_serializable(
                        data=parameter_values_for_fully_qualified_parameter_names
                    ),
                }
                for domain, parameter_values_for_fully_qualified_parameter_names in self.metrics_by_domain.items()
            ],
            "expectation_configurations": [
                expectation_configuration.to_json_dict()
                for expectation_configuration in self.expectation_configurations
            ],
            "execution_time": convert_to_json_serializable(data=self.execution_time),
        }

    def to_json_dict(self) -> dict:
        """
        Returns: This DataAssistantResult as JSON-serializable dictionary.
        """
        return self.to_dict()

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
    def _get_theme(theme: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        default_theme: Dict[str, Any] = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)
        if theme:
            return nested_update(default_theme, theme)
        else:
            return default_theme

    @staticmethod
    def apply_theme(
        charts: List[alt.Chart],
        theme: Optional[Dict[str, Any]],
    ) -> List[alt.Chart]:
        """
        Apply the Great Expectations default theme and any user-provided theme overrides to each chart

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            charts: A list of Altair chart objects to apply a theme to
            theme: An Optional Altair top-level chart configuration dictionary to apply over the base_theme

        Returns:
            A list of Altair charts with the theme applied
        """
        theme: Dict[str, Any] = DataAssistantResult._get_theme(theme=theme)
        return [chart.configure(**theme) for chart in charts]

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
            charts: A list of Altair chart objects to display
            theme: An Optional Altair top-level chart configuration dictionary to apply over the default theme
        """
        altair_theme: Dict[str, Any]
        if theme:
            altair_theme = DataAssistantResult._get_theme(theme=theme)
        else:
            altair_theme = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)

        themed_charts: List[alt.chart] = DataAssistantResult.apply_theme(
            charts=charts, theme=altair_theme
        )

        # Altair does not have a way to format the dropdown input so the rendered CSS must be altered directly
        dropdown_title_color: str = altair_theme["legend"]["titleColor"]
        dropdown_title_font: str = altair_theme["font"]
        dropdown_css: str = f"""
            <style>
            span.vega-bind-name {{
                color: {dropdown_title_color};
                font-family: "{dropdown_title_font}";
                font-weight: bold;
            }}
            form.vega-bindings {{
              position: relative;
              left: 70px;
              top: -350px;
            }}
            </style>
        """
        display(HTML(dropdown_css))

        chart: alt.Chart
        for chart in themed_charts:
            chart.display()

    @staticmethod
    def get_line_chart(
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
        subtitle: Optional[str] = None,
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
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=domain_name, alt_type=domain_type, subtitle=subtitle
        )
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name="batch_id", alt_type=AltairDataTypes.NOMINAL.value
        )
        return DataAssistantResult._get_line_chart(
            df=df,
            metric_component=metric_component,
            domain_component=domain_component,
            batch_component=batch_component,
        )

    @staticmethod
    def _get_line_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        domain_component: DomainPlotComponent,
        batch_component: BatchPlotComponent,
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = [
            batch_component.generate_tooltip(),
            metric_component.generate_tooltip(format=","),
        ]

        line: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_line()
            .encode(
                x=domain_component.plot_on_axis(),
                y=metric_component.plot_on_axis(),
                tooltip=tooltip,
            )
        )

        points: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_point()
            .encode(
                x=domain_component.plot_on_axis(),
                y=metric_component.plot_on_axis(),
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
        subtitle: Optional[str],
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
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=domain_name, alt_type=domain_type, subtitle=subtitle
        )
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name="batch_id", alt_type=AltairDataTypes.NOMINAL.value
        )
        min_value_component: ExpectationKwargPlotComponent = (
            ExpectationKwargPlotComponent(
                name="min_value",
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                metric_plot_component=metric_component,
            )
        )
        max_value_component: ExpectationKwargPlotComponent = (
            ExpectationKwargPlotComponent(
                name="max_value",
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                metric_plot_component=metric_component,
            )
        )

        return DataAssistantResult._get_expect_domain_values_to_be_between_chart(
            df=df,
            metric_component=metric_component,
            domain_component=domain_component,
            batch_component=batch_component,
            min_value_component=min_value_component,
            max_value_component=max_value_component,
        )

    @staticmethod
    def _get_expect_domain_values_to_be_between_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        domain_component: DomainPlotComponent,
        batch_component: BatchPlotComponent,
        min_value_component: PlotComponent,
        max_value_component: PlotComponent,
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = [
            batch_component.generate_tooltip(),
            metric_component.generate_tooltip(format=","),
            min_value_component.generate_tooltip(format=","),
            max_value_component.generate_tooltip(format=","),
        ]

        lower_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=domain_component.plot_on_axis(),
                y=min_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(title=title)
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=domain_component.plot_on_axis(),
                y=max_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(title=title)
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=domain_component.plot_on_axis(),
                y=min_value_component.plot_on_axis(),
                y2=alt.Y2(max_value_component.name, title=metric_component.title),
            )
            .properties(title=title)
        )

        line: alt.Chart = DataAssistantResult._get_line_chart(
            df=df,
            metric_component=metric_component,
            domain_component=domain_component,
            batch_component=batch_component,
        )

        # encode point color based on anomalies
        metric_name: str = metric_component.name
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

        return band + lower_limit + upper_limit + anomaly_coded_line

    @staticmethod
    def get_interactive_detail_multi_line_chart(
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
            A interactive detail altair multi-line chart
        """
        metric_title: str = metric_name.replace("_", " ").title()
        domain_title: str = domain_name.title()
        title: alt.TitleParams = alt.TitleParams(
            f"{metric_title} per {domain_title}",
            dy=-30,
        )

        batch_id: str = "batch_id"
        batch_id_title: str = batch_id.replace("_", " ").title().replace("Id", "ID")
        batch_id_type: alt.StandardType = AltairDataTypes.NOMINAL.value

        column_name: str = "column_name"
        column_name_title: str = "Column Name"
        column_name_type: alt.StandardType = AltairDataTypes.NOMINAL.value

        detail_title_font_size: int = 14
        detail_title_font_weight: str = "bold"

        line_chart_height: int = 150
        detail_line_chart_height: int = 75

        point_size: int = 50

        unselected_color: alt.value = alt.value("lightgray")

        selected_opacity: float = 1.0
        unselected_opacity: float = 0.4

        tooltip: List[alt.Tooltip] = [
            alt.Tooltip(
                field=column_name, type=column_name_type, title=column_name_title
            ),
            alt.Tooltip(field=batch_id, type=batch_id_type, title=batch_id_title),
            alt.Tooltip(
                field=metric_name, type=metric_type, title=metric_title, format=","
            ),
        ]

        df: pd.DataFrame = pd.DataFrame(
            columns=[column_name, domain_name, batch_id, metric_name]
        )
        for column, column_df in column_dfs:
            column_df[column_name] = column
            df = pd.concat([df, column_df], axis=0)

        columns: List[str] = [" "] + pd.unique(df[column_name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[column_name],
        )

        line: alt.Chart = (
            alt.Chart(df)
            .mark_line()
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(metric_name, type=metric_type, title=None),
                color=alt.condition(
                    selection,
                    alt.Color(
                        column_name,
                        type=AltairDataTypes.NOMINAL.value,
                        scale=alt.Scale(range=ColorPalettes.ORDINAL_7.value),
                        legend=None,
                    ),
                    unselected_color,
                ),
                opacity=alt.condition(
                    selection,
                    alt.value(selected_opacity),
                    alt.value(unselected_opacity),
                ),
                tooltip=tooltip,
            )
            .properties(height=line_chart_height, title=title)
        )

        points: alt.Chart = (
            alt.Chart(df)
            .mark_point(size=point_size)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(metric_name, type=metric_type, title=None),
                color=alt.condition(
                    selection,
                    alt.value(Colors.GREEN.value),
                    unselected_color,
                ),
                opacity=alt.condition(
                    selection,
                    alt.value(selected_opacity),
                    alt.value(unselected_opacity),
                ),
                tooltip=tooltip,
            )
            .properties(height=line_chart_height, title=title)
        )

        highlight_line: alt.Chart = (
            alt.Chart(df)
            .mark_line(strokeWidth=2.5)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(metric_name, type=metric_type, title=None),
                color=alt.condition(
                    selection,
                    alt.Color(
                        column_name,
                        type=AltairDataTypes.NOMINAL.value,
                        scale=alt.Scale(range=ColorPalettes.ORDINAL_7.value),
                        legend=None,
                    ),
                    unselected_color,
                ),
                opacity=alt.condition(
                    selection,
                    alt.value(selected_opacity),
                    alt.value(unselected_opacity),
                ),
                tooltip=tooltip,
            )
            .properties(height=line_chart_height, title=title)
            .transform_filter(selection)
        )

        highlight_points: alt.Chart = (
            alt.Chart(df)
            .mark_point(size=40)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(metric_name, type=metric_type, title=None),
                color=alt.condition(
                    selection,
                    alt.value(Colors.GREEN.value),
                    unselected_color,
                ),
                opacity=alt.condition(
                    selection,
                    alt.value(selected_opacity),
                    alt.value(unselected_opacity),
                ),
                tooltip=tooltip,
            )
            .properties(height=line_chart_height, title=title)
            .transform_filter(selection)
        )

        detail_line: alt.Chart = (
            alt.Chart(
                df,
            )
            .mark_line(opacity=selected_opacity)
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(metric_name, type=metric_type, title=None),
                color=alt.Color(
                    column_name,
                    type=AltairDataTypes.NOMINAL.value,
                    scale=alt.Scale(range=ColorPalettes.ORDINAL_7.value),
                ),
                tooltip=tooltip,
            )
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
        )

        detail_points: alt.Chart = (
            alt.Chart(
                df,
            )
            .mark_point(
                size=point_size, color=Colors.GREEN.value, opacity=selected_opacity
            )
            .encode(
                x=alt.X(
                    domain_name,
                    type=domain_type,
                    title=domain_title,
                ),
                y=alt.Y(metric_name, type=metric_type, title=None),
                tooltip=tooltip,
            )
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
        )

        detail_title_column_names: pd.DataFrame = pd.DataFrame(
            {column_name: pd.unique(df[column_name])}
        )
        detail_title_column_titles: str = "column_title"
        detail_title_column_names[
            detail_title_column_titles
        ] = detail_title_column_names[column_name].apply(
            lambda x: f"Column ({x}) Selection Detail"
        )
        detail_title_text: alt.condition = alt.condition(
            selection, detail_title_column_titles, alt.value("")
        )

        detail_title = (
            alt.Chart(detail_title_column_names)
            .mark_text(
                color=Colors.PURPLE.value,
                fontSize=detail_title_font_size,
                fontWeight=detail_title_font_weight,
            )
            .encode(text=detail_title_text)
            .transform_filter(selection)
            .properties(height=10)
        )

        # special title for combined y-axis across two charts
        y_axis_title = alt.TitleParams(
            metric_title,
            color=Colors.PURPLE.value,
            orient="left",
            angle=270,
            fontSize=14,
            dx=70,
            dy=-5,
        )

        return (
            alt.VConcatChart(
                vconcat=[
                    line + points + highlight_line + highlight_points,
                    detail_title,
                    detail_line + detail_points,
                ],
            )
            .properties(title=y_axis_title)
            .add_selection(selection)
        )

    @staticmethod
    def get_interactive_detail_expect_column_values_to_be_between_chart(
        column_dfs: List[Tuple[str, pd.DataFrame]],
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
    ) -> alt.Chart:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            domain_name: The name of the domain as it exists in the pandas dataframe
            domain_type: The altair data type for the domain being plotted

        Returns:
            An interactive detail multi line expect_column_values_to_be_between chart
        """
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        metric_title: str = metric_name.replace("_", " ").title()
        domain_title: str = domain_name.title()

        batch_id: str = "batch_id"
        batch_id_title: str = batch_id.replace("_", " ").title().replace("Id", "ID")
        batch_id_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        min_value: str = "min_value"
        min_value_title: str = min_value.replace("_", " ").title()
        min_value_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        max_value: str = "max_value"
        max_value_title: str = max_value.replace("_", " ").title()
        max_value_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        strict_min: str = "strict_min"
        strict_min_title: str = strict_min.replace("_", " ").title()
        strict_min_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        strict_max: str = "strict_max"
        strict_max_title: str = strict_max.replace("_", " ").title()
        strict_max_type: alt.StandardType = AltairDataTypes.NOMINAL.value

        tooltip: List[alt.Tooltip] = [
            alt.Tooltip(field=batch_id, type=batch_id_type, title=batch_id_title),
            alt.Tooltip(
                field=metric_name, type=metric_type, title=metric_title, format=","
            ),
            alt.Tooltip(
                field=min_value, type=min_value_type, title=min_value_title, format=","
            ),
            alt.Tooltip(
                field=max_value, type=max_value_type, title=max_value_title, format=","
            ),
            alt.Tooltip(field=strict_min, type=strict_min_type, title=strict_min_title),
            alt.Tooltip(field=strict_max, type=strict_max_type, title=strict_max_title),
        ]

        column_name: str = "column_name"
        batch: str = "batch"

        df: pd.DataFrame = pd.DataFrame(
            columns=[
                column_name,
                batch,
                batch_id,
                metric_name,
                min_value,
                max_value,
                strict_min,
                strict_max,
            ]
        )
        for column, column_df in column_dfs:
            column_df[column_name] = column
            df = pd.concat([df, column_df], axis=0, sort=False)

        df = df.drop(columns=["column"])

        detail_line_chart_height: int = 75

        interactive_detail_multi_line_chart: alt.VConcatChart = (
            DataAssistantResult.get_interactive_detail_multi_line_chart(
                column_dfs=column_dfs,
                metric_name=metric_name,
                metric_type=metric_type,
                domain_name=domain_name,
                domain_type=domain_type,
            )
        )

        # use existing selection
        selection_name: str = list(
            interactive_detail_multi_line_chart.vconcat[2].layer[0].selection.keys()
        )[0]
        selection_def: alt.SelectionDef = (
            interactive_detail_multi_line_chart.vconcat[2]
            .layer[0]
            .selection[selection_name]
        )
        selection: alt.selection = alt.selection(
            name=selection_name, **selection_def.to_dict()
        )

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
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
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
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
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
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
        )

        # encode point color based on anomalies
        predicate: alt.expr.core.BinaryExpression
        if strict_min and strict_max:
            predicate = (
                (alt.datum.min_value > alt.datum[metric_name])
                & (alt.datum.max_value > alt.datum[metric_name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_name])
                & (alt.datum.max_value < alt.datum[metric_name])
            )
        elif strict_min:
            predicate = (
                (alt.datum.min_value > alt.datum[metric_name])
                & (alt.datum.max_value >= alt.datum[metric_name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_name])
                & (alt.datum.max_value <= alt.datum[metric_name])
            )
        elif strict_max:
            predicate = (
                (alt.datum.min_value >= alt.datum[metric_name])
                & (alt.datum.max_value > alt.datum[metric_name])
            ) | (
                (alt.datum.min_value <= alt.datum[metric_name])
                & (alt.datum.max_value < alt.datum[metric_name])
            )
        else:
            predicate: alt.expr.core.BinaryExpression = (
                (alt.datum.min_value >= alt.datum[metric_name])
                & (alt.datum.max_value >= alt.datum[metric_name])
            ) | (
                (alt.datum.min_value <= alt.datum[metric_name])
                & (alt.datum.max_value <= alt.datum[metric_name])
            )
        point_color_condition: alt.condition = alt.condition(
            predicate=predicate,
            if_false=alt.value(Colors.GREEN.value),
            if_true=alt.value(Colors.PINK.value),
        )

        interactive_detail_multi_line_chart.vconcat[0].layer[3] = (
            interactive_detail_multi_line_chart.vconcat[0]
            .layer[3]
            .encode(color=point_color_condition, tooltip=tooltip)
        )

        interactive_detail_multi_line_chart.vconcat[2].layer[1] = (
            interactive_detail_multi_line_chart.vconcat[2]
            .layer[1]
            .encode(color=point_color_condition, tooltip=tooltip)
        )

        # add expectation kwargs
        detail_chart: alt.LayerChart = interactive_detail_multi_line_chart.vconcat[2]
        detail_chart_layers: list[alt.Chart] = [
            band,
            lower_limit,
            upper_limit,
            detail_chart.layer[0].encode(tooltip=tooltip),
            detail_chart.layer[1].encode(tooltip=tooltip),
        ]
        interactive_detail_multi_line_chart.vconcat[2].layer = detail_chart_layers

        return interactive_detail_multi_line_chart

    @abstractmethod
    def plot_metrics(
        self,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        pass

    @abstractmethod
    def plot_expectations_and_metrics(
        self,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        pass
