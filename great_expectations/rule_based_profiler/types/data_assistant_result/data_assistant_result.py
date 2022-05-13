import copy
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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
    def _get_theme(theme: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        default_theme: Dict[str, Any] = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)
        if theme:
            return nested_update(default_theme, theme)
        else:
            return default_theme

    @staticmethod
    def get_line_chart(
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart
        """
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        batch_name: str = "batch"
        batch_type: alt.StandardType = AltairDataTypes.ORDINAL.value
        batch_identifiers: List[str] = [
            column for column in df.columns if column not in [metric_name, batch_name]
        ]
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=None,
            alt_type=None,
            subtitle=subtitle,
        )

        return DataAssistantResult._get_line_chart(
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
        )

    @staticmethod
    def _get_line_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = batch_component.generate_tooltip() + [
            metric_component.generate_tooltip(format=","),
        ]

        line: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_line()
            .encode(
                x=batch_component.plot_on_axis(),
                y=metric_component.plot_on_axis(),
                tooltip=tooltip,
            )
        )

        points: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_point()
            .encode(
                x=batch_component.plot_on_axis(),
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
        subtitle: Optional[str],
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart with confidence intervals corresponding to "between" expectations
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column for column in df.columns if column not in [metric_name, batch_name]
        ]
        batch_type: alt.StandardType = AltairDataTypes.ORDINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=None,
            alt_type=None,
            subtitle=subtitle,
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
            batch_component=batch_component,
            domain_component=domain_component,
            min_value_component=min_value_component,
            max_value_component=max_value_component,
        )

    @staticmethod
    def _get_expect_domain_values_to_be_between_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        min_value_component: PlotComponent,
        max_value_component: PlotComponent,
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = batch_component.generate_tooltip() + [
            metric_component.generate_tooltip(format=","),
            min_value_component.generate_tooltip(format=","),
            max_value_component.generate_tooltip(format=","),
        ]

        lower_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=batch_component.plot_on_axis(),
                y=min_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(title=title)
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=batch_component.plot_on_axis(),
                y=max_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(title=title)
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=batch_component.plot_on_axis(),
                y=min_value_component.plot_on_axis(),
                y2=alt.Y2(max_value_component.name, title=metric_component.title),
            )
            .properties(title=title)
        )

        line: alt.Chart = DataAssistantResult._get_line_chart(
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
        )

        # encode point color based on anomalies
        metric_name: str = metric_component.name
        predicate: Union[bool, int] = (
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
    ) -> alt.VConcatChart:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted

        Returns:
            A interactive detail altair multi-line chart
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in column_dfs[0][1].columns
            if column not in [metric_name, batch_name]
        ]
        batch_type: alt.StandardType = AltairDataTypes.ORDINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        domain_name: str = "column"
        domain_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=domain_name,
            alt_type=domain_type,
        )

        df: pd.DataFrame = pd.DataFrame(
            columns=[batch_name, metric_name] + batch_identifiers
        )
        for column, column_df in column_dfs:
            column_df[domain_name] = column
            df = pd.concat([df, column_df], axis=0)

        return DataAssistantResult._get_interactive_detail_multi_line_chart(
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
        )

    @staticmethod
    def _get_interactive_detail_multi_line_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
    ) -> alt.VConcatChart:
        detail_title_font_size: int = 14
        detail_title_font_weight: str = "bold"

        line_chart_height: int = 150
        detail_line_chart_height: int = 75

        point_size: int = 50

        unselected_color: alt.value = alt.value("lightgray")

        selected_opacity: float = 1.0
        unselected_opacity: float = 0.4

        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = batch_component.generate_tooltip() + [
            domain_component.generate_tooltip(),
            metric_component.generate_tooltip(format=","),
        ]

        columns: List[str] = [" "] + pd.unique(df[domain_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_component.name],
        )

        line: alt.Chart = (
            alt.Chart(df)
            .mark_line()
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(
                    metric_component.name, type=metric_component.alt_type, title=None
                ),
                color=alt.condition(
                    selection,
                    alt.Color(
                        domain_component.name,
                        type=domain_component.alt_type,
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
                    batch_component.name,
                    type=batch_component.alt_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(
                    metric_component.name, type=metric_component.alt_type, title=None
                ),
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
                    batch_component.name,
                    type=batch_component.alt_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(
                    metric_component.name, type=metric_component.alt_type, title=None
                ),
                color=alt.condition(
                    selection,
                    alt.Color(
                        domain_component.name,
                        type=domain_component.alt_type,
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
                    batch_component.name,
                    type=batch_component.alt_type,
                    axis=alt.Axis(ticks=False, title=None, labels=False),
                ),
                y=alt.Y(
                    metric_component.name, type=metric_component.alt_type, title=None
                ),
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
                x=batch_component.plot_on_axis(),
                y=alt.Y(
                    metric_component.name, type=metric_component.alt_type, title=None
                ),
                color=alt.Color(
                    domain_component.name,
                    type=domain_component.alt_type,
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
                x=batch_component.plot_on_axis(),
                y=alt.Y(
                    metric_component.name, type=metric_component.alt_type, title=None
                ),
                tooltip=tooltip,
            )
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
        )

        detail_title_column_names: pd.DataFrame = pd.DataFrame(
            {domain_component.name: pd.unique(df[domain_component.name])}
        )
        detail_title_column_titles: str = "column_title"
        detail_title_column_names[
            detail_title_column_titles
        ] = detail_title_column_names[domain_component.name].apply(
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
            metric_component.title,
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
    ) -> alt.VConcatChart:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted

        Returns:
            An interactive detail multi line expect_column_values_to_be_between chart
        """
        min_value: str = "min_value"
        max_value: str = "max_value"
        strict_min: str = "strict_min"
        strict_max: str = "strict_max"

        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in column_dfs[0][1].columns
            if column
            not in [
                metric_name,
                batch_name,
                min_value,
                max_value,
                strict_min,
                strict_max,
            ]
        ]
        batch_type: alt.StandardType = AltairDataTypes.ORDINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        column_name: str = "column"
        column_name_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=column_name,
            alt_type=column_name_type,
        )

        min_value_component: ExpectationKwargPlotComponent = (
            ExpectationKwargPlotComponent(
                name=min_value,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                metric_plot_component=metric_component,
            )
        )
        max_value_component: ExpectationKwargPlotComponent = (
            ExpectationKwargPlotComponent(
                name=max_value,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                metric_plot_component=metric_component,
            )
        )
        strict_min_component: ExpectationKwargPlotComponent = (
            ExpectationKwargPlotComponent(
                name=strict_min,
                alt_type=AltairDataTypes.NOMINAL.value,
                metric_plot_component=metric_component,
            )
        )
        strict_max_component: ExpectationKwargPlotComponent = (
            ExpectationKwargPlotComponent(
                name=strict_max,
                alt_type=AltairDataTypes.NOMINAL.value,
                metric_plot_component=metric_component,
            )
        )

        df: pd.DataFrame = pd.DataFrame(
            columns=[
                batch_name,
            ]
            + batch_identifiers
            + [
                metric_name,
                min_value,
                max_value,
                strict_min,
                strict_max,
            ]
        )

        for _, column_df in column_dfs:
            df = pd.concat([df, column_df], axis=0)

        # encode point color based on anomalies
        predicate: Union[bool, int]
        if strict_min and strict_max:
            predicate = (
                (alt.datum.min_value > alt.datum[metric_component.name])
                & (alt.datum.max_value > alt.datum[metric_component.name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_component.name])
                & (alt.datum.max_value < alt.datum[metric_component.name])
            )
        elif strict_min:
            predicate = (
                (alt.datum.min_value > alt.datum[metric_component.name])
                & (alt.datum.max_value >= alt.datum[metric_component.name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_component.name])
                & (alt.datum.max_value <= alt.datum[metric_component.name])
            )
        elif strict_max:
            predicate = (
                (alt.datum.min_value >= alt.datum[metric_component.name])
                & (alt.datum.max_value > alt.datum[metric_component.name])
            ) | (
                (alt.datum.min_value <= alt.datum[metric_component.name])
                & (alt.datum.max_value < alt.datum[metric_component.name])
            )
        else:
            predicate = (
                (alt.datum.min_value >= alt.datum[metric_component.name])
                & (alt.datum.max_value >= alt.datum[metric_component.name])
            ) | (
                (alt.datum.min_value <= alt.datum[metric_component.name])
                & (alt.datum.max_value <= alt.datum[metric_component.name])
            )

        return DataAssistantResult._get_interactive_detail_expect_column_values_to_be_between_chart(
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
            min_value_component=min_value_component,
            max_value_component=max_value_component,
            strict_min_component=strict_min_component,
            strict_max_component=strict_max_component,
            predicate=predicate,
        )

    @staticmethod
    def _get_interactive_detail_expect_column_values_to_be_between_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        min_value_component: PlotComponent,
        max_value_component: PlotComponent,
        strict_min_component: PlotComponent,
        strict_max_component: PlotComponent,
        predicate: Union[bool, int],
    ) -> alt.VConcatChart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        tooltip: List[alt.Tooltip] = batch_component.generate_tooltip() + [
            metric_component.generate_tooltip(format=","),
            min_value_component.generate_tooltip(format=","),
            max_value_component.generate_tooltip(format=","),
            strict_min_component.generate_tooltip(),
            strict_max_component.generate_tooltip(),
        ]
        detail_line_chart_height: int = 75

        interactive_detail_multi_line_chart: alt.VConcatChart = (
            DataAssistantResult._get_interactive_detail_multi_line_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
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
                x=batch_component.plot_on_axis(),
                y=min_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=batch_component.plot_on_axis(),
                y=max_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=batch_component.plot_on_axis(),
                y=min_value_component.plot_on_axis(),
                y2=alt.Y2(max_value_component.name, title=max_value_component.title),
            )
            .properties(height=detail_line_chart_height)
            .transform_filter(selection)
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
    def plot(
        self,
        prescriptive: bool = False,
        theme: Optional[Dict[str, Any]] = None,
    ) -> PlotResult:
        """
        Use contents of "DataAssistantResult" object to display mentrics and other detail for visualization purposes.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            prescriptive: Type of plot to generate, prescriptive if True, descriptive if False
            theme: Altair top-level chart configuration dictionary

        Returns:
            PlotResult wrapper object around Altair charts.
        """
        pass
