import copy
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from typing import Any, Callable, Dict, KeysView, List, Optional, Set, Tuple, Union

import altair as alt
import pandas as pd
from IPython.display import HTML, display

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.util import convert_to_json_serializable, nested_update
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_or_create_expectation_suite,
    sanitize_parameter_name,
)
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    Domain,
    MetricValues,
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
    PlotMode,
    PlotResult,
)
from great_expectations.types import ColorPalettes, Colors, SerializableDictDot

ColumnDataFrame = namedtuple("ColumnDataFrame", ["column", "df"])


@dataclass
class DataAssistantResult(SerializableDictDot):
    """
    DataAssistantResult is a "dataclass" object, designed to hold results of executing "DataAssistant.run()" method.
    Available properties are: "metrics_by_domain", "expectation_configurations", and configuration object
    ("RuleBasedProfilerConfig") of effective Rule-Based Profiler, which embodies given "DataAssistant".
    Use "batch_id_to_batch_identifier_display_name_map" to translate "batch_id" values to display ("friendly") names.
    """

    # A mapping is defined for which metrics to plot and their associated expectations
    EXPECTATION_METRIC_MAP = {
        "expect_table_row_count_to_be_between": "table.row_count",
        "expect_column_unique_value_count_to_be_between": "column.distinct_values.count",
    }

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

    def get_expectation_suite(self, expectation_suite_name: str) -> ExpectationSuite:
        """
        Returns: "ExpectationSuite" object, built from properties, populated into this "DataAssistantResult" object.
        """
        expectation_suite: ExpectationSuite = get_or_create_expectation_suite(
            data_context=None,
            expectation_suite=None,
            expectation_suite_name=expectation_suite_name,
            component_name=self.__class__.__name__,
            persist=False,
        )
        expectation_suite.add_expectation_configurations(
            expectation_configurations=self.expectation_configurations,
            send_usage_event=False,
            match_type="domain",
            overwrite_existing=True,
        )
        expectation_suite.add_citation(
            **self.citation,
        )
        return expectation_suite

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

    def plot_metrics(
        self,
        sequential: bool = True,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """
        Use contents of "DataAssistantResult" object to display metrics for visualization purposes.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            sequential: Whether the batches are sequential or not
            theme: Altair top-level chart configuration dictionary
            include_column_names: Columns to include in metrics plot
            exclude_column_names: Columns to exclude from metrics plot

        Returns:
            PlotResult wrapper object around Altair charts.
        """
        return self._plot(
            plot_mode=PlotMode.DESCRIPTIVE,
            sequential=sequential,
            theme=theme,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
        )

    def plot_expectations_and_metrics(
        self,
        sequential: bool = True,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """
        Use contents of "DataAssistantResult" object to display metrics and expectations for visualization purposes.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            sequential: Whether the batches are sequential or not
            theme: Altair top-level chart configuration dictionary
            include_column_names: Columns to include in expectations and metrics plot
            exclude_column_names: Columns to exclude from expectations and metrics plot

        Returns:
            PlotResult wrapper object around Altair charts.
        """
        return self._plot(
            plot_mode=PlotMode.PRESCRIPTIVE,
            sequential=sequential,
            theme=theme,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
        )

    def _plot(
        self,
        plot_mode: PlotMode,
        sequential: bool,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "display()" for presentation.
        Display Charts are condensed and interactive while Return Charts are separated into an individual chart for
        each metric-domain/expectation-domain combination.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            plot_mode: Type of plot to generate, prescriptive or descriptive
            sequential: Whether batches are sequential in nature
            theme: Altair top-level chart configuration dictionary
            include_column_names: A list of columns to chart
            exclude_column_names: A list of columns not to chart

        Returns:
            A PlotResult object consisting of an individual chart for each metric-domain/expectation-domain
        """
        if include_column_names is not None and exclude_column_names is not None:
            raise ValueError(
                "You may either use `include_column_names` or `exclude_column_names` (but not both)."
            )

        display_charts: Union[
            List[alt.Chart], List[alt.LayerChart], List[alt.VConcatChart]
        ] = []
        return_charts: Union[List[alt.Chart], List[alt.LayerChart]] = []

        expectation_configurations: List[
            ExpectationConfiguration
        ] = self.expectation_configurations

        table_domain_charts: List[
            Union[List[alt.Chart], List[alt.LayerChart]]
        ] = self._plot_table_domain_charts(
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
            sequential=sequential,
        )
        display_charts.extend(table_domain_charts)
        return_charts.extend(table_domain_charts)

        column_domain_display_chart: List[alt.VConcatChart]
        column_domain_return_charts: List[alt.Chart]
        (
            column_domain_display_charts,
            column_domain_return_charts,
        ) = self._plot_column_domain_charts(
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
            sequential=sequential,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
        )
        display_charts.extend(column_domain_display_charts)
        return_charts.extend(column_domain_return_charts)

        self.display(charts=display_charts, theme=theme)

        return_charts = self.apply_theme(charts=return_charts, theme=theme)
        return PlotResult(charts=return_charts)

    @staticmethod
    def display(
        charts: Union[List[alt.Chart], List[alt.VConcatChart]],
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
              position: absolute;
              left: 75px;
              top: 30px;
            }}
            </style>
        """
        display(HTML(dropdown_css))

        # max rows for Altair charts is set to 5,000 without this
        alt.data_transformers.disable_max_rows()

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
        theme = DataAssistantResult._get_theme(theme=theme)
        return [chart.configure(**theme) for chart in charts]

    @staticmethod
    def get_quantitative_metric_chart(
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart
        """
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column for column in df.columns if column not in [metric_name, batch_name]
        ]
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=AltairDataTypes.NOMINAL.value,
            batch_identifiers=batch_identifiers,
        )

        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=None,
            alt_type=None,
            subtitle=subtitle,
        )

        if sequential:
            return DataAssistantResult._get_line_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
            )
        else:
            return DataAssistantResult._get_bar_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
            )

    @staticmethod
    def get_expect_domain_values_to_be_between_chart(
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart with confidence intervals corresponding to "between" expectations
        """

        if subtitle:
            domain_type: MetricDomainTypes = MetricDomainTypes.COLUMN
        else:
            domain_type: MetricDomainTypes = MetricDomainTypes.TABLE

        column_name: str = "column"
        batch_name: str = "batch"
        max_value: str = "max_value"
        min_value: str = "min_value"
        strict_max: str = "strict_max"
        strict_min: str = "strict_min"
        batch_identifiers: List[str] = [
            column
            for column in df.columns
            if column
            not in {
                metric_name,
                batch_name,
                column_name,
                max_value,
                min_value,
                strict_min,
                strict_max,
            }
        ]
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=AltairDataTypes.NOMINAL.value,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
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

        domain_component: DomainPlotComponent
        tooltip: List[alt.Tooltip]
        if domain_type == MetricDomainTypes.COLUMN:
            column_type: alt.StandardType = AltairDataTypes.NOMINAL.value
            domain_component = DomainPlotComponent(
                name=column_name,
                alt_type=column_type,
                subtitle=subtitle,
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
            tooltip = (
                [domain_component.generate_tooltip()]
                + batch_component.generate_tooltip()
                + [
                    min_value_component.generate_tooltip(format=","),
                    max_value_component.generate_tooltip(format=","),
                    strict_min_component.generate_tooltip(),
                    strict_max_component.generate_tooltip(),
                    metric_component.generate_tooltip(format=","),
                ]
            )
        else:
            domain_component = DomainPlotComponent(
                name=None,
                alt_type=None,
                subtitle=subtitle,
            )
            tooltip = batch_component.generate_tooltip() + [
                min_value_component.generate_tooltip(format=","),
                max_value_component.generate_tooltip(format=","),
                metric_component.generate_tooltip(format=","),
            ]

        if sequential:
            return (
                DataAssistantResult._get_expect_domain_values_to_be_between_line_chart(
                    df=df,
                    metric_component=metric_component,
                    batch_component=batch_component,
                    domain_component=domain_component,
                    min_value_component=min_value_component,
                    max_value_component=max_value_component,
                    tooltip=tooltip,
                )
            )
        else:
            return (
                DataAssistantResult._get_expect_domain_values_to_be_between_bar_chart(
                    df=df,
                    metric_component=metric_component,
                    batch_component=batch_component,
                    domain_component=domain_component,
                    min_value_component=min_value_component,
                    max_value_component=max_value_component,
                    tooltip=tooltip,
                )
            )

    @staticmethod
    def get_interactive_detail_multi_chart(
        column_dfs: List[ColumnDataFrame],
        metric_name: str,
        metric_type: alt.StandardType,
        sequential: bool,
    ) -> Union[alt.Chart, alt.VConcatChart]:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            sequential: Whether batches are sequential in nature

        Returns:
            A interactive detail altair multi-chart
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in column_dfs[0].df.columns
            if column not in [metric_name, batch_name]
        ]
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=AltairDataTypes.NOMINAL.value,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        domain_name: str = "column"
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=domain_name,
            alt_type=AltairDataTypes.NOMINAL.value,
        )

        df: pd.DataFrame = pd.DataFrame(
            columns=[batch_name, metric_name] + batch_identifiers
        )
        for column, column_df in column_dfs:
            column_df[domain_name] = column
            df = pd.concat([df, column_df], axis=0)

        if sequential:
            return DataAssistantResult._get_interactive_detail_multi_line_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
            )
        else:
            return DataAssistantResult._get_interactive_detail_multi_bar_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
            )

    @staticmethod
    def get_interactive_detail_expect_column_values_to_be_between_chart(
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        metric_name: str,
        metric_type: alt.StandardType,
        sequential: bool,
    ) -> alt.VConcatChart:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            metric_type: The altair data type for the metric being plotted
            sequential: Whether batches are sequential in nature

        Returns:
            An interactive detail multi line expect_column_values_to_be_between chart
        """
        column_name: str = "column"
        min_value: str = "min_value"
        max_value: str = "max_value"
        strict_min: str = "strict_min"
        strict_max: str = "strict_max"

        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in column_dfs[0].df.columns
            if column
            not in {
                metric_name,
                batch_name,
                column_name,
                min_value,
                max_value,
                strict_min,
                strict_max,
            }
        ]
        batch_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        domain_component: DomainPlotComponent = DomainPlotComponent(
            name="column",
            alt_type=AltairDataTypes.NOMINAL.value,
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
                column_name,
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

        if sequential:
            return DataAssistantResult._get_interactive_detail_expect_column_values_to_be_between_line_chart(
                expectation_type=expectation_type,
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
        else:
            return DataAssistantResult._get_interactive_detail_expect_column_values_to_be_between_bar_chart(
                expectation_type=expectation_type,
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
    def _get_bar_chart(
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

        bars: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_bar()
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=metric_component.plot_on_axis(),
                tooltip=tooltip,
            )
        )

        return bars

    @staticmethod
    def _get_expect_domain_values_to_be_between_line_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        min_value_component: PlotComponent,
        max_value_component: PlotComponent,
        tooltip: List[alt.Tooltip],
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

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
            color=point_color_condition,
            tooltip=tooltip,
        )
        anomaly_coded_line = alt.layer(
            line.layer[0].encode(tooltip=tooltip), anomaly_coded_points
        )

        return band + lower_limit + upper_limit + anomaly_coded_line

    @staticmethod
    def _get_expect_domain_values_to_be_between_bar_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        min_value_component: PlotComponent,
        max_value_component: PlotComponent,
        tooltip: List[alt.Tooltip],
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        lower_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False),
                ),
                y=min_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(title=title)
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False),
                ),
                y=max_value_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .properties(title=title)
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False),
                ),
                y=min_value_component.plot_on_axis(),
                y2=alt.Y2(max_value_component.name, title=metric_component.title),
            )
            .properties(title=title)
        )

        bars: alt.Chart = DataAssistantResult._get_bar_chart(
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
        bar_color_condition: alt.condition = alt.condition(
            predicate=predicate,
            if_false=alt.value(Colors.GREEN.value),
            if_true=alt.value(Colors.PINK.value),
        )

        anomaly_coded_bars = bars.encode(color=bar_color_condition, tooltip=tooltip)

        return band + lower_limit + upper_limit + anomaly_coded_bars

    @staticmethod
    def _get_interactive_detail_multi_line_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        expectation_type: Optional[str] = None,
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
            expectation_type=expectation_type,
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = (
            [domain_component.generate_tooltip()]
            + batch_component.generate_tooltip()
            + [
                metric_component.generate_tooltip(format=","),
            ]
        )

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
                    scale=alt.Scale(align=0.05),
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
                    scale=alt.Scale(align=0.05),
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
                    scale=alt.Scale(align=0.05),
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
            .properties(height=35)
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
    def _get_interactive_detail_multi_bar_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        expectation_type: Optional[str] = None,
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = (
            [domain_component.generate_tooltip()]
            + batch_component.generate_tooltip()
            + [
                metric_component.generate_tooltip(format=","),
            ]
        )

        input_dropdown_initial_state: pd.DataFrame = (
            df.groupby([batch_component.name]).max().reset_index()
        )
        input_dropdown_initial_state[
            batch_component.batch_identifiers + [domain_component.name]
        ] = " "
        df = pd.concat([input_dropdown_initial_state, df], axis=0)

        columns: List[str] = pd.unique(df[domain_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_component.name],
        )

        bars: alt.Chart = (
            alt.Chart(data=df, title=title)
            .mark_bar()
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=metric_component.plot_on_axis(),
                tooltip=tooltip,
            )
            .add_selection(selection)
            .transform_filter(selection)
        )

        return bars

    @staticmethod
    def _get_interactive_detail_expect_column_values_to_be_between_line_chart(
        expectation_type: str,
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

        tooltip: List[alt.Tooltip] = (
            [domain_component.generate_tooltip()]
            + batch_component.generate_tooltip()
            + [
                min_value_component.generate_tooltip(format=","),
                max_value_component.generate_tooltip(format=","),
                strict_min_component.generate_tooltip(),
                strict_max_component.generate_tooltip(),
                metric_component.generate_tooltip(format=","),
            ]
        )
        detail_line_chart_height: int = 75

        interactive_detail_multi_line_chart: alt.VConcatChart = (
            DataAssistantResult._get_interactive_detail_multi_line_chart(
                expectation_type=expectation_type,
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

    @staticmethod
    def _get_interactive_detail_expect_column_values_to_be_between_bar_chart(
        expectation_type: str,
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

        tooltip: List[alt.Tooltip] = (
            [domain_component.generate_tooltip()]
            + batch_component.generate_tooltip()
            + [
                min_value_component.generate_tooltip(format=","),
                max_value_component.generate_tooltip(format=","),
                strict_min_component.generate_tooltip(),
                strict_max_component.generate_tooltip(),
                metric_component.generate_tooltip(format=","),
            ]
        )

        bars: alt.Chart = DataAssistantResult._get_interactive_detail_multi_bar_chart(
            expectation_type=expectation_type,
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
        )

        bars.selection = alt.Undefined
        bars.transform = alt.Undefined

        input_dropdown_initial_state: pd.DataFrame = (
            df.groupby([batch_component.name]).max().reset_index()
        )
        input_dropdown_initial_state[
            batch_component.batch_identifiers
            + [
                domain_component.name,
                min_value_component.name,
                max_value_component.name,
                strict_min_component.name,
                strict_max_component.name,
            ]
        ] = " "
        df = pd.concat([input_dropdown_initial_state, df], axis=0)

        columns: List[str] = pd.unique(df[domain_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_component.name],
        )

        lower_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=alt.Y(
                    min_value_component.name,
                    type=min_value_component.alt_type,
                ),
                tooltip=tooltip,
            )
            .transform_filter(selection)
        )

        upper_limit: alt.Chart = (
            alt.Chart(data=df)
            .mark_line(color=line_color)
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=alt.Y(
                    max_value_component.name,
                    type=max_value_component.alt_type,
                ),
                tooltip=tooltip,
            )
            .transform_filter(selection)
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_component.name,
                    type=batch_component.alt_type,
                    title=batch_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=alt.Y(
                    min_value_component.name,
                    type=min_value_component.alt_type,
                ),
                y2=alt.Y2(max_value_component.name),
            )
            .transform_filter(selection)
        )

        bar_color_condition: alt.condition = alt.condition(
            predicate=predicate,
            if_false=alt.value(Colors.GREEN.value),
            if_true=alt.value(Colors.PINK.value),
        )

        anomaly_coded_bars = (
            bars.encode(color=bar_color_condition, tooltip=tooltip)
            .add_selection(selection)
            .transform_filter(selection)
        )

        return band + lower_limit + upper_limit + anomaly_coded_bars

    @staticmethod
    def _get_theme(theme: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        default_theme: Dict[str, Any] = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)
        if theme:
            return nested_update(default_theme, theme)
        else:
            return default_theme

    def _plot_table_domain_charts(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Union[List[alt.Chart], List[alt.LayerChart]]]:
        expectation_metric_map: Dict[str, str] = self.EXPECTATION_METRIC_MAP

        table_based_expectations: List[str] = [
            expectation
            for expectation in expectation_metric_map.keys()
            if expectation.startswith("expect_table_")
        ]
        table_based_expectation_configurations: List[ExpectationConfiguration] = list(
            filter(
                lambda e: e.expectation_type in table_based_expectations,
                expectation_configurations,
            )
        )

        attributed_metrics_by_table_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.TABLE)

        charts: List[alt.Chart] = []

        expectation_configuration: ExpectationConfiguration
        for expectation_configuration in table_based_expectation_configurations:
            table_domain_chart: alt.Chart = (
                self._create_chart_for_table_domain_expectation(
                    expectation_configuration=expectation_configuration,
                    attributed_metrics=attributed_metrics_by_table_domain,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
            )
            charts.append(table_domain_chart)

        return [chart for chart in charts if chart is not None]

    def _plot_column_domain_charts(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> Tuple[List[alt.VConcatChart], List[alt.Chart]]:
        column_based_expectation_configurations_by_type: Dict[
            str, List[ExpectationConfiguration]
        ] = self._filter_expectation_configurations_by_column_type(
            expectation_configurations, include_column_names, exclude_column_names
        )

        attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.COLUMN)

        display_charts: List[Optional[alt.VConcatChart]] = []
        return_charts: List[Optional[alt.Chart]] = []

        for (
            expectation_type,
            column_based_expectation_configurations,
        ) in column_based_expectation_configurations_by_type.items():
            display_charts_for_expectation: List[
                Optional[alt.VConcatChart]
            ] = self._create_display_chart_for_column_domain_expectation(
                expectation_type=expectation_type,
                expectation_configurations=column_based_expectation_configurations,
                attributed_metrics=attributed_metrics_by_column_domain,
                plot_mode=plot_mode,
                sequential=sequential,
            )
            display_charts.extend(display_charts_for_expectation)

            for expectation_configuration in column_based_expectation_configurations:
                return_chart: alt.Chart = (
                    self._create_return_chart_for_column_domain_expectation(
                        expectation_configuration=expectation_configuration,
                        attributed_metrics=attributed_metrics_by_column_domain,
                        plot_mode=plot_mode,
                        sequential=sequential,
                    )
                )
                return_charts.append(return_chart)

        display_charts = list(filter(None, display_charts))
        return_charts = list(filter(None, return_charts))

        return display_charts, return_charts

    def _filter_expectation_configurations_by_column_type(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
    ) -> Dict[str, List[ExpectationConfiguration]]:
        expectation_metric_map: Dict[str, str] = self.EXPECTATION_METRIC_MAP

        column_based_expectations: Set[str] = {
            expectation
            for expectation in expectation_metric_map.keys()
            if expectation.startswith("expect_column_")
        }

        def _filter(
            e: ExpectationConfiguration, column_based_expectations: Set[str]
        ) -> bool:
            if e.expectation_type not in column_based_expectations:
                return False
            column_name: str = e.kwargs["column"]
            if exclude_column_names and column_name in exclude_column_names:
                return False
            if include_column_names and column_name not in include_column_names:
                return False
            return True

        column_based_expectation_configurations: List[ExpectationConfiguration] = list(
            filter(
                lambda e: _filter(e, column_based_expectations),
                expectation_configurations,
            )
        )

        column_based_expectation_configurations_by_type: Dict[
            str, List[ExpectationConfiguration]
        ] = defaultdict(list)
        for expectation_configuration in column_based_expectation_configurations:
            type_: str = expectation_configuration.expectation_type
            column_based_expectation_configurations_by_type[type_].append(
                expectation_configuration
            )

        return column_based_expectation_configurations_by_type

    def _chart_domain_values(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        plot_mode: PlotMode,
        sequential: bool,
        subtitle: Optional[str],
    ) -> Optional[alt.Chart]:
        implemented_metrics: Set[str] = {
            sanitize_parameter_name(metric)
            for metric in self.EXPECTATION_METRIC_MAP.values()
        }

        plot_impl: Optional[
            Callable[
                [
                    pd.DataFrame,
                    str,
                    alt.StandardType,
                    bool,
                    Optional[str],
                ],
                alt.Chart,
            ]
        ] = None
        chart: Optional[alt.Chart] = None
        if metric_name in implemented_metrics:
            if plot_mode is PlotMode.PRESCRIPTIVE:
                plot_impl = self.get_expect_domain_values_to_be_between_chart
            elif plot_mode is PlotMode.DESCRIPTIVE:
                plot_impl = self.get_quantitative_metric_chart

        if plot_impl:
            chart = plot_impl(
                df=df,
                metric_name=metric_name,
                metric_type=metric_type,
                sequential=sequential,
                subtitle=subtitle,
            )
        return chart

    def _create_display_chart_for_column_domain_expectation(
        self,
        expectation_type: str,
        expectation_configurations: List[ExpectationConfiguration],
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Optional[alt.VConcatChart]]:
        column_dfs: List[ColumnDataFrame] = self._create_column_dfs_for_charting(
            attributed_metrics=attributed_metrics,
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
        )

        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        metric_name: Optional[str] = self.EXPECTATION_METRIC_MAP.get(expectation_type)
        if metric_name:
            metric_name = sanitize_parameter_name(metric_name)

        return self._chart_column_values(
            expectation_type=expectation_type,
            column_dfs=column_dfs,
            metric_name=metric_name,
            metric_type=metric_type,
            plot_mode=plot_mode,
            sequential=sequential,
        )

    def _create_return_chart_for_column_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> alt.Chart:
        expectation_metric_map: Dict[str, str] = self.EXPECTATION_METRIC_MAP

        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value

        domain: Domain
        domains_by_column_name: Dict[str, Domain] = {
            domain.domain_kwargs["column"]: domain
            for domain in list(attributed_metrics.keys())
        }

        profiler_details: dict = expectation_configuration.meta["profiler_details"]
        metric_configuration: dict = profiler_details["metric_configuration"]
        domain_kwargs: dict = metric_configuration["domain_kwargs"]
        column_name: str = domain_kwargs["column"]

        domain = domains_by_column_name[column_name]

        attributed_values_by_metric_name: Dict[str, ParameterNode] = attributed_metrics[
            domain
        ]

        for metric_name in attributed_values_by_metric_name.keys():
            type_: str = expectation_configuration.expectation_type
            if expectation_metric_map.get(type_) == metric_name:
                attributed_values: ParameterNode = attributed_values_by_metric_name[
                    metric_name
                ]

                df: pd.DataFrame = self._create_df_for_charting(
                    metric_name=metric_name,
                    attributed_values=attributed_values,
                    expectation_configuration=expectation_configuration,
                    plot_mode=plot_mode,
                )

                column_name: str = expectation_configuration.kwargs["column"]
                subtitle = f"Column: {column_name}"

                metric_name: str = sanitize_parameter_name(name=metric_name)

                return self._chart_domain_values(
                    df=df,
                    metric_name=metric_name,
                    metric_type=metric_type,
                    plot_mode=plot_mode,
                    sequential=sequential,
                    subtitle=subtitle,
                )

    def _chart_column_values(
        self,
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        metric_name: Optional[str],
        metric_type: alt.StandardType,
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Optional[alt.VConcatChart]]:

        display_chart: Optional[alt.VConcatChart] = None

        if metric_name:
            if plot_mode is PlotMode.PRESCRIPTIVE:
                display_chart = self.get_interactive_detail_expect_column_values_to_be_between_chart(
                    expectation_type=expectation_type,
                    column_dfs=column_dfs,
                    metric_name=metric_name,
                    metric_type=metric_type,
                    sequential=sequential,
                )
            else:
                display_chart = self.get_interactive_detail_multi_chart(
                    column_dfs=column_dfs,
                    metric_name=metric_name,
                    metric_type=metric_type,
                    sequential=sequential,
                )

        return [display_chart]

    def _create_df_for_charting(
        self,
        metric_name: str,
        attributed_values: ParameterNode,
        expectation_configuration: ExpectationConfiguration,
        plot_mode: PlotMode,
    ) -> pd.DataFrame:
        batch_ids: KeysView[str] = attributed_values.keys()
        metric_values: MetricValues = [
            value[0] if len(value) == 1 else value
            for value in attributed_values.values()
        ]

        df: pd.DataFrame = pd.DataFrame(
            {sanitize_parameter_name(name=metric_name): metric_values}
        )

        batch_identifier_list: List[Set[Tuple[str, str]]] = [
            self.batch_id_to_batch_identifier_display_name_map[batch_id]
            for batch_id in batch_ids
        ]

        # make sure batch_identifier keys are sorted the same from batch to batch
        # e.g. prevent batch 1 from having keys "month", "year" and batch 2 from having keys "year", "month"
        batch_identifier_set: Set
        batch_identifier_list_sorted: List
        batch_identifier_tuple: Tuple
        batch_identifier_key: str
        batch_identifier_value: str
        batch_identifier_keys: Set[str] = set()
        batch_identifier_record: List
        batch_identifier_records: List[List] = []
        for batch_identifier_set in batch_identifier_list:
            batch_identifier_list_sorted = sorted(
                batch_identifier_set,
                key=lambda batch_identifier_tuple: batch_identifier_tuple[0].casefold(),
            )
            batch_identifier_record = []
            for (
                batch_identifier_key,
                batch_identifier_value,
            ) in batch_identifier_list_sorted:
                batch_identifier_keys.add(batch_identifier_key)
                batch_identifier_record.append(batch_identifier_value)

            batch_identifier_records.append(batch_identifier_record)

        batch_identifier_keys_sorted: List[str] = sorted(batch_identifier_keys)
        batch_identifiers: pd.DataFrame = pd.DataFrame(
            batch_identifier_records, columns=batch_identifier_keys_sorted
        )

        idx: int
        batch_numbers: List[int] = [idx + 1 for idx in range(len(batch_identifiers))]
        df["batch"] = batch_numbers

        df = pd.concat([df, batch_identifiers], axis=1)

        if plot_mode is PlotMode.PRESCRIPTIVE:
            for kwarg_name in expectation_configuration.kwargs:
                df[kwarg_name] = expectation_configuration.kwargs[kwarg_name]

        return df

    def _determine_attributed_metrics_by_domain_type(
        self, metric_domain_type: MetricDomainTypes
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]] = dict(
            filter(
                lambda element: element[0].domain_type == metric_domain_type,
                self.get_attributed_metrics_by_domain().items(),
            )
        )
        return attributed_metrics_by_domain

    def _create_column_dfs_for_charting(
        self,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        expectation_configurations: List[ExpectationConfiguration],
        plot_mode: PlotMode,
    ) -> List[ColumnDataFrame]:
        expectation_metric_map: Dict[str, str] = self.EXPECTATION_METRIC_MAP

        domain: Domain
        domains_by_column_name: Dict[str, Domain] = {
            domain.domain_kwargs["column"]: domain
            for domain in list(attributed_metrics.keys())
        }

        metric_names: List[str]
        column_dfs: List[ColumnDataFrame] = []
        for expectation_configuration in expectation_configurations:
            profiler_details: dict = expectation_configuration.meta["profiler_details"]
            metric_configuration: dict = profiler_details["metric_configuration"]
            domain_kwargs: dict = metric_configuration["domain_kwargs"]
            column_name: str = domain_kwargs["column"]

            domain = domains_by_column_name[column_name]

            attributed_values_by_metric_name: Dict[
                str, ParameterNode
            ] = attributed_metrics[domain]

            for metric_name in attributed_values_by_metric_name.keys():
                type_: str = expectation_configuration.expectation_type
                if expectation_metric_map.get(type_) == metric_name:
                    attributed_values: ParameterNode = attributed_values_by_metric_name[
                        metric_name
                    ]

                    df: pd.DataFrame = self._create_df_for_charting(
                        metric_name=metric_name,
                        attributed_values=attributed_values,
                        expectation_configuration=expectation_configuration,
                        plot_mode=plot_mode,
                    )

                    column_name: str = expectation_configuration.kwargs["column"]
                    column_df: ColumnDataFrame = ColumnDataFrame(column_name, df)
                    column_dfs.append(column_df)

        return column_dfs

    def _create_chart_for_table_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> alt.Chart:
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value

        expectation_metric_map: Dict[str, str] = self.EXPECTATION_METRIC_MAP

        table_domain: Domain = Domain(
            domain_type=MetricDomainTypes.TABLE, rule_name="table_rule"
        )
        attributed_metrics_by_domain: Dict[str, ParameterNode] = attributed_metrics[
            table_domain
        ]

        for metric_name in attributed_metrics_by_domain.keys():
            if (
                expectation_configuration.expectation_type
                in expectation_metric_map.keys()
            ) and (
                metric_name
                == expectation_metric_map[expectation_configuration.expectation_type]
            ):
                attributed_values: ParameterNode = attributed_metrics_by_domain[
                    metric_name
                ]

                df: pd.DataFrame = self._create_df_for_charting(
                    metric_name=metric_name,
                    attributed_values=attributed_values,
                    expectation_configuration=expectation_configuration,
                    plot_mode=plot_mode,
                )

                metric_name: str = sanitize_parameter_name(metric_name)

                return self._chart_domain_values(
                    df=df,
                    metric_name=metric_name,
                    metric_type=metric_type,
                    plot_mode=plot_mode,
                    sequential=sequential,
                    subtitle=None,
                )
