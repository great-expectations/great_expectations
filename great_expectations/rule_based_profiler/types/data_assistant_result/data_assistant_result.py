import copy
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from typing import Any, Callable, Dict, KeysView, List, Optional, Set, Tuple, Union

import altair as alt
import numpy as np
import pandas as pd
from IPython.display import HTML, display

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    get_expectation_suite_usage_statistics,
    usage_statistics_enabled_method,
)
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
    METRIC_EXPECTATION_MAP = {
        "table.columns": "expect_table_columns_to_match_set",
        "table.row_count": "expect_table_row_count_to_be_between",
        "column.distinct_values.count": "expect_column_unique_value_count_to_be_between",
        "column.min": "expect_column_min_to_be_between",
        "column.max": "expect_column_max_to_be_between",
        "column.mean": "expect_column_mean_to_be_between",
        "column.median": "expect_column_median_to_be_between",
        "column.standard_deviation": "expect_column_stdev_to_be_between",
    }

    ALLOWED_KEYS = {
        "batch_id_to_batch_identifier_display_name_map",
        "profiler_config",
        "metrics_by_domain",
        "expectation_configurations",
        "execution_time",
        "usage_statistics_handler",
    }

    batch_id_to_batch_identifier_display_name_map: Optional[
        Dict[str, Set[Tuple[str, Any]]]
    ] = None
    profiler_config: Optional["RuleBasedProfilerConfig"] = None  # noqa: F821
    metrics_by_domain: Optional[Dict[Domain, Dict[str, ParameterNode]]] = None
    expectation_configurations: Optional[List[ExpectationConfiguration]] = None
    citation: Optional[dict] = None
    execution_time: Optional[float] = None  # Execution time (in seconds).
    usage_statistics_handler: Optional[UsageStatisticsHandler] = None

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
            "usage_statistics_handler": self.usage_statistics_handler.__class__.__name__,
        }

    def to_json_dict(self) -> dict:
        """
        Returns: This DataAssistantResult as JSON-serializable dictionary.
        """
        return self.to_dict()

    @property
    def _usage_statistics_handler(self) -> Optional[UsageStatisticsHandler]:
        """
        Returns: "UsageStatisticsHandler" object for this DataAssistantResult object (if configured).
        """
        return self.usage_statistics_handler

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_ASSISTANT_RESULT_GET_EXPECTATION_SUITE.value,
        args_payload_fn=get_expectation_suite_usage_statistics,
    )
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
        parameter_node: ParameterNode
        metrics_attributed_values_by_domain: Dict[Domain, Dict[str, ParameterNode]] = {
            domain: {
                parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                ].metric_configuration.metric_name: parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                ]
                for fully_qualified_parameter_name, parameter_node in parameter_values_for_fully_qualified_parameter_names.items()
                if FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY in parameter_node
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
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
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
    def _transform_table_column_list_to_rows(
        df: pd.DataFrame, metric_name: str
    ) -> pd.DataFrame:
        # explode list of column names into separate rows for each name in list
        # flatten columns of lists
        col_flat: List[str] = [item for sublist in df[metric_name] for item in sublist]
        # row numbers to repeat
        ilocations: np.ndarray = np.repeat(
            range(df.shape[0]), df[metric_name].apply(len)
        )
        # replicate rows and add flattened column of lists
        cols: List[int] = [i for i, c in enumerate(df.columns) if c != metric_name]
        df = df.iloc[ilocations, cols]
        df[metric_name] = col_flat

        # create column number by encoding the categorical column name and adding 1 since encoding starts at 0
        df["column_number"] = pd.factorize(df[metric_name])[0] + 1

        # rename table_columns to table_column
        df_columns = [
            "table_column" if column == "table_columns" else column
            for column in list(df.columns)
        ]
        df.columns = df_columns
        return df

    @staticmethod
    def _get_column_set_text(column_set: List[str]) -> Tuple[str, int]:
        dy: int
        if len(column_set) > 50:
            text = f"All batches have the same set of columns. The number of columns ({len(column_set)}) is too long to list here."
            dy = 0
        else:
            column_set_text: str = ""
            idx: int = 1
            for column in column_set:
                # line break for every 4 column names
                if idx % 4 == 0:
                    column_set_text += f"{column},$"
                else:
                    column_set_text += f"{column}, "
                idx += 1
            text = f"All batches have columns matching the set:${column_set_text[:-2]}."
            dy = -100
        return text, dy

    @staticmethod
    def get_nominal_metric_chart(
        df: pd.DataFrame,
        metric_name: str,
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair chart for nominal metrics
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column for column in df.columns if column not in [metric_name, batch_name]
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        column_number: str = "column_number"

        metric_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        column_set: Optional[List[str]]
        metric_component: MetricPlotComponent
        if metric_name == "table_columns":
            table_column: str = "table_column"
            unique_column_sets: np.ndarray = np.unique(df[metric_name])
            if len(unique_column_sets) == 1:
                column_set = df[metric_name].iloc[0]
            else:
                column_set = None
                # filter only on batches that do not contain every possible column
                unique_columns: set = {
                    column for column_list in df[metric_name] for column in column_list
                }
                df = df[df[metric_name].apply(set) != unique_columns]
                # record containing all columns to be compared against
                empty_columns: List[None] = [None] * (len(batch_identifiers) + 1)
                all_columns_record: pd.DataFrame = pd.DataFrame(
                    data=[[unique_columns] + empty_columns], columns=df.columns
                )
                df = pd.concat([all_columns_record, df], axis=0)
                df[batch_name] = df[batch_name].fillna(value="All Columns")
                df = DataAssistantResult._transform_table_column_list_to_rows(
                    df=df, metric_name=metric_name
                )

            metric_component = MetricPlotComponent(
                name=table_column,
                alt_type=metric_type,
            )
        else:
            metric_component = MetricPlotComponent(
                name=metric_name,
                alt_type=metric_type,
            )

        # we need at least one record to plot the column_set as text
        if column_set is not None:
            df = df.iloc[:1]

        column_number_component: PlotComponent = PlotComponent(
            name=column_number,
            alt_type=AltairDataTypes.ORDINAL.value,
        )

        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=None,
            alt_type=AltairDataTypes.NOMINAL.value,
            subtitle=subtitle,
        )

        if sequential:
            return DataAssistantResult._get_sequential_isotype_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
                column_number_component=column_number_component,
                column_set=column_set,
            )
        else:
            return DataAssistantResult._get_nonsequential_isotype_chart(
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
                column_number_component=column_number_component,
                column_set=column_set,
            )

    @staticmethod
    def get_expect_domain_values_to_match_set_chart(
        expectation_type: str,
        df: pd.DataFrame,
        metric_name: str,
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            expectation_type: The name of the expectation
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair chart for nominal metrics
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column for column in df.columns if column not in [metric_name, batch_name]
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        column_number: str = "column_number"

        metric_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        column_set: Optional[List[str]] = None
        metric_component: MetricPlotComponent
        if metric_name == "table_columns":
            table_column: str = "table_column"
            unique_column_sets: np.ndarray = np.unique(df[metric_name])
            if len(unique_column_sets) == 1:
                column_set = df[metric_name].iloc[0]

            df = DataAssistantResult._transform_table_column_list_to_rows(
                df=df, metric_name=metric_name
            )

            metric_component = MetricPlotComponent(
                name=table_column,
                alt_type=metric_type,
            )
        else:
            metric_component = MetricPlotComponent(
                name=metric_name,
                alt_type=metric_type,
            )

        # we need at least one record to plot the column_set as text
        if column_set is not None:
            df = df.iloc[:1]

        column_number_component: PlotComponent = PlotComponent(
            name=column_number,
            alt_type=AltairDataTypes.ORDINAL.value,
        )

        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=None,
            alt_type=AltairDataTypes.NOMINAL.value,
            subtitle=subtitle,
        )

        if sequential:
            return DataAssistantResult._get_sequential_expect_domain_values_to_match_set_isotype_chart(
                expectation_type=expectation_type,
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
                column_number_component=column_number_component,
                column_set=column_set,
            )
        else:
            return DataAssistantResult._get_nonsequential_expect_domain_values_to_match_set_isotype_chart(
                expectation_type=expectation_type,
                df=df,
                metric_component=metric_component,
                batch_component=batch_component,
                domain_component=domain_component,
                column_number_component=column_number_component,
                column_set=column_set,
            )

    @staticmethod
    def _get_sequential_isotype_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        column_number_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = batch_component.generate_tooltip() + [
            metric_component.generate_tooltip(),
        ]

        chart: Union[alt.Chart, alt.LayerChart]
        if column_set is None:
            chart = (
                alt.Chart(data=df, title=title)
                .mark_point(color=Colors.PURPLE.value)
                .encode(
                    x=alt.X(
                        batch_component.name,
                        type=batch_component.alt_type,
                        title=batch_component.title,
                        axis=alt.Axis(grid=False),
                    ),
                    y=alt.Y(
                        column_number_component.name,
                        type=column_number_component.alt_type,
                        title=column_number_component.title,
                        axis=alt.Axis(grid=True),
                    ),
                    tooltip=tooltip,
                )
            )
        else:
            dy: int
            text, dy = DataAssistantResult._get_column_set_text(column_set=column_set)

            chart = (
                alt.Chart(data=df, title=title)
                .mark_point(opacity=0.0)
                .encode(
                    x=alt.X(
                        batch_component.name,
                        type=batch_component.alt_type,
                        title=None,
                        axis=alt.Axis(labels=False, ticks=False, grid=False),
                    ),
                    y=alt.Y(
                        column_number_component.name,
                        type=column_number_component.alt_type,
                        title=" ",
                        axis=alt.Axis(labels=False, ticks=False),
                    ),
                )
            ).mark_text(
                text=text,
                color=Colors.PURPLE.value,
                lineBreak=r"$",
                dy=dy,
            )

        return chart

    @staticmethod
    def _get_nonsequential_isotype_chart(
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        column_number_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        tooltip: List[alt.Tooltip] = batch_component.generate_tooltip() + [
            metric_component.generate_tooltip(),
        ]

        chart: alt.Chart
        if column_set is None:
            chart = (
                alt.Chart(data=df, title=title)
                .mark_point(color=Colors.PURPLE.value)
                .encode(
                    x=alt.X(
                        batch_component.name,
                        type=batch_component.alt_type,
                        title=batch_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=alt.Y(
                        column_number_component.name,
                        type=column_number_component.alt_type,
                        title=column_number_component.title,
                        axis=alt.Axis(grid=True),
                    ),
                    tooltip=tooltip,
                )
            )
        else:
            dy: int
            text, dy = DataAssistantResult._get_column_set_text(column_set=column_set)

            chart = (
                alt.Chart(data=df, title=title)
                .mark_point(opacity=0.0)
                .encode(
                    x=alt.X(
                        batch_component.name,
                        type=batch_component.alt_type,
                        title=None,
                        axis=alt.Axis(labels=False, ticks=False, grid=False),
                    ),
                    y=alt.Y(
                        column_number_component.name,
                        type=column_number_component.alt_type,
                        title=" ",
                        axis=alt.Axis(labels=False, ticks=False),
                    ),
                )
            ).mark_text(
                text=text,
                color=Colors.PURPLE.value,
                lineBreak=r"$",
                dy=dy,
            )

        return chart

    @staticmethod
    def _get_sequential_expect_domain_values_to_match_set_isotype_chart(
        expectation_type: str,
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        column_number_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        chart: alt.Chart = DataAssistantResult._get_sequential_isotype_chart(
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
            column_number_component=column_number_component,
            column_set=column_set,
        ).properties(title=title)

        return chart

    @staticmethod
    def _get_nonsequential_expect_domain_values_to_match_set_isotype_chart(
        expectation_type: str,
        df: pd.DataFrame,
        metric_component: MetricPlotComponent,
        batch_component: BatchPlotComponent,
        domain_component: DomainPlotComponent,
        column_number_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_component=metric_component,
            batch_plot_component=batch_component,
            domain_plot_component=domain_component,
        )

        chart: alt.Chart = DataAssistantResult._get_nonsequential_isotype_chart(
            df=df,
            metric_component=metric_component,
            batch_component=batch_component,
            domain_component=domain_component,
            column_number_component=column_number_component,
            column_set=column_set,
        ).properties(title=title)

        return chart

    @staticmethod
    def get_quantitative_metric_chart(
        df: pd.DataFrame,
        metric_name: str,
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart
        """
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column for column in df.columns if column not in {metric_name, batch_name}
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
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
        expectation_type: str,
        df: pd.DataFrame,
        metric_name: str,
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.Chart:
        """
        Args:
            expectation_type: The name of the expectation
            df: A pandas dataframe containing the data to be plotted
            metric_name: The name of the metric as it exists in the pandas dataframe
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

        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=AltairDataTypes.QUANTITATIVE.value
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
            domain_component = DomainPlotComponent(
                name=column_name,
                alt_type=AltairDataTypes.NOMINAL.value,
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
                    expectation_type=expectation_type,
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
                    expectation_type=expectation_type,
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
        sequential: bool,
    ) -> Union[alt.Chart, alt.VConcatChart]:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
            sequential: Whether batches are sequential in nature

        Returns:
            A interactive detail altair multi-chart
        """
        batch_name: str = "batch"
        all_columns: List[str] = list(column_dfs[0].df.columns)
        batch_identifiers: List[str] = [
            column for column in all_columns if column not in {metric_name, batch_name}
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=metric_type
        )

        domain_name: str = "column"
        domain_component: DomainPlotComponent = DomainPlotComponent(
            name=domain_name,
            alt_type=AltairDataTypes.NOMINAL.value,
        )

        df: pd.DataFrame = pd.DataFrame(
            columns=[batch_name, domain_name, metric_name] + batch_identifiers
        )
        for column_df in column_dfs:
            column_df.df[domain_name] = column_df.column
            df = pd.concat([df, column_df.df], axis=0)

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
        sequential: bool,
    ) -> alt.VConcatChart:
        """
        Args:
            expectation_type: The name of the expectation
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            metric_name: The name of the metric as it exists in the pandas dataframe
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
        all_columns: List[str] = list(column_dfs[0].df.columns)
        batch_identifiers: List[str] = [
            column
            for column in all_columns
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
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_component: BatchPlotComponent = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_component: MetricPlotComponent = MetricPlotComponent(
            name=metric_name, alt_type=AltairDataTypes.QUANTITATIVE.value
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
        min_value_component: ExpectationKwargPlotComponent,
        max_value_component: ExpectationKwargPlotComponent,
        tooltip: List[alt.Tooltip],
        expectation_type: Optional[str] = None,
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
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
        min_value_component: ExpectationKwargPlotComponent,
        max_value_component: ExpectationKwargPlotComponent,
        tooltip: List[alt.Tooltip],
        expectation_type: Optional[str] = None,
    ) -> alt.Chart:
        line_color: alt.HexColor = alt.HexColor(ColorPalettes.HEATMAP_6.value[4])

        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
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
        min_value_component: ExpectationKwargPlotComponent,
        max_value_component: ExpectationKwargPlotComponent,
        strict_min_component: ExpectationKwargPlotComponent,
        strict_max_component: ExpectationKwargPlotComponent,
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
        min_value_component: ExpectationKwargPlotComponent,
        max_value_component: ExpectationKwargPlotComponent,
        strict_min_component: ExpectationKwargPlotComponent,
        strict_max_component: ExpectationKwargPlotComponent,
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
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Union[List[alt.Chart], List[alt.LayerChart]]]:
        metric_expectation_map: Dict[str, str] = self.METRIC_EXPECTATION_MAP

        table_based_expectations: List[str] = [
            expectation
            for expectation in metric_expectation_map.values()
            if expectation.startswith("expect_table_")
        ]
        table_based_expectation_configurations: List[ExpectationConfiguration] = list(
            filter(
                lambda e: e.expectation_type in table_based_expectations,
                expectation_configurations,
            )
        )

        attributed_metrics_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.TABLE)
        table_domain: Domain = Domain(
            domain_type=MetricDomainTypes.TABLE, rule_name="table_rule"
        )
        attributed_metrics_by_table_domain: Dict[
            str, ParameterNode
        ] = attributed_metrics_by_domain[table_domain]

        charts: List[alt.Chart] = []

        metric_name: str
        attributed_values: ParameterNode
        expectation_type: str
        expectation_configuration: ExpectationConfiguration
        for (
            metric_name,
            attributed_values,
        ) in attributed_metrics_by_table_domain.items():
            expectation_type = metric_expectation_map[metric_name]

            if plot_mode == PlotMode.PRESCRIPTIVE:
                for expectation_configuration in table_based_expectation_configurations:
                    if expectation_configuration.expectation_type == expectation_type:
                        table_domain_chart: alt.Chart = (
                            self._create_chart_for_table_domain_expectation(
                                expectation_type=expectation_type,
                                expectation_configuration=expectation_configuration,
                                metric_name=metric_name,
                                attributed_values=attributed_values,
                                include_column_names=include_column_names,
                                exclude_column_names=exclude_column_names,
                                plot_mode=plot_mode,
                                sequential=sequential,
                            )
                        )
                        charts.append(table_domain_chart)
            else:
                table_domain_chart: alt.Chart = (
                    self._create_chart_for_table_domain_expectation(
                        expectation_type=expectation_type,
                        expectation_configuration=None,
                        metric_name=metric_name,
                        attributed_values=attributed_values,
                        include_column_names=include_column_names,
                        exclude_column_names=exclude_column_names,
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
        metric_expectation_map: Dict[str, str] = self.METRIC_EXPECTATION_MAP
        column_based_metrics: List[str] = [
            metric
            for metric in metric_expectation_map.keys()
            if metric.startswith("column")
        ]

        column_based_expectation_configurations_by_type: Dict[
            str, List[ExpectationConfiguration]
        ] = self._filter_expectation_configurations_by_column_type(
            expectation_configurations, include_column_names, exclude_column_names
        )

        attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.COLUMN)

        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]]
        attributed_metrics_by_domain = self._filter_attributed_metrics_by_column_names(
            attributed_metrics_by_column_domain,
            include_column_names,
            exclude_column_names,
        )

        column_based_expectation_configurations: List[ExpectationConfiguration]
        metric_display_charts: List[Optional[alt.VConcatChart]]
        display_charts: List[Optional[alt.VConcatChart]] = []
        metric_return_charts: Optional[alt.Chart]
        return_charts: List[Optional[alt.Chart]] = []
        expectation_type: str
        for metric_name in column_based_metrics:
            expectation_type = metric_expectation_map[metric_name]
            column_based_expectation_configurations = (
                column_based_expectation_configurations_by_type[expectation_type]
            )
            filtered_attributed_metrics_by_domain = (
                self._filter_attributed_metrics_by_metric_name(
                    attributed_metrics_by_domain,
                    metric_name,
                )
            )

            metric_display_charts = (
                self._create_display_chart_for_column_domain_expectation(
                    expectation_type=expectation_type,
                    expectation_configurations=column_based_expectation_configurations,
                    metric_name=metric_name,
                    attributed_metrics_by_domain=filtered_attributed_metrics_by_domain,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
            )
            display_charts.extend(metric_display_charts)

            metric_return_charts = (
                self._create_return_charts_for_column_domain_expectation(
                    expectation_type=expectation_type,
                    expectation_configurations=column_based_expectation_configurations,
                    metric_name=metric_name,
                    attributed_metrics_by_domain=filtered_attributed_metrics_by_domain,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
            )
            return_charts.extend(metric_return_charts)

        display_charts = list(filter(None, display_charts))
        return_charts = list(filter(None, return_charts))

        return display_charts, return_charts

    def _filter_expectation_configurations_by_column_type(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
    ) -> Dict[str, List[ExpectationConfiguration]]:
        metric_expectation_map: Dict[str, str] = self.METRIC_EXPECTATION_MAP

        column_based_expectations: Set[str] = {
            expectation
            for expectation in metric_expectation_map.values()
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

    def _filter_attributed_metrics_by_column_names(
        self,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        def _filter(m: Dict[Domain, Dict[str, ParameterNode]]) -> bool:
            column_name: str = m.domain_kwargs.column
            if exclude_column_names and column_name in exclude_column_names:
                return False
            if include_column_names and column_name not in include_column_names:
                return False
            return True

        domains: Set[Domain] = set(filter(lambda m: _filter(m), attributed_metrics))
        filtered_attributed_metrics: Dict[Domain, Dict[str, ParameterNode]] = {
            domain: attributed_metrics[domain] for domain in domains
        }

        return filtered_attributed_metrics

    def _filter_attributed_metrics_by_metric_name(
        self,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        metric_name: str,
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        domain: Set[Domain]
        filtered_attributed_metrics: Dict[Domain, Dict[str, ParameterNode]] = {}
        for domain, attributed_metric_values in attributed_metrics.items():
            if metric_name in attributed_metric_values.keys():
                filtered_attributed_metrics[domain] = {
                    metric_name: attributed_metric_values[metric_name]
                }

        return filtered_attributed_metrics

    def _chart_domain_values(
        self,
        expectation_type: str,
        df: pd.DataFrame,
        metric_name: str,
        plot_mode: PlotMode,
        sequential: bool,
        subtitle: Optional[str],
    ) -> Optional[alt.Chart]:
        nominal_metrics: Set[str] = {"table_columns"}

        quantitative_metrics: Set[str] = {
            "table_row_count",
            "column_distinct_values_count",
            "column_min",
            "column_max",
            "column_values_between",
            "column_mean",
            "column_median",
            "column_quantile_values",
            "column_standard_deviation",
        }

        plot_impl: Optional[
            Callable[
                [
                    pd.DataFrame,
                    str,
                    bool,
                    Optional[str],
                ],
                alt.Chart,
            ]
        ] = None
        chart: Optional[alt.Chart] = None
        if plot_mode is PlotMode.PRESCRIPTIVE:
            if metric_name in quantitative_metrics:
                chart = self.get_expect_domain_values_to_be_between_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_name=metric_name,
                    sequential=sequential,
                    subtitle=subtitle,
                )
            elif metric_name in nominal_metrics:
                chart = self.get_expect_domain_values_to_match_set_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_name=metric_name,
                    sequential=sequential,
                    subtitle=subtitle,
                )
        elif plot_mode is PlotMode.DESCRIPTIVE:
            if metric_name in quantitative_metrics:
                chart = self.get_quantitative_metric_chart(
                    df=df,
                    metric_name=metric_name,
                    sequential=sequential,
                    subtitle=subtitle,
                )
            elif metric_name in nominal_metrics:
                chart = self.get_nominal_metric_chart(
                    df=df,
                    metric_name=metric_name,
                    sequential=sequential,
                    subtitle=subtitle,
                )

        return chart

    def _create_display_chart_for_column_domain_expectation(
        self,
        expectation_type: str,
        expectation_configurations: List[ExpectationConfiguration],
        metric_name: str,
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Optional[alt.VConcatChart]]:
        column_dfs: List[ColumnDataFrame] = self._create_column_dfs_for_charting(
            metric_name=metric_name,
            attributed_metrics_by_domain=attributed_metrics_by_domain,
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
        )

        return self._chart_column_values(
            expectation_type=expectation_type,
            column_dfs=column_dfs,
            metric_name=metric_name,
            plot_mode=plot_mode,
            sequential=sequential,
        )

    def _create_return_charts_for_column_domain_expectation(
        self,
        expectation_type: str,
        expectation_configurations: List[ExpectationConfiguration],
        metric_name: str,
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> alt.Chart:
        metric_expectation_map: Dict[str, str] = self.METRIC_EXPECTATION_MAP

        expectation_configuration: ExpectationConfiguration
        attributed_metrics: Dict[str, ParameterNode]
        attributed_values: ParameterNode
        return_charts: List[alt.Chart] = []
        for domain, attributed_metrics in attributed_metrics_by_domain.items():
            if (
                metric_name in attributed_metrics.keys()
                and metric_expectation_map[metric_name] == expectation_type
            ):
                attributed_values = attributed_metrics[metric_name]

                for expectation_configuration in expectation_configurations:
                    if (
                        expectation_configuration.kwargs["column"]
                        == domain.domain_kwargs.column
                    ):
                        df: pd.DataFrame = self._create_df_for_charting(
                            metric_name=metric_name,
                            attributed_values=attributed_values,
                            expectation_configuration=expectation_configuration,
                            plot_mode=plot_mode,
                        )

                        sanitized_metric_name: str = sanitize_parameter_name(
                            name=metric_name
                        )

                        column_name: str = domain.domain_kwargs.column
                        subtitle = f"Column: {column_name}"

                        return_chart = self._chart_domain_values(
                            expectation_type=expectation_type,
                            df=df,
                            metric_name=sanitized_metric_name,
                            plot_mode=plot_mode,
                            sequential=sequential,
                            subtitle=subtitle,
                        )

                        return_charts.append(return_chart)

        return return_charts

    def _chart_column_values(
        self,
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        metric_name: str,
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Optional[alt.VConcatChart]]:
        metric_name = sanitize_parameter_name(metric_name)

        display_chart: Optional[alt.VConcatChart]
        if plot_mode is PlotMode.PRESCRIPTIVE:
            display_chart = (
                self.get_interactive_detail_expect_column_values_to_be_between_chart(
                    expectation_type=expectation_type,
                    column_dfs=column_dfs,
                    metric_name=metric_name,
                    sequential=sequential,
                )
            )
        else:
            display_chart = self.get_interactive_detail_multi_chart(
                column_dfs=column_dfs,
                metric_name=metric_name,
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
                if isinstance(expectation_configuration.kwargs[kwarg_name], list):
                    df[kwarg_name] = [
                        expectation_configuration.kwargs[kwarg_name] for _ in df.index
                    ]
                else:
                    df[kwarg_name] = expectation_configuration.kwargs[kwarg_name]

        return df

    def _determine_attributed_metrics_by_domain_type(
        self, metric_domain_type: MetricDomainTypes
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        # noinspection PyTypeChecker
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]] = dict(
            filter(
                lambda element: element[0].domain_type == metric_domain_type,
                self.get_attributed_metrics_by_domain().items(),
            )
        )
        return attributed_metrics_by_domain

    def _create_column_dfs_for_charting(
        self,
        metric_name: str,
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]],
        expectation_configurations: List[ExpectationConfiguration],
        plot_mode: PlotMode,
    ) -> List[ColumnDataFrame]:
        metric_expectation_map: Dict[str, str] = self.METRIC_EXPECTATION_MAP

        metric_domains: Set[Domain] = set(attributed_metrics_by_domain.keys())

        profiler_details: dict
        metric_configuration: dict
        domain_kwargs: dict
        column_name: str
        column_domain: Domain
        column_dfs: List[ColumnDataFrame] = []
        for expectation_configuration in expectation_configurations:
            profiler_details = expectation_configuration.meta["profiler_details"]
            metric_configuration = profiler_details["metric_configuration"]
            domain_kwargs = metric_configuration["domain_kwargs"]
            column_name = domain_kwargs["column"]

            column_domain = [
                d for d in metric_domains if d.domain_kwargs.column == column_name
            ][0]

            attributed_values_by_metric_name: Dict[
                str, ParameterNode
            ] = attributed_metrics_by_domain[column_domain]

            if (
                metric_expectation_map.get(metric_name)
                == expectation_configuration.expectation_type
            ):
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
        expectation_type: str,
        expectation_configuration: Optional[ExpectationConfiguration],
        metric_name: str,
        attributed_values: ParameterNode,
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> alt.Chart:

        df: pd.DataFrame = self._create_df_for_charting(
            metric_name=metric_name,
            attributed_values=attributed_values,
            expectation_configuration=expectation_configuration,
            plot_mode=plot_mode,
        )

        metric_name: str = sanitize_parameter_name(metric_name)

        # If columns are included/excluded we need to filter them out for table level metrics here
        table_column_metrics: List[str] = ["table_columns"]
        new_column_list: List[str]
        new_record_list: List[List[str]] = []
        if metric_name in table_column_metrics:
            if (include_column_names is not None) or (exclude_column_names is not None):
                all_columns = df[metric_name].apply(pd.Series).values.tolist()
                for record in all_columns:
                    new_column_list = []
                    for column in record:
                        if (
                            include_column_names is not None
                            and column in include_column_names
                        ) or (
                            exclude_column_names is not None
                            and column not in exclude_column_names
                        ):
                            new_column_list.append(column)
                    new_record_list.append(new_column_list)
                df[metric_name] = new_record_list

        return self._chart_domain_values(
            expectation_type=expectation_type,
            df=df,
            metric_name=metric_name,
            plot_mode=plot_mode,
            sequential=sequential,
            subtitle=None,
        )
