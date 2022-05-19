from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import altair as alt
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import Domain, ParameterNode
from great_expectations.rule_based_profiler.types.altair import AltairDataTypes
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.types.data_assistant_result.plot_result import (
    PlotMode,
    PlotResult,
)


class VolumeDataAssistantResult(DataAssistantResult):
    def plot_metrics(
        self,
        sequential: bool = True,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """
        See parent `DataAssistantResult.plot_metrics()`.
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
        See parent `DataAssistantResult.plot_expectations_metrics()`.
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

        table_domain_chart: Union[
            List[alt.Chart], List[alt.LayerChart]
        ] = self._plot_table_domain_charts(
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
            sequential=sequential,
        )
        display_charts.extend(table_domain_chart)
        return_charts.extend(table_domain_chart)

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

    def _plot_table_domain_charts(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> Union[List[alt.Chart], List[alt.LayerChart]]:
        table_based_expectations: List[ExpectationConfiguration] = list(
            filter(
                lambda e: e.expectation_type == "expect_table_row_count_to_be_between",
                expectation_configurations,
            )
        )

        attributed_metrics_by_table_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.TABLE)

        charts: List[alt.Chart] = []

        expectation_configuration: ExpectationConfiguration
        for expectation_configuration in table_based_expectations:
            table_domain_chart: alt.Chart = (
                self._create_chart_for_table_domain_expectation(
                    expectation_configuration=expectation_configuration,
                    attributed_metrics=attributed_metrics_by_table_domain,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
            )
            charts.append(table_domain_chart)

        return charts

    def _plot_column_domain_charts(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> Tuple[List[alt.VConcatChart], List[alt.Chart]]:
        def _filter(e: ExpectationConfiguration) -> bool:
            if e.expectation_type != "expect_column_unique_value_count_to_be_between":
                return False
            column_name: str = e.kwargs["column"]
            if exclude_column_names and column_name in exclude_column_names:
                return False
            if include_column_names and column_name not in include_column_names:
                return False
            return True

        column_based_expectation_configurations: List[ExpectationConfiguration] = list(
            filter(
                lambda e: _filter(e),
                expectation_configurations,
            )
        )

        attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.COLUMN)

        display_charts: List[
            alt.VConcatChart
        ] = self._create_display_chart_for_column_domain_expectation(
            expectation_configurations=column_based_expectation_configurations,
            attributed_metrics=attributed_metrics_by_column_domain,
            plot_mode=plot_mode,
            sequential=sequential,
        )

        return_charts: List[alt.Chart] = []
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

        return display_charts, return_charts

    def _create_chart_for_table_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> alt.Chart:
        attributed_values_by_metric_name: Dict[str, ParameterNode] = list(
            attributed_metrics.values()
        )[0]

        # Altair does not accept periods.
        metric_name: str = list(attributed_values_by_metric_name.keys())[0].replace(
            ".", "_"
        )
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value

        df: pd.DataFrame = self._create_df_for_charting(
            metric_name=metric_name,
            attributed_values_by_metric_name=attributed_values_by_metric_name,
            expectation_configuration=expectation_configuration,
            plot_mode=plot_mode,
        )

        return self._chart_domain_values(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            plot_mode=plot_mode,
            sequential=sequential,
            subtitle=None,
        )

    def _chart_domain_values(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        plot_mode: PlotMode,
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.Chart:
        plot_impl: Callable[
            [
                pd.DataFrame,
                str,
                alt.StandardType,
                str,
                alt.StandardType,
                Optional[str],
            ],
            alt.Chart,
        ]
        if plot_mode is PlotMode.PRESCRIPTIVE:
            plot_impl = self.get_expect_domain_values_to_be_between_chart
        elif plot_mode is PlotMode.DESCRIPTIVE:
            plot_impl = self.get_quantitative_metric_chart

        chart: alt.Chart = plot_impl(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            sequential=sequential,
            subtitle=subtitle,
        )
        return chart

    def _create_display_chart_for_column_domain_expectation(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[alt.VConcatChart]:
        column_dfs: List[pd.DataFrame] = self._create_column_dfs_for_charting(
            attributed_metrics=attributed_metrics,
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
        )

        attributed_values_by_metric_name: Dict[str, ParameterNode] = list(
            attributed_metrics.values()
        )[0]

        # Altair does not accept periods.
        metric_name: str = list(attributed_values_by_metric_name.keys())[0].replace(
            ".", "_"
        )
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value

        return self._chart_column_values(
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
        domain: Domain
        domains_by_column_name: Dict[str, Domain] = {
            domain.domain_kwargs["column"]: domain
            for domain in list(attributed_metrics.keys())
        }

        metric_configuration: dict = expectation_configuration.meta["profiler_details"][
            "metric_configuration"
        ]
        domain_kwargs: dict = metric_configuration["domain_kwargs"]

        domain = domains_by_column_name[domain_kwargs["column"]]

        attributed_values_by_metric_name: Dict[str, ParameterNode] = attributed_metrics[
            domain
        ]

        # Altair does not accept periods.
        metric_name: str = list(attributed_values_by_metric_name.keys())[0].replace(
            ".", "_"
        )
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value

        df: pd.DataFrame = self._create_df_for_charting(
            metric_name=metric_name,
            attributed_values_by_metric_name=attributed_values_by_metric_name,
            expectation_configuration=expectation_configuration,
            plot_mode=plot_mode,
        )

        column_name: str = expectation_configuration.kwargs["column"]
        subtitle = f"Column: {column_name}"

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
        column_dfs: List[Tuple[str, pd.DataFrame]],
        metric_name: str,
        metric_type: alt.StandardType,
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[alt.VConcatChart]:
        plot_impl: Callable[
            [
                List[Tuple[str, pd.DataFrame]],
                str,
                alt.StandardType,
            ],
            alt.VConcatChart,
        ]
        if plot_mode is PlotMode.PRESCRIPTIVE:
            plot_impl = (
                self.get_interactive_detail_expect_column_values_to_be_between_chart
            )
        else:
            plot_impl = self.get_interactive_detail_multi_chart

        display_chart: alt.VConcatChart = plot_impl(
            column_dfs=column_dfs,
            metric_name=metric_name,
            metric_type=metric_type,
            sequential=sequential,
        )

        return [display_chart]
