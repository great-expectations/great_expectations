from typing import Any, Callable, Dict, KeysView, List, Optional, Set, Tuple

import altair as alt
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import (
    Domain,
    MetricValues,
    ParameterNode,
)
from great_expectations.rule_based_profiler.types.altair import AltairDataTypes
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.types.data_assistant_result.plot_result import (
    PlotResult,
)


class VolumeDataAssistantResult(DataAssistantResult):
    def plot(
        self,
        prescriptive: bool = False,
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
            prescriptive: Type of plot to generate, prescriptive if True, descriptive if False
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

        display_charts: List[alt.Chart] = []
        return_charts: List[alt.Chart] = []

        expectation_configurations: List[
            ExpectationConfiguration
        ] = self.expectation_configurations

        table_domain_chart: List[alt.Chart] = self._plot_table_domain_charts(
            expectation_configurations=expectation_configurations,
            prescriptive=prescriptive,
        )
        display_charts.extend(table_domain_chart)
        return_charts.extend(table_domain_chart)

        column_domain_display_chart: alt.VConcatChart
        column_domain_return_charts: List[alt.Chart]
        (
            column_domain_display_charts,
            column_domain_return_charts,
        ) = self._plot_column_domain_charts(
            expectation_configurations=expectation_configurations,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
            prescriptive=prescriptive,
        )
        display_charts.extend(column_domain_display_charts)
        return_charts.extend(column_domain_return_charts)

        self.display(charts=display_charts, theme=theme)

        return_charts = self.apply_theme(charts=return_charts, theme=theme)
        return PlotResult(charts=return_charts)

    def _plot_table_domain_charts(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        prescriptive: bool,
    ) -> List[alt.Chart]:
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
                    prescriptive=prescriptive,
                )
            )
            charts.append(table_domain_chart)

        return charts

    def _plot_column_domain_charts(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        prescriptive: bool,
    ) -> Tuple[alt.VConcatChart, List[alt.Chart]]:
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

        display_chart: List[
            alt.VConcatChart
        ] = self._create_display_chart_for_column_domain_expectation(
            expectation_configurations=column_based_expectation_configurations,
            attributed_metrics=attributed_metrics_by_column_domain,
            prescriptive=prescriptive,
        )

        return_charts: List[alt.Chart] = []
        for expectation_configuration in column_based_expectation_configurations:
            return_chart: alt.Chart = (
                self._create_return_chart_for_column_domain_expectation(
                    expectation_configuration=expectation_configuration,
                    attributed_metrics=attributed_metrics_by_column_domain,
                    prescriptive=prescriptive,
                )
            )
            return_charts.extend([return_chart])

        return display_chart, return_charts

    def _create_chart_for_table_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        prescriptive: bool,
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
            prescriptive=prescriptive,
        )

        return self._chart_domain_values(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            prescriptive=prescriptive,
            subtitle=None,
        )

    def _chart_domain_values(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        prescriptive: bool,
        subtitle: Optional[str],
    ) -> alt.Chart:
        return_impl: Callable[
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
        if prescriptive:
            return_impl = self.get_expect_domain_values_to_be_between_chart
        else:
            return_impl = self.get_line_chart

        chart: alt.Chart = return_impl(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            subtitle=subtitle,
        )
        return chart

    def _create_display_chart_for_column_domain_expectation(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        prescriptive: bool,
    ) -> List[alt.VConcatChart]:
        column_dfs: List[pd.DataFrame] = self._create_column_dfs_for_charting(
            attributed_metrics=attributed_metrics,
            expectation_configurations=expectation_configurations,
            prescriptive=prescriptive,
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
            prescriptive=prescriptive,
        )

    def _create_return_chart_for_column_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        prescriptive: bool,
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
            prescriptive=prescriptive,
        )

        column_name: str = expectation_configuration.kwargs["column"]
        subtitle = f"Column: {column_name}"

        return self._chart_domain_values(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            prescriptive=prescriptive,
            subtitle=subtitle,
        )

    def _chart_column_values(
        self,
        column_dfs: List[Tuple[str, pd.DataFrame]],
        metric_name: str,
        metric_type: alt.StandardType,
        prescriptive: bool,
    ) -> List[alt.VConcatChart]:
        display_impl: Callable[
            [
                List[Tuple[str, pd.DataFrame]],
                str,
                alt.StandardType,
            ],
            alt.VConcatChart,
        ]
        if prescriptive:
            display_impl = (
                self.get_interactive_detail_expect_column_values_to_be_between_chart
            )
        else:
            display_impl = self.get_interactive_detail_multi_line_chart

        display_chart: alt.VConcatChart = display_impl(
            column_dfs=column_dfs,
            metric_name=metric_name,
            metric_type=metric_type,
        )

        return [display_chart]

    def _create_df_for_charting(
        self,
        metric_name: str,
        attributed_values_by_metric_name: Dict[str, ParameterNode],
        expectation_configuration: ExpectationConfiguration,
        prescriptive: bool,
    ) -> pd.DataFrame:
        batch_ids: KeysView[str]
        metric_values: MetricValues
        batch_ids, metric_values = list(attributed_values_by_metric_name.values())[
            0
        ].keys(), sum(list(attributed_values_by_metric_name.values())[0].values(), [])

        df: pd.DataFrame = pd.DataFrame({metric_name: metric_values})

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

        if prescriptive:
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
        prescriptive: bool,
    ) -> List[pd.DataFrame]:
        domain: Domain
        domains_by_column_name: Dict[str, Domain] = {
            domain.domain_kwargs["column"]: domain
            for domain in list(attributed_metrics.keys())
        }

        column_dfs: List[Tuple[str, pd.DataFrame]] = []
        for expectation_configuration in expectation_configurations:
            metric_configuration: dict = expectation_configuration.meta[
                "profiler_details"
            ]["metric_configuration"]
            domain_kwargs: dict = metric_configuration["domain_kwargs"]

            domain = domains_by_column_name[domain_kwargs["column"]]

            attributed_values_by_metric_name: Dict[
                str, ParameterNode
            ] = attributed_metrics[domain]

            # Altair does not accept periods.
            metric_name = list(attributed_values_by_metric_name.keys())[0].replace(
                ".", "_"
            )

            df: pd.DataFrame = self._create_df_for_charting(
                metric_name,
                attributed_values_by_metric_name,
                expectation_configuration,
                prescriptive,
            )

            column_name: str = expectation_configuration.kwargs["column"]
            column_dfs.append((column_name, df))

        return column_dfs
