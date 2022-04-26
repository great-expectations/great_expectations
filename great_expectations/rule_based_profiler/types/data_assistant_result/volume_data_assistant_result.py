from typing import Any, Callable, Dict, KeysView, List, Optional, Tuple, Union

import altair as alt
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_builder import MetricValues
from great_expectations.rule_based_profiler.types import Domain, ParameterNode
from great_expectations.rule_based_profiler.types.altair import AltairDataTypes
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)


class VolumeDataAssistantResult(DataAssistantResult):
    def plot(
        self,
        prescriptive: bool = False,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> None:
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "display()" for presentation.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            prescriptive: Type of plot to generate, prescriptive if True, descriptive if False
            theme: Altair top-level chart configuration dictionary
            include_column_names: A list of columns to chart
            exclude_column_names: A list of columns not to chart
        """
        if include_column_names is not None and exclude_column_names is not None:
            raise ValueError(
                "You may either use `include_column_names` or `exclude_column_names` (but not both)."
            )

        charts: List[Union[alt.Chart, alt.VConcatChart]] = []

        expectation_configurations: List[
            ExpectationConfiguration
        ] = self.expectation_suite.expectations

        table_domain_charts: List[alt.Chart] = self._plot_table_domain_charts(
            expectation_configurations, prescriptive
        )
        charts.extend(table_domain_charts)

        column_domain_chart: alt.VConcatChart = self._plot_column_domain_chart(
            expectation_configurations,
            include_column_names,
            exclude_column_names,
            prescriptive,
        )
        charts.append(column_domain_chart)

        self.display(charts=charts, theme=theme)

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
        domain_name: str = "batch"
        metric_type: str = AltairDataTypes.QUANTITATIVE.value
        domain_type: str = AltairDataTypes.ORDINAL.value

        df: pd.DataFrame = VolumeDataAssistantResult._create_df_for_charting(
            metric_name, domain_name, attributed_values_by_metric_name
        )

        plot_impl: Callable[
            [
                pd.DataFrame,
                str,
                alt.StandardType,
                str,
                alt.StandardType,
            ],
            alt.Chart,
        ]

        if prescriptive:
            for kwarg_name in expectation_configuration.kwargs:
                df[kwarg_name] = expectation_configuration.kwargs[kwarg_name]
            plot_impl = self.get_expect_table_values_to_be_between_chart
        else:
            plot_impl = self.get_line_chart

        chart: alt.Chart = plot_impl(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
        )
        return chart

    def _plot_column_domain_chart(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        prescriptive: bool,
    ) -> alt.VConcatChart:
        def _filter(e: ExpectationConfiguration) -> bool:
            if e.expectation_type != "expect_column_unique_value_count_to_be_between":
                return False
            column_name: str = e.kwargs["column"]
            if exclude_column_names and column_name in exclude_column_names:
                return False
            if include_column_names and column_name not in include_column_names:
                return False
            return True

        column_based_expectations: List[ExpectationConfiguration] = list(
            filter(
                lambda e: _filter(e),
                expectation_configurations,
            )
        )

        attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.COLUMN)

        column_domain_chart: alt.VConcatChart = (
            self._create_chart_for_column_domain_expectations(
                expectation_configurations=column_based_expectations,
                attributed_metrics=attributed_metrics_by_column_domain,
                prescriptive=prescriptive,
            )
        )
        return column_domain_chart

    def _create_chart_for_column_domain_expectations(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        prescriptive: bool,
    ) -> alt.VConcatChart:
        metric_name: Optional[str] = None
        domain_name: str = "batch"
        metric_type: str = AltairDataTypes.QUANTITATIVE.value
        domain_type: str = AltairDataTypes.ORDINAL.value

        column_dfs: List[Tuple[str, pd.DataFrame]] = []
        for expectation_configuration in expectation_configurations:
            metric_configuration: dict = expectation_configuration.meta[
                "profiler_details"
            ]["metric_configuration"]
            domain_kwargs: dict = metric_configuration["domain_kwargs"]

            domain: Domain = Domain(
                domain_type=MetricDomainTypes.COLUMN,
                domain_kwargs=domain_kwargs,
            )
            attributed_values_by_metric_name: Dict[
                str, ParameterNode
            ] = attributed_metrics[domain]

            # Altair does not accept periods.
            metric_name = list(attributed_values_by_metric_name.keys())[0].replace(
                ".", "_"
            )

            df: pd.DataFrame = VolumeDataAssistantResult._create_df_for_charting(
                metric_name, domain_name, attributed_values_by_metric_name
            )

            if prescriptive:
                for kwarg_name in expectation_configuration.kwargs:
                    df[kwarg_name] = expectation_configuration.kwargs[kwarg_name]

            column_name: str = expectation_configuration.kwargs["column"]
            column_dfs.append((column_name, df))

        assert metric_name is not None

        plot_impl: Callable[
            [
                List[Tuple[str, pd.DataFrame]],
                str,
                alt.StandardType,
                str,
                alt.StandardType,
            ],
            alt.VConcatChart,
        ]

        if prescriptive:
            plot_impl = self.get_expect_column_values_to_be_between_chart
        else:
            plot_impl = self.get_vertically_concatenated_line_chart

        chart: alt.VConcatChart = plot_impl(
            column_dfs=column_dfs,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
        )
        return chart

    @staticmethod
    def _create_df_for_charting(
        metric_name: str,
        domain_name: str,
        attributed_values_by_metric_name: Dict[str, ParameterNode],
    ) -> pd.DataFrame:
        batch_ids: KeysView[str]
        metric_values: MetricValues
        batch_ids, metric_values = list(attributed_values_by_metric_name.values())[
            0
        ].keys(), sum(list(attributed_values_by_metric_name.values())[0].values(), [])

        idx: int
        batch_numbers: List[int] = [idx + 1 for idx in range(len(batch_ids))]

        df: pd.DataFrame = pd.DataFrame(batch_numbers, columns=[domain_name])
        df["batch_id"] = batch_ids
        df[metric_name] = metric_values

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
