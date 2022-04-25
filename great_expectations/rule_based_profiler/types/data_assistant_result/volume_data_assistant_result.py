from typing import Any, Callable, Dict, KeysView, List, Optional, cast

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
    ) -> None:
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "display()" for presentation.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            prescriptive: Type of plot to generate, prescriptive if True, descriptive if False
            theme: Altair top-level chart configuration dictionary
        """
        attributed_metrics_by_table_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.TABLE)
        attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.COLUMN)

        expectation_configurations: List[
            ExpectationConfiguration
        ] = self.expectation_suite.expectations

        charts: List[alt.Chart] = []
        column_domain_charts: List[alt.Chart] = []

        expectation_configuration: ExpectationConfiguration
        for expectation_configuration in expectation_configurations:
            expectation_type: str = expectation_configuration.expectation_type

            if expectation_type == "expect_table_row_count_to_be_between":
                table_domain_chart: alt.Chart = (
                    self._create_chart_for_table_domain_expectation(
                        expectation_configuration=expectation_configuration,
                        attributed_metrics=attributed_metrics_by_table_domain,
                        prescriptive=prescriptive,
                    )
                )
                charts.append(table_domain_chart)
            elif expectation_type == "expect_column_unique_value_count_to_be_between":
                column_domain_chart: alt.Chart = (
                    self._create_chart_for_column_domain_expectation(
                        expectation_configuration=expectation_configuration,
                        attributed_metrics=attributed_metrics_by_column_domain,
                        prescriptive=prescriptive,
                    )
                )
                column_domain_charts.append(column_domain_chart)
            else:
                raise Exception()

        concatenated_column_domain_chart: alt.Chart = cast(
            alt.Chart, alt.vconcat(*column_domain_charts)
        )
        charts.append(concatenated_column_domain_chart)

        self.display(charts=charts, theme=theme)

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

    def _create_chart_for_table_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        prescriptive: bool,
    ) -> alt.Chart:
        attributed_values_by_metric_name: Dict[str, ParameterNode] = list(
            attributed_metrics.values()
        )[0]

        chart: alt.Chart = self._create_chart(
            expectation_configuration, attributed_values_by_metric_name, prescriptive
        )
        return chart

    def _create_chart_for_column_domain_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_metrics: Dict[Domain, Dict[str, ParameterNode]],
        prescriptive: bool,
    ) -> alt.Chart:
        metric_configuration: dict = expectation_configuration.meta["profiler_details"][
            "metric_configuration"
        ]
        domain_kwargs: dict = metric_configuration["domain_kwargs"]

        domain: Domain = Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs=domain_kwargs,
        )
        attributed_values_by_metric_name: Dict[str, ParameterNode] = attributed_metrics[
            domain
        ]

        chart: alt.Chart = self._create_chart(
            expectation_configuration, attributed_values_by_metric_name, prescriptive
        )
        return chart

    def _create_chart(
        self,
        expectation_configuration: ExpectationConfiguration,
        attributed_values_by_metric_name: dict,
        prescriptive: bool,
    ) -> alt.Chart:
        # Altair does not accept periods.
        metric_name: str = list(attributed_values_by_metric_name.keys())[0].replace(
            ".", "_"
        )
        domain_name: str = "batch"

        domain_type: str = AltairDataTypes.ORDINAL.value
        metric_type: str = AltairDataTypes.QUANTITATIVE.value

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

        plot_impl: Callable[
            [
                pd.DataFrame,
                str,
                alt.StandardType,
                str,
                alt.StandardType,
                Optional[List[str]],
            ],
            alt.Chart,
        ]

        if prescriptive:
            for kwarg_name in ("min_value", "max_value"):
                df[kwarg_name] = expectation_configuration.kwargs[kwarg_name]
            plot_impl = self._plot_prescriptive
        else:
            plot_impl = self._plot_descriptive

        subtitle: Optional[List[str]] = None
        if "column" in expectation_configuration.kwargs:
            subtitle = [f"Column: {expectation_configuration.kwargs['column']}"]

        chart: alt.Chart = plot_impl(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
            subtitle=subtitle,
        )
        return chart

    def _plot_descriptive(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
        subtitle: Optional[List[str]] = None,
    ) -> alt.Chart:
        descriptive_chart: alt.Chart = self.get_line_chart(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
            subtitle=subtitle,
        )
        return descriptive_chart

    def _plot_prescriptive(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
        subtitle: Optional[List[str]] = None,
    ) -> alt.Chart:
        prescriptive_chart: alt.Chart = (
            self.get_expect_domain_values_to_be_between_chart(
                df=df,
                metric_name=metric_name,
                metric_type=metric_type,
                domain_name=domain_name,
                domain_type=domain_type,
                subtitle=subtitle,
            )
        )
        return prescriptive_chart
