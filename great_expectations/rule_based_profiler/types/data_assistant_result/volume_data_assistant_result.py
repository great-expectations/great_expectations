from typing import Callable, Dict, KeysView, List

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
    ) -> None:
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "display()" for presentation.
        """
        # TODO: <Alex>ALEX Currently, only one Domain key (with domain_type of MetricDomainTypes.TABLE) is utilized; enhancements may require additional Domain key(s) with different domain_type value(s) to be incorporated.</Alex>
        # noinspection PyTypeChecker
        attributed_metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]] = dict(
            filter(
                lambda element: element[0].domain_type == MetricDomainTypes.TABLE,
                self.get_attributed_metrics_by_domain().items(),
            )
        )

        # TODO: <Alex>ALEX Currently, only one Domain key (with domain_type of MetricDomainTypes.TABLE) is utilized; enhancements may require additional Domain key(s) with different domain_type value(s) to be incorporated.</Alex>
        attributed_values_by_metric_name: Dict[str, ParameterNode] = list(
            attributed_metrics_by_domain.values()
        )[0]

        # Altair does not accept periods.
        # TODO: <Alex>ALEX Currently, only one Domain key (with domain_type of MetricDomainTypes.TABLE) is utilized; enhancements may require additional Domain key(s) with different domain_type value(s) to be incorporated.</Alex>
        metric_name: str = list(attributed_values_by_metric_name.keys())[0].replace(
            ".", "_"
        )
        domain_name: str = "batch"

        # available data types: https://altair-viz.github.io/user_guide/encoding.html#encoding-data-types
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

        plot_impl: Callable
        charts: List[alt.Chart] = []

        expectation_configurations: List[
            ExpectationConfiguration
        ] = self.expectation_suite.expectations
        expectation_configuration: ExpectationConfiguration
        if prescriptive:
            for expectation_configuration in expectation_configurations:
                if (
                    expectation_configuration.expectation_type
                    == "expect_table_row_count_to_be_between"
                ):
                    for (
                        kwarg_name,
                        kwarg_value,
                    ) in expectation_configuration.kwargs.items():
                        df[kwarg_name] = kwarg_value

            plot_impl = self._plot_prescriptive
        else:
            plot_impl = self._plot_descriptive

        table_row_count_chart: alt.Chart = plot_impl(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
        )

        charts.append(table_row_count_chart)

        self.display(charts=charts)

    def _plot_descriptive(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
    ) -> alt.Chart:
        descriptive_chart: alt.Chart = self.get_line_chart(
            df=df,
            metric_name=metric_name,
            metric_type=metric_type,
            domain_name=domain_name,
            domain_type=domain_type,
        )
        return descriptive_chart

    def _plot_prescriptive(
        self,
        df: pd.DataFrame,
        metric_name: str,
        metric_type: alt.StandardType,
        domain_name: str,
        domain_type: alt.StandardType,
    ) -> alt.Chart:
        prescriptive_chart: alt.Chart = (
            self.get_expect_domain_values_to_be_between_chart(
                df=df,
                metric_name=metric_name,
                metric_type=metric_type,
                domain_name=domain_name,
                domain_type=domain_type,
            )
        )
        return prescriptive_chart
