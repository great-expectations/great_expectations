from __future__ import annotations

import copy
import datetime
import json
import os
from collections import defaultdict, namedtuple
from dataclasses import asdict, dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    KeysView,
    List,
    Optional,
    Set,
    Union,
)

import altair as alt
import ipywidgets as widgets
import numpy as np
import pandas as pd
from IPython.display import HTML, display

from great_expectations import __version__ as ge_version
from great_expectations import exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    get_expectation_suite_usage_statistics,
    usage_statistics_enabled_method,
)
from great_expectations.core.util import (
    convert_to_json_serializable,
    in_jupyter_notebook,
    nested_update,
)
from great_expectations.rule_based_profiler.altair import AltairDataTypes, AltairThemes
from great_expectations.rule_based_profiler.data_assistant_result.plot_components import (
    BatchPlotComponent,
    DomainPlotComponent,
    ExpectationKwargPlotComponent,
    MetricPlotComponent,
    PlotComponent,
    determine_plot_title,
)
from great_expectations.rule_based_profiler.data_assistant_result.plot_result import (
    PlotMode,
    PlotResult,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_or_create_expectation_suite,
    sanitize_parameter_name,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    ParameterNode,
)
from great_expectations.types import (
    FontFamily,
    FontFamilyURL,
    SecondaryColors,
    SerializableDictDot,
    TintsAndShades,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationConfiguration,
        ExpectationSuite,
    )
    from great_expectations.rule_based_profiler.config import (
        RuleBasedProfilerConfig,
        RuleConfig,
    )
    from great_expectations.rule_based_profiler.metric_computation_result import (
        MetricValues,
    )

ColumnDataFrame = namedtuple("ColumnDataFrame", ["column", "df"])


@dataclass
class RuleStats(SerializableDictDot):
    """
    This class encapsulates basic "Rule" execution statistics.
    """

    num_domains: int = 0
    domains_count_by_domain_type: Dict[MetricDomainTypes, int] = field(
        default_factory=dict
    )
    domains_by_domain_type: Dict[MetricDomainTypes, List[dict]] = field(
        default_factory=dict
    )
    num_parameter_builders: int = 0
    num_expectation_configuration_builders: int = 0
    rule_domain_builder_execution_time: Optional[float] = None
    rule_execution_time: Optional[float] = None

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())


@public_api
@dataclass
class DataAssistantResult(SerializableDictDot):
    """Result from a Data Assistant run, plus plotting functionality.

    Args:
        profiler_config: Effective Rule-Based Profiler configuration.
        profiler_execution_time: Effective Rule-Based Profiler overall execution time in seconds.
        rule_domain_builder_execution_time: Effective Rule-Based Profiler per-Rule DomainBuilder execution time in seconds.
        rule_execution_time: Effective Rule-Based Profiler per-Rule execution time in seconds.
        metrics_by_domain: Metrics by Domain.
        expectation_configurations: Expectation configurations.
        citation: Citations.
        _batch_id_to_batch_identifier_display_name_map: Mapping from "batch_id" values to friendly display names.
    """

    ALLOWED_KEYS = {
        "_batch_id_to_batch_identifier_display_name_map",
        "profiler_config",
        "profiler_execution_time",
        "rule_domain_builder_execution_time",
        "rule_execution_time",
        "metrics_by_domain",
        "expectation_configurations",
        "citation",
    }

    IN_JUPYTER_NOTEBOOK_KEYS = {
        "profiler_execution_time",
    }

    _batch_id_to_batch_identifier_display_name_map: Optional[
        Dict[str, Set[tuple[str, Any]]]
    ] = field(default=None)
    profiler_config: Optional[RuleBasedProfilerConfig] = None
    profiler_execution_time: Optional[float] = None
    rule_domain_builder_execution_time: Optional[Dict[str, float]] = None
    rule_execution_time: Optional[Dict[str, float]] = None
    metrics_by_domain: Optional[Dict[Domain, Dict[str, ParameterNode]]] = None
    expectation_configurations: Optional[List[ExpectationConfiguration]] = None
    citation: Optional[dict] = None
    # Reference to "UsageStatisticsHandler" object for this "DataAssistantResult" object (if configured).
    _usage_statistics_handler: Optional[UsageStatisticsHandler] = field(default=None)

    @property
    def metric_expectation_map(self) -> Dict[Union[str, tuple[str, ...]], str]:
        """
        A mapping is defined for which metrics to plot and their associated expectations.
        """
        raise NotImplementedError("Subclasses must implement this property.")

    @property
    def metric_types(self) -> Dict[str, AltairDataTypes]:
        """
        A mapping is defined for the Altair data type associated with each metric.
        """
        raise NotImplementedError("Subclasses must implement this property.")

    def show_expectations_by_domain_type(
        self,
        expectation_suite_name: Optional[str] = None,
        include_profiler_config: bool = False,
        send_usage_event: bool = True,
    ) -> None:
        """
        Populates named "ExpectationSuite" with "ExpectationConfiguration" list, stored in "DataAssistantResult" object,
        and displays this "ExpectationConfiguration" list, grouped by "domain_type", in predetermined order.
        """
        self.get_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            include_profiler_config=include_profiler_config,
            send_usage_event=send_usage_event,
        ).show_expectations_by_domain_type()

    @public_api
    def show_expectations_by_expectation_type(
        self,
        expectation_suite_name: Optional[str] = None,
        include_profiler_config: bool = False,
        send_usage_event: bool = True,
    ) -> None:
        """Populates an `ExpectationSuite` and displays `ExpectationConfiguration` list grouped by `expectation_type`.

        Args:
            expectation_suite_name: The name for the Expectation Suite. Default generated if none provided.
            include_profiler_config: Whether to include the rule-based profiler config used by the data assistant to
                generate the Expectation Suite.
            send_usage_event: Set to False to disable sending usage events for this method.
        """
        self.get_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            include_profiler_config=include_profiler_config,
            send_usage_event=send_usage_event,
        ).show_expectations_by_expectation_type()

    @public_api
    def get_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        include_profiler_config: bool = False,
        send_usage_event: bool = True,
    ) -> ExpectationSuite:
        """Get Expectation Suite from "DataAssistantResult" object.

        Args:
            expectation_suite_name: The name for the Expectation Suite. Default generated if none provided.
            include_profiler_config: Whether to include the rule-based profiler config used by the data assistant to generate the Expectation Suite.
            send_usage_event: Set to False to disable sending usage events for this method.

        Returns:
            ExpectationSuite object.

        """
        if send_usage_event:
            return self._get_expectation_suite_with_usage_statistics(
                expectation_suite_name=expectation_suite_name,
                include_profiler_config=include_profiler_config,
            )

        return self._get_expectation_suite_without_usage_statistics(
            expectation_suite_name=expectation_suite_name,
            include_profiler_config=include_profiler_config,
        )

    def to_dict(self) -> dict:
        """
        Returns: This DataAssistantResult as dictionary (JSON-serializable dictionary for DataAssistantResult objects).
        """
        domain: Domain
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        expectation_configuration: ExpectationConfiguration
        return {
            "_batch_id_to_batch_identifier_display_name_map": convert_to_json_serializable(
                data=self._batch_id_to_batch_identifier_display_name_map
            ),
            "profiler_config": self.profiler_config.to_json_dict()
            if self.profiler_config
            else None,
            "profiler_execution_time": convert_to_json_serializable(
                data=self.profiler_execution_time
            ),
            "rule_domain_builder_execution_time": convert_to_json_serializable(
                data=self.rule_domain_builder_execution_time
            ),
            "rule_execution_time": convert_to_json_serializable(
                data=self.rule_execution_time
            ),
            "metrics_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "parameter_values_for_fully_qualified_parameter_names": convert_to_json_serializable(
                        data=parameter_values_for_fully_qualified_parameter_names
                    ),
                }
                for domain, parameter_values_for_fully_qualified_parameter_names in self.metrics_by_domain.items()
            ]
            if self.metrics_by_domain
            else None,
            "expectation_configurations": [
                expectation_configuration.to_json_dict()
                for expectation_configuration in self.expectation_configurations
            ]
            if self.expectation_configurations
            else None,
            "citation": convert_to_json_serializable(data=self.citation),
        }

    @public_api
    def to_json_dict(self) -> dict:
        """Returns JSON dictionary equivalent of this object.

        Returns:
            A JSON-serializable dictionary representation of the DataAssistantResult object.
        """
        return self.to_dict()

    def __dir__(self) -> List[str]:
        """
        This custom magic method is used to enable tab completion on "DataAssistantResult" objects.
        """
        return list(
            DataAssistantResult.ALLOWED_KEYS
            | {
                "get_expectation_suite",
                "show_expectations_by_domain_type",
                "show_expectations_by_expectation_type",
                "plot_metrics",
                "plot_expectations_and_metrics",
            }
        )

    def __repr__(self) -> str:
        """
        # TODO: <Alex>6/23/2022</Alex>
        This implementation is non-ideal (it was agreed to employ it for development expediency).  A better approach
        would consist of "__str__()" calling "__repr__()", while all output options are handled through state variables.
        """
        json_dict: dict = self.to_json_dict()
        if in_jupyter_notebook():
            key: str
            value: Any
            json_dict = {
                key: value
                for key, value in json_dict.items()
                if key in DataAssistantResult.IN_JUPYTER_NOTEBOOK_KEYS
            }

            verbose_from_env: str = str(
                os.getenv("GE_TROUBLESHOOTING", False)  # noqa: PLW1508
            ).lower()
            if verbose_from_env != "true":
                verbose_from_env = "false"

            verbose: bool = json.loads(verbose_from_env)

            auxiliary_profiler_execution_details: dict = (
                self._get_auxiliary_profiler_execution_details(verbose=verbose)
            )
            json_dict.update(auxiliary_profiler_execution_details)

        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>6/23/2022</Alex>
        This implementation is non-ideal (it was agreed to employ it for development expediency).  A better approach
        would consist of "__str__()" calling "__repr__()", while all output options are handled through state variables.
        """
        json_dict: dict = self.to_json_dict()
        auxiliary_profiler_execution_details: dict = (
            self._get_auxiliary_profiler_execution_details(verbose=True)
        )
        json_dict.update(auxiliary_profiler_execution_details)
        return json.dumps(json_dict, indent=2)

    def _get_metric_expectation_map(self) -> dict[tuple[str, ...], str]:
        if not all(
            [
                isinstance(metric_names, str)  # noqa: PLR1701
                or isinstance(metric_names, tuple)
            ]
            for metric_names in self.metric_expectation_map.keys()
        ):
            raise gx_exceptions.DataAssistantResultExecutionError(
                "All metric_expectation_map keys must be of type str or tuple."
            )

        return {
            (
                (metric_names,) if isinstance(metric_names, str) else metric_names
            ): expectation_name
            for metric_names, expectation_name in self.metric_expectation_map.items()
        }

    def _get_auxiliary_profiler_execution_details(self, verbose: bool) -> dict:
        auxiliary_info: dict = {
            "num_profiler_rules": len(self.profiler_config.rules)
            if self.profiler_config and self.profiler_config.rules
            else 0,
            "num_expectation_configurations": len(self.expectation_configurations)
            if self.expectation_configurations
            else 0,
            "auto_generated_at": datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            ),
            "great_expectations_version": ge_version,
        }

        if verbose:
            rule_name_to_rule_stats_map: Dict[str, RuleStats] = {}

            rule_domains: List[Domain] = (
                list(self.metrics_by_domain.keys()) if self.metrics_by_domain else []
            )

            rule_stats: RuleStats | None
            domains: List[Domain]
            domain: Domain
            domain_as_json_dict: dict
            num_domains: int

            if not (self.profiler_config and self.profiler_config.rules):
                return auxiliary_info

            rule_name: str
            rule_config: Union[RuleConfig, dict]
            for rule_name, rule_config in self.profiler_config.rules.items():
                domains = list(
                    filter(
                        lambda element: element.rule_name == rule_name,
                        rule_domains,
                    )
                )
                num_domains = len(domains)

                rule_stats = rule_name_to_rule_stats_map.get(rule_name)
                if rule_stats is None:
                    rule_stats = RuleStats(
                        num_domains=num_domains,
                        num_parameter_builders=len(rule_config["parameter_builders"]),
                        num_expectation_configuration_builders=len(
                            rule_config["expectation_configuration_builders"]
                        ),
                    )
                    rule_name_to_rule_stats_map[rule_name] = rule_stats
                    rule_stats.rule_domain_builder_execution_time = (
                        self.rule_domain_builder_execution_time[rule_name]
                        if self.rule_domain_builder_execution_time
                        else float(np.nan)
                    )
                    rule_stats.rule_execution_time = (
                        self.rule_execution_time[rule_name]
                        if self.rule_execution_time
                        else float(np.nan)
                    )

                if num_domains > 0:
                    for domain in domains:
                        if (
                            rule_stats.domains_count_by_domain_type.get(
                                domain.domain_type
                            )
                            is None
                        ):
                            rule_stats.domains_count_by_domain_type[
                                domain.domain_type
                            ] = 0

                        if (
                            rule_stats.domains_by_domain_type.get(domain.domain_type)
                            is None
                        ):
                            rule_stats.domains_by_domain_type[domain.domain_type] = []

                        rule_stats.domains_count_by_domain_type[domain.domain_type] += 1

                        domain_as_json_dict = domain.to_json_dict()
                        domain_as_json_dict.pop("domain_type")
                        domain_as_json_dict.pop("rule_name")
                        rule_stats.domains_by_domain_type[domain.domain_type].append(
                            domain_as_json_dict
                        )

                auxiliary_info.update(
                    convert_to_json_serializable(data=rule_name_to_rule_stats_map)
                )

        return auxiliary_info

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_ASSISTANT_RESULT_GET_EXPECTATION_SUITE,
        args_payload_fn=get_expectation_suite_usage_statistics,
    )
    def _get_expectation_suite_with_usage_statistics(
        self,
        expectation_suite_name: Optional[str] = None,
        include_profiler_config: bool = False,
    ) -> ExpectationSuite:
        """
        Returns: "ExpectationSuite" object, built from properties, populated into this "DataAssistantResult" object.
        Side Effects: One usage statistics event (specified in "usage_statistics_enabled_method" decorator) is emitted.
        """
        return self._get_expectation_suite_without_usage_statistics(
            expectation_suite_name=expectation_suite_name,
            include_profiler_config=include_profiler_config,
        )

    def _get_expectation_suite_without_usage_statistics(
        self,
        expectation_suite_name: Optional[str] = None,
        include_profiler_config: bool = False,
    ) -> ExpectationSuite:
        """
        Returns: "ExpectationSuite" object, built from properties, populated into this "DataAssistantResult" object.
        Side Effects: None -- no usage statistics event is emitted.
        """
        expectation_suite: ExpectationSuite = get_or_create_expectation_suite(
            data_context=None,
            expectation_suite=None,
            expectation_suite_name=expectation_suite_name,
            component_name=self.__class__.__name__,
            persist=False,
        )
        expectation_suite.add_expectation_configurations(
            expectation_configurations=self.expectation_configurations
            if self.expectation_configurations
            else [],
            send_usage_event=False,
            match_type="domain",
            overwrite_existing=True,
        )

        citation: Dict[str, Any] = self.citation or {}
        if not include_profiler_config:
            key: str
            value: Any
            citation = {
                key: value
                for key, value in citation.items()
                if key != "profiler_config"
            }

        expectation_suite.add_citation(**citation)

        return expectation_suite

    @public_api
    def plot_metrics(
        self,
        sequential: bool = True,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """Use contents of `DataAssistantResult` object to display metrics for visualization purposes.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            sequential: Whether the batches are sequential or not.
            theme: Altair top-level chart configuration dictionary.
            include_column_names: Columns to include in metrics plot.
            exclude_column_names: Columns to exclude from metrics plot.

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

    @public_api
    def plot_expectations_and_metrics(
        self,
        sequential: bool = True,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """Use contents of `DataAssistantResult` object to display metrics and expectations for visualization purposes.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            sequential: Whether the batches are sequential or not.
            theme: Altair top-level chart configuration dictionary.
            include_column_names: Columns to include in expectations and metrics plot.
            exclude_column_names: Columns to exclude from expectations and metrics plot.

        Returns:
            PlotResult wrapper object around Altair charts.
        """
        return self._plot(
            plot_mode=PlotMode.DIAGNOSTIC,
            sequential=sequential,
            theme=theme,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
        )

    def _plot(  # noqa: PLR0913
        self,
        plot_mode: PlotMode,
        sequential: bool,
        theme: Optional[Dict[str, Any]] = None,
        include_column_names: Optional[List[str]] = None,
        exclude_column_names: Optional[List[str]] = None,
    ) -> PlotResult:
        """
        VolumeDataAssistant-specific plots are defined with Altair and passed to "_display()" for presentation.
        Display Charts are condensed and interactive while Return Charts are separated into an individual chart for
        each metric-domain/expectation-domain combination.

        Altair theme configuration reference:
            https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration

        Args:
            plot_mode: Type of plot to generate, diagnostic or descriptive
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

        display_charts: List[Union[alt.Chart, alt.LayerChart, alt.VConcatChart]] = []
        return_charts: List[Union[alt.Chart, alt.LayerChart]] = []

        expectation_configurations: list[ExpectationConfiguration] = (
            self.expectation_configurations or []
        )

        table_domain_charts: List[
            Union[alt.Chart, alt.LayerChart]
        ] = self._plot_table_domain_charts(
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
            sequential=sequential,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
        )
        display_charts.extend(table_domain_charts)
        return_charts.extend(table_domain_charts)

        column_domain_display_charts: List[alt.VConcatChart]
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

        self._display(charts=display_charts, plot_mode=plot_mode, theme=theme)

        return_charts = self._apply_theme(charts=return_charts, theme=theme)
        return PlotResult(charts=return_charts)

    def _display(
        self,
        charts: Union[List[alt.Chart], List[alt.VConcatChart]],
        plot_mode: PlotMode,
        theme: Optional[Dict[str, Any]] = None,
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
            altair_theme = self._get_theme(theme=theme)
        else:
            altair_theme = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)

        themed_charts: List[alt.Chart] = self._apply_theme(
            charts=charts, theme=altair_theme
        )

        chart_titles: List[str] = self._get_chart_titles(charts=themed_charts)

        if chart_titles:
            metric_plot_count = self._get_metric_plot_count(charts=themed_charts)
            if plot_mode == plot_mode.DIAGNOSTIC:
                expectations_produced = (
                    len(self.expectation_configurations)
                    if self.expectation_configurations
                    else 0
                )
                print(
                    f"""{expectations_produced} Expectations produced, {metric_plot_count} Expectation and Metric plots implemented
Use DataAssistantResult.show_expectations_by_domain_type() or
DataAssistantResult.show_expectations_by_expectation_type() to show all produced Expectations"""
                )
            else:
                metrics_count: int = 0
                if self.metrics_by_domain is not None:
                    metrics_count = sum(
                        len(metrics) for metrics in self.metrics_by_domain.values()
                    )
                print(
                    f"""{metrics_count} Metrics calculated, {metric_plot_count} Metric plots implemented
Use DataAssistantResult.metrics_by_domain to show all calculated Metrics"""
                )

            display_chart_dict: Dict[str, Union[alt.Chart, alt.LayerChart]] = {
                " ": None
            }
            for idx in range(len(chart_titles)):
                display_chart_dict[chart_titles[idx]] = themed_charts[idx]

            dropdown_title_color: str = altair_theme["legend"]["titleColor"]
            dropdown_font_size: str = altair_theme["axis"]["titleFontSize"]
            dropdown_font_weight: int = altair_theme["title"]["subtitleFontWeight"]
            dropdown_text_color: str = altair_theme["axis"]["labelColor"]

            font_family: str = altair_theme["font"]

            gx_font_family = True
            try:
                FontFamily(font_family)
            except ValueError:
                gx_font_family = False

            if gx_font_family:
                font_family_url = FontFamilyURL[FontFamily(font_family).name].value

                title_font_weight: int = altair_theme["title"]["fontWeight"]
                subtitle_font_weight: int = altair_theme["title"]["subtitleFontWeight"]
                url_font_weights: str = ";".join(
                    [str(subtitle_font_weight), str(title_font_weight)]
                )

                font_url = f"{font_family_url}:wght@{url_font_weights}&display=swap"

                font_css = f"""
                <style>
                @import url('{font_url}');
                </style>
                """
                display(HTML(font_css))

            # Altair does not have a way to format the dropdown input so the rendered CSS must be altered directly
            altair_dropdown_css: str = f"""
                <style>
                span.vega-bind-name {{
                    color: {dropdown_title_color};
                    font-family: {font_family};
                    font-size: {dropdown_font_size}px;
                    font-weight: {dropdown_font_weight};
                }}
                form.vega-bindings {{
                    color: {dropdown_text_color};
                    font-family: {font_family};
                    font-size: {dropdown_font_size}px;
                    font-weight: {dropdown_font_weight};
                    position: absolute;
                    left: 75px;
                    top: 28px;
                }}
                </style>
            """
            display(HTML(altair_dropdown_css))

            # max rows for Altair charts is set to 5,000 without this
            alt.data_transformers.disable_max_rows()

            ipywidgets_dropdown_css: str = f"""
                <style>
                .widget-inline-hbox .widget-label {{
                    color: {dropdown_title_color};
                    font-family: {font_family};
                    font-size: {dropdown_font_size}px;
                    font-weight: {dropdown_font_weight};
                }}
                .widget-dropdown > select {{
                    padding-right: 21px;
                    padding-left: 3px;
                    color: {dropdown_text_color};
                    font-family: {font_family};
                    font-size: {dropdown_font_size}px;
                    font-weight: {dropdown_font_weight};
                    height: 20px;
                    line-height: {dropdown_font_size}px;
                    background-size: 20px;
                    border-radius: 2px;
                }}
                </style>
            """
            display(HTML(ipywidgets_dropdown_css))

            dropdown_selection: widgets.Dropdown = widgets.Dropdown(
                options=chart_titles,
                description="Select Plot Type: ",
                style={"description_width": "initial"},
                layout={"width": "max-content", "margin": "0px"},
            )

            widgets.interact(
                DataAssistantResult._display_chart_from_dict,
                display_chart_dict=widgets.fixed(display_chart_dict),
                chart_title=dropdown_selection,
            )

    @staticmethod
    def _display_chart_from_dict(
        display_chart_dict: Dict[str, Union[alt.Chart, alt.LayerChart]],
        chart_title: str,
    ) -> None:
        display_chart_dict[chart_title].display()

    @staticmethod
    def _get_chart_layer_title(
        layer: Union[alt.Chart, alt.LayerChart]
    ) -> Optional[str]:
        """Recursively searches through the chart layers for a title and returns one if it exists."""
        chart_title: Optional[str] = None
        if isinstance(layer.title, str):
            chart_title = layer.title
        else:
            try:
                chart_title = layer.title.text
            except AttributeError:
                try:
                    for chart_layer in layer.layer:
                        chart_title = DataAssistantResult._get_chart_layer_title(
                            layer=chart_layer
                        )
                        if chart_title is not None:
                            return chart_title
                except AttributeError:
                    return None
        return chart_title

    @staticmethod
    def _get_chart_titles(charts: List[Union[alt.Chart, alt.LayerChart]]) -> List[str]:
        """Recursively searches through each chart layer for a title and returns a list of titles."""
        chart_titles: List[str] = []
        chart_title: Optional[str]
        for chart in charts:
            chart_title = DataAssistantResult._get_chart_layer_title(layer=chart)
            if chart_title is None:
                raise gx_exceptions.DataAssistantResultExecutionError(
                    "All DataAssistantResult charts must have a title."
                )

            chart_titles.append(chart_title)

        return chart_titles

    @staticmethod
    def _get_metric_plot_count(charts: List[Union[alt.Chart, alt.LayerChart]]) -> int:
        plot_count = 0
        for chart in charts:
            if "column" in chart.data.columns:
                plot_count += chart.data.column.nunique()
            else:
                plot_count += 1
        return plot_count

    @staticmethod
    def _apply_theme(
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
    def _transform_column_lists_to_rows(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        col_has_list: pd.DataFrame = pd.DataFrame(
            {"column_name": df.columns, "has_list": (df.applymap(type) == list).any()}
        )
        list_column_names: List[str] = list(
            col_has_list[col_has_list["has_list"]]["column_name"]
        )

        if (
            "table_columns" in list_column_names
            and len(np.unique(df["table_columns"])) == 1
        ):
            df_transformed = df.iloc[:1]
        else:
            column_name: str
            cols_flat: List[List[str]] = []
            for idx, column_name in enumerate(list_column_names):
                # explode list of column names into separate rows for each name in list
                # flatten columns of lists
                cols_flat.append(
                    [item for sublist in df[column_name] for item in sublist]
                )

            # row numbers to repeat
            ilocations: List[int] = list(
                np.repeat(range(df.shape[0]), df[list_column_names[0]].apply(len))
            )
            # replicate rows and add flattened column of lists
            columns: List[int] = [
                idx
                for idx, col in enumerate(df.columns)
                if col not in list_column_names
            ]
            df_new_shape = df.iloc[ilocations, columns].reset_index(drop=True)
            cols_flat_df: pd.DataFrame = pd.DataFrame(cols_flat).T
            cols_flat_df.columns = list_column_names
            df_transformed = pd.concat([df_new_shape, cols_flat_df], axis=1)

            if "table_columns" in list_column_names:
                # create column number by encoding the categorical column name and adding 1 since encoding starts at 0
                df_transformed["column_number"] = (
                    pd.factorize(df_transformed["table_columns"])[0] + 1
                )

            if "value_ranges" in list_column_names:
                # split value ranges into two columns
                df_transformed["min_value"] = 0
                df_transformed["max_value"] = 0
                df_transformed[["min_value", "max_value"]] = df_transformed[
                    "value_ranges"
                ].values.tolist()
                df_transformed = df_transformed.drop(columns=["value_ranges"], axis=1)

        return df_transformed

    @staticmethod
    def _get_column_set_text(column_set: List[str]) -> tuple[str, int]:
        dy: int
        if len(column_set) > 50:  # noqa: PLR2004
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
    def _get_nominal_metrics_chart(
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            sanitized_metric_names: A set containing the names of the metrics as they exist in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair chart for nominal metrics
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in df.columns
            if column not in (sanitized_metric_names | {batch_name})
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_plot_component = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        metric_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        column_set: Optional[List[str]] = None
        metric_plot_component: MetricPlotComponent
        metric_plot_components: List[MetricPlotComponent] = []
        for sanitized_metric_name in sanitized_metric_names:
            if sanitized_metric_name == "table_columns" and len(df.index) == 1:
                column_set = df[sanitized_metric_name].iloc[0]

            metric_plot_component = MetricPlotComponent(
                name=sanitized_metric_name,
                alt_type=metric_type,
            )

            metric_plot_components.append(metric_plot_component)

        column_number: str = "column_number"

        column_number_plot_component = PlotComponent(
            name=column_number,
            alt_type=AltairDataTypes.ORDINAL.value,
        )

        domain_plot_component = DomainPlotComponent(
            alt_type=AltairDataTypes.NOMINAL.value,
            subtitle=subtitle,
        )

        if sequential:
            return DataAssistantResult._get_sequential_isotype_chart(
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
                column_number_plot_component=column_number_plot_component,
                column_set=column_set,
            )
        else:
            return DataAssistantResult._get_nonsequential_isotype_chart(
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
                column_number_plot_component=column_number_plot_component,
                column_set=column_set,
            )

    @staticmethod
    def _get_expect_domain_values_to_match_set_chart(
        expectation_type: str,
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.Chart:
        """
        Args:
            expectation_type: The name of the expectation
            df: A pandas dataframe containing the data to be plotted
            sanitized_metric_names: A set containing the names of the metrics as they exist in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair chart for nominal metrics
        """
        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in df.columns
            if column not in (sanitized_metric_names | {batch_name})
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_plot_component = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        metric_type: alt.StandardType = AltairDataTypes.NOMINAL.value
        column_set: Optional[List[str]] = None
        metric_plot_component: MetricPlotComponent
        metric_plot_components: List[MetricPlotComponent] = []
        for sanitized_metric_name in sanitized_metric_names:
            if sanitized_metric_name == "table_columns" and len(df.index) == 1:
                column_set = df[sanitized_metric_name].iloc[0]

            metric_plot_component = MetricPlotComponent(
                name=sanitized_metric_name,
                alt_type=metric_type,
            )

            metric_plot_components.append(metric_plot_component)

        column_number: str = "column_number"

        column_number_plot_component = PlotComponent(
            name=column_number,
            alt_type=AltairDataTypes.ORDINAL.value,
        )

        domain_plot_component = DomainPlotComponent(
            alt_type=AltairDataTypes.NOMINAL.value,
            subtitle=subtitle,
        )

        if sequential:
            return DataAssistantResult._get_sequential_expect_domain_values_to_match_set_isotype_chart(
                expectation_type=expectation_type,
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
                column_number_plot_component=column_number_plot_component,
                column_set=column_set,
            )
        else:
            return DataAssistantResult._get_nonsequential_expect_domain_values_to_match_set_isotype_chart(
                expectation_type=expectation_type,
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
                column_number_plot_component=column_number_plot_component,
                column_set=column_set,
            )

    @staticmethod
    def _get_sequential_isotype_chart(  # noqa: PLR0913
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        column_number_plot_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = batch_plot_component.generate_tooltip() + [
            metric_plot_component.generate_tooltip()
            for metric_plot_component in metric_plot_components
        ]

        chart: Union[alt.Chart, alt.LayerChart]
        if column_set is None:
            chart = (
                alt.Chart(data=df, title=title)
                .mark_point(color=SecondaryColors.MIDNIGHT_BLUE)
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(grid=False),
                    ),
                    y=alt.Y(
                        column_number_plot_component.name,
                        type=column_number_plot_component.alt_type,
                        title=column_number_plot_component.title,
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
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=None,
                        axis=alt.Axis(labels=False, ticks=False, grid=False),
                    ),
                    y=alt.Y(
                        column_number_plot_component.name,
                        type=column_number_plot_component.alt_type,
                        title=" ",
                        axis=alt.Axis(labels=False, ticks=False),
                    ),
                )
            ).mark_text(
                text=text,
                color=SecondaryColors.MIDNIGHT_BLUE,
                lineBreak=r"$",
                dy=dy,
            )

        return chart

    @staticmethod
    def _get_nonsequential_isotype_chart(  # noqa: PLR0913
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        column_number_plot_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = batch_plot_component.generate_tooltip() + [
            metric_plot_component.generate_tooltip()
            for metric_plot_component in metric_plot_components
        ]

        chart: alt.Chart
        if column_set is None:
            chart = (
                alt.Chart(data=df, title=title)
                .mark_point(color=SecondaryColors.MIDNIGHT_BLUE)
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=alt.Y(
                        column_number_plot_component.name,
                        type=column_number_plot_component.alt_type,
                        title=column_number_plot_component.title,
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
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=None,
                        axis=alt.Axis(labels=False, ticks=False, grid=False),
                    ),
                    y=alt.Y(
                        column_number_plot_component.name,
                        type=column_number_plot_component.alt_type,
                        title=" ",
                        axis=alt.Axis(labels=False, ticks=False),
                    ),
                )
            ).mark_text(
                text=text,
                color=SecondaryColors.MIDNIGHT_BLUE,
                lineBreak=r"$",
                dy=dy,
            )

        return chart

    @staticmethod
    def _get_sequential_expect_domain_values_to_match_set_isotype_chart(  # noqa: PLR0913
        expectation_type: str,
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        column_number_plot_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        chart: alt.Chart = DataAssistantResult._get_sequential_isotype_chart(
            df=df,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
            column_number_plot_component=column_number_plot_component,
            column_set=column_set,
        ).properties(title=title)

        return chart

    @staticmethod
    def _get_nonsequential_expect_domain_values_to_match_set_isotype_chart(  # noqa: PLR0913
        expectation_type: str,
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        column_number_plot_component: PlotComponent,
        column_set: Optional[List[str]],
    ) -> alt.Chart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        chart: alt.Chart = DataAssistantResult._get_nonsequential_isotype_chart(
            df=df,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
            column_number_plot_component=column_number_plot_component,
            column_set=column_set,
        ).properties(title=title)

        return chart

    @staticmethod
    def _get_quantitative_metrics_chart(
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str] = None,
    ) -> alt.LayerChart:
        """
        Args:
            df: A pandas dataframe containing the data to be plotted
            sanitized_metric_names: A set containing the names of the metrics as they exist in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart
        """
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        metric_plot_component: MetricPlotComponent
        metric_plot_components: List[MetricPlotComponent] = []
        for sanitized_metric_name in sanitized_metric_names:
            metric_plot_component = MetricPlotComponent(
                name=sanitized_metric_name, alt_type=metric_type
            )
            metric_plot_components.append(metric_plot_component)

        batch_name: str = "batch"
        batch_identifiers: List[str] = [
            column
            for column in df.columns
            if column not in (sanitized_metric_names | {batch_name})
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_plot_component = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        domain_plot_component = DomainPlotComponent(
            alt_type=AltairDataTypes.NOMINAL.value,
            subtitle=subtitle,
        )

        if sequential:
            return DataAssistantResult._get_line_chart(
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
            )
        else:
            if "column_quantile_values" in df.columns:  # noqa: PLR5501
                return DataAssistantResult._get_range_chart(
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                )
            else:
                return DataAssistantResult._get_bar_chart(
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                )

    @staticmethod
    def _get_expect_domain_values_to_be_between_chart(  # noqa: PLR0912
        expectation_type: str,
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str],
    ) -> Union[alt.Chart, alt.LayerChart]:
        """
        Args:
            expectation_type: The name of the expectation
            df: A pandas dataframe containing the data to be plotted
            sanitized_metric_names: A set containing the names of the metrics as they exist in the pandas dataframe
            sequential: Whether batches are sequential in nature
            subtitle: The subtitle, if applicable

        Returns:
            An altair line chart with confidence intervals corresponding to "between" expectations
        """
        domain_type: MetricDomainTypes
        if subtitle:
            domain_type = MetricDomainTypes.COLUMN
        else:
            domain_type = MetricDomainTypes.TABLE

        column_name: str = "column"
        batch_name: str = "batch"
        max_value: str = "max_value"
        min_value: str = "min_value"
        strict_max: str = "strict_max"
        strict_min: str = "strict_min"
        quantiles: str = "quantiles"
        allow_relative_error: str = "allow_relative_error"
        batch_identifiers: List[str] = [
            column
            for column in df.columns
            if column
            not in (
                sanitized_metric_names
                | {
                    batch_name,
                    column_name,
                    max_value,
                    min_value,
                    strict_min,
                    strict_max,
                    quantiles,
                    allow_relative_error,
                }
            )
        ]

        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_plot_component = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        y_axis_title: str
        if len(sanitized_metric_names) > 1:
            y_axis_title = "Column Values"
        else:
            y_axis_title = list(sanitized_metric_names)[0].replace("_", " ").title()

        metric_plot_component: MetricPlotComponent
        metric_plot_components: List[MetricPlotComponent] = []
        for sanitized_metric_name in sanitized_metric_names:
            metric_plot_component = MetricPlotComponent(
                name=sanitized_metric_name,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                axis_title=y_axis_title,
            )
            metric_plot_components.append(metric_plot_component)

        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent] = []

        expectation_kwarg_plot_components.append(
            ExpectationKwargPlotComponent(
                name=min_value,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                axis_title=y_axis_title,
            )
        )
        expectation_kwarg_plot_components.append(
            ExpectationKwargPlotComponent(
                name=max_value,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                axis_title=y_axis_title,
            )
        )

        domain_plot_component: DomainPlotComponent
        tooltip: List[alt.Tooltip]
        if domain_type == MetricDomainTypes.COLUMN:
            domain_plot_component = DomainPlotComponent(
                name=column_name,
                alt_type=AltairDataTypes.NOMINAL.value,
                subtitle=subtitle,
            )
            if strict_min in df.columns:
                expectation_kwarg_plot_components.append(
                    ExpectationKwargPlotComponent(
                        name=strict_min,
                        alt_type=AltairDataTypes.NOMINAL.value,
                        axis_title=y_axis_title,
                    )
                )
            if strict_max in df.columns:
                expectation_kwarg_plot_components.append(
                    ExpectationKwargPlotComponent(
                        name=strict_max,
                        alt_type=AltairDataTypes.NOMINAL.value,
                        axis_title=y_axis_title,
                    )
                )
            if quantiles in df.columns:
                expectation_kwarg_plot_components.append(
                    ExpectationKwargPlotComponent(
                        name=quantiles,
                        alt_type=AltairDataTypes.QUANTITATIVE.value,
                        axis_title=y_axis_title,
                    )
                )
            if allow_relative_error in df.columns:
                expectation_kwarg_plot_components.append(
                    ExpectationKwargPlotComponent(
                        name=allow_relative_error,
                        alt_type=AltairDataTypes.NOMINAL.value,
                        axis_title=y_axis_title,
                    )
                )
        else:
            domain_plot_component = DomainPlotComponent(
                alt_type=AltairDataTypes.NOMINAL.value,
                subtitle=subtitle,
            )

        if sequential:
            return (
                DataAssistantResult._get_expect_domain_values_to_be_between_line_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                    expectation_kwarg_plot_components=expectation_kwarg_plot_components,
                )
            )
        else:
            if "column_quantile_values" in df.columns:  # noqa: PLR5501
                return DataAssistantResult._get_expect_domain_values_to_be_between_range_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                    expectation_kwarg_plot_components=expectation_kwarg_plot_components,
                )
            else:
                return DataAssistantResult._get_expect_domain_values_to_be_between_bar_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                    expectation_kwarg_plot_components=expectation_kwarg_plot_components,
                )

    @staticmethod
    def _get_interactive_metrics_chart(
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> Union[alt.Chart, alt.LayerChart]:
        """
        Args:
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            sanitized_metric_names: A set containing the names of the metrics as they exist in the pandas dataframe
            sequential: Whether batches are sequential in nature

        Returns:
            An interactive chart
        """
        column_dfs = DataAssistantResult._clean_quantitative_metrics_column_dfs(
            column_dfs=column_dfs, sanitized_metric_names=sanitized_metric_names
        )

        batch_name: str = "batch"
        all_columns: List[str] = DataAssistantResult._get_all_columns_from_column_dfs(
            column_dfs=column_dfs
        )
        batch_identifiers: List[str] = [
            column
            for column in all_columns
            if column not in (sanitized_metric_names | {batch_name})
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_plot_component = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )
        metric_type: alt.StandardType = AltairDataTypes.QUANTITATIVE.value
        metric_plot_component: MetricPlotComponent
        metric_plot_components: List[MetricPlotComponent] = []
        for sanitized_metric_name in sanitized_metric_names:
            metric_plot_component = MetricPlotComponent(
                name=sanitized_metric_name, alt_type=metric_type
            )
            metric_plot_components.append(metric_plot_component)

        domain_name: str = "column"
        domain_plot_component = DomainPlotComponent(
            name=domain_name,
            alt_type=AltairDataTypes.NOMINAL.value,
        )

        df_columns: List[str] = (
            [batch_name, domain_name] + list(sanitized_metric_names) + batch_identifiers
        )

        df: pd.DataFrame = pd.DataFrame(columns=df_columns)
        for column, column_df in column_dfs:
            column_df[domain_name] = column
            df = pd.concat(
                [df, column_df[column_df.columns.intersection(df_columns)]], axis=0
            )

        if sequential:
            return DataAssistantResult._get_interactive_line_chart(
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
            )
        else:
            if "column_quantile_values" in df.columns:  # noqa: PLR5501
                return DataAssistantResult._get_interactive_range_chart(
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                )
            else:
                return DataAssistantResult._get_interactive_bar_chart(
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                )

    @staticmethod  # - complexity 16
    def _get_interactive_expect_column_values_to_be_between_chart(  # noqa: C901, PLR0912, PLR0915
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> Union[alt.LayerChart, alt.VConcatChart]:
        """
        Args:
            expectation_type: The name of the expectation
            column_dfs: A list of tuples pairing pandas dataframes with the columns they correspond to
            sanitized_metric_names: A set containing the names of the metrics as they exist in the pandas dataframe
            sequential: Whether batches are sequential in nature

        Returns:
            An interactive expect_column_values_to_be_between chart
        """
        column_dfs = DataAssistantResult._clean_quantitative_metrics_column_dfs(
            column_dfs=column_dfs, sanitized_metric_names=sanitized_metric_names
        )

        column_name: str = "column"
        min_value: str = "min_value"
        max_value: str = "max_value"
        strict_min: str = "strict_min"
        strict_max: str = "strict_max"
        quantiles: str = "quantiles"
        allow_relative_error: str = "allow_relative_error"

        batch_name: str = "batch"
        all_columns: List[str] = DataAssistantResult._get_all_columns_from_column_dfs(
            column_dfs=column_dfs
        )
        batch_identifiers: List[str] = [
            column
            for column in all_columns
            if column
            not in (
                sanitized_metric_names
                | {
                    batch_name,
                    column_name,
                    min_value,
                    max_value,
                    strict_min,
                    strict_max,
                    quantiles,
                    allow_relative_error,
                }
            )
        ]
        batch_type: alt.StandardType
        if sequential:
            batch_type = AltairDataTypes.ORDINAL.value
        else:
            batch_type = AltairDataTypes.NOMINAL.value
        batch_plot_component = BatchPlotComponent(
            name=batch_name,
            alt_type=batch_type,
            batch_identifiers=batch_identifiers,
        )

        y_axis_title: Optional[str]
        if len(sanitized_metric_names) > 1:
            y_axis_title = "Column Values"
        else:
            y_axis_title = None

        metric_plot_component: MetricPlotComponent
        metric_plot_components: List[MetricPlotComponent] = []
        for sanitized_metric_name in sanitized_metric_names:
            metric_plot_component = MetricPlotComponent(
                name=sanitized_metric_name,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                axis_title=y_axis_title,
            )
            metric_plot_components.append(metric_plot_component)

        domain_plot_component = DomainPlotComponent(
            name="column",
            alt_type=AltairDataTypes.NOMINAL.value,
        )

        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent] = []

        expectation_kwarg_plot_components.append(
            ExpectationKwargPlotComponent(
                name=min_value,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                axis_title=y_axis_title,
            )
        )
        expectation_kwarg_plot_components.append(
            ExpectationKwargPlotComponent(
                name=max_value,
                alt_type=AltairDataTypes.QUANTITATIVE.value,
                axis_title=y_axis_title,
            )
        )

        possible_df_columns: List[str] = (
            [
                batch_name,
            ]
            + batch_identifiers
            + list(sanitized_metric_names)
            + [
                column_name,
                min_value,
                max_value,
                strict_min,
                strict_max,
                quantiles,
                allow_relative_error,
            ]
        )

        df_columns: pd.Index
        df: Optional[pd.DataFrame] = None
        for _, column_df in column_dfs:
            df_columns = column_df.columns.intersection(possible_df_columns)

            if df is None:
                df = pd.DataFrame(columns=df_columns)

            df = pd.concat([df, column_df[df_columns]], axis=0)

        if df is None:
            raise ValueError(
                f"There is no data to plot for this expectation: {expectation_type}"
            )

        strict_min_predicate: bool = False
        if strict_min in df.columns:
            strict_min_predicate = bool(df[strict_min].all())
            expectation_kwarg_plot_components.append(
                ExpectationKwargPlotComponent(
                    name=strict_min,
                    alt_type=AltairDataTypes.NOMINAL.value,
                    axis_title=y_axis_title,
                )
            )

        strict_max_predicate: bool = False
        if strict_max in df.columns:
            strict_max_predicate = bool(df[strict_max].all())
            expectation_kwarg_plot_components.append(
                ExpectationKwargPlotComponent(
                    name=strict_max,
                    alt_type=AltairDataTypes.NOMINAL.value,
                    axis_title=y_axis_title,
                )
            )

        if quantiles in df.columns:
            expectation_kwarg_plot_components.append(
                ExpectationKwargPlotComponent(
                    name=quantiles,
                    alt_type=AltairDataTypes.QUANTITATIVE.value,
                    axis_title=y_axis_title,
                )
            )

        if allow_relative_error in df.columns:
            expectation_kwarg_plot_components.append(
                ExpectationKwargPlotComponent(
                    name=allow_relative_error,
                    alt_type=AltairDataTypes.NOMINAL.value,
                    axis_title=y_axis_title,
                )
            )

        # encode point color based on anomalies
        min_value_predicate: Union[bool, int]
        max_value_predicate: Union[bool, int]
        predicates: List[Union[bool, int]] = []
        for metric_plot_component in metric_plot_components:
            if strict_min_predicate:
                min_value_predicate = (
                    alt.datum.min_value < alt.datum[metric_plot_component.name]
                )
            else:
                min_value_predicate = (
                    alt.datum.min_value <= alt.datum[metric_plot_component.name]
                )

            if strict_max_predicate:
                max_value_predicate = (
                    alt.datum.max_value > alt.datum[metric_plot_component.name]
                )
            else:
                max_value_predicate = (
                    alt.datum.max_value >= alt.datum[metric_plot_component.name]
                )

            predicates.append(min_value_predicate & max_value_predicate)

        if sequential:
            return DataAssistantResult._get_interactive_expect_column_values_to_be_between_line_chart(
                expectation_type=expectation_type,
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
                expectation_kwarg_plot_components=expectation_kwarg_plot_components,
                predicates=predicates,
            )
        else:
            if "column_quantile_values" in df.columns:  # noqa: PLR5501
                return DataAssistantResult._get_interactive_expect_column_values_to_be_between_range_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                    expectation_kwarg_plot_components=expectation_kwarg_plot_components,
                    predicates=predicates,
                )
            else:
                return DataAssistantResult._get_interactive_expect_column_values_to_be_between_bar_chart(
                    expectation_type=expectation_type,
                    df=df,
                    metric_plot_components=metric_plot_components,
                    batch_plot_component=batch_plot_component,
                    domain_plot_component=domain_plot_component,
                    expectation_kwarg_plot_components=expectation_kwarg_plot_components,
                    predicates=predicates,
                )

    @staticmethod
    def _get_line_chart(
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
    ) -> alt.LayerChart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = batch_plot_component.generate_tooltip() + [
            metric_plot_component.generate_tooltip(format=",")
            for metric_plot_component in metric_plot_components
        ]

        quantiles: str = "quantiles"
        lines_and_points_list: List[alt.Chart] = []
        line: alt.Chart
        for metric_plot_component in metric_plot_components:
            if quantiles in df.columns:
                line = (
                    alt.Chart(data=df, title=title)
                    .mark_line()
                    .encode(
                        x=batch_plot_component.plot_on_axis(),
                        y=metric_plot_component.plot_on_axis(),
                        tooltip=tooltip,
                        detail=quantiles,
                    )
                )
            else:
                line = (
                    alt.Chart(data=df, title=title)
                    .mark_line()
                    .encode(
                        x=batch_plot_component.plot_on_axis(),
                        y=metric_plot_component.plot_on_axis(),
                        tooltip=tooltip,
                    )
                )

            points: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_point()
                .encode(
                    x=batch_plot_component.plot_on_axis(),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
            )

            lines_and_points_list.append(line + points)

        return alt.layer(*lines_and_points_list)

    @staticmethod
    def _get_bar_chart(
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
    ) -> alt.LayerChart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = batch_plot_component.generate_tooltip() + [
            metric_plot_component.generate_tooltip(format=",")
            for metric_plot_component in metric_plot_components
        ]

        bars: alt.Chart
        bars_list: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            bars = (
                alt.Chart(data=df, title=title)
                .mark_bar()
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
            )

            bars_list.append(bars)

        return alt.layer(*bars_list)

    @staticmethod
    def _get_range_chart(
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
    ) -> alt.LayerChart:
        title: alt.TitleParams = determine_plot_title(
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = batch_plot_component.generate_tooltip() + [
            metric_plot_component.generate_tooltip(format=",")
            for metric_plot_component in metric_plot_components
        ]

        lines_and_points_list: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            line: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_line()
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                    detail="batch",
                )
            )

            points: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_point()
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
            )

            lines_and_points_list.append(line + points)

        return alt.layer(*lines_and_points_list)

    @staticmethod
    def _get_expect_domain_values_to_be_between_line_chart(  # noqa: PLR0913
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent],
        expectation_type: Optional[str] = None,
    ) -> alt.Chart:
        expectation_kwarg_line_color: alt.HexColor = alt.HexColor(
            TintsAndShades.ROYAL_BLUE_30
        )
        expectation_kwarg_line_stroke_width: int = 5

        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        expectation_kwarg_tooltips: List[alt.Tooltip] = []
        min_value_plot_component: ExpectationKwargPlotComponent
        max_value_plot_component: ExpectationKwargPlotComponent
        for expectation_kwarg_plot_component in expectation_kwarg_plot_components:
            expectation_kwarg_tooltips.append(
                expectation_kwarg_plot_component.generate_tooltip()
            )

            if expectation_kwarg_plot_component.name == "min_value":
                min_value_plot_component = expectation_kwarg_plot_component
            elif expectation_kwarg_plot_component.name == "max_value":
                max_value_plot_component = expectation_kwarg_plot_component

        tooltip: List[alt.Tooltip] = []
        if domain_plot_component.name:
            tooltip.append(domain_plot_component.generate_tooltip())

        tooltip.extend(batch_plot_component.generate_tooltip())
        tooltip.extend(expectation_kwarg_tooltips)
        tooltip.extend(
            [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        lower_limit: alt.Chart
        upper_limit: alt.Chart
        lower_limit, upper_limit = (
            (
                alt.Chart(data=df)
                .mark_line(
                    color=expectation_kwarg_line_color,
                    strokeWidth=expectation_kwarg_line_stroke_width,
                )
                .encode(
                    x=batch_plot_component.plot_on_axis(),
                    y=expectation_kwarg_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
                .properties(title=title)
            )
            for expectation_kwarg_plot_component in (
                min_value_plot_component,
                max_value_plot_component,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=batch_plot_component.plot_on_axis(),
                y=min_value_plot_component.plot_on_axis(),
                y2=alt.Y2(
                    max_value_plot_component.name, title=max_value_plot_component.title
                ),
            )
            .properties(title=title)
        )

        if "quantiles" in df.columns:
            lower_limit = lower_limit.encode(detail="quantiles")
            upper_limit = upper_limit.encode(detail="quantiles")
            band = band.encode(detail="quantiles")

        metric_name: str
        predicate: Union[bool, int]
        anomaly_coded_line: alt.Chart
        anomaly_coded_lines: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            # encode point color based on anomalies
            assert (
                metric_plot_component.name
            ), f"Metric name must be set: {metric_plot_component}"
            metric_name = metric_plot_component.name
            predicate = (
                (alt.datum.min_value > alt.datum[metric_name])
                & (alt.datum.max_value > alt.datum[metric_name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_name])
                & (alt.datum.max_value < alt.datum[metric_name])
            )
            point_color_condition = alt.condition(
                predicate=predicate,
                if_false=alt.value(SecondaryColors.LEAF_GREEN),
                if_true=alt.value(SecondaryColors.POMEGRANATE_PINK),
            )

            anomaly_coded_base = alt.Chart(data=df, title=title)

            anomaly_coded_line = anomaly_coded_base.mark_line().encode(
                x=batch_plot_component.plot_on_axis(),
                y=metric_plot_component.plot_on_axis(),
                tooltip=tooltip,
            )

            if "quantiles" in df.columns:
                anomaly_coded_line = anomaly_coded_line.encode(detail="quantiles")

            anomaly_coded_points = anomaly_coded_base.mark_point().encode(
                x=batch_plot_component.plot_on_axis(),
                y=metric_plot_component.plot_on_axis(),
                tooltip=tooltip,
                color=point_color_condition,
            )
            anomaly_coded_lines.append(anomaly_coded_line + anomaly_coded_points)

        return alt.layer(band, lower_limit, upper_limit, *anomaly_coded_lines)

    @staticmethod
    def _get_expect_domain_values_to_be_between_bar_chart(  # noqa: PLR0913
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent],
        expectation_type: Optional[str] = None,
    ) -> alt.LayerChart:
        expectation_kwarg_line_color: alt.HexColor = alt.HexColor(
            TintsAndShades.ROYAL_BLUE_30
        )
        expectation_kwarg_line_stroke_width: int = 5

        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        expectation_kwarg_tooltips: List[alt.Tooltip] = []
        min_value_plot_component: ExpectationKwargPlotComponent
        max_value_plot_component: ExpectationKwargPlotComponent
        for expectation_kwarg_plot_component in expectation_kwarg_plot_components:
            expectation_kwarg_tooltips.append(
                expectation_kwarg_plot_component.generate_tooltip()
            )

            if expectation_kwarg_plot_component.name == "min_value":
                min_value_plot_component = expectation_kwarg_plot_component
            elif expectation_kwarg_plot_component.name == "max_value":
                max_value_plot_component = expectation_kwarg_plot_component

        tooltip: List[alt.Tooltip] = []
        if domain_plot_component.name:
            tooltip.append(domain_plot_component.generate_tooltip())

        tooltip.extend(batch_plot_component.generate_tooltip())
        tooltip.extend(expectation_kwarg_tooltips)
        tooltip.extend(
            [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        lower_limit: alt.Chart
        upper_limit: alt.Chart
        lower_limit, upper_limit = (
            (
                alt.Chart(data=df)
                .mark_line(
                    color=expectation_kwarg_line_color,
                    strokeWidth=expectation_kwarg_line_stroke_width,
                )
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False),
                    ),
                    y=expectation_kwarg_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
                .properties(title=title)
            )
            for expectation_kwarg_plot_component in (
                min_value_plot_component,
                max_value_plot_component,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                    axis=alt.Axis(labels=False),
                ),
                y=min_value_plot_component.plot_on_axis(),
                y2=alt.Y2(
                    max_value_plot_component.name, title=max_value_plot_component.title
                ),
            )
            .properties(title=title)
        )

        if "quantiles" in df.columns:
            lower_limit = lower_limit.encode(detail="quantiles")
            upper_limit = upper_limit.encode(detail="quantiles")
            band = band.encode(detail="quantiles")

        metric_name: str
        predicate: Union[bool, int]
        anomaly_coded_bar: alt.Chart
        anomaly_coded_bars: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            # encode bar color based on anomalies
            assert (
                metric_plot_component.name
            ), f"Metric name must be set: {metric_plot_component}"
            metric_name = metric_plot_component.name
            predicate = (
                (alt.datum.min_value > alt.datum[metric_name])
                & (alt.datum.max_value > alt.datum[metric_name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_name])
                & (alt.datum.max_value < alt.datum[metric_name])
            )
            bar_color_condition: alt.condition = alt.condition(
                predicate=predicate,
                if_false=alt.value(SecondaryColors.ROYAL_BLUE),
                if_true=alt.value(SecondaryColors.POMEGRANATE_PINK),
            )

            anomaly_coded_base = alt.Chart(data=df, title=title)

            anomaly_coded_bar = anomaly_coded_base.mark_bar().encode(
                x=batch_plot_component.plot_on_axis(),
                y=metric_plot_component.plot_on_axis(),
                tooltip=tooltip,
                color=bar_color_condition,
            )
            anomaly_coded_bars.append(anomaly_coded_bar)

        return alt.layer(band, lower_limit, upper_limit, *anomaly_coded_bars)

    @staticmethod
    def _get_expect_domain_values_to_be_between_range_chart(  # noqa: PLR0913
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent],
        expectation_type: Optional[str] = None,
    ) -> alt.Chart:
        expectation_kwarg_line_color: alt.HexColor = alt.HexColor(
            TintsAndShades.ROYAL_BLUE_30
        )
        expectation_kwarg_line_stroke_width: int = 5

        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        expectation_kwarg_tooltips: List[alt.Tooltip] = []
        min_value_plot_component: ExpectationKwargPlotComponent
        max_value_plot_component: ExpectationKwargPlotComponent
        for expectation_kwarg_plot_component in expectation_kwarg_plot_components:
            expectation_kwarg_tooltips.append(
                expectation_kwarg_plot_component.generate_tooltip()
            )

            if expectation_kwarg_plot_component.name == "min_value":
                min_value_plot_component = expectation_kwarg_plot_component
            elif expectation_kwarg_plot_component.name == "max_value":
                max_value_plot_component = expectation_kwarg_plot_component

        tooltip: List[alt.Tooltip] = []
        if domain_plot_component.name:
            tooltip.append(domain_plot_component.generate_tooltip())

        tooltip.extend(batch_plot_component.generate_tooltip())
        tooltip.extend(expectation_kwarg_tooltips)
        tooltip.extend(
            [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        lower_limit: alt.Chart
        upper_limit: alt.Chart
        lower_limit, upper_limit = (
            (
                alt.Chart(data=df)
                .mark_line(
                    color=expectation_kwarg_line_color,
                    strokeWidth=expectation_kwarg_line_stroke_width,
                )
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=expectation_kwarg_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
                .properties(title=title)
            )
            for expectation_kwarg_plot_component in (
                min_value_plot_component,
                max_value_plot_component,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=min_value_plot_component.plot_on_axis(),
                y2=alt.Y2(
                    max_value_plot_component.name, title=max_value_plot_component.title
                ),
            )
            .properties(title=title)
        )

        if "quantiles" in df.columns:
            lower_limit = lower_limit.encode(detail="quantiles")
            upper_limit = upper_limit.encode(detail="quantiles")
            band = band.encode(detail="quantiles")

        metric_name: str
        predicate: Union[bool, int]
        anomaly_coded_line: alt.Chart
        anomaly_coded_lines: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            # encode point color based on anomalies
            assert (
                metric_plot_component.name
            ), f"Metric name must be set: {metric_plot_component}"
            metric_name = metric_plot_component.name
            predicate = (
                (alt.datum.min_value > alt.datum[metric_name])
                & (alt.datum.max_value > alt.datum[metric_name])
            ) | (
                (alt.datum.min_value < alt.datum[metric_name])
                & (alt.datum.max_value < alt.datum[metric_name])
            )
            point_color_condition = alt.condition(
                predicate=predicate,
                if_false=alt.value(SecondaryColors.LEAF_GREEN),
                if_true=alt.value(SecondaryColors.POMEGRANATE_PINK),
            )

            anomaly_coded_base = alt.Chart(data=df, title=title)

            anomaly_coded_line = anomaly_coded_base.mark_line().encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=metric_plot_component.plot_on_axis(),
                tooltip=tooltip,
                detail="batch",
            )

            anomaly_coded_points = anomaly_coded_base.mark_point().encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=metric_plot_component.plot_on_axis(),
                tooltip=tooltip,
                color=point_color_condition,
            )
            anomaly_coded_lines.append(anomaly_coded_line + anomaly_coded_points)

        return alt.layer(band, lower_limit, upper_limit, *anomaly_coded_lines)

    @staticmethod
    def _get_interactive_line_chart(
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_type: Optional[str] = None,
    ) -> alt.LayerChart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = (
            [domain_plot_component.generate_tooltip()]
            + batch_plot_component.generate_tooltip()
            + [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        columns: List[str] = [""] + pd.unique(df[domain_plot_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_plot_component.name],
        )

        quantiles: str = "quantiles"
        line_and_points_list: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            line: alt.Chart
            if quantiles in df.columns:
                line = (
                    alt.Chart(data=df, title=title)
                    .mark_line()
                    .encode(
                        x=batch_plot_component.plot_on_axis(),
                        y=metric_plot_component.plot_on_axis(),
                        tooltip=tooltip,
                        detail=quantiles,
                    )
                )
            else:
                line = (
                    alt.Chart(data=df, title=title)
                    .mark_line()
                    .encode(
                        x=batch_plot_component.plot_on_axis(),
                        y=metric_plot_component.plot_on_axis(),
                        tooltip=tooltip,
                    )
                )

            points: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_point()
                .encode(
                    x=batch_plot_component.plot_on_axis(),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
            )

            line_and_points_list.append(
                alt.layer(line, points)
                .add_selection(selection)
                .transform_filter(selection)
            )

        return (
            alt.layer(*line_and_points_list)
            .add_selection(selection)
            .transform_filter(selection)
        )

    @staticmethod
    def _get_interactive_bar_chart(
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_type: Optional[str] = None,
    ) -> alt.LayerChart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = (
            [domain_plot_component.generate_tooltip()]
            + batch_plot_component.generate_tooltip()
            + [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        columns: List[str] = [""] + pd.unique(df[domain_plot_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_plot_component.name],
        )

        bars: alt.Chart
        bars_list: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            bars = (
                alt.Chart(data=df, title=title)
                .mark_bar()
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
                .add_selection(selection)
                .transform_filter(selection)
            )

            bars_list.append(bars)

        return alt.layer(*bars_list)

    @staticmethod
    def _get_interactive_range_chart(
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_type: Optional[str] = None,
    ) -> alt.LayerChart:
        title: alt.TitleParams = determine_plot_title(
            expectation_type=expectation_type,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        tooltip: List[alt.Tooltip] = (
            [domain_plot_component.generate_tooltip()]
            + batch_plot_component.generate_tooltip()
            + [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        columns: List[str] = [""] + pd.unique(df[domain_plot_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_plot_component.name],
        )

        line_and_points_list: List[alt.Chart] = []
        for metric_plot_component in metric_plot_components:
            line: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_line()
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                    detail="batch",
                )
            )

            points: alt.Chart = (
                alt.Chart(data=df, title=title)
                .mark_point()
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=metric_plot_component.plot_on_axis(),
                    tooltip=tooltip,
                )
            )

            line_and_points_list.append(
                alt.layer(line, points)
                .add_selection(selection)
                .transform_filter(selection)
            )

        return (
            alt.layer(*line_and_points_list)
            .add_selection(selection)
            .transform_filter(selection)
        )

    @staticmethod
    def _get_interactive_expect_column_values_to_be_between_line_chart(  # noqa: PLR0913
        expectation_type: str,
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent],
        predicates: List[Union[bool, int]],
    ) -> alt.LayerChart:
        expectation_kwarg_line_color: alt.HexColor = alt.HexColor(
            TintsAndShades.ROYAL_BLUE_30
        )
        expectation_kwarg_line_stroke_width: int = 5

        expectation_kwargs_tooltip: List[alt.Tooltip] = []
        expectation_kwargs_initial_dropdown_state: List[str] = []
        min_value_plot_component: ExpectationKwargPlotComponent
        max_value_plot_component: ExpectationKwargPlotComponent
        for expectation_kwarg_plot_component in expectation_kwarg_plot_components:
            expectation_kwargs_tooltip.append(
                expectation_kwarg_plot_component.generate_tooltip(),
            )
            assert (
                expectation_kwarg_plot_component.name
            ), f"Expectation kwargs name must be set: {expectation_kwarg_plot_component}"
            expectation_kwargs_initial_dropdown_state.append(
                expectation_kwarg_plot_component.name,
            )

            if expectation_kwarg_plot_component.name == "min_value":
                min_value_plot_component = expectation_kwarg_plot_component
            elif expectation_kwarg_plot_component.name == "max_value":
                max_value_plot_component = expectation_kwarg_plot_component

        tooltip: List[alt.Tooltip] = (
            [domain_plot_component.generate_tooltip()]
            + batch_plot_component.generate_tooltip()
            + expectation_kwargs_tooltip
            + [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        columns: List[str] = [""] + pd.unique(df[domain_plot_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_plot_component.name],
        )

        lower_limit: alt.Chart
        upper_limit: alt.Chart
        lower_limit, upper_limit = (
            (
                alt.Chart(data=df)
                .mark_line(
                    color=expectation_kwarg_line_color,
                    strokeWidth=expectation_kwarg_line_stroke_width,
                )
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                    ),
                    y=alt.Y(
                        expectation_kwarg_plot_component.name,
                        type=expectation_kwarg_plot_component.alt_type,
                    ),
                    tooltip=tooltip,
                )
                .transform_filter(selection)
            )
            for expectation_kwarg_plot_component in (
                min_value_plot_component,
                max_value_plot_component,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                ),
                y=alt.Y(
                    min_value_plot_component.name,
                    type=min_value_plot_component.alt_type,
                ),
                y2=alt.Y2(max_value_plot_component.name),
            )
            .transform_filter(selection)
        )

        if "quantiles" in df.columns:
            lower_limit = lower_limit.encode(detail="quantiles")
            upper_limit = upper_limit.encode(detail="quantiles")
            band = band.encode(detail="quantiles")

        lines_and_points: alt.LayerChart = (
            DataAssistantResult._get_interactive_line_chart(
                expectation_type=expectation_type,
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
            )
        )

        line: alt.Chart
        points: alt.Chart
        anomaly_coded_points: alt.Chart
        for idx, line_layer in enumerate(lines_and_points.layer):
            line = line_layer.layer[0]
            points = line_layer.layer[1]

            line_layer.selection = alt.Undefined
            line_layer.transform = alt.Undefined
            line.selection = alt.Undefined
            line.transform = alt.Undefined
            points.selection = alt.Undefined
            points.transform = alt.Undefined

            point_color_condition = alt.condition(
                predicate=predicates[idx],
                if_false=alt.value(SecondaryColors.POMEGRANATE_PINK),
                if_true=alt.value(SecondaryColors.LEAF_GREEN),
            )

            anomaly_coded_points = points.encode(
                color=point_color_condition, tooltip=tooltip
            ).transform_filter(selection)

            line = line.transform_filter(selection)

            line_layer.layer[0] = line
            line_layer.layer[1] = anomaly_coded_points

        lines_and_points.selection = alt.Undefined
        lines_and_points.transform = alt.Undefined
        lines_and_points = lines_and_points.transform_filter(selection)

        return (
            alt.layer(band, lower_limit, upper_limit, lines_and_points)
        ).add_selection(selection)

    @staticmethod
    def _get_interactive_expect_column_values_to_be_between_bar_chart(  # noqa: PLR0913
        expectation_type: str,
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent],
        predicates: List[Union[bool, int]],
    ) -> alt.VConcatChart:
        expectation_kwarg_line_color: alt.HexColor = alt.HexColor(
            TintsAndShades.ROYAL_BLUE_30
        )
        expectation_kwarg_line_stroke_width: int = 5

        expectation_kwargs_tooltip: List[alt.Tooltip] = []
        expectation_kwargs_initial_dropdown_state: List[str] = []
        min_value_plot_component: ExpectationKwargPlotComponent
        max_value_plot_component: ExpectationKwargPlotComponent
        for expectation_kwarg_plot_component in expectation_kwarg_plot_components:
            expectation_kwargs_tooltip.append(
                expectation_kwarg_plot_component.generate_tooltip(),
            )
            expectation_kwargs_initial_dropdown_state.append(
                expectation_kwarg_plot_component.name,
            )

            if expectation_kwarg_plot_component.name == "min_value":
                min_value_plot_component = expectation_kwarg_plot_component
            elif expectation_kwarg_plot_component.name == "max_value":
                max_value_plot_component = expectation_kwarg_plot_component

        tooltip: List[alt.Tooltip] = (
            [domain_plot_component.generate_tooltip()]
            + batch_plot_component.generate_tooltip()
            + expectation_kwargs_tooltip
            + [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        columns: List[str] = [""] + pd.unique(df[domain_plot_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_plot_component.name],
        )

        lower_limit: alt.Chart
        upper_limit: alt.Chart
        lower_limit, upper_limit = (
            (
                alt.Chart(data=df)
                .mark_line(
                    color=expectation_kwarg_line_color,
                    strokeWidth=expectation_kwarg_line_stroke_width,
                )
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=alt.Y(
                        expectation_kwarg_plot_component.name,
                        type=expectation_kwarg_plot_component.alt_type,
                    ),
                    tooltip=tooltip,
                )
                .transform_filter(selection)
            )
            for expectation_kwarg_plot_component in (
                min_value_plot_component,
                max_value_plot_component,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=alt.Y(
                    min_value_plot_component.name,
                    type=min_value_plot_component.alt_type,
                ),
                y2=alt.Y2(max_value_plot_component.name),
            )
            .transform_filter(selection)
        )

        if "quantiles" in df.columns:
            lower_limit = lower_limit.encode(detail="quantiles")
            upper_limit = upper_limit.encode(detail="quantiles")
            band = band.encode(detail="quantiles")

        bars: alt.LayerChart = DataAssistantResult._get_interactive_bar_chart(
            expectation_type=expectation_type,
            df=df,
            metric_plot_components=metric_plot_components,
            batch_plot_component=batch_plot_component,
            domain_plot_component=domain_plot_component,
        )

        bar: alt.Chart
        bar_color_condition: alt.condition
        for idx, bar_layer in enumerate(bars.layer):
            bar_layer.selection = alt.Undefined
            bar_layer.transform = alt.Undefined

            bar_color_condition = alt.condition(
                predicate=predicates[idx],
                if_false=alt.value(SecondaryColors.POMEGRANATE_PINK),
                if_true=alt.value(SecondaryColors.ROYAL_BLUE),
            )

            bars.layer[idx] = bar_layer.encode(
                color=bar_color_condition, tooltip=tooltip
            ).transform_filter(selection)

        bars.selection = alt.Undefined
        bars.transform = alt.Undefined
        bars = bars.transform_filter(selection)

        return (alt.layer(band, lower_limit, upper_limit, bars)).add_selection(
            selection
        )

    @staticmethod
    def _get_interactive_expect_column_values_to_be_between_range_chart(  # noqa: PLR0913
        expectation_type: str,
        df: pd.DataFrame,
        metric_plot_components: List[MetricPlotComponent],
        batch_plot_component: BatchPlotComponent,
        domain_plot_component: DomainPlotComponent,
        expectation_kwarg_plot_components: List[ExpectationKwargPlotComponent],
        predicates: List[Union[bool, int]],
    ) -> alt.LayerChart:
        expectation_kwarg_line_color: alt.HexColor = alt.HexColor(
            TintsAndShades.ROYAL_BLUE_30
        )
        expectation_kwarg_line_stroke_width: int = 5

        expectation_kwargs_tooltip: List[alt.Tooltip] = []
        expectation_kwargs_initial_dropdown_state: List[str] = []
        min_value_plot_component: ExpectationKwargPlotComponent
        max_value_plot_component: ExpectationKwargPlotComponent
        for expectation_kwarg_plot_component in expectation_kwarg_plot_components:
            expectation_kwargs_tooltip.append(
                expectation_kwarg_plot_component.generate_tooltip()
            )
            expectation_kwargs_initial_dropdown_state.append(
                expectation_kwarg_plot_component.name
            )

            if expectation_kwarg_plot_component.name == "min_value":
                min_value_plot_component = expectation_kwarg_plot_component
            elif expectation_kwarg_plot_component.name == "max_value":
                max_value_plot_component = expectation_kwarg_plot_component

        tooltip: List[alt.Tooltip] = (
            [domain_plot_component.generate_tooltip()]
            + batch_plot_component.generate_tooltip()
            + expectation_kwargs_tooltip
            + [
                metric_plot_component.generate_tooltip(format=",")
                for metric_plot_component in metric_plot_components
            ]
        )

        columns: List[str] = [""] + pd.unique(df[domain_plot_component.name]).tolist()
        input_dropdown: alt.binding_select = alt.binding_select(
            options=columns, name="Select Column: "
        )
        selection: alt.selection_single = alt.selection_single(
            empty="none",
            bind=input_dropdown,
            fields=[domain_plot_component.name],
        )

        lower_limit: alt.Chart
        upper_limit: alt.Chart
        lower_limit, upper_limit = (
            (
                alt.Chart(data=df)
                .mark_line(
                    color=expectation_kwarg_line_color,
                    strokeWidth=expectation_kwarg_line_stroke_width,
                )
                .encode(
                    x=alt.X(
                        batch_plot_component.name,
                        type=batch_plot_component.alt_type,
                        title=batch_plot_component.title,
                        axis=alt.Axis(labels=False, grid=False),
                    ),
                    y=alt.Y(
                        expectation_kwarg_plot_component.name,
                        type=expectation_kwarg_plot_component.alt_type,
                    ),
                    tooltip=tooltip,
                )
                .transform_filter(selection)
            )
            for expectation_kwarg_plot_component in (
                min_value_plot_component,
                max_value_plot_component,
            )
        )

        band: alt.Chart = (
            alt.Chart(data=df)
            .mark_area()
            .encode(
                x=alt.X(
                    batch_plot_component.name,
                    type=batch_plot_component.alt_type,
                    title=batch_plot_component.title,
                    axis=alt.Axis(labels=False, grid=False),
                ),
                y=alt.Y(
                    min_value_plot_component.name,
                    type=min_value_plot_component.alt_type,
                ),
                y2=alt.Y2(max_value_plot_component.name),
            )
            .transform_filter(selection)
        )

        if "quantiles" in df.columns:
            lower_limit = lower_limit.encode(detail="quantiles")
            upper_limit = upper_limit.encode(detail="quantiles")
            band = band.encode(detail="quantiles")

        lines_and_points: alt.LayerChart = (
            DataAssistantResult._get_interactive_range_chart(
                expectation_type=expectation_type,
                df=df,
                metric_plot_components=metric_plot_components,
                batch_plot_component=batch_plot_component,
                domain_plot_component=domain_plot_component,
            )
        )

        line: alt.Chart
        points: alt.Chart
        anomaly_coded_points: alt.Chart
        for idx, line_layer in enumerate(lines_and_points.layer):
            line = line_layer.layer[0]
            points = line_layer.layer[1]

            line_layer.selection = alt.Undefined
            line_layer.transform = alt.Undefined
            line.selection = alt.Undefined
            line.transform = alt.Undefined
            points.selection = alt.Undefined
            points.transform = alt.Undefined

            point_color_condition = alt.condition(
                predicate=predicates[idx],
                if_false=alt.value(SecondaryColors.POMEGRANATE_PINK),
                if_true=alt.value(SecondaryColors.LEAF_GREEN),
            )

            anomaly_coded_points = points.encode(
                color=point_color_condition, tooltip=tooltip
            ).transform_filter(selection)

            line = line.transform_filter(selection)

            line_layer.layer[0] = line
            line_layer.layer[1] = anomaly_coded_points

        lines_and_points.selection = alt.Undefined
        lines_and_points.transform = alt.Undefined
        lines_and_points = lines_and_points.transform_filter(selection)

        return (
            alt.layer(band, lower_limit, upper_limit, lines_and_points)
        ).add_selection(selection)

    @staticmethod
    def _get_theme(theme: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        default_theme: Dict[str, Any] = copy.deepcopy(AltairThemes.DEFAULT_THEME.value)
        if theme:
            return nested_update(default_theme, theme)
        else:
            return default_theme

    def _plot_table_domain_charts(  # noqa: PLR0913
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Union[alt.Chart, alt.LayerChart]]:
        metric_expectation_map: dict[
            tuple[str, ...], str
        ] = self._get_metric_expectation_map()

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

        attributed_metrics_by_table_domain: Dict[
            Domain, Dict[str, List[ParameterNode]]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.TABLE)

        table_domain = Domain(
            domain_type=MetricDomainTypes.TABLE, rule_name="table_rule"
        )
        attributed_metrics: Dict[
            str, List[ParameterNode]
        ] = attributed_metrics_by_table_domain[table_domain]

        table_based_metric_names: Set[tuple[str, ...]] = set()
        for metrics in metric_expectation_map.keys():
            if all(metric.startswith("table") for metric in metrics):
                table_based_metric_names.add(tuple(metrics))

        charts: list[alt.Chart] = []
        metric_names: tuple[str, ...]
        attributed_values: list[list[ParameterNode]]
        expectation_type: str
        expectation_configuration: ExpectationConfiguration
        for metric_names in table_based_metric_names:
            expectation_type = metric_expectation_map[metric_names]

            attributed_values = [
                attributed_metrics[metric_name] for metric_name in metric_names
            ]

            table_domain_chart: alt.Chart
            if plot_mode == PlotMode.DIAGNOSTIC:
                for expectation_configuration in table_based_expectation_configurations:
                    if expectation_configuration.expectation_type == expectation_type:
                        table_domain_chart = (
                            self._create_chart_for_table_domain_expectation(
                                expectation_type=expectation_type,
                                expectation_configuration=expectation_configuration,
                                metric_names=metric_names,
                                attributed_values=attributed_values,
                                include_column_names=include_column_names,
                                exclude_column_names=exclude_column_names,
                                plot_mode=plot_mode,
                                sequential=sequential,
                            )
                        )
                        charts.append(table_domain_chart)
            else:
                table_domain_chart = self._create_chart_for_table_domain_expectation(
                    expectation_type=expectation_type,
                    expectation_configuration=None,
                    metric_names=metric_names,
                    attributed_values=attributed_values,
                    include_column_names=include_column_names,
                    exclude_column_names=exclude_column_names,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
                charts.append(table_domain_chart)

        # we want the table row count chart to be displayed first if it exists
        chart_titles: List[str] = self._get_chart_titles(charts=charts)
        first_chart_idx: int = 0
        for idx, chart_title in enumerate(chart_titles):
            if (
                chart_title == "Table Row Count per Batch"
                or chart_title == "expect_table_row_count_to_be_between"
            ):
                first_chart_idx = idx

        # return the sorted charts
        sorted_charts: List[alt.Chart]
        if len(charts) > 1:
            sorted_charts = [charts[first_chart_idx]] + [
                chart
                for idx, chart in enumerate(charts)
                if chart is not None and idx != first_chart_idx
            ]
        else:
            sorted_charts = charts

        return sorted_charts

    def _plot_column_domain_charts(  # noqa: PLR0913
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> tuple[List[alt.VConcatChart], List[alt.Chart]]:
        metric_expectation_map: Dict[
            tuple[str, ...], str
        ] = self._get_metric_expectation_map()

        column_based_expectation_configurations_by_type: Dict[
            str, List[ExpectationConfiguration]
        ] = self._filter_expectation_configurations_by_column_type(
            expectation_configurations, include_column_names, exclude_column_names
        )

        attributed_metrics_by_domain: Dict[
            Domain, Dict[str, List[ParameterNode]]
        ] = self._determine_attributed_metrics_by_domain_type(MetricDomainTypes.COLUMN)

        attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, List[ParameterNode]]
        ] = self._filter_attributed_metrics_by_column_names(
            attributed_metrics_by_domain,
            include_column_names,
            exclude_column_names,
        )

        column_based_metric_names: Set[tuple[str, ...]] = set()
        for metrics in metric_expectation_map.keys():
            if all(metric.startswith("column") for metric in metrics):
                if plot_mode == PlotMode.DIAGNOSTIC:
                    column_based_metric_names.add(tuple(metrics))
                if plot_mode == PlotMode.DESCRIPTIVE:
                    for metric in metrics:
                        column_based_metric_names.add((metric,))

        filtered_attributed_metrics_by_column_domain: Dict[
            Domain, Dict[str, List[ParameterNode]]
        ]
        column_based_expectation_configurations: List[ExpectationConfiguration]
        display_charts: List[alt.VConcatChart] = []
        return_charts: List[Optional[alt.Chart]] = []
        expectation_type: Optional[str]
        for metric_names in column_based_metric_names:
            expectation_type = metric_expectation_map[metric_names]
            column_based_expectation_configurations = (
                column_based_expectation_configurations_by_type[expectation_type]
            )

            filtered_attributed_metrics_by_column_domain = (
                self._filter_attributed_metrics_by_metric_names(
                    attributed_metrics_by_column_domain,
                    metric_names,
                )
            )

            display_charts.extend(
                self._create_display_chart_for_column_domain_expectation(
                    expectation_type=expectation_type,
                    expectation_configurations=column_based_expectation_configurations,
                    metric_names=metric_names,
                    attributed_metrics_by_domain=filtered_attributed_metrics_by_column_domain,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
            )

            return_charts.extend(
                self._create_return_charts_for_column_domain_expectation(
                    expectation_type=expectation_type,
                    expectation_configurations=column_based_expectation_configurations,
                    metric_names=metric_names,
                    attributed_metrics_by_domain=filtered_attributed_metrics_by_column_domain,
                    plot_mode=plot_mode,
                    sequential=sequential,
                )
            )

        display_charts = list(filter(None, display_charts))
        return_charts = list(filter(None, return_charts))

        return display_charts, return_charts

    def _filter_expectation_configurations_by_column_type(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
    ) -> Dict[str, List[ExpectationConfiguration]]:
        metric_expectation_map: Dict[
            tuple[str, ...], str
        ] = self._get_metric_expectation_map()

        column_based_expectations: Set[str] = {
            expectation
            for expectation in metric_expectation_map.values()
            if expectation.startswith("expect_column_")
        }

        def _filter(e: ExpectationConfiguration, expectations: Set[str]) -> bool:
            if e.expectation_type not in expectations:
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

    @staticmethod
    def _filter_attributed_metrics_by_column_names(
        attributed_metrics: Dict[Domain, Dict[str, List[ParameterNode]]],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
    ) -> Dict[Domain, Dict[str, List[ParameterNode]]]:
        def _filter(domain: Domain) -> bool:
            column_name: str = domain.domain_kwargs.column
            if exclude_column_names and column_name in exclude_column_names:
                return False

            if include_column_names and column_name not in include_column_names:
                return False

            return True

        domains: Set[Domain] = set(
            filter(lambda m: _filter(m), list(attributed_metrics.keys()))
        )
        filtered_attributed_metrics: Dict[Domain, Dict[str, List[ParameterNode]]] = {
            domain: attributed_metrics[domain] for domain in domains
        }

        return filtered_attributed_metrics

    @staticmethod
    def _filter_attributed_metrics_by_metric_names(
        attributed_metrics: Dict[Domain, Dict[str, List[ParameterNode]]],
        metric_names: tuple[str, ...],
    ) -> Dict[Domain, Dict[str, List[ParameterNode]]]:
        domain: Domain
        filtered_attributed_metrics: Dict[Domain, Dict[str, List[ParameterNode]]] = {}
        for domain, attributed_metric_values in attributed_metrics.items():
            filtered_attributed_metrics[domain] = {}
            for metric_name in metric_names:
                if metric_name in attributed_metric_values.keys():
                    filtered_attributed_metrics[domain][
                        metric_name
                    ] = attributed_metric_values[metric_name]
            if filtered_attributed_metrics[domain] == {}:
                filtered_attributed_metrics.pop(domain)

        return filtered_attributed_metrics

    def _chart_domain_values(  # noqa: PLR0913
        self,
        expectation_type: str,
        df: pd.DataFrame,
        metric_names: tuple[str, ...],
        plot_mode: PlotMode,
        sequential: bool,
        subtitle: Optional[str],
    ) -> Optional[alt.Chart]:
        sanitized_metric_names: Set[
            str
        ] = self._get_sanitized_metric_names_from_metric_names(
            metric_names=metric_names
        )

        nominal_metrics: Set[str] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.NOMINAL
        )
        ordinal_metrics: Set[str] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.ORDINAL
        )
        quantitative_metrics: Set[
            str
        ] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.QUANTITATIVE
        )
        temporal_metrics: Set[str] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.ORDINAL
        )

        if plot_mode is PlotMode.DIAGNOSTIC:
            expectation_plot_impl: Callable[
                [
                    str,
                    pd.DataFrame,
                    Set[str],
                    bool,
                    Optional[str],
                ],
                Union[alt.Chart, alt.LayerChart],
            ]

            if DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=nominal_metrics
            ):
                expectation_plot_impl = (
                    self._get_expect_domain_values_to_match_set_chart
                )
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=ordinal_metrics
            ):
                expectation_plot_impl = self._get_expect_domain_values_ordinal_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=quantitative_metrics
            ):
                expectation_plot_impl = (
                    self._get_expect_domain_values_to_be_between_chart
                )
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=temporal_metrics
            ):
                expectation_plot_impl = self._get_expect_domain_values_temporal_chart
            else:
                raise gx_exceptions.DataAssistantResultExecutionError(
                    f"All metrics to chart should be of the same AltairDataType, but metrics: {metric_names} are not."
                )

            return expectation_plot_impl(
                expectation_type=expectation_type,
                df=df,
                sanitized_metric_names=sanitized_metric_names,
                sequential=sequential,
                subtitle=subtitle,
            )
        else:
            metric_plot_impl: Callable[
                [
                    pd.DataFrame,
                    Set[str],
                    bool,
                    Optional[str],
                ],
                Union[alt.Chart, alt.LayerChart],
            ]

            if DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=nominal_metrics
            ):
                metric_plot_impl = self._get_nominal_metrics_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=ordinal_metrics
            ):
                metric_plot_impl = self._get_ordinal_metrics_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=quantitative_metrics
            ):
                metric_plot_impl = self._get_quantitative_metrics_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=temporal_metrics
            ):
                metric_plot_impl = self._get_temporal_metrics_chart
            else:
                raise gx_exceptions.DataAssistantResultExecutionError(
                    f"All metrics to chart should be of the same AltairDataType, but metrics: {metric_names} are not."
                )

            return metric_plot_impl(
                df=df,
                sanitized_metric_names=sanitized_metric_names,
                sequential=sequential,
                subtitle=subtitle,
            )

    def _create_display_chart_for_column_domain_expectation(  # noqa: PLR0913
        self,
        expectation_type: str,
        expectation_configurations: List[ExpectationConfiguration],
        metric_names: tuple[str, ...],
        attributed_metrics_by_domain: Dict[Domain, Dict[str, List[ParameterNode]]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Optional[alt.VConcatChart]]:
        column_dfs: List[ColumnDataFrame] = self._create_column_dfs_for_charting(
            metric_names=metric_names,
            attributed_metrics_by_domain=attributed_metrics_by_domain,
            expectation_configurations=expectation_configurations,
            plot_mode=plot_mode,
        )

        # if all metrics in metric_names failed to resolve, the list will be empty and we return without attempting
        # to chart column values
        if len(column_dfs) > 0:
            return self._chart_column_values(
                expectation_type=expectation_type,
                column_dfs=column_dfs,
                metric_names=metric_names,
                plot_mode=plot_mode,
                sequential=sequential,
            )
        else:
            return []

    def _create_return_charts_for_column_domain_expectation(  # noqa: PLR0913
        self,
        expectation_type: str,
        expectation_configurations: List[ExpectationConfiguration],
        metric_names: tuple[str, ...],
        attributed_metrics_by_domain: Dict[Domain, Dict[str, List[ParameterNode]]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[alt.Chart]:
        metric_expectation_map: Dict[
            tuple[str, ...], str
        ] = self._get_metric_expectation_map()

        sanitized_metric_names: Set[
            str
        ] = self._get_sanitized_metric_names_from_metric_names(
            metric_names=metric_names
        )

        expectation_configuration: ExpectationConfiguration
        attributed_metrics: Dict[str, List[ParameterNode]]
        df: pd.DataFrame
        attributed_values: List[ParameterNode]
        metric_df: pd.DataFrame
        return_charts: List[alt.Chart] = []
        for domain, attributed_metrics in attributed_metrics_by_domain.items():
            for expectation_configuration in expectation_configurations:
                if (
                    expectation_configuration.kwargs["column"]
                    == domain.domain_kwargs.column
                ) and (
                    metric_expectation_map.get(metric_names)
                    == expectation_configuration.expectation_type
                ):
                    df = pd.DataFrame()
                    for metric_name in metric_names:
                        attributed_values = attributed_metrics[metric_name]
                        metric_df = self._create_df_for_charting(
                            metric_name=metric_name,
                            attributed_values=attributed_values,
                            expectation_configuration=expectation_configuration,
                            plot_mode=plot_mode,
                        )
                        if len(df.index) == 0:
                            df = metric_df.copy()
                        else:
                            join_keys = [
                                column
                                for column in metric_df.columns
                                if column not in sanitized_metric_names
                            ]
                            df = df.merge(metric_df, on=join_keys).reset_index(
                                drop=True
                            )

                    column_name: str = domain.domain_kwargs.column
                    subtitle = f"Column: {column_name}"

                    return_chart = self._chart_domain_values(
                        expectation_type=expectation_type,
                        df=df,
                        metric_names=metric_names,
                        plot_mode=plot_mode,
                        sequential=sequential,
                        subtitle=subtitle,
                    )

                    return_charts.append(return_chart)

        return return_charts

    def _chart_column_values(  # noqa: PLR0913
        self,
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        metric_names: tuple[str, ...],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> List[Optional[alt.VConcatChart]]:
        sanitized_metric_names: Set[
            str
        ] = DataAssistantResult._get_sanitized_metric_names_from_metric_names(
            metric_names=metric_names
        )

        nominal_metrics: Set[str] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.NOMINAL
        )
        ordinal_metrics: Set[str] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.ORDINAL
        )
        quantitative_metrics: Set[
            str
        ] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.QUANTITATIVE
        )
        temporal_metrics: Set[str] = self._get_sanitized_metric_names_from_altair_type(
            altair_type=AltairDataTypes.ORDINAL
        )

        if plot_mode is PlotMode.DIAGNOSTIC:
            expectation_plot_impl: Callable[
                [
                    str,
                    List[ColumnDataFrame],
                    Set[str],
                    bool,
                ],
                alt.VConcatChart,
            ]

            if DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=nominal_metrics
            ):
                expectation_plot_impl = (
                    self._get_interactive_expect_column_values_nominal_chart
                )
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=ordinal_metrics
            ):
                expectation_plot_impl = (
                    self._get_interactive_expect_column_values_ordinal_chart
                )
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=quantitative_metrics
            ):
                expectation_plot_impl = (
                    self._get_interactive_expect_column_values_to_be_between_chart
                )
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=temporal_metrics
            ):
                expectation_plot_impl = (
                    self._get_interactive_expect_column_values_temporal_chart
                )
            else:
                raise gx_exceptions.DataAssistantResultExecutionError(
                    f"All metrics to chart should be of the same AltairDataType, but metrics: {metric_names} are not."
                )

            return [
                expectation_plot_impl(
                    expectation_type=expectation_type,
                    column_dfs=column_dfs,
                    sanitized_metric_names=sanitized_metric_names,
                    sequential=sequential,
                )
            ]
        else:
            plot_impl: Callable[
                [
                    List[ColumnDataFrame],
                    Set[str],
                    bool,
                ],
                Union[alt.LayerChart, alt.VConcatChart],
            ]

            if DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=nominal_metrics
            ):
                plot_impl = self._get_interactive_nominal_metrics_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=ordinal_metrics
            ):
                plot_impl = self._get_interactive_ordinal_metrics_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=quantitative_metrics
            ):
                plot_impl = self._get_interactive_metrics_chart
            elif DataAssistantResult._all_metric_names_in_iterable(
                metric_names=sanitized_metric_names, iterable=temporal_metrics
            ):
                plot_impl = self._get_interactive_temporal_metrics_chart
            else:
                raise gx_exceptions.DataAssistantResultExecutionError(
                    f"All metrics to chart should be of the same AltairDataType, but metrics: {metric_names} are not."
                )

            return [
                plot_impl(
                    column_dfs=column_dfs,
                    sanitized_metric_names=sanitized_metric_names,
                    sequential=sequential,
                )
            ]

    def _create_df_for_charting(  # noqa: PLR0912
        self,
        metric_name: str,
        attributed_values: List[ParameterNode],
        expectation_configuration: Optional[ExpectationConfiguration],
        plot_mode: PlotMode,
    ) -> pd.DataFrame:
        batch_ids: KeysView[str] = attributed_values[0].keys()
        metric_values: MetricValues = [
            value[0] if len(value) == 1 else value
            for value in attributed_values[0].values()
        ]

        sanitized_metric_name: str = sanitize_parameter_name(
            name=metric_name, suffix=None
        )

        df: pd.DataFrame = pd.DataFrame({sanitized_metric_name: metric_values})

        if (
            metric_name == "column.quantile_values"
            and plot_mode == PlotMode.DESCRIPTIVE
        ):
            quantiles: Union[List[float], float] = attributed_values[
                1
            ].metric_configuration.metric_value_kwargs.quantiles
            if isinstance(quantiles, list) and len(quantiles) == 1:
                quantiles = quantiles[0]
            df["quantiles"] = [quantiles for idx in df.index]

        batch_identifier_list: List[Set[tuple[str, str]]] = []
        if self._batch_id_to_batch_identifier_display_name_map is not None:
            batch_identifier_list = [
                self._batch_id_to_batch_identifier_display_name_map[batch_id]
                for batch_id in batch_ids
            ]

        batch_identifier_set: Set
        batch_identifier_list_sorted: List
        batch_identifier_key: str
        batch_identifier_value: str
        batch_identifier_keys: Set[str] = set()
        batch_identifier_record: List
        batch_identifier_records: List[List] = []
        for batch_identifier_set in batch_identifier_list:
            # make sure batch_identifier keys are sorted the same from batch to batch
            # e.g. prevent batch 1 from displaying keys "month", "year" and batch 2 from displaying keys "year", "month"
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
                # if dictionary type batch_identifier values are detected, format them as a string for tooltip display
                if isinstance(batch_identifier_value, dict):
                    batch_identifier_value = str(  # noqa: PLW2901
                        {
                            str(key).title(): value
                            for key, value in batch_identifier_value.items()
                        }
                    ).replace("'", "")
                batch_identifier_record.append(batch_identifier_value)

            batch_identifier_records.append(batch_identifier_record)

        batch_identifier_keys_sorted: List[str] = sorted(batch_identifier_keys)
        batch_identifier_df: pd.DataFrame = pd.DataFrame(
            batch_identifier_records, columns=batch_identifier_keys_sorted
        )

        idx: int
        batch_numbers: List[int] = [idx + 1 for idx in range(len(batch_identifier_df))]
        df["batch"] = batch_numbers

        df = pd.concat([df, batch_identifier_df], axis=1)

        if plot_mode == PlotMode.DIAGNOSTIC:
            if expectation_configuration is not None:
                for kwarg_name in expectation_configuration.kwargs:
                    if isinstance(expectation_configuration.kwargs[kwarg_name], dict):
                        for key, value in expectation_configuration.kwargs[
                            kwarg_name
                        ].items():
                            if isinstance(value, list):
                                df[key] = [value for _ in df.index]
                            else:
                                df[key] = value

                    elif isinstance(expectation_configuration.kwargs[kwarg_name], list):
                        df[kwarg_name] = [
                            expectation_configuration.kwargs[kwarg_name]
                            for _ in df.index
                        ]
                    else:
                        df[kwarg_name] = expectation_configuration.kwargs[kwarg_name]
            else:
                return pd.DataFrame()

        # if there are any lists in the dataframe
        if (df.applymap(type) == list).any().any():
            df = DataAssistantResult._transform_column_lists_to_rows(
                df=df,
            )

        df = df.reset_index(drop=True)

        return df

    def _create_column_dfs_for_charting(
        self,
        metric_names: tuple[str, ...],
        attributed_metrics_by_domain: Dict[Domain, Dict[str, List[ParameterNode]]],
        expectation_configurations: List[ExpectationConfiguration],
        plot_mode: PlotMode,
    ) -> List[ColumnDataFrame]:
        sanitized_metric_names: Set[
            str
        ] = self._get_sanitized_metric_names_from_metric_names(
            metric_names=metric_names
        )

        metric_domains: Set[Domain] = set(attributed_metrics_by_domain.keys())

        column_name: str
        column_domain: Domain
        metric_df: pd.DataFrame
        join_keys: List[str]
        df: pd.DataFrame
        column_df: ColumnDataFrame
        column_dfs: List[ColumnDataFrame] = []
        for metric_domain in metric_domains:
            attributed_values_by_metric_name: Dict[
                str, List[ParameterNode]
            ] = attributed_metrics_by_domain[metric_domain]
            column_name = metric_domain.domain_kwargs.column

            metric_domain_expectation_configuration: Optional[
                ExpectationConfiguration
            ] = None
            for expectation_configuration in expectation_configurations:
                if expectation_configuration.kwargs["column"] == column_name:
                    metric_domain_expectation_configuration = expectation_configuration

            df = pd.DataFrame()
            for metric_name in metric_names:
                metric_df = self._create_df_for_charting(
                    metric_name=metric_name,
                    attributed_values=attributed_values_by_metric_name[metric_name],
                    expectation_configuration=metric_domain_expectation_configuration,
                    plot_mode=plot_mode,
                )
                if len(metric_df) > 0:
                    if len(df.index) == 0:
                        df = metric_df.copy()
                    else:
                        join_keys = [
                            column
                            for column in metric_df.columns
                            if column not in sanitized_metric_names
                        ]
                        df = df.merge(metric_df, on=join_keys).reset_index(drop=True)
            if len(df.index) > 0:
                column_df = ColumnDataFrame(column_name, df)
                column_dfs.append(column_df)

        return column_dfs

    def _create_chart_for_table_domain_expectation(  # noqa: PLR0913
        self,
        expectation_type: str,
        expectation_configuration: Optional[ExpectationConfiguration],
        metric_names: tuple[str, ...],
        attributed_values: List[List[ParameterNode]],
        include_column_names: Optional[List[str]],
        exclude_column_names: Optional[List[str]],
        plot_mode: PlotMode,
        sequential: bool,
    ) -> alt.Chart:
        sanitized_metric_names: Set[
            str
        ] = self._get_sanitized_metric_names_from_metric_names(
            metric_names=metric_names
        )

        metric_df: pd.DataFrame
        df: pd.DataFrame = pd.DataFrame()
        for metric_name in metric_names:
            metric_df = self._create_df_for_charting(
                metric_name=metric_name,
                attributed_values=attributed_values[0],
                expectation_configuration=expectation_configuration,
                plot_mode=plot_mode,
            )
            if len(df.index) == 0:
                df = metric_df.copy()
            else:
                join_keys = [
                    column
                    for column in metric_df.columns
                    if column not in sanitized_metric_names
                ]
                df = df.merge(metric_df, on=join_keys).reset_index(drop=True)

        # If columns are included/excluded we need to filter them out for table level metrics here
        table_column_metrics: List[str] = ["table_columns"]
        new_column_list: List[str]
        new_record_list: List[List[str]] = []
        if all(
            sanitized_metric_name in table_column_metrics
            for sanitized_metric_name in sanitized_metric_names
        ):
            if (include_column_names is not None) or (exclude_column_names is not None):
                for sanitized_metric_name in sanitized_metric_names:
                    all_columns = (
                        df[sanitized_metric_name].apply(pd.Series).values.tolist()
                    )
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
                    df[sanitized_metric_name] = new_record_list

        return self._chart_domain_values(
            expectation_type=expectation_type,
            df=df,
            metric_names=metric_names,
            plot_mode=plot_mode,
            sequential=sequential,
            subtitle=None,
        )

    def _get_attributed_metrics_by_domain(
        self,
    ) -> Dict[Domain, Dict[str, List[ParameterNode]]]:
        domain: Domain
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        fully_qualified_parameter_name: str
        parameter_node: ParameterNode
        metrics_attributed_values_by_domain: Dict[
            Domain, Dict[str, List[ParameterNode]]
        ] = {}
        if self.metrics_by_domain:
            for (
                domain,
                parameter_values_for_fully_qualified_parameter_names,
            ) in self.metrics_by_domain.items():
                metrics_attributed_values_by_domain[domain] = {}
                for (
                    fully_qualified_parameter_name,
                    parameter_node,
                ) in parameter_values_for_fully_qualified_parameter_names.items():
                    if (
                        FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                        in parameter_node
                        and FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                        in parameter_node
                    ):
                        metrics_attributed_values_by_domain[domain].update(
                            {
                                parameter_node[
                                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                                ].metric_configuration.metric_name: [
                                    parameter_node[
                                        FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                                    ],
                                    parameter_node[
                                        FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                                    ],
                                ]
                            }
                        )
                    elif (
                        FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                        in parameter_node
                    ):
                        metrics_attributed_values_by_domain[domain].update(
                            {
                                parameter_node[
                                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                                ].metric_configuration.metric_name: [
                                    parameter_node[
                                        FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                                    ]
                                ]
                            }
                        )

        return metrics_attributed_values_by_domain

    def _determine_attributed_metrics_by_domain_type(
        self, metric_domain_type: MetricDomainTypes
    ) -> Dict[Domain, Dict[str, List[ParameterNode]]]:
        # noinspection PyTypeChecker
        attributed_metrics_by_domain: Dict[
            Domain, Dict[str, List[ParameterNode]]
        ] = dict(
            filter(
                lambda element: element[0].domain_type == metric_domain_type,
                self._get_attributed_metrics_by_domain().items(),
            )
        )
        return attributed_metrics_by_domain

    def _get_sanitized_metric_names_from_altair_type(
        self, altair_type: AltairDataTypes
    ) -> Set[str]:
        metric_types: Dict[str, AltairDataTypes] = self.metric_types
        return {
            sanitize_parameter_name(name=metric, suffix=None)
            for metric in metric_types.keys()
            if metric_types[metric] == altair_type
        }

    @staticmethod
    def _get_sanitized_metric_names_from_metric_names(
        metric_names: tuple[str, ...],
    ) -> Set[str]:
        return {
            sanitize_parameter_name(name=metric_name, suffix=None)
            for metric_name in metric_names
        }

    @staticmethod
    def _all_metric_names_in_iterable(
        metric_names: Set[str], iterable: Iterable[str]
    ) -> bool:
        return all(metric_name in iterable for metric_name in metric_names)

    @staticmethod
    def _get_all_columns_from_column_dfs(
        column_dfs: List[ColumnDataFrame],
    ) -> List[str]:
        column_set: Set[str] = set()
        for column_df in column_dfs:
            column_set.update(column_df.df.columns)
        return list(column_set)

    @staticmethod
    def _clean_quantitative_metrics_column_dfs(
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
    ) -> List[ColumnDataFrame]:
        cleaned_column_dfs: List[ColumnDataFrame] = []
        for idx, (column_name, column_df) in enumerate(column_dfs):
            cleaned_column_df = DataAssistantResult._clean_quantitative_metrics_df(
                df=column_df, sanitized_metric_names=sanitized_metric_names
            )
            if len(cleaned_column_df.index) > 0:
                cleaned_column_dfs.append(
                    ColumnDataFrame(column_name, cleaned_column_df)
                )

        return cleaned_column_dfs

    @staticmethod
    def _clean_quantitative_metrics_df(
        df: pd.DataFrame, sanitized_metric_names: Set[str]
    ) -> pd.DataFrame:
        sanitized_metric_names_list = list(sanitized_metric_names)
        df[sanitized_metric_names_list] = df[sanitized_metric_names_list].apply(
            pd.to_numeric, errors="coerce"
        )
        return df.dropna()

    @staticmethod
    def _get_expect_domain_values_ordinal_chart(
        expectation_type: str,
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.Chart:
        return DataAssistantResult._get_expect_domain_values_to_be_between_chart(
            expectation_type=expectation_type,
            df=df,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
            subtitle=subtitle,
        )

    @staticmethod
    def _get_expect_domain_values_temporal_chart(
        expectation_type: str,
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.Chart:
        return DataAssistantResult._get_expect_domain_values_to_be_between_chart(
            expectation_type=expectation_type,
            df=df,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
            subtitle=subtitle,
        )

    @staticmethod
    def _get_ordinal_metrics_chart(
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.LayerChart:
        return DataAssistantResult._get_quantitative_metrics_chart(
            df=df,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
            subtitle=subtitle,
        )

    @staticmethod
    def _get_temporal_metrics_chart(
        df: pd.DataFrame,
        sanitized_metric_names: Set[str],
        sequential: bool,
        subtitle: Optional[str],
    ) -> alt.LayerChart:
        return DataAssistantResult._get_quantitative_metrics_chart(
            df=df,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
            subtitle=subtitle,
        )

    @staticmethod
    def _get_interactive_expect_column_values_nominal_chart(
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> alt.VConcatChart:
        raise NotImplementedError(
            "Nominal expectation charts have not been implemented for the column domain type."
        )

    @staticmethod
    def _get_interactive_expect_column_values_ordinal_chart(
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> alt.VConcatChart:
        return DataAssistantResult._get_interactive_expect_column_values_to_be_between_chart(
            expectation_type=expectation_type,
            column_dfs=column_dfs,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
        )

    @staticmethod
    def _get_interactive_expect_column_values_temporal_chart(
        expectation_type: str,
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> alt.VConcatChart:
        return DataAssistantResult._get_interactive_expect_column_values_to_be_between_chart(
            expectation_type=expectation_type,
            column_dfs=column_dfs,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
        )

    @staticmethod
    def _get_interactive_nominal_metrics_chart(
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> alt.VConcatChart:
        raise NotImplementedError(
            "Nominal metric charts have not been implemented for the column domain type."
        )

    @staticmethod
    def _get_interactive_ordinal_metrics_chart(
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> alt.LayerChart:
        return DataAssistantResult._get_interactive_metrics_chart(
            column_dfs=column_dfs,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
        )

    @staticmethod
    def _get_interactive_temporal_metrics_chart(
        column_dfs: List[ColumnDataFrame],
        sanitized_metric_names: Set[str],
        sequential: bool,
    ) -> alt.LayerChart:
        return DataAssistantResult._get_interactive_metrics_chart(
            column_dfs=column_dfs,
            sanitized_metric_names=sanitized_metric_names,
            sequential=sequential,
        )
