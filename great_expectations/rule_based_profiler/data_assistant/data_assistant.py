from abc import ABCMeta, abstractmethod
from inspect import isabstract
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.core.domain import Domain, SemanticDomainTypes
from great_expectations.core.id_dict import deep_convert_properties_iterable_to_id_dict
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,  # noqa: TCH001
)
from great_expectations.datasource.fluent.interfaces import (
    Batch as FluentBatch,  # noqa: TCH001
)
from great_expectations.rule_based_profiler import (
    RuleBasedProfilerResult,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import (
    MapMetricColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
)
from great_expectations.rule_based_profiler.helpers.runtime_environment import (
    RuntimeEnvironmentDomainTypeDirectives,
    RuntimeEnvironmentVariablesDirectives,
)
from great_expectations.rule_based_profiler.helpers.util import sanitize_parameter_name
from great_expectations.rule_based_profiler.parameter_builder import (
    HistogramSingleBatchParameterBuilder,
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
    MetricMultiBatchParameterBuilder,
    MetricSingleBatchParameterBuilder,
    NumericMetricRangeMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.regex_pattern_string_parameter_builder import (
    RegexPatternStringParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    VARIABLES_KEY,
    ParameterNode,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import (
    BaseRuleBasedProfiler,
    RuleBasedProfiler,
)
from great_expectations.util import camel_to_snake, measure_execution_time
from great_expectations.validator.validator import Validator

# noinspection PyMethodParameters


class MetaDataAssistant(ABCMeta):
    """
    MetaDataAssistant registers every DataAssistant class as it is defined, it them to the DataAssistant registry.

    Any class inheriting from DataAssistant will be registered by snake-casing the name of the class.
    """

    def __new__(cls, clsname, bases, attrs):
        """
        Instantiate class as part of descentants calling "__init__()" and register its type in "DataAssistant" registry.
        """
        newclass = super().__new__(cls, clsname, bases, attrs)

        # noinspection PyUnresolvedReferences
        if not newclass.is_abstract():
            # Only particular "DataAssistant" implementations must be registered.
            newclass.data_assistant_type = camel_to_snake(name=clsname)

            from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
                DataAssistantDispatcher,
            )

            # noinspection PyTypeChecker
            DataAssistantDispatcher._register_data_assistant(data_assistant=newclass)

        return newclass


class DataAssistant(metaclass=MetaDataAssistant):
    """
    DataAssistant is an application built on top of the Rule-Based Profiler component.
    DataAssistant subclasses provide exploration and validation of particular aspects of specified data Batch objects.

    DataAssistant usage (e.g., in Jupyter notebook) adheres to the following pattern:

    data_assistant = VolumeDataAssistant(
        name="my_volume_data_assistant",
        validator=validator,
    )
    result: DataAssistantResult = data_assistant.run(
        variables=None,
        rules=None,
    )

    Then:
        metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]] = result.metrics_by_domain
        expectation_configurations: List[ExpectationConfiguration] = result.expectation_configurations
        profiler_config: RuleBasedProfilerConfig = result.profiler_config
        ...
    """

    data_assistant_type: str  # Added by `MetaDataAssistant`

    class CommonlyUsedParameterBuilders:
        def get_table_row_count_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="table.row_count",
                metric_domain_kwargs=None,
                metric_value_kwargs=None,
            )

        def get_table_columns_metric_multi_batch_parameter_builder(
            self, include_nested: bool = False
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="table.columns",
                suffix=None,
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs={
                    "include_nested": include_nested,
                },
            )

        def get_column_distinct_values_count_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.distinct_values.count",
                metric_value_kwargs=None,
            )

        def get_column_values_unique_unexpected_count_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_value_kwargs=None,
            )

        def get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_value_kwargs=None,
            )

        def get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name=f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_value_kwargs=None,
            )

        def get_column_quantile_values_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.quantile_values",
                metric_value_kwargs={
                    "quantiles": f"{VARIABLES_KEY}quantiles",
                    "allow_relative_error": f"{VARIABLES_KEY}allow_relative_error",
                },
            )

        def get_column_min_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.min",
                metric_value_kwargs=None,
            )

        def get_column_max_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.max",
                metric_value_kwargs=None,
            )

        def get_column_min_length_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column_values.length.min",
                metric_value_kwargs=None,
            )

        def get_column_max_length_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column_values.length.max",
                metric_value_kwargs=None,
            )

        def get_column_median_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.median",
                metric_value_kwargs=None,
            )

        def get_column_mean_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.mean",
                metric_value_kwargs=None,
            )

        def get_column_standard_deviation_metric_multi_batch_parameter_builder(
            self,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_numeric_metric_multi_batch_parameter_builder(
                metric_name="column.standard_deviation",
                metric_value_kwargs=None,
            )

        @staticmethod
        def build_metric_multi_batch_parameter_builder(
            metric_name: str,
            suffix: Optional[str] = None,
            metric_domain_kwargs: Optional[
                Union[str, dict]
            ] = DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
        ) -> ParameterBuilder:
            """
            This method instantiates "MetricMultiBatchParameterBuilder" with specific arguments for given purpose.
            """
            name: str = sanitize_parameter_name(name=metric_name, suffix=suffix)
            return MetricMultiBatchParameterBuilder(
                name=name,
                metric_name=metric_name,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
                single_batch_mode=False,
                enforce_numeric_metric=False,
                replace_nan_with_zero=False,
                reduce_scalar_metric=True,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )

        @staticmethod
        def build_metric_single_batch_parameter_builder(
            metric_name: str,
            suffix: Optional[str] = None,
            metric_domain_kwargs: Optional[
                Union[str, dict]
            ] = DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
        ) -> ParameterBuilder:
            """
            This method instantiates "MetricSingleBatchParameterBuilder" class with arguments for specific purpose.
            """
            name: str = sanitize_parameter_name(name=metric_name, suffix=suffix)
            return MetricSingleBatchParameterBuilder(
                name=name,
                metric_name=metric_name,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
                enforce_numeric_metric=False,
                replace_nan_with_zero=False,
                reduce_scalar_metric=True,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )

        @staticmethod
        def build_numeric_metric_multi_batch_parameter_builder(
            metric_name: str,
            suffix: Optional[str] = None,
            metric_domain_kwargs: Optional[
                Union[str, dict]
            ] = DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
        ) -> ParameterBuilder:
            """
            This method instantiates "MetricMultiBatchParameterBuilder" class with specific arguments for given purpose.
            """
            name: str = sanitize_parameter_name(name=metric_name, suffix=suffix)
            return MetricMultiBatchParameterBuilder(
                name=name,
                metric_name=metric_name,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
                single_batch_mode=False,
                enforce_numeric_metric=True,
                replace_nan_with_zero=True,
                reduce_scalar_metric=True,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )

        @staticmethod
        def build_numeric_metric_single_batch_parameter_builder(
            metric_name: str,
            suffix: Optional[str] = None,
            metric_domain_kwargs: Optional[
                Union[str, dict]
            ] = DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
        ) -> ParameterBuilder:
            """
            This method instantiates "MetricSingleBatchParameterBuilder" class with arguments for specific purpose.
            """
            name: str = sanitize_parameter_name(name=metric_name, suffix=suffix)
            return MetricSingleBatchParameterBuilder(
                name=name,
                metric_name=metric_name,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
                enforce_numeric_metric=True,
                replace_nan_with_zero=True,
                reduce_scalar_metric=True,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )

        @staticmethod
        def build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name: Optional[str] = None,
            suffix: Optional[str] = None,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
            evaluation_parameter_builder_configs: Optional[
                List[ParameterBuilderConfig]
            ] = None,
        ) -> ParameterBuilder:
            """
            This method instantiates "NumericMetricRangeMultiBatchParameterBuilder" class with specific arguments for given purpose.
            """
            if evaluation_parameter_builder_configs is None:
                evaluation_parameter_builder_configs = []

            metric_multi_batch_parameter_builder_name: Optional[str] = None
            if metric_name is None:
                metric_name = evaluation_parameter_builder_configs[0]["metric_name"]
                metric_multi_batch_parameter_builder_name = (
                    evaluation_parameter_builder_configs[0].name
                )

            name: str = f"{metric_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}range"
            name = sanitize_parameter_name(name=name, suffix=suffix)
            return NumericMetricRangeMultiBatchParameterBuilder(
                name=name,
                metric_name=metric_name,
                metric_multi_batch_parameter_builder_name=metric_multi_batch_parameter_builder_name,
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=metric_value_kwargs,
                enforce_numeric_metric=True,
                replace_nan_with_zero=True,
                reduce_scalar_metric=True,
                false_positive_rate=f"{VARIABLES_KEY}false_positive_rate",
                estimator=f"{VARIABLES_KEY}estimator",
                n_resamples=f"{VARIABLES_KEY}n_resamples",
                random_seed=f"{VARIABLES_KEY}random_seed",
                quantile_statistic_interpolation_method=f"{VARIABLES_KEY}quantile_statistic_interpolation_method",
                quantile_bias_correction=f"{VARIABLES_KEY}quantile_bias_correction",
                quantile_bias_std_error_ratio_threshold=f"{VARIABLES_KEY}quantile_bias_std_error_ratio_threshold",
                include_estimator_samples_histogram_in_details=f"{VARIABLES_KEY}include_estimator_samples_histogram_in_details",
                truncate_values=f"{VARIABLES_KEY}truncate_values",
                round_decimals=f"{VARIABLES_KEY}round_decimals",
                evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
                data_context=None,
            )

        @staticmethod
        def build_regex_pattern_string_parameter_builder(
            name: str,
        ) -> ParameterBuilder:
            """
            This method instantiates "RegexPatternStringParameterBuilder" class with specific arguments for given purpose.
            """
            name = sanitize_parameter_name(name=name, suffix=None)
            return RegexPatternStringParameterBuilder(
                name=name,
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=None,
                threshold=1.0,
                candidate_regexes=f"{VARIABLES_KEY}candidate_regexes",
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )

        @staticmethod
        def build_histogram_single_batch_parameter_builder(
            name: str,
        ) -> ParameterBuilder:
            """
            This method instantiates "HistogramSingleBatchParameterBuilder" class with specific arguments for given purpose.
            """
            name = sanitize_parameter_name(name=name, suffix=None)
            return HistogramSingleBatchParameterBuilder(
                name=name,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )

    commonly_used_parameter_builders: CommonlyUsedParameterBuilders = (
        CommonlyUsedParameterBuilders()
    )

    __alias__: Optional[str] = None

    def __init__(
        self,
        name: str,
        validator: Optional[Validator],
    ) -> None:
        """
        DataAssistant subclasses guide "RuleBasedProfiler" to contain Rule configurations to embody profiling behaviors,
        corresponding to indended exploration and validation goals.  Then executing "RuleBasedProfiler.run()" yields
        "RuleBasedProfilerResult" object, containing "fully_qualified_parameter_names_by_domain",
        "parameter_values_for_fully_qualified_parameter_names_by_domain", "expectation_configurations", and "citation",
        immediately available for composing "ExpectationSuite" and validating underlying data "Batch" objects.

        Args:
            name: the name of this DataAssistant object
            validator: Validator object, containing loaded Batch objects as well as Expectation and Metric operations
        """
        self._name = name

        self._validator = validator

        self._data_context = (
            self._validator.data_context if self._validator is not None else None
        )
        self._batches: Optional[Dict[str, Union[Batch, FluentBatch]]] = (
            self._validator.batches if self._validator is not None else None
        )

        variables: Optional[Dict[str, Any]] = self.get_variables() or {}
        self._profiler = RuleBasedProfiler(
            name=self.name,
            config_version=1.0,
            variables=variables,
            data_context=self._data_context,
        )

        self._metrics_parameter_builders_by_domain = {}

        rules: List[Rule] = self.get_rules() or []

        rule: Rule
        for rule in rules:
            self.profiler.add_rule(rule=rule)
            self._metrics_parameter_builders_by_domain[
                Domain(
                    domain_type=rule.domain_builder.domain_type
                    if rule.domain_builder
                    else "",
                    rule_name=rule.name,
                )
            ] = (
                rule.parameter_builders or []
            )

    @public_api
    def run(
        self,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        variables_directives_list: Optional[
            List[RuntimeEnvironmentVariablesDirectives]
        ] = None,
        domain_type_directives_list: Optional[
            List[RuntimeEnvironmentDomainTypeDirectives]
        ] = None,
    ) -> DataAssistantResult:
        """Run the DataAssistant as it is currently configured.

        Example Usage::

            data_assistant = VolumeDataAssistant(
                name="my_volume_data_assistant",
                validator=validator,
            )
            result: DataAssistantResult = data_assistant.run(
                variables=None,
                rules=None,
            )

        Args:
            variables: Attribute name/value pairs (overrides); commonly-used in Builder objects.
            rules: Name/configuration dictionary (overrides)
            variables_directives_list: Additional/override runtime variables directives (modify `BaseRuleBasedProfiler`).
            domain_type_directives_list: Additional/override runtime domain directives (modify `BaseRuleBasedProfiler`).

        Returns:
            An instance of `DataAssistantResult`.
        """
        usage_statistics_handler: Optional[UsageStatisticsHandler]
        if self._data_context is None:
            usage_statistics_handler = None
        else:
            usage_statistics_handler = self._data_context._usage_statistics_handler

        batches: Dict[str, Union[Batch, FluentBatch]] = self._batches or {}

        data_assistant_result = DataAssistantResult(
            _batch_id_to_batch_identifier_display_name_map=self._batch_id_to_batch_identifier_display_name_map(),
            _usage_statistics_handler=usage_statistics_handler,
        )
        run_profiler_on_data(
            data_assistant=self,
            data_assistant_result=data_assistant_result,
            profiler=self._profiler,
            variables=variables,
            rules=rules,
            batch_list=list(batches.values()),
            batch_request=None,
            variables_directives_list=variables_directives_list,
            domain_type_directives_list=domain_type_directives_list,
        )
        return self._build_data_assistant_result(
            data_assistant_result=data_assistant_result
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def profiler(self) -> BaseRuleBasedProfiler:
        return self._profiler

    @classmethod
    def is_abstract(cls) -> bool:
        """
        This method inspects the present class and determines whether or not it contains abstract methods.

        Returns:
            Boolean value (True if all interface methods are implemented; otherwise, False)
        """
        return isabstract(cls)

    @abstractmethod
    def get_variables(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.
        """
        pass

    @abstractmethod
    def get_rules(self) -> Optional[List[Rule]]:
        """
        Returns:
            Optional custom list of "Rule" objects (overrides) can be added by subclasses (return "None" if not needed).
        """
        pass

    @abstractmethod
    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        """
        DataAssistant subclasses implement this method to return subclasses of DataAssistantResult object, which imbue
        base DataAssistantResult class with methods, pertaining to specifics of particular DataAssistantResult subclass.

        Args:
            data_assistant_result: Base DataAssistantResult result object of DataAssistant (contains only data fields)

        Returns:
            DataAssistantResult: The appropriate subclass of base DataAssistantResult result object of the DataAssistant
        """
        pass

    # noinspection PyShadowingNames
    def get_metrics_by_domain(self) -> Dict[Domain, Dict[str, ParameterNode]]:
        """
        Obtain subset of all parameter values for fully-qualified parameter names by domain, available from entire
        "RuleBasedProfiler" state, where "Domain" objects are among keys included in provisions as proscribed by value
        of "DataAssistant._metrics_parameter_builders_by_domain" private attribute and fully-qualified parameter names
        match interface properties of "ParameterBuilder" objects, corresponding to these partial "Domain" objects.

        Returns:
            Dictionaries of values for fully-qualified parameter names by Domain for metrics, from "RuleBasedpRofiler"
        """
        domain_key: Domain

        # noinspection PyTypeChecker
        parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = dict(
            filter(
                lambda element: any(
                    element[0].is_superset(other=domain_key)
                    for domain_key in list(
                        self._metrics_parameter_builders_by_domain.keys()
                    )
                ),
                self.profiler.get_parameter_values_for_fully_qualified_parameter_names_by_domain().items(),
            )
        )

        domain: Domain

        parameter_builders: List[ParameterBuilder]
        parameter_builder: ParameterBuilder
        fully_qualified_metrics_parameter_names_by_domain: Dict[Domain, List[str]] = {
            domain: [
                parameter_builder.json_serialized_fully_qualified_parameter_name
                for parameter_builder in parameter_builders
            ]
            + [
                parameter_builder.raw_fully_qualified_parameter_name
                for parameter_builder in parameter_builders
            ]
            for domain, parameter_builders in self._metrics_parameter_builders_by_domain.items()
        }

        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        fully_qualified_metrics_parameter_names: List[str]

        # noinspection PyTypeChecker
        parameter_values_for_fully_qualified_parameter_names_by_domain = {
            domain: dict(
                filter(
                    lambda element: element[0]
                    in fully_qualified_metrics_parameter_names_by_domain[domain_key],
                    parameter_values_for_fully_qualified_parameter_names.items(),
                )
            )
            for domain_key, fully_qualified_metrics_parameter_names in fully_qualified_metrics_parameter_names_by_domain.items()
            for domain, parameter_values_for_fully_qualified_parameter_names in parameter_values_for_fully_qualified_parameter_names_by_domain.items()
            if domain.is_superset(other=domain_key)
        }

        return parameter_values_for_fully_qualified_parameter_names_by_domain

    def _batch_id_to_batch_identifier_display_name_map(
        self,
    ) -> Dict[str, Set[Tuple[str, Any]]]:
        """
        This method uses loaded "Batch" objects to return the mapping between unique "batch_id" and "batch_identifiers".
        """
        batches: Dict[str, Union[Batch, FluentBatch]] = self._batches or {}

        batch_id: str
        batch: Batch
        return {
            batch_id: set(
                deep_convert_properties_iterable_to_id_dict(
                    source=batch.batch_definition.batch_identifiers
                ).items()
            )
            for batch_id, batch in batches.items()
        }


@measure_execution_time(
    execution_time_holder_object_reference_name="data_assistant_result",
    execution_time_property_name="profiler_execution_time",
    pretty_print=False,
)
def run_profiler_on_data(  # noqa: PLR0913
    data_assistant: DataAssistant,
    data_assistant_result: DataAssistantResult,
    profiler: BaseRuleBasedProfiler,
    variables: Optional[Dict[str, Any]] = None,
    rules: Optional[Dict[str, Dict[str, Any]]] = None,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[BatchRequestBase, dict]] = None,
    variables_directives_list: Optional[
        List[RuntimeEnvironmentVariablesDirectives]
    ] = None,
    domain_type_directives_list: Optional[
        List[RuntimeEnvironmentDomainTypeDirectives]
    ] = None,
) -> None:
    """
    This method executes "run()" of effective "RuleBasedProfiler" and fills "DataAssistantResult" object with outputs.

    Args:
        data_assistant: Containing "DataAssistant" object, which defines interfaces for computing "DataAssistantResult"
        data_assistant_result: Destination "DataAssistantResult" object to hold outputs of executing "RuleBasedProfiler"
        profiler: Effective "BaseRuleBasedProfiler", representing containing "DataAssistant" object
        variables: attribute name/value pairs (overrides), commonly-used in Builder objects
        rules: name/(configuration-dictionary) (overrides)
        batch_list: Explicit list of Batch objects to supply data at runtime
        batch_request: Explicit batch_request used to supply data at runtime
        variables_directives_list: additional/override runtime variables directives (modify "BaseRuleBasedProfiler")
        domain_type_directives_list: additional/override runtime domain directives (modify "BaseRuleBasedProfiler")
    """
    comment: str = f"""Created by effective Rule-Based Profiler of {data_assistant.__class__.__name__} with the \
configuration included.
"""
    rule_based_profiler_result: RuleBasedProfilerResult = profiler.run(
        variables=variables,
        rules=rules,
        batch_list=batch_list,
        batch_request=batch_request,
        runtime_configuration=None,
        reconciliation_directives=DEFAULT_RECONCILATION_DIRECTIVES,
        variables_directives_list=variables_directives_list,
        domain_type_directives_list=domain_type_directives_list,
        comment=comment,
    )
    result: DataAssistantResult = data_assistant_result
    result.profiler_config = profiler.config
    result.rule_domain_builder_execution_time = (
        rule_based_profiler_result.rule_domain_builder_execution_time
    )
    result.rule_execution_time = rule_based_profiler_result.rule_execution_time
    result.metrics_by_domain = data_assistant.get_metrics_by_domain()
    result.expectation_configurations = (
        rule_based_profiler_result.expectation_configurations
    )
    result.citation = rule_based_profiler_result.citation


def build_map_metric_rule(  # noqa: PLR0913
    data_assistant_class_name: str,
    rule_name: str,
    expectation_type: str,
    map_metric_name: str,
    total_count_metric_multi_batch_parameter_builder_for_evaluations: Optional[
        ParameterBuilder
    ] = None,
    include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
    exclude_column_names: Optional[Union[str, Optional[List[str]]]] = None,
    include_column_name_suffixes: Optional[Union[str, Iterable, List[str]]] = None,
    exclude_column_name_suffixes: Optional[Union[str, Iterable, List[str]]] = None,
    semantic_type_filter_module_name: Optional[str] = None,
    semantic_type_filter_class_name: Optional[str] = None,
    include_semantic_types: Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ] = None,
    exclude_semantic_types: Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ] = None,
    max_unexpected_values: Union[str, int] = 0,
    max_unexpected_ratio: Optional[Union[str, float]] = None,
    min_max_unexpected_values_proportion: Union[str, float] = 9.75e-1,
) -> Rule:
    """
    This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for any "map" style metric.
    """

    # Step-1: Instantiate "MapMetricColumnDomainBuilder" for specified "map_metric_name" (subject to directives).

    map_metric_column_domain_builder: MapMetricColumnDomainBuilder = (
        MapMetricColumnDomainBuilder(
            map_metric_name=map_metric_name,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
            include_column_name_suffixes=include_column_name_suffixes,
            exclude_column_name_suffixes=exclude_column_name_suffixes,
            semantic_type_filter_module_name=semantic_type_filter_module_name,
            semantic_type_filter_class_name=semantic_type_filter_class_name,
            include_semantic_types=include_semantic_types,
            exclude_semantic_types=exclude_semantic_types,
            max_unexpected_values=max_unexpected_values,
            max_unexpected_ratio=max_unexpected_ratio,
            min_max_unexpected_values_proportion=min_max_unexpected_values_proportion,
            data_context=None,
        )
    )

    # Step-2: Declare "ParameterBuilder" for every relevant metric of interest.

    parameter_builders: List[ParameterBuilder] = []

    column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations: Optional[
        ParameterBuilder
    ] = None

    if map_metric_name == "column_values.unique":
        column_values_unique_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_unique_unexpected_count_metric_multi_batch_parameter_builder()
        )
        parameter_builders.append(
            column_values_unique_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        )
    elif map_metric_name == "column_values.null":
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder()
        )
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations = column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        parameter_builders.append(
            column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        )
    elif map_metric_name == "column_values.nonnull":
        column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder()
        )
        parameter_builders.append(
            column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        )
    else:
        raise ValueError(
            f"""Metric "{map_metric_name}" is not supported as part of "{data_assistant_class_name}" implementation."""
        )

    # Step-3: Set up "MeanUnexpectedMapMetricMultiBatchParameterBuilder" to compute "condition" for emitting "ExpectationConfiguration" (based on "Domain" data).

    if total_count_metric_multi_batch_parameter_builder_for_evaluations is None:
        total_count_metric_multi_batch_parameter_builder_for_evaluations = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )

    if (
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations
        is None
    ):
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder()
        )

    evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        ParameterBuilderConfig(
            **total_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
        ),
        ParameterBuilderConfig(
            **column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
        ),
    ]
    column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
        name=f"{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        map_metric_name=map_metric_name,
        total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
        null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        data_context=None,
    )

    # Step-4: Pass "MeanUnexpectedMapMetricMultiBatchParameterBuilder" as "validation" "ParameterBuilder" for "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

    validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        ParameterBuilderConfig(
            **column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations.to_json_dict()
        ),
    ]
    expect_column_values_to_be_attribute_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type=expectation_type,
        validation_parameter_builder_configs=validation_parameter_builder_configs,
        column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
        condition=f"{column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY} <= 1.0 - {VARIABLES_KEY}success_ratio",
        meta={
            "profiler_details": f"{column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
        },
    )

    # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

    variables: dict = {
        "success_ratio": 7.5e-1,
    }

    expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
        expect_column_values_to_be_attribute_expectation_configuration_builder,
    ]
    rule = Rule(
        name=rule_name,
        variables=variables,
        domain_builder=map_metric_column_domain_builder,
        parameter_builders=parameter_builders,
        expectation_configuration_builders=expectation_configuration_builders,
    )

    return rule
