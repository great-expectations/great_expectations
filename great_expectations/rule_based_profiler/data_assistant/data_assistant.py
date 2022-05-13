from abc import ABCMeta, abstractmethod
from inspect import isabstract
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
    MapMetricColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
    MetricMultiBatchParameterBuilder,
    NumericMetricRangeMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import (
    BaseRuleBasedProfiler,
    RuleBasedProfiler,
)
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    VARIABLES_KEY,
    Domain,
    ParameterContainer,
    ParameterNode,
    SemanticDomainTypes,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
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
            DataAssistantDispatcher.register_data_assistant(data_assistant=newclass)

        return newclass


class DataAssistant(metaclass=MetaDataAssistant):
    """
    DataAssistant is an application built on top of the Rule-Based Profiler component.
    DataAssistant subclasses provide exploration and validation of particular aspects of specified data Batch objects.

    DataAssistant usage (e.g., in Jupyter notebook) adheres to the following pattern:

    data_assistant: DataAssistant = VolumeDataAssistant(
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

    class CommonlyUsedParameterBuilders:
        def get_table_row_count_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="table.row_count",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_distinct_values_count_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.distinct_values.count",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_values_unique_unexpected_count_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column_values.unique.unexpected_count",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column_values.nonnull.unexpected_count",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column_values.null.unexpected_count",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_histogram_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.histogram",
                metric_value_kwargs={
                    "bins": f"{VARIABLES_KEY}bins",
                },
                json_serialize=json_serialize,
            )

        def get_column_quantile_values_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.quantile_values",
                metric_value_kwargs={
                    "quantiles": f"{VARIABLES_KEY}quantiles",
                    "allow_relative_error": f"{VARIABLES_KEY}allow_relative_error",
                },
                json_serialize=json_serialize,
            )

        def get_column_min_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.min",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_max_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.max",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_median_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.median",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_mean_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.mean",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        def get_column_standard_deviation_metric_multi_batch_parameter_builder(
            self,
            json_serialize: Union[str, bool] = True,
        ) -> ParameterBuilder:
            """
            This method instantiates one commonly used "MetricMultiBatchParameterBuilder" with specified directives.
            """
            return self.build_metric_multi_batch_parameter_builder(
                metric_name="column.standard_deviation",
                metric_value_kwargs=None,
                json_serialize=json_serialize,
            )

        @staticmethod
        def build_metric_multi_batch_parameter_builder(
            metric_name: str,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
            json_serialize: Union[str, bool] = True,
        ) -> MetricMultiBatchParameterBuilder:
            """
            This method instantiates "MetricMultiBatchParameterBuilder" class with specific arguments for given purpose.
            """
            return MetricMultiBatchParameterBuilder(
                name=metric_name,
                metric_name=metric_name,
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=metric_value_kwargs,
                enforce_numeric_metric=True,
                replace_nan_with_zero=True,
                reduce_scalar_metric=True,
                evaluation_parameter_builder_configs=None,
                json_serialize=json_serialize,
                data_context=None,
            )

        @staticmethod
        def build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name: str,
            metric_value_kwargs: Optional[Union[str, dict]] = None,
            json_serialize: Union[str, bool] = True,
        ) -> NumericMetricRangeMultiBatchParameterBuilder:
            """
            This method instantiates "MetricMultiBatchParameterBuilder" class with specific arguments for given purpose.
            """
            return NumericMetricRangeMultiBatchParameterBuilder(
                name=f"{metric_name}.range",
                metric_name=metric_name,
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=metric_value_kwargs,
                enforce_numeric_metric=True,
                replace_nan_with_zero=True,
                reduce_scalar_metric=True,
                false_positive_rate=f"{VARIABLES_KEY}false_positive_rate",
                quantile_statistic_interpolation_method=f"{VARIABLES_KEY}quantile_statistic_interpolation_method",
                estimator=f"{VARIABLES_KEY}estimator",
                n_resamples=f"{VARIABLES_KEY}n_resamples",
                random_seed=f"{VARIABLES_KEY}random_seed",
                include_estimator_samples_histogram_in_details=f"{VARIABLES_KEY}include_estimator_samples_histogram_in_details",
                truncate_values=f"{VARIABLES_KEY}truncate_values",
                round_decimals=f"{VARIABLES_KEY}round_decimals",
                evaluation_parameter_builder_configs=None,
                json_serialize=json_serialize,
            )

    COMMONLY_USED_PARAMETER_BUILDERS: CommonlyUsedParameterBuilders = (
        CommonlyUsedParameterBuilders()
    )

    __alias__: Optional[str] = None

    def __init__(
        self,
        name: str,
        validator: Validator,
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

        self._profiler = RuleBasedProfiler(
            name=self.name,
            config_version=1.0,
            variables=None,
            data_context=self._validator.data_context,
        )
        self._build_profiler()

        self._batches = self._validator.batches

    def _build_profiler(self) -> None:
        """
        Builds "RuleBasedProfiler", corresponding to present DataAssistant use case.

        Starts with empty "RuleBasedProfiler" (initialized in constructor) and adds Rule objects.

        Subclasses can add custom "Rule" objects as appropriate for their respective particular DataAssistant use cases.
        """
        variables: dict = {}

        profiler: Optional[BaseRuleBasedProfiler]
        rules: List[Rule]
        rule: Rule
        domain_builder: DomainBuilder
        rule_parameter_builders: List[ParameterBuilder]
        metric_parameter_builders: List[ParameterBuilder]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder]

        """
        For each Self-Initializing "Expectation" as specified by "DataAssistant.expectation_kwargs_by_expectation_type"
        interface property, retrieve its "RuleBasedProfiler" configuration, construct "Rule" object based on it, while
        incorporating metrics "ParameterBuilder" objects for "MetricDomainTypes", emitted by "DomainBuilder"
        of comprised "Rule", specified by "DataAssistant.metrics_parameter_builders_by_domain" interface property.
        Append this "Rule" object to overall DataAssistant "RuleBasedProfiler" object; incorporate "variables" as well.
        """
        expectation_type: str
        expectation_kwargs: Dict[str, Any]
        for (
            expectation_type,
            expectation_kwargs,
        ) in self.expectation_kwargs_by_expectation_type.items():
            profiler = self._validator.build_rule_based_profiler_for_expectation(
                expectation_type=expectation_type
            )(**expectation_kwargs)
            variables.update(convert_variables_to_dict(variables=profiler.variables))
            rules = profiler.rules
            self._add_rules_to_profiler(rules=rules)

        self._validate_profiler_rule_name_uniqueness()

        self._add_rules_to_profiler(rules=self.rules)

        custom_variables: Optional[Dict[str, Any]] = self.variables
        if custom_variables is None:
            custom_variables = {}

        variables.update(custom_variables)

        self.profiler.variables = self.profiler.reconcile_profiler_variables(
            variables=variables,
            reconciliation_strategy=DEFAULT_RECONCILATION_DIRECTIVES.variables,
        )

    def run(
        self,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> DataAssistantResult:
        """
        Run the DataAssistant as it is currently configured.

        Args:
            variables: attribute name/value pairs (overrides), commonly-used in Builder objects
            rules: name/(configuration-dictionary) (overrides)

        Returns:
            DataAssistantResult: The result object for the DataAssistant
        """
        data_assistant_result: DataAssistantResult = DataAssistantResult(
            batch_id_to_batch_identifier_display_name_map=self.batch_id_to_batch_identifier_display_name_map(),
            execution_time=0.0,
        )
        run_profiler_on_data(
            data_assistant=self,
            data_assistant_result=data_assistant_result,
            profiler=self.profiler,
            variables=variables,
            rules=rules,
            batch_list=list(self._batches.values()),
            batch_request=None,
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

    @property
    @abstractmethod
    def expectation_kwargs_by_expectation_type(self) -> Dict[str, Dict[str, Any]]:
        """
        DataAssistant subclasses implement this method to return relevant Self-Initializing Expectations with "kwargs".

        Returns:
            Dictionary of Expectation "kwargs", keyed by "expectation_type".
        """
        pass

    @property
    @abstractmethod
    def metrics_parameter_builders_by_domain(
        self,
    ) -> Dict[Domain, List[ParameterBuilder]]:
        """
        DataAssistant subclasses implement this method to return "ParameterBuilder" objects for "MetricDomainTypes", for
        every "Domain" type, for which generating metrics of interest is desired.  These metrics will be computed in
        addition to metrics already computed as part of "Rule" evaluation for every "Domain", emitted by "DomainBuilder"
        of comprised "Rule"; these auxiliary metrics are aimed entirely for data exploration / visualization purposes.

        Returns:
            Dictionary of "ParameterBuilder" objects, keyed by members of "MetricDomainTypes" Enum.
        """
        pass

    @property
    @abstractmethod
    def variables(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.
        """
        pass

    @property
    @abstractmethod
    def rules(self) -> Optional[List[Rule]]:
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
        "RuleBasedProfiler" state, where "Domain" objects are among keys included in provisions as proscribed by return
        value of "DataAssistant.metrics_parameter_builders_by_domain" interface property and fully-qualified parameter
        names match interface properties of "ParameterBuilder" objects, corresponding to these "Domain" objects.

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
                        self.metrics_parameter_builders_by_domain.keys()
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
                parameter_builder.fully_qualified_parameter_name
                for parameter_builder in parameter_builders
            ]
            for domain, parameter_builders in self.metrics_parameter_builders_by_domain.items()
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
            if domain.is_superset(domain_key)
        }

        return parameter_values_for_fully_qualified_parameter_names_by_domain

    def batch_id_to_batch_identifier_display_name_map(
        self,
    ) -> Dict[str, Set[Tuple[str, Any]]]:
        """
        This method uses loaded "Batch" objects to return the mapping between unique "batch_id" and "batch_identifiers".
        """
        batch_id: str
        batch: Batch
        return {
            batch_id: set(batch.batch_definition.batch_identifiers.items())
            for batch_id, batch in self._batches.items()
        }

    def get_rule_variables_and_validation_parameter_builders_from_self_initializing_expectation(
        self,
        expectation_type: str,
        expectation_kwargs: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[ParameterContainer], Optional[List[ParameterBuilder]]]:
        """
        This method obtains "variables" and "validation_parameter_builder" (from "expectation_configuration_builder")
        from "Rule" implementing self-initialization logic in optional "RuleBasedProfilerConfig" of "Expectation".
        """
        profiler: Optional[
            BaseRuleBasedProfiler
        ] = self._validator.build_rule_based_profiler_for_expectation(
            expectation_type=expectation_type
        )(
            **expectation_kwargs
        )
        if profiler is None:
            return None, None

        rules: List[Rule] = profiler.rules
        rule: Rule = rules[0]
        variables: ParameterContainer = rule.variables
        validation_parameter_builders: Optional[
            List[ParameterBuilder]
        ] = rule.expectation_configuration_builders[0].validation_parameter_builders
        return variables, validation_parameter_builders

    def _validate_profiler_rule_name_uniqueness(self) -> None:
        """
        This private utility method insures that all "Rule" objects in underlying "BaseRuleBasedProfiler" are unique.
        """
        rule: Rule

        profiler_rules: List[Rule] = self.profiler.rules
        if profiler_rules is None:
            profiler_rules = []

        profiler_rule_names: Set[str] = {rule.name for rule in profiler_rules}

        custom_rules: List[Rule] = self.rules
        if custom_rules is None:
            custom_rules = []

        custom_rule_names: Set[str] = {rule.name for rule in custom_rules}

        common_rule_names: Set[str] = profiler_rule_names & custom_rule_names
        if common_rule_names:
            raise ge_exceptions.ProfilerConfigurationError(
                message=f"""Rule names in {self.__class__.__name__} must be unique; duplicate(s) found \
({common_rule_names}).
"""
            )

    def _add_rules_to_profiler(
        self,
        rules: Optional[List[Rule]] = None,
    ) -> None:
        """
        This private utility method adds supplied "Rule" objects to underlying "BaseRuleBasedProfiler" object.

        Args:
            rules: List of "Rule" objects to be added to given "BaseRuleBasedProfiler" object
        """
        rule: Rule
        domain_builder: DomainBuilder
        rule_parameter_builders: List[ParameterBuilder]
        metric_parameter_builders: Optional[List[ParameterBuilder]]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder]

        rules = rules or []
        for rule in rules:
            domain_builder = rule.domain_builder
            rule_parameter_builders = rule.parameter_builders or []
            metric_parameter_builders = self.metrics_parameter_builders_by_domain.get(
                Domain(
                    domain_builder.domain_type,
                )
            )
            if metric_parameter_builders:
                rule_parameter_builders.extend(metric_parameter_builders)

            expectation_configuration_builders = (
                rule.expectation_configuration_builders or []
            )
            self.profiler.add_rule(
                rule=Rule(
                    name=rule.name,
                    variables=rule.variables,
                    domain_builder=domain_builder,
                    parameter_builders=rule_parameter_builders,
                    expectation_configuration_builders=expectation_configuration_builders,
                )
            )


@measure_execution_time(
    execution_time_holder_object_reference_name="data_assistant_result",
    execution_time_property_name="execution_time",
    pretty_print=False,
)
def run_profiler_on_data(
    data_assistant: DataAssistant,
    data_assistant_result: DataAssistantResult,
    profiler: BaseRuleBasedProfiler,
    variables: Optional[Dict[str, Any]] = None,
    rules: Optional[Dict[str, Dict[str, Any]]] = None,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[BatchRequestBase, dict]] = None,
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
    """
    if rules is None:
        rules = []

    rule: Rule
    rules_configs: Optional[Dict[str, Dict[str, Any]]] = {
        rule.name: rule.to_json_dict() for rule in rules
    }
    rule_based_profiler_result: RuleBasedProfilerResult = profiler.run(
        variables=variables,
        rules=rules_configs,
        batch_list=batch_list,
        batch_request=batch_request,
        recompute_existing_parameter_values=False,
        reconciliation_directives=DEFAULT_RECONCILATION_DIRECTIVES,
    )
    result: DataAssistantResult = data_assistant_result
    result.profiler_config = profiler.config
    result.metrics_by_domain = data_assistant.get_metrics_by_domain()
    result.expectation_configurations = (
        rule_based_profiler_result.expectation_configurations
    )
    result.citation = rule_based_profiler_result.citation


def build_map_metric_rule(
    rule_name: str,
    expectation_type: str,
    map_metric_name: str,
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

    column_values_unique_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_unique_unexpected_count_metric_multi_batch_parameter_builder(
        json_serialize=True
    )
    column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder(
        json_serialize=True
    )
    column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder(
        json_serialize=True
    )

    total_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_table_row_count_metric_multi_batch_parameter_builder(
        json_serialize=False
    )
    column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder(
        json_serialize=False
    )
    evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        ParameterBuilderConfig(
            **total_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
        ),
        ParameterBuilderConfig(
            **column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
        ),
    ]
    column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations: MeanUnexpectedMapMetricMultiBatchParameterBuilder = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
        name=f"{map_metric_name}.unexpected_value",
        map_metric_name=map_metric_name,
        total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
        null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        json_serialize=True,
        data_context=None,
    )

    validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        ParameterBuilderConfig(
            **column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations.to_json_dict()
        ),
    ]
    max_column_attribute_metric_mean_unexpected_value_ratio: float = 1.0e-2
    expect_column_values_to_be_attribute_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type=expectation_type,
        validation_parameter_builder_configs=validation_parameter_builder_configs,
        column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
        condition=f"{column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations.fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY} <= {max_column_attribute_metric_mean_unexpected_value_ratio}",
        meta={
            "profiler_details": f"{column_values_attribute_mean_unexpected_value_multi_batch_parameter_builder_for_validations.fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
        },
    )

    parameter_builders: List[ParameterBuilder] = [
        column_values_unique_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
        column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
    ]
    expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
        expect_column_values_to_be_attribute_expectation_configuration_builder,
    ]
    rule: Rule = Rule(
        name=rule_name,
        variables=None,
        domain_builder=map_metric_column_domain_builder,
        parameter_builders=parameter_builders,
        expectation_configuration_builders=expectation_configuration_builders,
    )

    return rule
