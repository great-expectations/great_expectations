from abc import ABCMeta, abstractmethod
from inspect import isabstract
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import (
    BaseRuleBasedProfiler,
    RuleBasedProfiler,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterNode
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.util import camel_to_snake, measure_execution_time
from great_expectations.validator.validator import Validator


class MetaDataAssistant(ABCMeta):
    "\n    MetaDataAssistant registers every DataAssistant class as it is defined, it them to the DataAssistant registry.\n\n    Any class inheriting from DataAssistant will be registered by snake-casing the name of the class.\n"

    def __new__(cls, clsname, bases, attrs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Instantiate class as part of descentants calling "__init__()" and register its type in "DataAssistant" registry.\n        '
        newclass = super().__new__(cls, clsname, bases, attrs)
        if not newclass.is_abstract():
            newclass.data_assistant_type = camel_to_snake(name=clsname)
            from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
                DataAssistantDispatcher,
            )

            DataAssistantDispatcher.register_data_assistant(data_assistant=newclass)
        return newclass


class DataAssistant(metaclass=MetaDataAssistant):
    '\n    DataAssistant is an application built on top of the Rule-Based Profiler component.\n    DataAssistant subclasses provide exploration and validation of particular aspects of specified data Batch objects.\n\n    DataAssistant usage (e.g., in Jupyter notebook) adheres to the following pattern:\n\n    data_assistant: DataAssistant = VolumeDataAssistant(\n        name="my_volume_data_assistant",\n        validator=validator,\n    )\n    result: DataAssistantResult = data_assistant.run(\n        expectation_suite=None,\n        expectation_suite_name="my_suite",\n        include_citation=True,\n        save_updated_expectation_suite=False,\n    )\n\n    Then:\n        metrics: Dict[Domain, Dict[str, ParameterNode]] = result.metrics\n        expectation_suite: ExpectationSuite = result.expectation_suite\n        expectation_configurations: List[ExpectationConfiguration] = result.expectation_suite.expectations\n        expectation_suite_meta: Dict[str, Any] = expectation_suite.meta\n        profiler_config: RuleBasedProfilerConfig = result.profiler_config\n'
    __alias__: Optional[str] = None

    def __init__(self, name: str, validator: Validator) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        DataAssistant subclasses guide "RuleBasedProfiler" to contain Rule configurations to embody profiling behaviors,\n        corresponding to indended exploration and validation goals.  Then executing "RuleBasedProfiler.run()" yields\n        "RuleBasedProfilerResult" object, containing "fully_qualified_parameter_names_by_domain",\n        "parameter_values_for_fully_qualified_parameter_names_by_domain", "expectation_configurations", and "citation",\n        immediately available for composing "ExpectationSuite" and validating underlying data "Batch" objects.\n\n        Args:\n            name: the name of this DataAssistant object\n            validator: Validator object, containing loaded Batch objects as well as Expectation and Metric operations\n        '
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Builds "RuleBasedProfiler", corresponding to present DataAssistant use case.\n\n        Starts with empty "RuleBasedProfiler" (initialized in constructor) and adds Rule objects.\n\n        Subclasses can add custom "Rule" objects as appropriate for their respective particular DataAssistant use cases.\n        '
        variables: dict = {}
        profiler: Optional[BaseRuleBasedProfiler]
        rules: List[Rule]
        rule: Rule
        domain_builder: DomainBuilder
        parameter_builders: List[ParameterBuilder]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder]
        '\n        For each Self-Initializing "Expectation" as specified by "DataAssistant.expectation_kwargs_by_expectation_type"\n        interface property, retrieve its "RuleBasedProfiler" configuration, construct "Rule" object based on it, while\n        incorporating metrics "ParameterBuilder" objects for "MetricDomainTypes", emitted by "DomainBuilder"\n        of comprised "Rule", specified by "DataAssistant.metrics_parameter_builders_by_domain" interface property.\n        Append this "Rule" object to overall DataAssistant "RuleBasedProfiler" object; incorporate "variables" as well.\n        '
        expectation_type: str
        expectation_kwargs: Dict[(str, Any)]
        for (
            expectation_type,
            expectation_kwargs,
        ) in self.expectation_kwargs_by_expectation_type.items():
            profiler = self._validator.build_rule_based_profiler_for_expectation(
                expectation_type=expectation_type
            )(**expectation_kwargs)
            variables.update(convert_variables_to_dict(variables=profiler.variables))
            rules = profiler.rules
            for rule in rules:
                domain_builder = rule.domain_builder
                parameter_builders = rule.parameter_builders or []
                parameter_builders.extend(
                    self.metrics_parameter_builders_by_domain[
                        Domain(domain_builder.domain_type)
                    ]
                )
                expectation_configuration_builders = (
                    rule.expectation_configuration_builders or []
                )
                self.profiler.add_rule(
                    rule=Rule(
                        name=rule.name,
                        variables=rule.variables,
                        domain_builder=domain_builder,
                        parameter_builders=parameter_builders,
                        expectation_configuration_builders=expectation_configuration_builders,
                    )
                )
        self.profiler.variables = self.profiler.reconcile_profiler_variables(
            variables=variables,
            reconciliation_strategy=DEFAULT_RECONCILATION_DIRECTIVES.variables,
        )

    def run(self) -> DataAssistantResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Run the DataAssistant as it is currently configured.\n\n        Returns:\n            DataAssistantResult: The result object for the DataAssistant\n        "
        data_assistant_result: DataAssistantResult = DataAssistantResult(
            batch_id_to_batch_identifier_display_name_map=self.batch_id_to_batch_identifier_display_name_map(),
            execution_time=0.0,
        )
        run_profiler_on_data(
            data_assistant=self,
            data_assistant_result=data_assistant_result,
            profiler=self.profiler,
            variables=self.variables,
            rules=self.rules,
            batch_list=list(self._batches.values()),
            batch_request=None,
        )
        return self._build_data_assistant_result(
            data_assistant_result=data_assistant_result
        )

    @property
    def name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._name

    @property
    def profiler(self) -> BaseRuleBasedProfiler:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._profiler

    @classmethod
    def is_abstract(cls) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        This method inspects the present class and determines whether or not it contains abstract methods.\n\n        Returns:\n            Boolean value (True if all interface methods are implemented; otherwise, False)\n        "
        return isabstract(cls)

    @property
    @abstractmethod
    def expectation_kwargs_by_expectation_type(self) -> Dict[(str, Dict[(str, Any)])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        DataAssistant subclasses implement this method to return relevant Self-Initializing Expectations with "kwargs".\n\n        Returns:\n            Dictionary of Expectation "kwargs", keyed by "expectation_type".\n        '
        pass

    @property
    @abstractmethod
    def metrics_parameter_builders_by_domain(
        self,
    ) -> Dict[(Domain, List[ParameterBuilder])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        DataAssistant subclasses implement this method to return "ParameterBuilder" objects for "MetricDomainTypes", for\n        every "Domain" type, for which generating metrics of interest is desired.  These metrics will be computed in\n        addition to metrics already computed as part of "Rule" evaluation for every "Domain", emitted by "DomainBuilder"\n        of comprised "Rule"; these auxiliary metrics are aimed entirely for data exploration / visualization purposes.\n\n        Returns:\n            Dictionary of "ParameterBuilder" objects, keyed by members of "MetricDomainTypes" Enum.\n        '
        pass

    @property
    @abstractmethod
    def variables(self) -> Optional[Dict[(str, Any)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Returns:\n            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.\n        '
        pass

    @property
    @abstractmethod
    def rules(self) -> Optional[List[Rule]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Returns:\n            Optional custom list of "Rule" objects (overrides) can be added by subclasses (return "None" if not needed).\n        '
        pass

    @abstractmethod
    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        DataAssistant subclasses implement this method to return subclasses of DataAssistantResult object, which imbue\n        base DataAssistantResult class with methods, pertaining to specifics of particular DataAssistantResult subclass.\n\n        Args:\n            data_assistant_result: Base DataAssistantResult result object of DataAssistant (contains only data fields)\n\n        Returns:\n            DataAssistantResult: The appropriate subclass of base DataAssistantResult result object of the DataAssistant\n        "
        pass

    def get_metrics_by_domain(self) -> Dict[(Domain, Dict[(str, ParameterNode)])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Obtain subset of all parameter values for fully-qualified parameter names by domain, available from entire\n        "RuleBasedProfiler" state, where "Domain" objects are among keys included in provisions as proscribed by return\n        value of "DataAssistant.metrics_parameter_builders_by_domain" interface property and fully-qualified parameter\n        names match interface properties of "ParameterBuilder" objects, corresponding to these "Domain" objects.\n\n        Returns:\n            Dictionaries of values for fully-qualified parameter names by Domain for metrics, from "RuleBasedpRofiler"\n        '
        domain_key: Domain
        parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
            (Domain, Dict[(str, ParameterNode)])
        ] = dict(
            filter(
                (
                    lambda element: any(
                        element[0].is_superset(other=domain_key)
                        for domain_key in list(
                            self.metrics_parameter_builders_by_domain.keys()
                        )
                    )
                ),
                self.profiler.get_parameter_values_for_fully_qualified_parameter_names_by_domain().items(),
            )
        )
        domain: Domain
        parameter_builders: List[ParameterBuilder]
        parameter_builder: ParameterBuilder
        fully_qualified_metrics_parameter_names_by_domain: Dict[(Domain, List[str])] = {
            domain: [
                parameter_builder.fully_qualified_parameter_name
                for parameter_builder in parameter_builders
            ]
            for (
                domain,
                parameter_builders,
            ) in self.metrics_parameter_builders_by_domain.items()
        }
        parameter_values_for_fully_qualified_parameter_names: Dict[(str, ParameterNode)]
        fully_qualified_metrics_parameter_names: List[str]
        parameter_values_for_fully_qualified_parameter_names_by_domain = {
            domain: dict(
                filter(
                    (
                        lambda element: (
                            element[0]
                            in fully_qualified_metrics_parameter_names_by_domain[
                                domain_key
                            ]
                        )
                    ),
                    parameter_values_for_fully_qualified_parameter_names.items(),
                )
            )
            for (
                domain_key,
                fully_qualified_metrics_parameter_names,
            ) in fully_qualified_metrics_parameter_names_by_domain.items()
            for (
                domain,
                parameter_values_for_fully_qualified_parameter_names,
            ) in parameter_values_for_fully_qualified_parameter_names_by_domain.items()
            if domain.is_superset(domain_key)
        }
        return parameter_values_for_fully_qualified_parameter_names_by_domain

    def batch_id_to_batch_identifier_display_name_map(
        self,
    ) -> Dict[(str, Set[Tuple[(str, Any)]])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        This method uses loaded "Batch" objects to return the mapping between unique "batch_id" and "batch_identifiers".\n        '
        batch_id: str
        batch: Batch
        return {
            batch_id: set(batch.batch_definition.batch_identifiers.items())
            for (batch_id, batch) in self._batches.items()
        }


@measure_execution_time(
    execution_time_holder_object_reference_name="data_assistant_result",
    execution_time_property_name="execution_time",
    pretty_print=False,
)
def run_profiler_on_data(
    data_assistant: DataAssistant,
    data_assistant_result: DataAssistantResult,
    profiler: BaseRuleBasedProfiler,
    variables: Optional[Dict[(str, Any)]] = None,
    rules: Optional[Dict[(str, Dict[(str, Any)])]] = None,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    '\n    This method executes "run()" of effective "RuleBasedProfiler" and fills "DataAssistantResult" object with outputs.\n\n    Args:\n        data_assistant: Containing "DataAssistant" object, which defines interfaces for computing "DataAssistantResult"\n        data_assistant_result: Destination "DataAssistantResult" object to hold outputs of executing "RuleBasedProfiler"\n        profiler: Effective "RuleBasedProfiler", representing containing "DataAssistant" object\n        variables: attribute name/value pairs (overrides), commonly-used in Builder objects\n        rules: name/(configuration-dictionary) (overrides)\n        batch_list: Explicit list of Batch objects to supply data at runtime\n        batch_request: Explicit batch_request used to supply data at runtime\n    '
    if rules is None:
        rules = []
    rule: Rule
    rules_configs: Optional[Dict[(str, Dict[(str, Any)])]] = {
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
