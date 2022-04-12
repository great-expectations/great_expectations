import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Union

from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
)
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
    init_rule_parameter_builders,
)
from great_expectations.rule_based_profiler.types import (
    Builder,
    Domain,
    ParameterContainer,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExpectationConfigurationBuilder(Builder, ABC):
    exclude_field_names: Set[str] = Builder.exclude_field_names | {
        "validation_parameter_builders",
    }

    def __init__(
        self,
        expectation_type: str,
        validation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        **kwargs
    ):
        """
        The ExpectationConfigurationBuilder will build ExpectationConfiguration objects for a Domain from the Rule.

        Args:
            expectation_type: the "expectation_type" argument of "ExpectationConfiguration" object to be emitted.
            validation_parameter_builder_configs: ParameterBuilder configurations, having whose outputs available (as
            fully-qualified parameter names) is pre-requisite for present ExpectationConfigurationBuilder instance.
            These "ParameterBuilder" configurations help build kwargs needed for this "ExpectationConfigurationBuilder".
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
            data_context: DataContext
            kwargs: additional arguments
        """

        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        self._expectation_type = expectation_type

        self._validation_parameter_builders = init_rule_parameter_builders(
            parameter_builder_configs=validation_parameter_builder_configs,
            data_context=self._data_context,
        )

        """
        Since ExpectationConfigurationBuilderConfigSchema allows arbitrary fields (as ExpectationConfiguration kwargs)
        to be provided, they must be all converted to public property accessors and/or public fields in order for all
        provisions by Builder, SerializableDictDot, and DictDot to operate properly in compliance with their interfaces.
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.debug(
                'Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".',
                k,
                v,
                self.__class__.__name__,
            )

    def build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        force_batch_data: bool = False,
    ) -> ExpectationConfiguration:
        """
        Args:
            domain: Domain object that is context for execution of this ParameterBuilder object.
            variables: attribute name/value pairs
            parameters: Dictionary of ParameterContainer objects corresponding to all Domain objects in memory.
            batch_list: Explicit list of Batch objects to supply data at runtime.
            batch_request: Explicit batch_request used to supply data at runtime.
            force_batch_data: Whether or not to overwrite existing batch_request value in ParameterBuilder components.

        Returns:
            ExpectationConfiguration object.
        """
        self.resolve_validation_dependencies(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_list=batch_list,
            batch_request=batch_request,
            force_batch_data=force_batch_data,
        )

        return self._build_expectation_configuration(
            domain=domain, variables=variables, parameters=parameters
        )

    def resolve_validation_dependencies(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        force_batch_data: bool = False,
    ) -> None:
        validation_parameter_builders: List[ParameterBuilder] = (
            self.validation_parameter_builders or []
        )

        validation_parameter_builder: ParameterBuilder
        for validation_parameter_builder in validation_parameter_builders:
            validation_parameter_builder.build_parameters(
                domain=domain,
                variables=variables,
                parameters=parameters,
                parameter_computation_impl=None,
                json_serialize=None,
                batch_list=batch_list,
                batch_request=batch_request,
                force_batch_data=force_batch_data,
            )

    @abstractmethod
    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        pass

    @property
    def expectation_type(self) -> str:
        return self._expectation_type

    @property
    def validation_parameter_builders(self) -> Optional[List[ParameterBuilder]]:
        return self._validation_parameter_builders


def init_rule_expectation_configuration_builders(
    expectation_configuration_builder_configs: List[dict],
    data_context: Optional["DataContext"] = None,  # noqa: F821
) -> List["ExpectationConfigurationBuilder"]:  # noqa: F821
    expectation_configuration_builder_config: dict
    return [
        init_expectation_configuration_builder(
            expectation_configuration_builder_config=expectation_configuration_builder_config,
            data_context=data_context,
        )
        for expectation_configuration_builder_config in expectation_configuration_builder_configs
    ]


def init_expectation_configuration_builder(
    expectation_configuration_builder_config: Union[
        "ExpectationConfigurationBuilder", dict  # noqa: F821
    ],
    data_context: Optional["DataContext"] = None,  # noqa: F821
) -> "ExpectationConfigurationBuilder":  # noqa: F821
    if not isinstance(expectation_configuration_builder_config, dict):
        expectation_configuration_builder_config = (
            expectation_configuration_builder_config.to_dict()
        )

    expectation_configuration_builder: "ExpectationConfigurationBuilder" = instantiate_class_from_config(  # noqa: F821
        config=expectation_configuration_builder_config,
        runtime_environment={"data_context": data_context},
        config_defaults={
            "class_name": "DefaultExpectationConfigurationBuilder",
            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
        },
    )
    return expectation_configuration_builder
