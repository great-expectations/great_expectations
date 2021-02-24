from typing import Dict, Optional

from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.validator.validator import Validator

from .. import DataContext
from ..core import ExpectationSuite
from .exceptions import (
    ProfilerConfigurationError,
    ProfilerError,
    ProfilerExecutionError,
)
from .profiler_rule import ProfilerRule


class Profiler:
    def __init__(
        self,
        *,
        rules: Optional[Dict[str:ProfilerRule]] = None,
        rule_configs: Optional[Dict[str, Dict]] = None,
        data_context: Optional[DataContext] = None,
    ):
        """
        Create a new Profiler using configured rules.

        Args:
            rules:
        """
        self._data_context = data_context
        assert (
            sum([bool(x) for x in (rules, rule_configs)]) == 1
        ), "Exactly one of rules or rule_configs must be provided"

        if rules is None:
            self._rules = []
            for rule_name, rule_config in rule_configs.items():
                domain_builder_config = rule_config.get("domain_builder")
                if domain_builder_config is None:
                    raise ProfilerConfigurationError(
                        f"Invalid rule {rule_name}: no domain_builder found"
                    )
                domain_builder = instantiate_class_from_config(
                    domain_builder_config,
                    runtime_environment={"data_context": data_context},
                )

                parameter_builders = []
                parameter_builder_configs = rule_config.get("parameter_builders")
                for parameter_builder_config in parameter_builder_configs:
                    parameter_builders.append(
                        instantiate_class_from_config(
                            parameter_builder_config,
                            runtime_environment={"data_context": data_context},
                        )
                    )

                configuration_builders = []
                configuration_builder_configs = rule_config.get(
                    "configuration_builders"
                )
                for configuration_builder_config in configuration_builder_configs:
                    configuration_builders.append(
                        instantiate_class_from_config(
                            configuration_builder_config,
                            runtime_environment={"data_context": data_context},
                            config_defaults={
                                "class_name": "ParameterIdConfigurationBuilder"
                            },
                        )
                    )

                self._rules.append(
                    ProfilerRule(
                        rule_name,
                        domain_builder,
                        parameter_builders,
                        configuration_builders,
                    )
                )

    @property
    def data_context(self):
        return self._data_context

    def profile(
        self,
        *,
        validator=None,
        batch=None,
        batches=None,
        batch_request=None,
        batch_ids=None,
        data_context=None,
    ):
        if sum([bool(x) for x in (validator, batch, batches, batch_request)]) != 1:
            raise ProfilerError(
                "Exactly one of validator, batch, batches, or batch_request must be provided."
            )

        if data_context is not None:
            self._data_context = data_context

        if validator is None:
            if batch:
                validator = Validator(
                    execution_engine=batch.data.execution_engine, batches=[batch]
                )
            elif batches:
                execution_engine = batches[0].data.execution_engine
                for batch in batches:
                    if batch.data.execution_engine != execution_engine:
                        raise ProfilerExecutionError(
                            f"batch {batch.id} does not share an execution engine with all other batches in the same batches list."
                        )
                validator = Validator(
                    execution_engine=execution_engine, batches=batches
                )
            elif batch_request:
                if not self.data_context:
                    raise ProfilerExecutionError(
                        "Unable to profile using a batch_request if no data_context is provided."
                    )
                validator = self.data_context.get_validator(batch_request)

        # Verify that all requested batch_ids are loaded
        if batch_ids is None:
            batch_ids = [validator.active_batch_id]

        unloaded_batch_ids = []
        for batch_id in batch_ids:
            if batch_id not in validator.loaded_batch_ids:
                unloaded_batch_ids.append(batch_id)

        if len(unloaded_batch_ids) > 0:
            raise ProfilerExecutionError(
                f"batch_ids {unloaded_batch_ids} were requested but are not available."
            )

        suite = ExpectationSuite(expectation_suite_name=f"self.__class__.__name__")
        for rule in self._rules:
            result = rule.evaluate(validator, batch_ids)
            for config in result:
                suite.add_expectation(config)

        return suite
