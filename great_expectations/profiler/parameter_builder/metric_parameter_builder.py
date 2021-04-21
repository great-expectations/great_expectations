from typing import List, Optional

from great_expectations.profiler.parameter_builder.parameter import Parameter
from great_expectations.profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.profiler.profiler_rule.rule_state import RuleState
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


# TODO: <Alex>ALEX -- this class is not used anywhere in the codebase.</Alex>
class MetricParameterBuilder(ParameterBuilder):
    """Class utilized for obtaining a resolved (evaluated) metric (which is labeled a 'parameter') using domain kwargs, value
    kwargs, and a metric name"""

    def __init__(
        self,
        *,
        parameter_name,
        data_context=None,
        metric_name,
        metric_domain_kwargs="$domain.domain_kwargs",
        metric_value_kwargs=None
    ):
        super().__init__(parameter_name=parameter_name, data_context=data_context)
        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    def _build_parameters(
        self,
        *,
        rule_state: Optional[RuleState] = None,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        **kwargs
    ) -> Parameter:
        """
        Builds a dictionary of format {'parameters': A given resolved metric}
            Args:
            :param rule_state: An object keeping track of the state information necessary for rule validation, such as domain,
                    metric parameters, and necessary variables
            :param validator: A Validator object used to obtain metrics
        :return: a dictionary of format {'parameters': A given resolved metric}
        """
        # Obtaining any necessary domain kwargs from rule state, otherwise using instance var
        if self._metric_domain_kwargs.startswith("$"):
            metric_domain_kwargs = rule_state.get_parameter_value(
                fully_qualified_parameter_name=self._metric_domain_kwargs
            )
        else:
            metric_domain_kwargs = self._metric_domain_kwargs

        # Obtaining any necessary value kwargs from rule state, otherwise using instance var
        if (
            self._metric_value_kwargs is not None
            and self._metric_value_kwargs.startswith("$")
        ):
            metric_value_kwargs = rule_state.get_parameter_value(
                fully_qualified_parameter_name=self._metric_value_kwargs
            )
        else:
            metric_value_kwargs = self._metric_value_kwargs

        return Parameter(
            parameters=validator.get_metric(
                metric=MetricConfiguration(
                    metric_name=self._metric_name,
                    metric_domain_kwargs=metric_domain_kwargs,
                    metric_value_kwargs=metric_value_kwargs,
                    metric_dependencies=None,
                )
            ),
            details=None,
        )
