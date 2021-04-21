from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import IDDict
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.profiler.parameter_builder.parameter import Parameter


class RuleState:
    """Manages state for ProfilerRule objects. Keeps track of rule domain, rule parameters,
    and any other necessary variables for validating the rule"""

    # TODO: <Alex>ALEX -- Add type hints; what are the types?</Alex>
    def __init__(
        self,
        active_domain: Optional[
            Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]
        ] = None,
        domains: Optional[
            List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]]
        ] = None,
        # TODO: <Alex>ALEX -- what is the structure of this "parameters" argument?</Alex>
        parameters: Optional[Dict[str, Parameter]] = None,
        variables=None,
    ):
        self._active_domain = active_domain
        if domains is None:
            domains = {}
        self._domains = domains
        if parameters is None:
            parameters = {}
        self._parameters = parameters
        # TODO: <Alex>ALEX -- what is the type -- what kind of a dictionary is "variables"?</Alex>
        if variables is None:
            variables = {}
        self._variables = variables

    @property
    def parameters(self) -> Dict[str, Parameter]:
        return self._parameters

    @property
    def domains(self) -> List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]]:
        return self._domains

    @domains.setter
    def domains(
        self, domains: List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]]
    ):
        self._domains = domains

    @property
    def active_domain(self) -> Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]:
        return self._active_domain

    @active_domain.setter
    def active_domain(
        self, active_domain: Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]
    ):
        self._active_domain = active_domain

    # TODO: <Alex>ALEX -- what is the return type?</Alex>
    @property
    def variables(self):
        """
        Getter for rule_state variables
        :return: variables necessary for validating rule
        """
        return self._variables

    # TODO: <Alex>ALEX -- Change some of these methods into properties.</Alex>
    def get_active_domain_id(self) -> str:
        """
        Getter for the id of the rule domain
        :return: the id of the rule domain
        """
        return IDDict(self.active_domain).to_id()

    def get_value(self, value: str) -> Any:
        """
        Get a value from the current rule state. Values must be dot-delimited, and may start either with
        the key 'domain' or the id of a parameter.
        Args
            :param value: str - A string key starting with $ and corresponding to internal state arguments, eg: domain kwargs
        :return: requested value
        """
        if not value.startswith("$"):
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Unable to get value "{value}" - values must start with $'
            )

        if value == "$domain.domain_kwargs":
            return self.active_domain["domain_kwargs"]

        variables_key: str = "$variables."
        lookup: List[str]
        if value.startswith(variables_key):
            lookup = value[len(variables_key) :].split(".")
            curr = self.variables
        else:
            lookup = value[1:].split(".")
            curr = self.parameters.get(self.get_active_domain_id(), {})

        try:
            for level in lookup:
                curr = curr[level]
        except KeyError:
            # TODO: <Alex>ALEX -- The next line needs to be fixed.</Alex>
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Unable to find value "{value}": key "{level}" was missing.'
            )

        return curr
