from typing import Any, Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    DOMAIN_KWARGS_PARAMETER_NAME,
    VARIABLES_KEY,
    ParameterContainer,
    ParameterNode,
    validate_fully_qualified_parameter_name,
)


class RuleState:
    """Manages state for ProfilerRule objects. Keeps track of rule domain, rule parameters,
    and any other necessary variables for validating the rule."""

    def __init__(
        self,
        active_domain: Optional[Domain],
        domains: Optional[List[Domain]] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        variables: Optional[ParameterContainer] = None,
    ):
        self._active_domain = active_domain

        if domains is None:
            domains = [Domain(domain_kwargs=None, domain_type=None)]
        self._domains = domains

        if parameters is None:
            parameters = {}
        self._parameters = parameters

        if variables is None:
            variables = ParameterContainer(parameter_nodes=None)
        self._variables = variables

    @property
    def parameters(self) -> Dict[str, ParameterContainer]:
        return self._parameters

    @property
    def domains(self) -> List[Domain]:
        return self._domains

    @domains.setter
    def domains(self, domains: List[Domain]):
        self._domains = domains

    @property
    def active_domain(self) -> Domain:
        return self._active_domain

    @active_domain.setter
    def active_domain(self, active_domain: Domain):
        self._active_domain = active_domain

    @property
    def variables(self) -> ParameterContainer:
        """
        Getter for rule_state variables
        :return: variables necessary for validating rule
        """
        return self._variables

    def get_parameter_value(self, fully_qualified_parameter_name: str) -> Optional[Any]:
        """
        Get the parameter value from the current rule state using the fully-qualified parameter name.
        A fully-qualified parameter name must be a dot-delimited string, or the name of a parameter (without the dots).
        Args
            :param fully_qualified_parameter_name: str -- A string key starting with $ and corresponding to internal
            state arguments (e.g.: domain kwargs)
        :return: requested value
        """
        validate_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name
        )

        if (
            fully_qualified_parameter_name
            == DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME
        ):
            return self.active_domain[DOMAIN_KWARGS_PARAMETER_NAME]

        fully_qualified_parameter_name_references_variable: bool = False
        if fully_qualified_parameter_name.startswith(VARIABLES_KEY):
            fully_qualified_parameter_name = fully_qualified_parameter_name[
                len(VARIABLES_KEY) :
            ]
            fully_qualified_parameter_name_references_variable = True
        else:
            fully_qualified_parameter_name = fully_qualified_parameter_name[1:]

        fully_qualified_parameter_as_list: List[
            str
        ] = fully_qualified_parameter_name.split(".")

        if len(fully_qualified_parameter_as_list) == 0:
            return None

        parameter_container: ParameterContainer
        if fully_qualified_parameter_name_references_variable:
            parameter_container = self.variables
        else:
            parameter_container = self.parameters.get(
                self.active_domain.id, ParameterContainer(parameter_nodes=None)
            )

        parameter_node: Optional[
            ParameterNode
        ] = parameter_container.get_parameter_node(
            parameter_name_root=fully_qualified_parameter_as_list[0]
        )

        parameter_name_part: Optional[str] = None
        try:
            for parameter_name_part in fully_qualified_parameter_as_list:
                if (
                    parameter_node.attributes
                    and parameter_name_part in parameter_node.attributes
                ):
                    return parameter_node.attributes[parameter_name_part]

                parameter_node = parameter_node.descendants[parameter_name_part]
        except KeyError:
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Unable to find value for parameter name "{fully_qualified_parameter_name}": key "{parameter_name_part}" was missing.'
            )
