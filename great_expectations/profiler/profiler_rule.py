from ..core import IDDict
from .exceptions import ProfilerConfigurationError, ProfilerExecutionError


class RuleState:
    """Manages state for ProfilerRule objects."""

    def __init__(
        self, active_domain=None, domains=None, parameters=None, variables=None
    ):
        self.active_domain = active_domain
        if domains is None:
            domains = {}
        self.domains = domains
        if parameters is None:
            parameters = {}
        self.parameters = parameters
        if variables is None:
            variables = {}
        self._variables = variables

    @property
    def variables(self):
        return self._variables

    def get_active_domain_id(self):
        return IDDict(self.active_domain).to_id()

    def get_value(self, value):
        """
        Get a value from the current rule state. Values must be dot-delimited, and may start either with
        the key 'domain' or the id of a parameter.
        """
        if not value.startswith("$"):
            raise ProfilerExecutionError(
                f"Unable to get value '{value}' - values must start with $"
            )

        if value == "$domain.domain_kwargs":
            return self.active_domain["domain_kwargs"]

        variables_key = "$variables"
        if value.startswith(variables_key):
            lookup = value[len(variables_key) :].split(".")
            curr = self.variables
        else:
            lookup = value[1:].split(".")
            curr = self.parameters

        try:
            for level in lookup:
                curr = curr[level]
        except KeyError:
            raise ProfilerExecutionError(
                f"Unable to find value '{value}': key '{level}' was missing."
            )

        return curr


class ProfilerRule:
    def __init__(
        self,
        name,
        domain_builder,
        parameter_builders,
        configuration_builders,
        variables=None,
    ):
        self._name = name
        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._configuration_builders = configuration_builders
        if variables is None:
            variables = dict()
        self._variables = variables

    def evaluate(self, validator, batch_ids=None):
        rule_state = RuleState(variables=self._variables)
        configurations = []

        rule_state.domains = self._domain_builder.get_domains(
            validator=validator, batch_ids=batch_ids
        )
        for domain in rule_state.domains:
            rule_state.active_domain = domain
            domain_id = rule_state.get_active_domain_id()
            for parameter_builder in self._parameter_builders:
                id = parameter_builder.id
                parameter_result = parameter_builder.build_parameters(
                    rule_state=rule_state, validator=validator, batch_ids=batch_ids
                )
                rule_state.parameters[domain_id][id] = parameter_result["parameters"]
            for configuration_builder in self._configuration_builders:
                configurations.append(
                    configuration_builder.build_configuration(rule_state=rule_state)
                )

        return configurations
