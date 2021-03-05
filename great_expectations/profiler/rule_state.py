from great_expectations.core import IDDict
from great_expectations.profiler.exceptions import ProfilerExecutionError


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

        variables_key = "$variables."
        if value.startswith(variables_key):
            lookup = value[len(variables_key) :].split(".")
            curr = self.variables
        else:
            lookup = value[1:].split(".")
            curr = self.parameters.get(self.get_active_domain_id(), dict())

        try:
            for level in lookup:
                curr = curr[level]
        except KeyError:
            raise ProfilerExecutionError(
                f"Unable to find value '{value}': key '{level}' was missing."
            )

        return curr
