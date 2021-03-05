from .rule_state import RuleState


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
