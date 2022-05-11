
from typing import Dict, List, Optional
import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer

class RuleState():
    '\n    RuleState maintains state information, resulting from executing "Rule.run()" method by combining passed "Batch" data\n    with currently loaded configuration of "Rule" components ("DomainBuilder" object, "ParameterBuilder" objects, and\n    "ExpectationConfigurationBuilder" objects).  Using "RuleState" with correponding flags is sufficient for generating\n    outputs for different purposes (in raw and aggregated form) from available "Domain" objects and computed parameters.\n    '

    def __init__(self, rule: 'Rule', variables: Optional[ParameterContainer]=None, domains: Optional[List[Domain]]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            rule: Rule object for which present RuleState object corresponds (needed for various Rule properties).\n            variables: attribute name/value pairs (part of state, relevant for associated Rule).\n            domains: List of Domain objects, which DomainBuilder of associated Rule generated.\n            parameters: Dictionary of ParameterContainer objects corresponding to all Domain objects in memory.\n        '
        self._rule = rule
        self._variables = variables
        if (domains is None):
            domains = []
        self._domains = domains
        if (parameters is None):
            parameters = {}
        self._parameters = parameters

    @property
    def rule(self) -> 'Rule':
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._rule

    @property
    def variables(self) -> Optional[ParameterContainer]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._variables

    @property
    def domains(self) -> List[Domain]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._domains

    @domains.setter
    def domains(self, value: Optional[List[Domain]]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._domains = value

    @property
    def parameters(self) -> Dict[(str, ParameterContainer)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._parameters

    @parameters.setter
    def parameters(self, value: Optional[Dict[(str, ParameterContainer)]]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._parameters = value

    def reset(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.reset_domains()
        self.reset_parameter_containers()

    def reset_domains(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.domains = []

    def reset_parameter_containers(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.parameters = {}

    def add_domain(self, domain: Domain, allow_duplicates: bool=False) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        domain_cursor: Domain
        if ((not allow_duplicates) and (domain.id in [domain_cursor.id for domain_cursor in self.domains])):
            raise ge_exceptions.ProfilerConfigurationError(f'''Error: Domain
{domain}
already exists.  In order to add it, either pass "allow_duplicates=True" or call "RuleState.remove_domain_if_exists()" with Domain having ID equal to "{domain.id}" as argument first.
''')
        self.domains.append(domain)

    def remove_domain_if_exists(self, domain: Domain) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        domain_cursor: Domain
        if (domain.id in [domain_cursor.id for domain_cursor in self.domains]):
            self.domains.remove(domain)
            self.remove_domain_if_exists(domain=domain)

    def get_domains_as_dict(self) -> Dict[(str, Domain)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        domain: Domain
        return {domain.id: domain for domain in self.domains}

    def initialize_parameter_container_for_domain(self, domain: Domain, overwrite: bool=True) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ((not overwrite) and (domain.id in self.parameters)):
            raise ge_exceptions.ProfilerConfigurationError(f'''Error: ParameterContainer for Domain
{domain}
already exists.  In order to overwrite it, either pass "overwrite=True" or call "RuleState.remove_parameter_container_from_domain()" with Domain having ID equal to "{domain.id}" as argument first.
''')
        parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
        self._parameters[domain.id] = parameter_container

    def remove_parameter_container_from_domain_if_exists(self, domain: Domain) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.parameters.pop(domain.id, None)
