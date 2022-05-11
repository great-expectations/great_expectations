
import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Union
from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.types import SerializableDictDot, SerializableDotDict
from great_expectations.util import deep_filter_properties_iterable, is_candidate_subset_of_target
INFERRED_SEMANTIC_TYPE_KEY: str = 'inferred_semantic_domain_type'

class SemanticDomainTypes(Enum):
    NUMERIC = 'numeric'
    TEXT = 'text'
    LOGIC = 'logic'
    DATETIME = 'datetime'
    BINARY = 'binary'
    CURRENCY = 'currency'
    IDENTIFIER = 'identifier'
    MISCELLANEOUS = 'miscellaneous'
    UNKNOWN = 'unknown'

@dataclass
class InferredSemanticDomainType(SerializableDictDot):
    semantic_domain_type: Optional[Union[(str, SemanticDomainTypes)]] = None
    details: Optional[Dict[(str, Any)]] = None

    def to_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return asdict(self)

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return convert_to_json_serializable(data=self.to_dict())

class DomainKwargs(SerializableDotDict):

    def to_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return dict(self)

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return convert_to_json_serializable(data=self.to_dict())

class Domain(SerializableDotDict):

    def __init__(self, domain_type: Union[(str, MetricDomainTypes)], domain_kwargs: Optional[Union[(Dict[(str, Any)], DomainKwargs)]]=None, details: Optional[Dict[(str, Any)]]=None, rule_name: Optional[str]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if isinstance(domain_type, str):
            try:
                domain_type = MetricDomainTypes(domain_type)
            except (TypeError, KeyError) as e:
                raise ValueError(f''' {e}: Cannot instantiate Domain (domain_type "{str(domain_type)}" of type "{str(type(domain_type))}" is not supported).
''')
        elif (not isinstance(domain_type, MetricDomainTypes)):
            raise ValueError(f'''Cannot instantiate Domain (domain_type "{str(domain_type)}" of type "{str(type(domain_type))}" is not supported).
''')
        if (domain_kwargs is None):
            domain_kwargs = DomainKwargs({})
        elif isinstance(domain_kwargs, dict):
            domain_kwargs = DomainKwargs(domain_kwargs)
        domain_kwargs_dot_dict: DomainKwargs = self._convert_dictionaries_to_domain_kwargs(source=domain_kwargs)
        if (details is None):
            details = {}
        inferred_semantic_domain_type: Dict[(str, Union[(str, SemanticDomainTypes)])] = details.get(INFERRED_SEMANTIC_TYPE_KEY)
        if inferred_semantic_domain_type:
            semantic_domain_key: str
            metric_domain_key: str
            metric_domain_value: Any
            is_consistent: bool
            for semantic_domain_key in inferred_semantic_domain_type:
                is_consistent = False
                for (metric_domain_key, metric_domain_value) in domain_kwargs_dot_dict.items():
                    if ((isinstance(metric_domain_value, (list, set, tuple)) and (semantic_domain_key in metric_domain_value)) or (semantic_domain_key == metric_domain_value)):
                        is_consistent = True
                        break
                if (not is_consistent):
                    raise ValueError(f'''Cannot instantiate Domain (domain_type "{str(domain_type)}" of type "{str(type(domain_type))}" -- key "{semantic_domain_key}", detected in "{INFERRED_SEMANTIC_TYPE_KEY}" dictionary, does not exist as value of appropriate key in "domain_kwargs" dictionary.
''')
        super().__init__(domain_type=domain_type, domain_kwargs=domain_kwargs_dot_dict, details=details, rule_name=rule_name)

    def __repr__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.__repr__()

    def __eq__(self, other):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return ((other is not None) and ((hasattr(other, 'to_json_dict') and (self.to_json_dict() == other.to_json_dict())) or (isinstance(other, dict) and (deep_filter_properties_iterable(properties=self.to_json_dict(), clean_falsy=True) == deep_filter_properties_iterable(properties=other, clean_falsy=True))) or (self.__str__() == str(other))))

    def __ne__(self, other):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (not self.__eq__(other=other))

    def __hash__(self) -> int:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Overrides the default implementation'
        _result_hash: int = hash(self.id)
        return _result_hash

    def is_superset(self, other: 'Domain') -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Determines if other "Domain" object (provided as argument) is contained within this "Domain" object.'
        if (other is None):
            return True
        return other.is_subset(other=self)

    def is_subset(self, other: 'Domain') -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Determines if this "Domain" object is contained within other "Domain" object (provided as argument).'
        if (other is None):
            return False
        this_json_dict: dict = self.to_json_dict()
        other_json_dict: dict = other.to_json_dict()
        return is_candidate_subset_of_target(candidate=this_json_dict, target=other_json_dict)

    @property
    def id(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return IDDict(self.to_json_dict()).to_id()

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        details: dict = {}
        key: str
        value: Any
        for (key, value) in self['details'].items():
            if value:
                if (key == INFERRED_SEMANTIC_TYPE_KEY):
                    column_name: str
                    semantic_type: Union[(str, SemanticDomainTypes)]
                    value = {column_name: (SemanticDomainTypes(semantic_type.lower()).value if isinstance(semantic_type, str) else semantic_type.value) for (column_name, semantic_type) in value.items()}
            details[key] = convert_to_json_serializable(data=value)
        json_dict: dict = {'domain_type': self['domain_type'].value, 'domain_kwargs': self['domain_kwargs'].to_json_dict(), 'details': details, 'rule_name': self['rule_name']}
        json_dict = convert_to_json_serializable(data=json_dict)
        return deep_filter_properties_iterable(properties=json_dict, clean_falsy=True)

    def _convert_dictionaries_to_domain_kwargs(self, source: Optional[Any]=None) -> Optional[Union[(Any, 'Domain')]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (source is None):
            return None
        if isinstance(source, dict):
            if (not isinstance(source, Domain)):
                deep_filter_properties_iterable(properties=source, inplace=True)
                source = DomainKwargs(source)
            key: str
            value: Any
            for (key, value) in source.items():
                source[key] = self._convert_dictionaries_to_domain_kwargs(source=value)
        return source
