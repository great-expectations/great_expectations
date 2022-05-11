
import json
import logging
from typing import Any, Dict, List, Optional, Type
from ruamel.yaml.comments import CommentedMap
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import INCLUDE, Schema, ValidationError, fields, post_dump, post_load
from great_expectations.rule_based_profiler.helpers.util import convert_variables_to_dict, get_parameter_value_and_validate_return_type
from great_expectations.rule_based_profiler.types import VARIABLES_PREFIX, ParameterContainer
from great_expectations.types import DictDot, SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable, filter_properties_dict
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class NotNullSchema(Schema):
    '\n    Extension of Marshmallow Schema to facilitate implicit removal of null values before serialization.\n\n    The __config_class__ attribute is utilized to point a Schema to a configuration. It is the responsibility\n    of the child class to define its own __config_class__ to ensure proper serialization/deserialization.\n\n    Reference: https://marshmallow.readthedocs.io/en/stable/extending.html\n\n    '

    @post_load
    def make_config(self, data: dict, **kwargs) -> Type[DictDot]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Hook to convert the schema object into its respective config type.\n\n        Args:\n            data: The dictionary representation of the configuration object\n            kwargs: Marshmallow-specific kwargs required to maintain hook signature (unused herein)\n\n        Returns:\n            An instance of configuration class, which subclasses the DictDot serialization class\n\n        Raises:\n            NotImplementedError: If the subclass inheriting NotNullSchema fails to define a __config_class__\n\n        '
        if (not hasattr(self, '__config_class__')):
            raise NotImplementedError('The subclass extending NotNullSchema must define its own custom __config_class__')
        return self.__config_class__(**data)

    @post_dump(pass_original=True)
    def remove_nulls_and_keep_unknowns(self, output: dict, original: Type[DictDot], **kwargs) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Hook to clear the config object of any null values before being written as a dictionary.\n        Additionally, it bypasses strict schema validation before writing to dict to ensure that dynamic\n        attributes set through `setattr` are captured in the resulting object.\n        It is important to note that only public attributes are captured through this process.\n        Chetan - 20220126 - Note that if we tighten up the schema (remove the dynamic `setattr` behavior),\n        the functionality to keep unknowns should also be removed.\n\n        Args:\n            output: Processed dictionary representation of the configuration object (leaving original intact)\n            original: The dictionary representation of the configuration object\n            kwargs: Marshmallow-specific kwargs required to maintain hook signature (unused herein)\n\n        Returns:\n            A cleaned dictionary that has no null values\n        '
        for key in original.keys():
            if ((key not in output) and (not key.startswith('_'))):
                output[key] = original[key]
        cleaned_output = filter_properties_dict(properties=output, clean_nulls=True, clean_falsy=False)
        return cleaned_output

class DomainBuilderConfig(SerializableDictDot):

    def __init__(self, class_name: str, module_name: Optional[str]=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.module_name = module_name
        self.class_name = class_name
        for (k, v) in kwargs.items():
            setattr(self, k, v)
            logger.debug('Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".', k, v, self.__class__.__name__)

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the\n        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,\n        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules\n        make this refactoring infeasible at the present time.\n        '
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(properties=json_dict, inplace=True)
        keys: List[str] = sorted(list(json_dict.keys()))
        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}
        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        return self.__repr__()

class DomainBuilderConfigSchema(NotNullSchema):

    class Meta():
        unknown = INCLUDE
    __config_class__ = DomainBuilderConfig
    module_name = fields.String(required=False, allow_none=True, missing='great_expectations.rule_based_profiler.domain_builder')
    class_name = fields.String(required=True, allow_none=False)

class ParameterBuilderConfig(SerializableDictDot):

    def __init__(self, name: str, class_name: str, module_name: Optional[str]=None, evaluation_parameter_builder_configs: Optional[list]=None, json_serialize: bool=True, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.module_name = module_name
        self.class_name = class_name
        self.name = name
        self.evaluation_parameter_builder_configs = evaluation_parameter_builder_configs
        self.json_serialize = json_serialize
        for (k, v) in kwargs.items():
            setattr(self, k, v)
            logger.debug('Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".', k, v, self.__class__.__name__)

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the\n        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,\n        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules\n        make this refactoring infeasible at the present time.\n        '
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(properties=json_dict, inplace=True)
        keys: List[str] = sorted(list(json_dict.keys()))
        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}
        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        return self.__repr__()

class ParameterBuilderConfigSchema(NotNullSchema):

    class Meta():
        unknown = INCLUDE
    __config_class__ = ParameterBuilderConfig
    name = fields.String(required=True, allow_none=False)
    module_name = fields.String(required=False, allow_none=True, missing='great_expectations.rule_based_profiler.parameter_builder')
    class_name = fields.String(required=True, allow_none=False)
    evaluation_parameter_builder_configs = fields.List(cls_or_instance=fields.Nested((lambda : ParameterBuilderConfigSchema()), required=True, allow_none=False), required=False, allow_none=True)
    json_serialize = fields.Boolean(required=False, allow_none=True, missing=True)

class ExpectationConfigurationBuilderConfig(SerializableDictDot):

    def __init__(self, expectation_type: str, class_name: str, module_name: Optional[str]=None, meta: Optional[dict]=None, validation_parameter_builder_configs: Optional[list]=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.module_name = module_name
        self.class_name = class_name
        self.expectation_type = expectation_type
        self.meta = meta
        self.validation_parameter_builder_configs = validation_parameter_builder_configs
        for (k, v) in kwargs.items():
            setattr(self, k, v)
            logger.debug('Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".', k, v, self.__class__.__name__)

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the\n        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,\n        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules\n        make this refactoring infeasible at the present time.\n        '
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(properties=json_dict, inplace=True)
        keys: List[str] = sorted(list(json_dict.keys()))
        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}
        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        return self.__repr__()

class ExpectationConfigurationBuilderConfigSchema(NotNullSchema):

    class Meta():
        unknown = INCLUDE
    __config_class__ = ExpectationConfigurationBuilderConfig
    module_name = fields.String(required=False, allow_none=True, missing='great_expectations.rule_based_profiler.expectation_configuration_builder')
    class_name = fields.String(required=True, allow_none=False)
    expectation_type = fields.Str(required=True, error_messages={'required': 'expectation_type missing in expectation configuration builder'})
    meta = fields.Dict(keys=fields.String(required=True, allow_none=False), required=False, allow_none=True)
    validation_parameter_builder_configs = fields.List(cls_or_instance=fields.Nested((lambda : ParameterBuilderConfigSchema()), required=True, allow_none=False), required=False, allow_none=True)

class RuleConfig(SerializableDictDot):

    def __init__(self, variables: Optional[Dict[(str, Any)]]=None, domain_builder: Optional[dict]=None, parameter_builders: Optional[List[dict]]=None, expectation_configuration_builders: Optional[List[dict]]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.variables = variables
        self.domain_builder = domain_builder
        self.parameter_builders = parameter_builders
        self.expectation_configuration_builders = expectation_configuration_builders

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the\n        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,\n        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules\n        make this refactoring infeasible at the present time.\n        '
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(properties=json_dict, inplace=True)
        keys: List[str] = sorted(list(json_dict.keys()))
        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}
        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        return self.__repr__()

class RuleConfigSchema(NotNullSchema):

    class Meta():
        unknown = INCLUDE
    __config_class__ = RuleConfig
    variables = fields.Dict(keys=fields.String(required=True, allow_none=False), required=False, allow_none=True)
    domain_builder = fields.Nested(DomainBuilderConfigSchema, required=False, allow_none=True)
    parameter_builders = fields.List(cls_or_instance=fields.Nested(ParameterBuilderConfigSchema, required=True, allow_none=False), required=False, allow_none=True)
    expectation_configuration_builders = fields.List(cls_or_instance=fields.Nested(ExpectationConfigurationBuilderConfigSchema, required=True, allow_none=False), required=False, allow_none=True)

class RuleBasedProfilerConfig(BaseYamlConfig):

    def __init__(self, name: str, config_version: float, rules: Dict[(str, dict)], variables: Optional[Dict[(str, Any)]]=None, commented_map: Optional[CommentedMap]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.module_name = 'great_expectations.rule_based_profiler'
        self.class_name = 'RuleBasedProfiler'
        self.name = name
        self.config_version = config_version
        self.variables = variables
        self.rules = rules
        super().__init__(commented_map=commented_map)

    @classmethod
    def get_config_class(cls) -> Type['RuleBasedProfilerConfig']:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls

    @classmethod
    def get_schema_class(cls) -> Type['RuleBasedProfilerConfigSchema']:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return RuleBasedProfilerConfigSchema

    @classmethod
    def from_commented_map(cls, commented_map: CommentedMap) -> 'RuleBasedProfilerConfig':
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Override parent implementation to pop unnecessary attrs from config.\n\n        Please see parent BaseYamlConfig for more details.\n        '
        try:
            config: dict = cls._get_schema_instance().load(commented_map)
            config.pop('class_name', None)
            config.pop('module_name', None)
            return cls.get_config_class()(commented_map=commented_map, **config)
        except ValidationError:
            logger.error('Encountered errors during loading config.  See ValidationError for more details.')
            raise

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the\n        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,\n        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules\n        make this refactoring infeasible at the present time.\n        '
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(properties=json_dict, inplace=True)
        keys: List[str] = sorted(list(json_dict.keys()))
        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}
        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        return self.__repr__()

    @classmethod
    def resolve_config_using_acceptable_arguments(cls, profiler: 'RuleBasedProfiler', variables: Optional[Dict[(str, Any)]]=None, rules: Optional[Dict[(str, Dict[(str, Any)])]]=None) -> 'RuleBasedProfilerConfig':
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Reconciles variables/rules by taking into account runtime overrides and variable substitution.\n\n        Utilized in usage statistics to interact with the args provided in `RuleBasedProfiler.run()`.\n        NOTE: This is a lightweight version of the RBP's true reconiliation logic - see\n        `reconcile_profiler_variables` and `reconcile_profiler_rules`.\n\n        Args:\n            profiler: The profiler used to invoke `run()`.\n            variables: Any runtime override variables.\n            rules: Any runtime override rules.\n\n        Returns:\n            An instance of RuleBasedProfilerConfig that represents the reconciled profiler.\n        "
        effective_variables: Optional[ParameterContainer] = profiler.reconcile_profiler_variables(variables=variables)
        runtime_variables: Optional[Dict[(str, Any)]] = convert_variables_to_dict(variables=effective_variables)
        effective_rules: List['Rule'] = profiler.reconcile_profiler_rules(rules=rules)
        rule: 'Rule'
        effective_rules_dict: Dict[(str, 'Rule')] = {rule.name: rule for rule in effective_rules}
        runtime_rules: Dict[(str, dict)] = {name: RuleBasedProfilerConfig._substitute_variables_in_config(rule=rule, variables_container=effective_variables) for (name, rule) in effective_rules_dict.items()}
        return cls(name=profiler.config.name, config_version=profiler.config.config_version, variables=runtime_variables, rules=runtime_rules)

    @staticmethod
    def _substitute_variables_in_config(rule: 'Rule', variables_container: ParameterContainer) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Recursively updates a given rule to substitute $variable references.\n\n        Args:\n            rule: The Rule object to update.\n            variables_container: Keeps track of $variable values to be substituted.\n\n        Returns:\n            The dictionary representation of the rule with all $variable references substituted.\n        '

        def _traverse_and_substitute(node: Any) -> None:
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            if isinstance(node, dict):
                for (key, val) in node.copy().items():
                    if (isinstance(val, str) and val.startswith(VARIABLES_PREFIX)):
                        node[key] = get_parameter_value_and_validate_return_type(domain=None, parameter_reference=val, variables=variables_container, parameters=None)
                    _traverse_and_substitute(node=val)
            elif isinstance(node, list):
                for val in node:
                    _traverse_and_substitute(node=val)
        rule_dict: dict = rule.to_json_dict()
        _traverse_and_substitute(node=rule_dict)
        return rule_dict

class RuleBasedProfilerConfigSchema(Schema):
    '\n    Schema classes for configurations which extend from BaseYamlConfig must extend top-level Marshmallow Schema class.\n    Schema classes for their constituent configurations which extend DictDot leve must extend NotNullSchema class.\n    '

    class Meta():
        unknown = INCLUDE
        fields = ('name', 'config_version', 'module_name', 'class_name', 'variables', 'rules')
        ordered = True
    name = fields.String(required=True, allow_none=False)
    config_version = fields.Float(required=True, allow_none=False, validate=(lambda x: (x == 1.0)), error_messages={'invalid': 'config version is not supported; it must be 1.0 per the current version of Great Expectations'})
    module_name = fields.String(required=False, allow_none=True, missing='great_expectations.rule_based_profiler')
    class_name = fields.String(required=False, allow_none=True, missing='RuleBasedProfiler')
    variables = fields.Dict(keys=fields.String(required=True, allow_none=False), required=False, allow_none=True)
    rules = fields.Dict(keys=fields.String(required=True, allow_none=False), values=fields.Nested(RuleConfigSchema, required=True, allow_none=False), required=True, allow_none=False)
expectationConfigurationBuilderConfigSchema = ExpectationConfigurationBuilderConfigSchema()
parameterBuilderConfigSchema = ParameterBuilderConfigSchema()
domainBuilderConfigSchema = DomainBuilderConfigSchema()
ruleConfigSchema = RuleConfigSchema()
ruleBasedProfilerConfigSchema = RuleBasedProfilerConfigSchema()
