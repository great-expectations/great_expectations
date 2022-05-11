
import logging
import warnings
from typing import Optional, Union
from uuid import UUID
from dateutil.parser import parse
from great_expectations.core.data_context_key import DataContextKey
from great_expectations.core.id_dict import BatchKwargs, IDDict
from great_expectations.core.run_identifier import RunIdentifier, RunIdentifierSchema
from great_expectations.exceptions import DataContextError, InvalidDataContextKeyError
from great_expectations.marshmallow__shade import Schema, fields, post_load
logger = logging.getLogger(__name__)

class ExpectationSuiteIdentifier(DataContextKey):

    def __init__(self, expectation_suite_name: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__()
        if (not isinstance(expectation_suite_name, str)):
            raise InvalidDataContextKeyError(f'expectation_suite_name must be a string, not {type(expectation_suite_name).__name__}')
        self._expectation_suite_name = expectation_suite_name

    @property
    def expectation_suite_name(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._expectation_suite_name

    def to_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return tuple(self.expectation_suite_name.split('.'))

    def to_fixed_length_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (self.expectation_suite_name,)

    @classmethod
    def from_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls('.'.join(tuple_))

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls(expectation_suite_name=tuple_[0])

    def __repr__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return f'{self.__class__.__name__}::{self._expectation_suite_name}'

class ExpectationSuiteIdentifierSchema(Schema):
    expectation_suite_name = fields.Str()

    @post_load
    def make_expectation_suite_identifier(self, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return ExpectationSuiteIdentifier(**data)

class BatchIdentifier(DataContextKey):
    'A BatchIdentifier tracks'

    def __init__(self, batch_identifier: Union[(BatchKwargs, dict, str)], data_asset_name: str=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__()
        self._batch_identifier = batch_identifier
        self._data_asset_name = data_asset_name

    @property
    def batch_identifier(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._batch_identifier

    @property
    def data_asset_name(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._data_asset_name

    def to_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (self.batch_identifier,)

    @classmethod
    def from_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls(batch_identifier=tuple_[0])

class BatchIdentifierSchema(Schema):
    batch_identifier = fields.Str()
    data_asset_name = fields.Str()

    @post_load
    def make_batch_identifier(self, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return BatchIdentifier(**data)

class ValidationResultIdentifier(DataContextKey):
    'A ValidationResultIdentifier identifies a validation result by the fully-qualified expectation_suite_identifier\n    and run_id.\n    '

    def __init__(self, expectation_suite_identifier, run_id, batch_identifier) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Constructs a ValidationResultIdentifier\n\n        Args:\n            expectation_suite_identifier (ExpectationSuiteIdentifier, list, tuple, or dict):\n                identifying information for the fully-qualified expectation suite used to validate\n            run_id (RunIdentifier): The run_id for which validation occurred\n        '
        super().__init__()
        self._expectation_suite_identifier = expectation_suite_identifier
        if isinstance(run_id, str):
            warnings.warn('String run_ids are deprecated as of v0.11.0 and support will be removed in v0.16. Please provide a run_id of type RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name and run_time (both optional).', DeprecationWarning)
            try:
                run_time = parse(run_id)
            except (ValueError, TypeError):
                run_time = None
            run_id = RunIdentifier(run_name=run_id, run_time=run_time)
        elif isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif (run_id is None):
            run_id = RunIdentifier()
        elif (not isinstance(run_id, RunIdentifier)):
            run_id = RunIdentifier(run_name=str(run_id))
        self._run_id = run_id
        self._batch_identifier = batch_identifier

    @property
    def expectation_suite_identifier(self) -> ExpectationSuiteIdentifier:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._expectation_suite_identifier

    @property
    def run_id(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._run_id

    @property
    def batch_identifier(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._batch_identifier

    def to_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return tuple(((list(self.expectation_suite_identifier.to_tuple()) + list(self.run_id.to_tuple())) + [(self.batch_identifier or '__none__')]))

    def to_fixed_length_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return tuple((([self.expectation_suite_identifier.expectation_suite_name] + list(self.run_id.to_tuple())) + [(self.batch_identifier or '__none__')]))

    @classmethod
    def from_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls(ExpectationSuiteIdentifier.from_tuple(tuple_[0:(- 3)]), RunIdentifier.from_tuple((tuple_[(- 3)], tuple_[(- 2)])), tuple_[(- 1)])

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls(ExpectationSuiteIdentifier(tuple_[0]), RunIdentifier.from_tuple((tuple_[1], tuple_[2])), tuple_[3])

    @classmethod
    def from_object(cls, validation_result):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        batch_kwargs = validation_result.meta.get('batch_kwargs', {})
        if isinstance(batch_kwargs, IDDict):
            batch_identifier = batch_kwargs.to_id()
        elif isinstance(batch_kwargs, dict):
            batch_identifier = IDDict(batch_kwargs).to_id()
        else:
            raise DataContextError('Unable to construct ValidationResultIdentifier from provided object.')
        return cls(expectation_suite_identifier=ExpectationSuiteIdentifier(validation_result.meta['expectation_suite_name']), run_id=validation_result.meta.get('run_id'), batch_identifier=batch_identifier)

class GeCloudIdentifier(DataContextKey):

    def __init__(self, resource_type: str, ge_cloud_id: Optional[str]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__()
        self._resource_type = resource_type
        self._ge_cloud_id = (ge_cloud_id if (ge_cloud_id is not None) else '')

    @property
    def resource_type(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._resource_type

    @resource_type.setter
    def resource_type(self, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._resource_type = value

    @property
    def ge_cloud_id(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._ge_cloud_id

    @ge_cloud_id.setter
    def ge_cloud_id(self, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._ge_cloud_id = value

    def to_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (self.resource_type, self.ge_cloud_id)

    def to_fixed_length_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.to_tuple()

    @classmethod
    def from_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls(resource_type=tuple_[0], ge_cloud_id=tuple_[1])

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls.from_tuple(tuple_)

    def __repr__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return f'{self.__class__.__name__}::{self.resource_type}::{self.ge_cloud_id}'

class ValidationResultIdentifierSchema(Schema):
    expectation_suite_identifier = fields.Nested(ExpectationSuiteIdentifierSchema, required=True, error_messages={'required': 'expectation_suite_identifier is required for a ValidationResultIdentifier'})
    run_id = fields.Nested(RunIdentifierSchema, required=True, error_messages={'required': 'run_id is required for a ValidationResultIdentifier'})
    batch_identifier = fields.Nested(BatchIdentifierSchema, required=True)

    @post_load
    def make_validation_result_identifier(self, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return ValidationResultIdentifier(**data)

class SiteSectionIdentifier(DataContextKey):

    def __init__(self, site_section_name, resource_identifier) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._site_section_name = site_section_name
        if (site_section_name in ['validations', 'profiling']):
            if isinstance(resource_identifier, ValidationResultIdentifier):
                self._resource_identifier = resource_identifier
            elif isinstance(resource_identifier, (tuple, list)):
                self._resource_identifier = ValidationResultIdentifier(*resource_identifier)
            else:
                self._resource_identifier = ValidationResultIdentifier(**resource_identifier)
        elif (site_section_name == 'expectations'):
            if isinstance(resource_identifier, ExpectationSuiteIdentifier):
                self._resource_identifier = resource_identifier
            elif isinstance(resource_identifier, (tuple, list)):
                self._resource_identifier = ExpectationSuiteIdentifier(*resource_identifier)
            else:
                self._resource_identifier = ExpectationSuiteIdentifier(**resource_identifier)
        else:
            raise InvalidDataContextKeyError("SiteSectionIdentifier only supports 'validations' and 'expectations' as site section names")

    @property
    def site_section_name(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._site_section_name

    @property
    def resource_identifier(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._resource_identifier

    def to_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        site_section_identifier_tuple_list = ([self.site_section_name] + list(self.resource_identifier.to_tuple()))
        return tuple(site_section_identifier_tuple_list)

    @classmethod
    def from_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (tuple_[0] == 'validations'):
            return cls(site_section_name=tuple_[0], resource_identifier=ValidationResultIdentifier.from_tuple(tuple_[1:]))
        elif (tuple_[0] == 'expectations'):
            return cls(site_section_name=tuple_[0], resource_identifier=ExpectationSuiteIdentifier.from_tuple(tuple_[1:]))
        else:
            raise InvalidDataContextKeyError("SiteSectionIdentifier only supports 'validations' and 'expectations' as site section names")

class ConfigurationIdentifier(DataContextKey):

    def __init__(self, configuration_key: Union[(str, UUID)]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__()
        if isinstance(configuration_key, UUID):
            configuration_key = str(configuration_key)
        if (not isinstance(configuration_key, str)):
            raise InvalidDataContextKeyError(f'configuration_key must be a string, not {type(configuration_key).__name__}')
        self._configuration_key = configuration_key

    @property
    def configuration_key(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._configuration_key

    def to_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return tuple(self.configuration_key.split('.'))

    def to_fixed_length_tuple(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (self.configuration_key,)

    @classmethod
    def from_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls('.'.join(tuple_))

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cls(configuration_key=tuple_[0])

    def __repr__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return f'{self.__class__.__name__}::{self._configuration_key}'

class ConfigurationIdentifierSchema(Schema):
    configuration_key = fields.Str()

    @post_load
    def make_configuration_identifier(self, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return ConfigurationIdentifier(**data)
expectationSuiteIdentifierSchema = ExpectationSuiteIdentifierSchema()
validationResultIdentifierSchema = ValidationResultIdentifierSchema()
runIdentifierSchema = RunIdentifierSchema()
batchIdentifierSchema = BatchIdentifierSchema()
configurationIdentifierSchema = ConfigurationIdentifierSchema()
