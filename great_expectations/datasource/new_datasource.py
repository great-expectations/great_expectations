
import copy
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchDefinition, BatchMarkers, BatchRequest, RuntimeBatchRequest
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.data_context.types.base import ConcurrencyConfig
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import DataConnector
from great_expectations.execution_engine import ExecutionEngine
logger = logging.getLogger(__name__)

class BaseDatasource():
    '\n    An Datasource is the glue between an ExecutionEngine and a DataConnector.\n    '
    recognized_batch_parameters: set = {'limit'}

    def __init__(self, name: str, execution_engine=None, data_context_root_directory: Optional[str]=None, concurrency: Optional[ConcurrencyConfig]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Build a new Datasource.\n\n        Args:\n            name: the name for the datasource\n            execution_engine (ClassConfig): the type of compute engine to produce\n            data_context_root_directory: Installation directory path (if installed on a filesystem).\n            concurrency: Concurrency config used to configure the execution engine.\n        '
        self._name = name
        self._data_context_root_directory = data_context_root_directory
        if (execution_engine is None):
            raise ge_exceptions.ExecutionEngineError(message='No ExecutionEngine configuration provided.')
        try:
            self._execution_engine = instantiate_class_from_config(config=execution_engine, runtime_environment={'concurrency': concurrency}, config_defaults={'module_name': 'great_expectations.execution_engine'})
            self._datasource_config = {'execution_engine': execution_engine}
        except Exception as e:
            raise ge_exceptions.ExecutionEngineError(message=str(e))
        self._data_connectors = {}

    def get_batch_from_batch_definition(self, batch_definition: BatchDefinition, batch_data: Any=None) -> Batch:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Note: this method should *not* be used when getting a Batch from a BatchRequest, since it does not capture BatchRequest metadata.\n        '
        if (not isinstance(batch_data, type(None))):
            (batch_spec, batch_markers) = (None, None)
        else:
            data_connector: DataConnector = self.data_connectors[batch_definition.data_connector_name]
            (batch_data, batch_spec, batch_markers) = data_connector.get_batch_data_and_metadata(batch_definition=batch_definition)
        new_batch: Batch = Batch(data=batch_data, batch_request=None, batch_definition=batch_definition, batch_spec=batch_spec, batch_markers=batch_markers)
        return new_batch

    def get_single_batch_from_batch_request(self, batch_request: Union[(BatchRequest, RuntimeBatchRequest)]) -> Batch:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        batch_list: List[Batch] = self.get_batch_list_from_batch_request(batch_request)
        if (len(batch_list) != 1):
            raise ValueError(f'Got {len(batch_list)} batches instead of a single batch.')
        return batch_list[0]

    def get_batch_definition_list_from_batch_request(self, batch_request: Union[(BatchRequest, RuntimeBatchRequest)]) -> List[BatchDefinition]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Validates batch request and utilizes the classes'\n        Data Connectors' property to get a list of batch definition given a batch request\n        Args:\n            :param batch_request: A BatchRequest or RuntimeBatchRequest object used to request a batch\n            :return: A list of batch definitions\n        "
        self._validate_batch_request(batch_request=batch_request)
        data_connector: DataConnector = self.data_connectors[batch_request.data_connector_name]
        return data_connector.get_batch_definition_list_from_batch_request(batch_request=batch_request)

    def get_batch_list_from_batch_request(self, batch_request: Union[(BatchRequest, RuntimeBatchRequest)]) -> List[Batch]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Processes batch_request and returns the (possibly empty) list of batch objects.\n\n        Args:\n            :batch_request encapsulation of request parameters necessary to identify the (possibly multiple) batches\n            :returns possibly empty list of batch objects; each batch object contains a dataset and associated metatada\n        '
        self._validate_batch_request(batch_request=batch_request)
        data_connector: DataConnector = self.data_connectors[batch_request.data_connector_name]
        batch_definition_list: List[BatchDefinition] = data_connector.get_batch_definition_list_from_batch_request(batch_request=batch_request)
        if isinstance(batch_request, RuntimeBatchRequest):
            if (len(batch_definition_list) != 1):
                raise ValueError('RuntimeBatchRequests must specify exactly one corresponding BatchDefinition')
            batch_definition = batch_definition_list[0]
            runtime_parameters = batch_request.runtime_parameters
            (batch_data, batch_spec, batch_markers) = data_connector.get_batch_data_and_metadata(batch_definition=batch_definition, runtime_parameters=runtime_parameters)
            new_batch: Batch = Batch(data=batch_data, batch_request=batch_request, batch_definition=batch_definition, batch_spec=batch_spec, batch_markers=batch_markers)
            return [new_batch]
        else:
            batches: List[Batch] = []
            for batch_definition in batch_definition_list:
                batch_definition.batch_spec_passthrough = batch_request.batch_spec_passthrough
                batch_data: Any
                batch_spec: PathBatchSpec
                batch_markers: BatchMarkers
                (batch_data, batch_spec, batch_markers) = data_connector.get_batch_data_and_metadata(batch_definition=batch_definition)
                new_batch: Batch = Batch(data=batch_data, batch_request=batch_request, batch_definition=batch_definition, batch_spec=batch_spec, batch_markers=batch_markers)
                batches.append(new_batch)
            return batches

    def _build_data_connector_from_config(self, name: str, config: Dict[(str, Any)]) -> DataConnector:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Build a DataConnector using the provided configuration and return the newly-built DataConnector.'
        new_data_connector: DataConnector = instantiate_class_from_config(config=config, runtime_environment={'name': name, 'datasource_name': self.name, 'execution_engine': self.execution_engine}, config_defaults={'module_name': 'great_expectations.datasource.data_connector'})
        new_data_connector.data_context_root_directory = self._data_context_root_directory
        self.data_connectors[name] = new_data_connector
        return new_data_connector

    def get_available_data_asset_names(self, data_connector_names: Optional[Union[(list, str)]]=None) -> Dict[(str, List[str])]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Returns a dictionary of data_asset_names that the specified data\n        connector can provide. Note that some data_connectors may not be\n        capable of describing specific named data assets, and some (such as\n        inferred_asset_data_connector) require the user to configure\n        data asset names.\n\n        Args:\n            data_connector_names: the DataConnector for which to get available data asset names.\n\n        Returns:\n            dictionary consisting of sets of data assets available for the specified data connectors:\n            ::\n\n                {\n                  data_connector_name: {\n                    names: [ data_asset_1, data_asset_2 ... ]\n                  }\n                  ...\n                }\n        '
        available_data_asset_names: dict = {}
        if (data_connector_names is None):
            data_connector_names = self.data_connectors.keys()
        elif isinstance(data_connector_names, str):
            data_connector_names = [data_connector_names]
        for data_connector_name in data_connector_names:
            data_connector: DataConnector = self.data_connectors[data_connector_name]
            available_data_asset_names[data_connector_name] = data_connector.get_available_data_asset_names()
        return available_data_asset_names

    def get_available_data_asset_names_and_types(self, data_connector_names: Optional[Union[(list, str)]]=None) -> Dict[(str, List[Tuple[(str, str)]])]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Returns a dictionary of data_asset_names that the specified data\n        connector can provide. Note that some data_connectors may not be\n        capable of describing specific named data assets, and some (such as\n        inferred_asset_data_connector) require the user to configure\n        data asset names.\n\n        Returns:\n            dictionary consisting of sets of data assets available for the specified data connectors:\n            For instance, in a SQL Database the data asset name corresponds to the table or\n            view name, and the data asset type is either 'table' or 'view'.\n            ::\n\n                {\n                  data_connector_name: {\n                    names: [ (data_asset_name_1, data_asset_1_type), (data_asset_name_2, data_asset_2_type) ... ]\n                  }\n                  ...\n                }\n        "
        available_data_asset_names_and_types: dict = {}
        if (data_connector_names is None):
            data_connector_names = self.data_connectors.keys()
        elif isinstance(data_connector_names, str):
            data_connector_names = [data_connector_names]
        for data_connector_name in data_connector_names:
            data_connector: DataConnector = self.data_connectors[data_connector_name]
            available_data_asset_names_and_types[data_connector_name] = data_connector.get_available_data_asset_names_and_types()
        return available_data_asset_names_and_types

    def get_available_batch_definitions(self, batch_request: Union[(BatchRequest, RuntimeBatchRequest)]) -> List[BatchDefinition]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._validate_batch_request(batch_request=batch_request)
        data_connector: DataConnector = self.data_connectors[batch_request.data_connector_name]
        batch_definition_list = data_connector.get_batch_definition_list_from_batch_request(batch_request=batch_request)
        return batch_definition_list

    def self_check(self, pretty_print=True, max_examples=3):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        report_object: dict = {'execution_engine': self.execution_engine.config}
        if pretty_print:
            print(f'''
ExecutionEngine class name: {self.execution_engine.__class__.__name__}''')
        if pretty_print:
            print('Data Connectors:')
        data_connector_list = list(self.data_connectors.keys())
        data_connector_list.sort()
        report_object['data_connectors'] = {'count': len(data_connector_list)}
        for data_connector_name in data_connector_list:
            data_connector_obj: DataConnector = self.data_connectors[data_connector_name]
            data_connector_return_obj = data_connector_obj.self_check(pretty_print=pretty_print, max_examples=max_examples)
            report_object['data_connectors'][data_connector_name] = data_connector_return_obj
        return report_object

    def _validate_batch_request(self, batch_request: Union[(BatchRequest, RuntimeBatchRequest)]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not ((batch_request.datasource_name is None) or (batch_request.datasource_name == self.name))):
            raise ValueError(f'''datasource_name in BatchRequest: "{batch_request.datasource_name}" does not
                match Datasource name: "{self.name}".
                ''')
        if (batch_request.data_connector_name not in self.data_connectors.keys()):
            raise ValueError(f'''data_connector_name in BatchRequest: "{batch_request.data_connector_name}" is not configured for DataSource: "{self.name}".
                    ''')

    @property
    def name(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Property for datasource name\n        '
        return self._name

    @property
    def execution_engine(self) -> ExecutionEngine:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._execution_engine

    @property
    def data_connectors(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._data_connectors

    @property
    def config(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return copy.deepcopy(self._datasource_config)

class Datasource(BaseDatasource):
    '\n    An Datasource is the glue between an ExecutionEngine and a DataConnector.\n    '
    recognized_batch_parameters: set = {'limit'}

    def __init__(self, name: str, execution_engine=None, data_connectors=None, data_context_root_directory: Optional[str]=None, concurrency: Optional[ConcurrencyConfig]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Build a new Datasource with data connectors.\n\n        Args:\n            name: the name for the datasource\n            execution_engine (ClassConfig): the type of compute engine to produce\n            data_connectors: DataConnectors to add to the datasource\n            data_context_root_directory: Installation directory path (if installed on a filesystem).\n            concurrency: Concurrency config used to configure the execution engine.\n        '
        self._name = name
        super().__init__(name=name, execution_engine=execution_engine, data_context_root_directory=data_context_root_directory, concurrency=concurrency)
        if (data_connectors is None):
            data_connectors = {}
        self._data_connectors = data_connectors
        self._datasource_config.update({'data_connectors': copy.deepcopy(data_connectors)})
        self._init_data_connectors(data_connector_configs=data_connectors)

    def _init_data_connectors(self, data_connector_configs: Dict[(str, Dict[(str, Any)])]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        for (name, config) in data_connector_configs.items():
            self._build_data_connector_from_config(name=name, config=config)
