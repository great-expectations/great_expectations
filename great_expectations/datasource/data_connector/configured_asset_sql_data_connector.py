
from copy import deepcopy
from typing import Dict, List, Optional, cast
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest, BatchSpec, IDDict
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.data_connector.util import batch_definition_matches_batch_request
from great_expectations.execution_engine import ExecutionEngine, SqlAlchemyExecutionEngine
try:
    import sqlalchemy as sa
except ImportError:
    sa = None
try:
    from sqlalchemy.sql import Selectable
except ImportError:
    Selectable = None

class ConfiguredAssetSqlDataConnector(DataConnector):
    '\n    A DataConnector that requires explicit listing of SQL tables you want to connect to.\n\n    Args:\n        name (str): The name of this DataConnector\n        datasource_name (str): The name of the Datasource that contains it\n        execution_engine (ExecutionEngine): An ExecutionEngine\n        assets (str): assets\n        batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n    '

    def __init__(self, name: str, datasource_name: str, execution_engine: Optional[ExecutionEngine]=None, assets: Optional[Dict[(str, dict)]]=None, batch_spec_passthrough: Optional[dict]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._assets: dict = {}
        if assets:
            for (asset_name, config) in assets.items():
                self.add_data_asset(asset_name, config)
        if execution_engine:
            execution_engine: SqlAlchemyExecutionEngine = cast(SqlAlchemyExecutionEngine, execution_engine)
        super().__init__(name=name, datasource_name=datasource_name, execution_engine=execution_engine, batch_spec_passthrough=batch_spec_passthrough)

    @property
    def assets(self) -> Dict[(str, dict)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._assets

    @property
    def execution_engine(self) -> SqlAlchemyExecutionEngine:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return cast(SqlAlchemyExecutionEngine, self._execution_engine)

    def add_data_asset(self, name: str, config: dict) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Add data_asset to DataConnector using data_asset name as key, and data_asset config as value.\n        '
        name = self._update_data_asset_name_from_config(name, config)
        self._assets[name] = config

    def _update_data_asset_name_from_config(self, data_asset_name: str, data_asset_config: dict) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        data_asset_name_prefix: str = data_asset_config.get('data_asset_name_prefix', '')
        data_asset_name_suffix: str = data_asset_config.get('data_asset_name_suffix', '')
        schema_name: str = data_asset_config.get('schema_name', '')
        include_schema_name: bool = data_asset_config.get('include_schema_name', True)
        if (schema_name and (include_schema_name is False)):
            raise ge_exceptions.DataConnectorError(message=f"{self.__class__.__name__} ran into an error while initializing Asset names. Schema {schema_name} was specified, but 'include_schema_name' flag was set to False.")
        if schema_name:
            data_asset_name: str = f'{schema_name}.{data_asset_name}'
        data_asset_name: str = f'{data_asset_name_prefix}{data_asset_name}{data_asset_name_suffix}'
        return data_asset_name

    def _get_batch_identifiers_list_from_data_asset_config(self, data_asset_name, data_asset_config):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ('table_name' in data_asset_config):
            table_name = data_asset_config['table_name']
        else:
            table_name = data_asset_name
        if ('splitter_method' in data_asset_config):
            splitter_method_name: str = data_asset_config['splitter_method']
            splitter_kwargs: dict = data_asset_config['splitter_kwargs']
            batch_identifiers_list: List[dict] = self.execution_engine.get_data_for_batch_identifiers(table_name, splitter_method_name, splitter_kwargs)
        else:
            batch_identifiers_list = [{}]
        return batch_identifiers_list

    def _refresh_data_references_cache(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._data_references_cache = {}
        for data_asset_name in self.assets:
            data_asset = self.assets[data_asset_name]
            batch_identifiers_list = self._get_batch_identifiers_list_from_data_asset_config(data_asset_name, data_asset)
            self._data_references_cache[data_asset_name] = batch_identifiers_list

    def get_available_data_asset_names(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Return the list of asset names known by this DataConnector.\n\n        Returns:\n            A list of available names\n        '
        return list(self.assets.keys())

    def get_unmatched_data_references(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Returns the list of data_references unmatched by configuration by looping through items in _data_references_cache\n        and returning data_reference that do not have an associated data_asset.\n\n        Returns:\n            list of data_references that are not matched by configuration.\n        '
        return []

    def get_batch_definition_list_from_batch_request(self, batch_request: BatchRequest):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._validate_batch_request(batch_request=batch_request)
        if (len(self._data_references_cache) == 0):
            self._refresh_data_references_cache()
        batch_definition_list: List[BatchDefinition] = []
        try:
            sub_cache = self._data_references_cache[batch_request.data_asset_name]
        except KeyError:
            raise KeyError(f'data_asset_name {batch_request.data_asset_name} is not recognized.')
        for batch_identifiers in sub_cache:
            batch_definition: BatchDefinition = BatchDefinition(datasource_name=self.datasource_name, data_connector_name=self.name, data_asset_name=batch_request.data_asset_name, batch_identifiers=IDDict(batch_identifiers), batch_spec_passthrough=batch_request.batch_spec_passthrough)
            if batch_definition_matches_batch_request(batch_definition, batch_request):
                batch_definition_list.append(batch_definition)
        return batch_definition_list

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name: str) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._data_references_cache[data_asset_name]

    def _map_data_reference_to_batch_definition_list(self, data_reference, data_asset_name: Optional[str]=None) -> Optional[List[BatchDefinition]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return [BatchDefinition(datasource_name=self.datasource_name, data_connector_name=self.name, data_asset_name=data_asset_name, batch_identifiers=IDDict(data_reference))]

    def build_batch_spec(self, batch_definition: BatchDefinition) -> SqlAlchemyDatasourceBatchSpec:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.\n\n        Args:\n            batch_definition (BatchDefinition): to be used to build batch_spec\n\n        Returns:\n            BatchSpec built from batch_definition\n        "
        data_asset_name: str = batch_definition.data_asset_name
        if ((data_asset_name in self.assets) and self.assets[data_asset_name].get('batch_spec_passthrough') and isinstance(self.assets[data_asset_name].get('batch_spec_passthrough'), dict)):
            batch_spec_passthrough = deepcopy(self.assets[data_asset_name]['batch_spec_passthrough'])
            batch_definition_batch_spec_passthrough = (deepcopy(batch_definition.batch_spec_passthrough) or {})
            batch_spec_passthrough.update(batch_definition_batch_spec_passthrough)
            batch_definition.batch_spec_passthrough = batch_spec_passthrough
        batch_spec: BatchSpec = super().build_batch_spec(batch_definition=batch_definition)
        return SqlAlchemyDatasourceBatchSpec(batch_spec)

    def _generate_batch_spec_parameters_from_batch_definition(self, batch_definition: BatchDefinition) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Build BatchSpec parameters from batch_definition with the following components:\n            1. data_asset_name from batch_definition\n            2. batch_identifiers from batch_definition\n            3. data_asset from data_connector\n\n        Args:\n            batch_definition (BatchDefinition): to be used to build batch_spec\n\n        Returns:\n            dict built from batch_definition\n        '
        data_asset_name: str = batch_definition.data_asset_name
        table_name: str = self._get_table_name_from_batch_definition(batch_definition)
        return {'data_asset_name': data_asset_name, 'table_name': table_name, 'batch_identifiers': batch_definition.batch_identifiers, **self.assets[data_asset_name]}

    def _get_table_name_from_batch_definition(self, batch_definition: BatchDefinition) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n            Helper method called by _get_batch_identifiers_list_from_data_asset_config() to parse table_name from data_asset_name in cases\n            where schema is included.\n\n            data_asset_name in those cases are [schema].[table_name].\n\n        function will split data_asset_name on [schema]. and return the resulting table_name.\n        '
        table_name: str = batch_definition.data_asset_name
        data_asset_dict: dict = self.assets[batch_definition.data_asset_name]
        if ('schema_name' in data_asset_dict):
            schema_name_str: str = data_asset_dict['schema_name']
            if (schema_name_str in table_name):
                table_name = table_name.split(f'{schema_name_str}.')[1]
        return table_name
