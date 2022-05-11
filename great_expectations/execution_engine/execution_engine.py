
import copy
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union
import pandas as pd
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchMarkers, BatchSpec
from great_expectations.core.util import AzureUrl, DBFSPath, GCSUrl, S3Url
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.expectations.row_conditions import RowCondition, RowConditionParserType
from great_expectations.util import filter_properties_dict
from great_expectations.validator.metric_configuration import MetricConfiguration
logger = logging.getLogger(__name__)

class NoOpDict():

    def __getitem__(self, item):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return None

    def __setitem__(self, key, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return None

    def update(self, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return None

class BatchData():

    def __init__(self, execution_engine) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._execution_engine = execution_engine

    @property
    def execution_engine(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._execution_engine

    def head(self, *args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return pd.DataFrame({})

class MetricFunctionTypes(Enum):
    VALUE = 'value'
    MAP_VALUES = 'value'
    WINDOW_VALUES = 'value'
    AGGREGATE_VALUE = 'value'

class MetricDomainTypes(Enum):
    COLUMN = 'column'
    COLUMN_PAIR = 'column_pair'
    MULTICOLUMN = 'multicolumn'
    TABLE = 'table'

class DataConnectorStorageDataReferenceResolver():
    DATA_CONNECTOR_NAME_TO_STORAGE_NAME_MAP: Dict[(str, str)] = {'InferredAssetS3DataConnector': 'S3', 'ConfiguredAssetS3DataConnector': 'S3', 'InferredAssetGCSDataConnector': 'GCS', 'ConfiguredAssetGCSDataConnector': 'GCS', 'InferredAssetAzureDataConnector': 'ABS', 'ConfiguredAssetAzureDataConnector': 'ABS', 'InferredAssetDBFSDataConnector': 'DBFS', 'ConfiguredAssetDBFSDataConnector': 'DBFS'}
    STORAGE_NAME_EXECUTION_ENGINE_NAME_PATH_RESOLVERS: Dict[(Tuple[(str, str)], Callable)] = {('S3', 'PandasExecutionEngine'): (lambda template_arguments: S3Url.OBJECT_URL_TEMPLATE.format(**template_arguments)), ('S3', 'SparkDFExecutionEngine'): (lambda template_arguments: S3Url.OBJECT_URL_TEMPLATE.format(**template_arguments)), ('GCS', 'PandasExecutionEngine'): (lambda template_arguments: GCSUrl.OBJECT_URL_TEMPLATE.format(**template_arguments)), ('GCS', 'SparkDFExecutionEngine'): (lambda template_arguments: GCSUrl.OBJECT_URL_TEMPLATE.format(**template_arguments)), ('ABS', 'PandasExecutionEngine'): (lambda template_arguments: AzureUrl.AZURE_BLOB_STORAGE_HTTPS_URL_TEMPLATE.format(**template_arguments)), ('ABS', 'SparkDFExecutionEngine'): (lambda template_arguments: AzureUrl.AZURE_BLOB_STORAGE_WASBS_URL_TEMPLATE.format(**template_arguments)), ('DBFS', 'SparkDFExecutionEngine'): (lambda template_arguments: DBFSPath.convert_to_protocol_version(**template_arguments)), ('DBFS', 'PandasExecutionEngine'): (lambda template_arguments: DBFSPath.convert_to_file_semantics_version(**template_arguments))}

    @staticmethod
    def resolve_data_reference(data_connector_name: str, execution_engine_name: str, template_arguments: dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Resolve file path for a (data_connector_name, execution_engine_name) combination.'
        storage_name: str = DataConnectorStorageDataReferenceResolver.DATA_CONNECTOR_NAME_TO_STORAGE_NAME_MAP[data_connector_name]
        return DataConnectorStorageDataReferenceResolver.STORAGE_NAME_EXECUTION_ENGINE_NAME_PATH_RESOLVERS[(storage_name, execution_engine_name)](template_arguments)

@dataclass
class SplitDomainKwargs():
    'compute_domain_kwargs, accessor_domain_kwargs when split from domain_kwargs\n\n    The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n    '
    compute: dict
    accessor: dict

class ExecutionEngine(ABC):
    recognized_batch_spec_defaults = set()

    def __init__(self, name=None, caching=True, batch_spec_defaults=None, batch_data_dict=None, validator=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.name = name
        self._validator = validator
        self._caching = caching
        if self._caching:
            self._metric_cache = {}
        else:
            self._metric_cache = NoOpDict()
        if (batch_spec_defaults is None):
            batch_spec_defaults = {}
        batch_spec_defaults_keys = set(batch_spec_defaults.keys())
        if (not (batch_spec_defaults_keys <= self.recognized_batch_spec_defaults)):
            logger.warning(('Unrecognized batch_spec_default(s): %s' % str((batch_spec_defaults_keys - self.recognized_batch_spec_defaults))))
        self._batch_spec_defaults = {key: value for (key, value) in batch_spec_defaults.items() if (key in self.recognized_batch_spec_defaults)}
        self._batch_data_dict = {}
        if (batch_data_dict is None):
            batch_data_dict = {}
        self._active_batch_data_id = None
        self._load_batch_data_from_dict(batch_data_dict)
        self._config = {'name': name, 'caching': caching, 'batch_spec_defaults': batch_spec_defaults, 'batch_data_dict': batch_data_dict, 'validator': validator, 'module_name': self.__class__.__module__, 'class_name': self.__class__.__name__}
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def configure_validator(self, validator) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Optionally configure the validator as appropriate for the execution engine.'
        pass

    @property
    def active_batch_data_id(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'The batch id for the default batch data.\n\n        When an execution engine is asked to process a compute domain that does\n        not include a specific batch_id, then the data associated with the\n        active_batch_data_id will be used as the default.\n        '
        if (self._active_batch_data_id is not None):
            return self._active_batch_data_id
        elif (len(self.loaded_batch_data_dict) == 1):
            return list(self.loaded_batch_data_dict.keys())[0]
        else:
            return None

    @active_batch_data_id.setter
    def active_batch_data_id(self, batch_id) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (batch_id in self.loaded_batch_data_dict.keys()):
            self._active_batch_data_id = batch_id
        else:
            raise ge_exceptions.ExecutionEngineError(f'Unable to set active_batch_data_id to {batch_id}. The may data may not be loaded.')

    @property
    def active_batch_data(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'The data from the currently-active batch.'
        if (self.active_batch_data_id is None):
            return None
        else:
            return self.loaded_batch_data_dict.get(self.active_batch_data_id)

    @property
    def loaded_batch_data_dict(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'The current dictionary of batches.'
        return self._batch_data_dict

    @property
    def loaded_batch_data_ids(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return list(self.loaded_batch_data_dict.keys())

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
        return self._config

    @property
    def dialect(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return None

    def get_batch_data(self, batch_spec: BatchSpec) -> Any:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Interprets batch_data and returns the appropriate data.\n\n        This method is primarily useful for utility cases (e.g. testing) where\n        data is being fetched without a DataConnector and metadata like\n        batch_markers is unwanted\n\n        Note: this method is currently a thin wrapper for get_batch_data_and_markers.\n        It simply suppresses the batch_markers.\n        '
        (batch_data, _) = self.get_batch_data_and_markers(batch_spec)
        return batch_data

    @abstractmethod
    def get_batch_data_and_markers(self, batch_spec) -> Tuple[(BatchData, BatchMarkers)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        raise NotImplementedError

    def load_batch_data(self, batch_id: str, batch_data: Any) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Loads the specified batch_data into the execution engine\n        '
        self._batch_data_dict[batch_id] = batch_data
        self._active_batch_data_id = batch_id

    def _load_batch_data_from_dict(self, batch_data_dict) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Loads all data in batch_data_dict into load_batch_data\n        '
        for (batch_id, batch_data) in batch_data_dict.items():
            self.load_batch_data(batch_id, batch_data)

    def resolve_metrics(self, metrics_to_resolve: Iterable[MetricConfiguration], metrics: Optional[Dict[(Tuple[(str, str, str)], MetricConfiguration)]]=None, runtime_configuration: Optional[dict]=None) -> Dict[(Tuple[(str, str, str)], Any)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'resolve_metrics is the main entrypoint for an execution engine. The execution engine will compute the value\n        of the provided metrics.\n\n        Args:\n            metrics_to_resolve: the metrics to evaluate\n            metrics: already-computed metrics currently available to the engine\n            runtime_configuration: runtime configuration information\n\n        Returns:\n            resolved_metrics (Dict): a dictionary with the values for the metrics that have just been resolved.\n        '
        if (metrics is None):
            metrics = {}
        resolved_metrics: Dict[(Tuple[(str, str, str)], Any)] = {}
        metric_fn_bundle = []
        for metric_to_resolve in metrics_to_resolve:
            metric_dependencies = {}
            for (k, v) in metric_to_resolve.metric_dependencies.items():
                if (v.id in metrics):
                    metric_dependencies[k] = metrics[v.id]
                elif (self._caching and (v.id in self._metric_cache)):
                    metric_dependencies[k] = self._metric_cache[v.id]
                else:
                    raise ge_exceptions.MetricError(message=f'Missing metric dependency: {str(k)} for metric "{metric_to_resolve.metric_name}".')
            (metric_class, metric_fn) = get_metric_provider(metric_name=metric_to_resolve.metric_name, execution_engine=self)
            metric_provider_kwargs = {'cls': metric_class, 'execution_engine': self, 'metric_domain_kwargs': metric_to_resolve.metric_domain_kwargs, 'metric_value_kwargs': metric_to_resolve.metric_value_kwargs, 'metrics': metric_dependencies, 'runtime_configuration': runtime_configuration}
            if (metric_fn is None):
                try:
                    (metric_fn, compute_domain_kwargs, accessor_domain_kwargs) = metric_dependencies.pop('metric_partial_fn')
                except KeyError as e:
                    raise ge_exceptions.MetricError(message=f'Missing metric dependency: {str(e)} for metric "{metric_to_resolve.metric_name}".')
                metric_fn_bundle.append((metric_to_resolve, metric_fn, compute_domain_kwargs, accessor_domain_kwargs, metric_provider_kwargs))
                continue
            metric_fn_type = getattr(metric_fn, 'metric_fn_type', MetricFunctionTypes.VALUE)
            if (metric_fn_type not in [MetricPartialFunctionTypes.MAP_FN, MetricPartialFunctionTypes.MAP_CONDITION_FN, MetricPartialFunctionTypes.WINDOW_FN, MetricPartialFunctionTypes.WINDOW_CONDITION_FN, MetricPartialFunctionTypes.AGGREGATE_FN, MetricFunctionTypes.VALUE, MetricPartialFunctionTypes.MAP_SERIES, MetricPartialFunctionTypes.MAP_CONDITION_SERIES]):
                logger.warning(f'Unrecognized metric function type while trying to resolve {str(metric_to_resolve.id)}')
            try:
                resolved_metrics[metric_to_resolve.id] = metric_fn(**metric_provider_kwargs)
            except Exception as e:
                raise ge_exceptions.MetricResolutionError(message=str(e), failed_metrics=(metric_to_resolve,))
        if (len(metric_fn_bundle) > 0):
            try:
                new_resolved = self.resolve_metric_bundle(metric_fn_bundle)
                resolved_metrics.update(new_resolved)
            except Exception as e:
                raise ge_exceptions.MetricResolutionError(message=str(e), failed_metrics=[x[0] for x in metric_fn_bundle])
        if self._caching:
            self._metric_cache.update(resolved_metrics)
        return resolved_metrics

    def resolve_metric_bundle(self, metric_fn_bundle) -> Dict[(Tuple[(str, str, str)], Any)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Resolve a bundle of metrics with the same compute domain as part of a single trip to the compute engine.'
        raise NotImplementedError

    def get_domain_records(self, domain_kwargs: dict) -> Any:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        get_domain_records computes the full-access data (dataframe or selectable) for computing metrics based on the\n        given domain_kwargs and specific engine semantics.\n\n        Returns:\n            data corresponding to the compute domain\n        '
        raise NotImplementedError

    def get_compute_domain(self, domain_kwargs: dict, domain_type: Union[(str, MetricDomainTypes)]) -> Tuple[(Any, dict, dict)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'get_compute_domain computes the optimal domain_kwargs for computing metrics based on the given domain_kwargs\n        and specific engine semantics.\n\n        Returns:\n            A tuple consisting of three elements:\n\n            1. data corresponding to the compute domain;\n            2. a modified copy of domain_kwargs describing the domain of the data returned in (1);\n            3. a dictionary describing the access instructions for data elements included in the compute domain\n                (e.g. specific column name).\n\n            In general, the union of the compute_domain_kwargs and accessor_domain_kwargs will be the same as the domain_kwargs\n            provided to this method.\n        '
        raise NotImplementedError

    def add_column_row_condition(self, domain_kwargs, column_name=None, filter_null=True, filter_nan=False):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'EXPERIMENTAL\n\n        Add a row condition for handling null filter.\n\n        Args:\n            domain_kwargs: the domain kwargs to use as the base and to which to add the condition\n            column_name: if provided, use this name to add the condition; otherwise, will use "column" key from table_domain_kwargs\n            filter_null: if true, add a filter for null values\n            filter_nan: if true, add a filter for nan values\n        '
        if ((filter_null is False) and (filter_nan is False)):
            logger.warning('add_column_row_condition called with no filter condition requested')
            return domain_kwargs
        if filter_nan:
            raise ge_exceptions.GreatExpectationsError('Base ExecutionEngine does not support adding nan condition filters')
        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert (('column' in domain_kwargs) or (column_name is not None)), 'No column provided: A column must be provided in domain_kwargs or in the column_name parameter'
        if (column_name is not None):
            column = column_name
        else:
            column = domain_kwargs['column']
        row_condition: RowCondition = RowCondition(condition=f'col("{column}").notnull()', condition_type=RowConditionParserType.GE)
        new_domain_kwargs.setdefault('filter_conditions', []).append(row_condition)
        return new_domain_kwargs

    def resolve_data_reference(self, data_connector_name: str, template_arguments: dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Resolve file path for a (data_connector_name, execution_engine_name) combination.'
        return DataConnectorStorageDataReferenceResolver.resolve_data_reference(data_connector_name=data_connector_name, execution_engine_name=self.__class__.__name__, template_arguments=template_arguments)

    def _split_domain_kwargs(self, domain_kwargs: Dict, domain_type: Union[(str, MetricDomainTypes)], accessor_keys: Optional[Iterable[str]]=None) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for all domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using, or a corresponding string value representing it. String types include "identity",\n            "column", "column_pair", "table" and "other". Enum types include capitalized versions of these from the\n            class MetricDomainTypes.\n            accessor_keys: keys that are part of the compute domain but should be ignored when\n            describing the domain and simply transferred with their associated values into accessor_domain_kwargs.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        domain_type = MetricDomainTypes(domain_type)
        if ((domain_type != MetricDomainTypes.TABLE) and (accessor_keys is not None) and (len(list(accessor_keys)) > 0)):
            logger.warning('Accessor keys ignored since Metric Domain Type is not "table"')
        split_domain_kwargs: SplitDomainKwargs
        if (domain_type == MetricDomainTypes.TABLE):
            split_domain_kwargs = self._split_table_metric_domain_kwargs(domain_kwargs, domain_type, accessor_keys)
        elif (domain_type == MetricDomainTypes.COLUMN):
            split_domain_kwargs = self._split_column_metric_domain_kwargs(domain_kwargs, domain_type)
        elif (domain_type == MetricDomainTypes.COLUMN_PAIR):
            split_domain_kwargs = self._split_column_pair_metric_domain_kwargs(domain_kwargs, domain_type)
        elif (domain_type == MetricDomainTypes.MULTICOLUMN):
            split_domain_kwargs = self._split_multi_column_metric_domain_kwargs(domain_kwargs, domain_type)
        else:
            compute_domain_kwargs = copy.deepcopy(domain_kwargs)
            accessor_domain_kwargs = {}
            split_domain_kwargs = SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)
        return split_domain_kwargs

    @staticmethod
    def _split_table_metric_domain_kwargs(domain_kwargs: Dict, domain_type: MetricDomainTypes, accessor_keys: Optional[Iterable[str]]=None) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for table domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n            accessor_keys: keys that are part of the compute domain but should be ignored when\n            describing the domain and simply transferred with their associated values into accessor_domain_kwargs.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.TABLE), 'This method only supports MetricDomainTypes.TABLE'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if ((accessor_keys is not None) and (len(list(accessor_keys)) > 0)):
            for key in accessor_keys:
                accessor_domain_kwargs[key] = compute_domain_kwargs.pop(key)
        if (len(domain_kwargs.keys()) > 0):
            unexpected_keys: set = set(compute_domain_kwargs.keys()).difference({'batch_id', 'table', 'row_condition', 'condition_parser'})
            if (len(unexpected_keys) > 0):
                unexpected_keys_str: str = ', '.join(map((lambda element: f'"{element}"'), unexpected_keys))
                logger.warning(f'Unexpected key(s) {unexpected_keys_str} found in domain_kwargs for domain type "{domain_type.value}".')
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @staticmethod
    def _split_column_metric_domain_kwargs(domain_kwargs: Dict, domain_type: MetricDomainTypes) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for column domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.COLUMN), 'This method only supports MetricDomainTypes.COLUMN'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if ('column' not in compute_domain_kwargs):
            raise ge_exceptions.GreatExpectationsError('Column not provided in compute_domain_kwargs')
        accessor_domain_kwargs['column'] = compute_domain_kwargs.pop('column')
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @staticmethod
    def _split_column_pair_metric_domain_kwargs(domain_kwargs: Dict, domain_type: MetricDomainTypes) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for column pair domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.COLUMN_PAIR), 'This method only supports MetricDomainTypes.COLUMN_PAIR'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if (not (('column_A' in domain_kwargs) and ('column_B' in domain_kwargs))):
            raise ge_exceptions.GreatExpectationsError('column_A or column_B not found within domain_kwargs')
        accessor_domain_kwargs['column_A'] = compute_domain_kwargs.pop('column_A')
        accessor_domain_kwargs['column_B'] = compute_domain_kwargs.pop('column_B')
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @staticmethod
    def _split_multi_column_metric_domain_kwargs(domain_kwargs: Dict, domain_type: MetricDomainTypes) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for multicolumn domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.MULTICOLUMN), 'This method only supports MetricDomainTypes.MULTICOLUMN'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if ('column_list' not in domain_kwargs):
            raise ge_exceptions.GreatExpectationsError('column_list not found within domain_kwargs')
        column_list = compute_domain_kwargs.pop('column_list')
        if (len(column_list) < 2):
            raise ge_exceptions.GreatExpectationsError('column_list must contain at least 2 columns')
        accessor_domain_kwargs['column_list'] = column_list
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

class MetricPartialFunctionTypes(Enum):
    MAP_FN = 'map_fn'
    MAP_SERIES = 'map_series'
    MAP_CONDITION_FN = 'map_condition_fn'
    MAP_CONDITION_SERIES = 'map_condition_series'
    WINDOW_FN = 'window_fn'
    WINDOW_CONDITION_FN = 'window_condition_fn'
    AGGREGATE_FN = 'aggregate_fn'

    @property
    def metric_suffix(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (self.name in ['MAP_FN', 'MAP_SERIES', 'WINDOW_FN']):
            return 'map'
        elif (self.name in ['MAP_CONDITION_FN', 'MAP_CONDITION_SERIES', 'WINDOW_CONDITION_FN']):
            return 'condition'
        elif (self.name in ['AGGREGATE_FN']):
            return 'aggregate_fn'
