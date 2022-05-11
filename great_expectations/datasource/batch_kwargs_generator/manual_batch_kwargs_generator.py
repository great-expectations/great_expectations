
import logging
import warnings
from copy import deepcopy
from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import BatchKwargsGenerator
from great_expectations.exceptions import BatchKwargsError, InvalidBatchKwargsError
logger = logging.getLogger(__name__)

class ManualBatchKwargsGenerator(BatchKwargsGenerator):
    'ManualBatchKwargsGenerator returns manually-configured batch_kwargs for named data assets. It provides a\n    convenient way to capture complete batch requests without requiring the configuration of a more\n    fully-featured batch kwargs generator.\n\n    A fully configured ManualBatchKwargsGenerator in yml might look like the following::\n\n        my_datasource:\n          class_name: PandasDatasource\n          batch_kwargs_generators:\n            my_generator:\n              class_name: ManualBatchKwargsGenerator\n              assets:\n                asset1:\n                  - partition_id: 1\n                    path: /data/file_1.csv\n                    reader_options:\n                      sep: ;\n                  - partition_id: 2\n                    path: /data/file_2.csv\n                    reader_options:\n                      header: 0\n                logs:\n                  path: data/log.csv\n    '
    recognized_batch_parameters = {'data_asset_name', 'partition_id'}

    def __init__(self, name='default', datasource=None, assets=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug(f'Constructing ManualBatchKwargsGenerator {name!r}')
        super().__init__(name, datasource=datasource)
        if (assets is None):
            assets = {}
        self._assets = assets

    @property
    def assets(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._assets

    def get_available_data_asset_names(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return {'names': [(key, 'manual') for key in self.assets.keys()]}

    def _get_data_asset_config(self, data_asset_name):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (data_asset_name is None):
            return
        elif (data_asset_name in self.assets):
            return self.assets[data_asset_name]
        raise InvalidBatchKwargsError(f'No asset definition for requested asset {data_asset_name}')

    def _get_iterator(self, data_asset_name, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        datasource_batch_kwargs = self._datasource.process_batch_parameters(**kwargs)
        asset_definition = deepcopy(self._get_data_asset_config(data_asset_name))
        if isinstance(asset_definition, list):
            for batch_request in asset_definition:
                batch_request.update(datasource_batch_kwargs)
            return iter(asset_definition)
        else:
            asset_definition.update(datasource_batch_kwargs)
            return iter([asset_definition])

    def get_available_partition_ids(self, generator_asset=None, data_asset_name=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        assert ((generator_asset and (not data_asset_name)) or ((not generator_asset) and data_asset_name)), 'Please provide either generator_asset or data_asset_name.'
        if generator_asset:
            warnings.warn("The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.", DeprecationWarning)
            data_asset_name = generator_asset
        partition_ids = []
        asset_definition = self._get_data_asset_config(data_asset_name=data_asset_name)
        if isinstance(asset_definition, list):
            for batch_request in asset_definition:
                try:
                    partition_ids.append(batch_request['partition_id'])
                except KeyError:
                    pass
        elif isinstance(asset_definition, dict):
            try:
                partition_ids.append(asset_definition['partition_id'])
            except KeyError:
                pass
        return partition_ids

    def _build_batch_kwargs(self, batch_parameters):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Build batch kwargs from a partition id.'
        partition_id = batch_parameters.pop('partition_id', None)
        batch_kwargs = self._datasource.process_batch_parameters(batch_parameters)
        if partition_id:
            asset_definition = self._get_data_asset_config(data_asset_name=batch_parameters.get('data_asset_name'))
            if isinstance(asset_definition, list):
                for batch_request in asset_definition:
                    try:
                        if (batch_request['partition_id'] == partition_id):
                            batch_kwargs = deepcopy(batch_request)
                            batch_kwargs.pop('partition_id')
                    except KeyError:
                        pass
            elif isinstance(asset_definition, dict):
                try:
                    if (asset_definition['partition_id'] == partition_id):
                        batch_kwargs = deepcopy(asset_definition)
                        batch_kwargs.pop('partition_id')
                except KeyError:
                    pass
        else:
            batch_kwargs = next(self._get_iterator(data_asset_name=batch_parameters.get('data_asset_name')))
        if (batch_kwargs is not None):
            return batch_kwargs
        else:
            raise BatchKwargsError('Unable to find batch_kwargs for given batch_parameters', batch_parameters)
