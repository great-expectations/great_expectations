import logging

logger = logging.getLogger(__name__)

class BatchGenerator(object):

    def __init__(self, name, type_, datasource=None):
        self._generator_config = {
            "type": type_
        }
        self._data_asset_iterators = {}
        self._datasource = datasource

    def _get_iterator(self, data_asset_name, **kwargs):
        raise NotImplementedError

    def list_data_asset_names(self):
        raise NotImplementedError

    def get_config(self):
        return self._generator_config

    def _save_config(self):
        self._datasource._save_config()
        
    def reset_iterator(self, data_asset_name):
        self._data_asset_iterators[data_asset_name] = self._get_iterator(data_asset_name)

    def yield_batch_kwargs(self, data_asset_name):
        if data_asset_name not in self._data_asset_iterators:
            self.reset_iterator(data_asset_name)
        data_asset_iterator = self._data_asset_iterators[data_asset_name]
        try:
            return next(data_asset_iterator)
        except StopIteration:
            self.reset_iterator(data_asset_name)
            data_asset_iterator = self._data_asset_iterators[data_asset_name]
            return next(data_asset_iterator)
