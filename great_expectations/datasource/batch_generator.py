# -*- coding: utf-8 -*-

import os
import copy
import logging

logger = logging.getLogger(__name__)


class BatchGenerator(object):
    """Generators produce identifying information, called "batch_kwargs" that datasources 
    can use to get individual batches of data. They add flexibility in how to obtain data 
    such as with time-based partitioning, downsampling, or other techniques appropriate 
    for the datasource.

    For example, a generator could produce a SQL query that logically represents "rows in 
    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource 
    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and 
    ready for validation.
    """

    def __init__(self, name, type_, datasource=None):
        self._name = name
        self._generator_config = {
            "type": type_
        }
        self._data_asset_iterators = {}
        self._datasource = datasource

    def _get_iterator(self, data_asset_name, **kwargs):
        raise NotImplementedError

    def get_available_data_asset_names(self):
        raise NotImplementedError

    def get_config(self):
        return self._generator_config

    def _save_config(self):
        if self._datasource is not None:
            self._datasource.save_config()
        else:
            logger.warning("Unable to save generator config without a datasource attached.")
     
    def reset_iterator(self, data_asset_name):
        self._data_asset_iterators[data_asset_name] = self._get_iterator(data_asset_name)

    def get_iterator(self, data_asset_name):
        if data_asset_name in self._data_asset_iterators:
            return self._data_asset_iterators[data_asset_name]
        else:
            self.reset_iterator(data_asset_name)
            return self._data_asset_iterators[data_asset_name]

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
        except TypeError:
            # If we don't actually have an iterator we can generate, even after reseting, just return empty
            logger.warning("Unable to generate batch_kwargs for data_asset_name %s" % data_asset_name)
            return {}


class EmptyGenerator(BatchGenerator):

    def _get_iterator(self, data_asset_name, **kwargs):
        return iter([])

    def get_available_data_asset_names(self):
        return set()