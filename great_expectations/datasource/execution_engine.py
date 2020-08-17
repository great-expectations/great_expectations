# -*- coding: utf-8 -*-

import copy
import logging
import warnings

from ruamel.yaml import YAML

from great_expectations.data_context.util import (
    instantiate_class_from_config,
    load_class,
    verify_dynamic_loading_support,
)
from great_expectations.exceptions import ClassInstantiationError
from great_expectations.types import ClassConfig

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.default_flow_style = False


class ExecutionEngine(object):
    def __init__(self):
        pass

    def get_batch(self, batch_kwargs, batch_parameters=None):
        """Get a batch of data from the datasource.

        Args:
            batch_kwargs: the BatchKwargs to use to construct the batch
            batch_parameters: optional parameters to store as the reference description of the batch. They should
                reflect parameters that would provide the passed BatchKwargs.


        Returns:
            Batch

        """
        raise NotImplementedError

    def get_available_data_asset_names(self, batch_kwargs_generator_names=None):
        """
        Returns a dictionary of data_asset_names that the specified batch kwarg
        generator can provide. Note that some batch kwargs generators may not be
        capable of describing specific named data assets, and some (such as
        filesystem glob batch kwargs generators) require the user to configure
        data asset names.

        Args:
            batch_kwargs_generator_names: the BatchKwargGenerator for which to get available data asset names.

        Returns:
            dictionary consisting of sets of generator assets available for the specified generators:
            ::

                {
                  generator_name: {
                    names: [ (data_asset_1, data_asset_1_type), (data_asset_2, data_asset_2_type) ... ]
                  }
                  ...
                }

        """
        available_data_asset_names = {}
        if batch_kwargs_generator_names is None:
            batch_kwargs_generator_names = [
                generator["name"] for generator in self.list_batch_kwargs_generators()
            ]
        elif isinstance(batch_kwargs_generator_names, str):
            batch_kwargs_generator_names = [batch_kwargs_generator_names]

        for generator_name in batch_kwargs_generator_names:
            generator = self.get_batch_kwargs_generator(generator_name)
            available_data_asset_names[
                generator_name
            ] = generator.get_available_data_asset_names()
        return available_data_asset_names
