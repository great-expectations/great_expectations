# -*- coding: utf-8 -*-

import copy
from six import string_types

import logging

from ruamel.yaml import YAML

import warnings

from great_expectations.data_context.util import verify_dynamic_loading_support
from great_expectations.data_context.util import (
    load_class,
    instantiate_class_from_config
)
from great_expectations.types import ClassConfig
from great_expectations.exceptions import ClassInstantiationError

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.default_flow_style = False


class Datasource(object):
    """A Datasource connects to a compute environment and one or more storage environments and produces batches of data
    that Great Expectations can validate in that compute environment.

    Each Datasource provides Batches connected to a specific compute environment, such as a
    SQL database, a Spark cluster, or a local in-memory Pandas DataFrame.

    Datasources use Batch Kwargs to specify instructions for how to access data from
    relevant sources such as an existing object from a DAG runner, a SQL database, S3 bucket, or local filesystem.

    To bridge the gap between those worlds, Datasources interact closely with *generators* which
    are aware of a source of data and can produce produce identifying information, called
    "batch_kwargs" that datasources can use to get individual batches of data. They add flexibility
    in how to obtain data such as with time-based partitioning, downsampling, or other techniques
    appropriate for the datasource.

    For example, a generator could produce a SQL query that logically represents "rows in the Events
    table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource could use to materialize
    a SqlAlchemyDataset corresponding to that batch of data and ready for validation.

    Since opinionated DAG managers such as airflow, dbt, prefect.io, dagster can also act as datasources
    and/or generators for a more generic datasource.

    When adding custom expectations by subclassing an existing DataAsset type, use the data_asset_type parameter
    to configure the datasource to load and return DataAssets of the custom type.
    """
    recognized_batch_parameters = {'limit'}

    @classmethod
    def from_configuration(cls, **kwargs):
        """
        Build a new datasource from a configuration dictionary.

        Args:
            **kwargs: configuration key-value pairs

        Returns:
            datasource (Datasource): the newly-created datasource

        """
        return cls(**kwargs)

    @classmethod
    def build_configuration(
            cls,
            class_name,
            module_name="great_expectations.datasource",
            data_asset_type=None,
            generators=None,
            **kwargs
    ):
        """
        Build a full configuration object for a datasource, potentially including generators with defaults.

        Args:
            class_name: The name of the class for which to build the config
            module_name: The name of the module in which the datasource class is located
            data_asset_type: A ClassConfig dictionary
            generators: Generator configuration dictionary
            **kwargs: Additional kwargs to be part of the datasource constructor's initialization

        Returns:
            A complete datasource configuration.

        """
        verify_dynamic_loading_support(module_name=module_name, package_name=None)
        class_ = load_class(class_name=class_name, module_name=module_name)
        configuration = class_.build_configuration(data_asset_type=data_asset_type, generators=generators, **kwargs)
        return configuration

    def __init__(self, name, data_context=None, data_asset_type=None, generators=None, **kwargs):
        """
        Build a new datasource.

        Args:
            name: the name for the datasource
            data_context: data context to which to connect
            data_asset_type (ClassConfig): the type of DataAsset to produce
            generators: generators to add to the datasource
        """
        self._data_context = data_context
        self._name = name
        if isinstance(data_asset_type, string_types):
            warnings.warn(
                "String-only configuration for data_asset_type is deprecated. Use module_name and class_name instead.",
                DeprecationWarning)
        self._data_asset_type = data_asset_type
        self._datasource_config = kwargs
        self._generators = {}

        self._datasource_config["data_asset_type"] = data_asset_type
        if generators is not None:
            self._datasource_config["generators"] = generators

    @property
    def name(self):
        """
        Property for datasource name
        """
        return self._name

    @property
    def config(self):
        return copy.deepcopy(self._datasource_config)

    @property
    def data_context(self):
        """
        Property for attached DataContext
        """
        return self._data_context

    def _build_generators(self):
        """
        Build generator objects from the datasource configuration.

        Returns:
            None
        """
        try:
            for generator in self._datasource_config["generators"].keys():
                self.get_generator(generator)
        except KeyError:
            pass

    def add_generator(self, name, class_name, **kwargs):
        """Add a generator to the datasource.

        Args:
            name (str): the name of the new generator to add
            class_name: class of the generator to add
            kwargs: additional keyword arguments will be passed directly to the new generator's constructor

        Returns:
             generator (Generator)
        """

        # PENDING DELETION - 20200130 - JPC
        # 0.9.0 removes support for the type system
        # if isinstance(generator_config, string_types):
        #     warnings.warn("Configuring generators with a type name is no longer supported. Please update to new-style "
        #                   "configuration.")
        #     generator_config = {
        #         "type": generator_config
        #     }
        # generator_config.update(kwargs)
        kwargs["class_name"] = class_name
        generator = self._build_generator(**kwargs)
        if "generators" not in self._datasource_config:
            self._datasource_config["generators"] = dict()
        self._datasource_config["generators"][name] = kwargs

        return generator

    def _build_generator(self, **kwargs):
        """Build a generator using the provided configuration and return the newly-built generator."""
        module_name = 'great_expectations.datasource.generator'
        generator = instantiate_class_from_config(
            config=kwargs,
            runtime_environment={
                "datasource": self
            },
            config_defaults={
                "module_name": module_name
            }
        )
        if not generator:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=kwargs['class_name']
            )

        return generator

    def get_generator(self, generator_name):
        """Get the (named) generator from a datasource)

        Args:
            generator_name (str): name of generator (default value is 'default')

        Returns:
            generator (Generator)
        """
        if generator_name in self._generators:
            return self._generators[generator_name]
        elif "generators" in self._datasource_config and generator_name in self._datasource_config["generators"]:
            generator_config = copy.deepcopy(self._datasource_config["generators"][generator_name])
        else:
            raise ValueError(
                "Unable to load generator %s -- no configuration found or invalid configuration." % generator_name
            )
        generator = self._build_generator(**generator_config)
        self._generators[generator_name] = generator
        return generator

    def list_generators(self):
        """List currently-configured generators for this datasource.

        Returns:
            List(dict): each dictionary includes "name" and "type" keys
        """
        generators = []

        if "generators" in self._datasource_config:
            for key, value in self._datasource_config["generators"].items():
                generators.append({
                    "name": key,
                    "class_name": value["class_name"]
                })
        else:
            generators.append({
                "name": None,
                "class_name": None
            })
        return generators

    def process_batch_parameters(self, limit=None, dataset_options=None):
        """Use datasource-specific configuration to translate any batch parameters into batch kwargs at the datasource
        level.

        Args:
            limit (int): a parameter all datasources must accept to allow limiting a batch to a smaller number of rows.
            dataset_options (dict): a set of kwargs that will be passed to the constructor of a dataset built using
                these batch_kwargs

        Returns:
            batch_kwargs: Result will include both parameters passed via argument and configured parameters.
        """
        batch_kwargs = self._datasource_config.get("batch_kwargs", {})

        if limit is not None:
            batch_kwargs["limit"] = limit

        if dataset_options is not None:
            # Then update with any locally-specified reader options
            if not batch_kwargs.get("dataset_options"):
                batch_kwargs["dataset_options"] = dict()
            batch_kwargs["dataset_options"].update(dataset_options)

        return batch_kwargs

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

    def get_available_data_asset_names(self, generator_names=None):
        """Returns a dictionary of data_asset_names that the specified generator can provide. Note that some generators
        may not be capable of describing specific named data assets, and some
        generators (such as filesystem glob generators) require the user to configure data asset names.

        Args:
            generator_names: the generators for which to get available data asset names.

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
        if generator_names is None:
            generator_names = [generator["name"] for generator in self.list_generators()]
        elif isinstance(generator_names, string_types):
            generator_names = [generator_names]

        for generator_name in generator_names:
            generator = self.get_generator(generator_name)
            available_data_asset_names[generator_name] = generator.get_available_data_asset_names()
        return available_data_asset_names

    def build_batch_kwargs(self, generator, name=None, partition_id=None, **kwargs):
        generator_obj = self.get_generator(generator)
        if partition_id is not None:
            kwargs["partition_id"] = partition_id
        return generator_obj.build_batch_kwargs(name=name, **kwargs)
