# -*- coding: utf-8 -*-

import copy
from six import string_types

import logging

from ruamel.yaml import YAML

from great_expectations.data_context.types import (
    DataAssetIdentifier,
    NormalizedDataAssetName,
)
from great_expectations.data_context.util import (
    load_class,
    instantiate_class_from_config
)
from great_expectations.data_asset.util import get_empty_expectation_suite
from great_expectations.exceptions import BatchKwargsError
from great_expectations.datasource.types import ReaderMethods
from great_expectations.types import ClassConfig
from great_expectations.exceptions import InvalidConfigError
import warnings
from importlib import import_module

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.default_flow_style = False


class Datasource(object):
    """Datasources are responsible for connecting data and compute infrastructure. Each Datasource provides
    Great Expectations DataAssets (or batches in a DataContext) connected to a specific compute environment, such as a
    SQL database, a Spark cluster, or a local in-memory Pandas DataFrame. Datasources know how to access data from
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
    def build_configuration(cls, class_name, module_name="great_expectations.datasource", data_asset_type=None, generators=None, **kwargs):
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
        self._generators = {}
        if generators is None:
            generators = {}
        self._datasource_config = kwargs

        self._datasource_config.update({
            "generators": generators,
            "data_asset_type": data_asset_type
        })

    @property
    def data_context(self):
        """
        Property for attached DataContext
        """
        return self._data_context

    @property
    def name(self):
        """
        Property for datasource name
        """
        return self._name

    def _build_generators(self):
        """
        Build generator objects from the datasource configuration.

        Returns:
            None
        """
        for generator in self._datasource_config["generators"].keys():
            self.get_generator(generator)


    def get_config(self):
        """
        Get the current configuration.

        Returns:
            datasource configuration dictionary
        """
        return self._datasource_config

    def _build_generator_from_config(self, **kwargs):
        if "type" in kwargs:
            warnings.warn("Using type to configure generators is now deprecated. Please use module_name and class_name"
                          "instead.")
            type_ = kwargs.pop("type")
            generator_class = self._get_generator_class_from_type(type_)
            kwargs.update({
                "class_name": generator_class.__name__
            })
        generator = instantiate_class_from_config(
            config=kwargs,
            runtime_config={
                "datasource": self
            },
            config_defaults={
                "module_name": "great_expectations.datasource.generator"
            }
        )
        return generator

    def add_generator(self, name, generator_config, **kwargs):
        """Add a generator to the datasource.

        Args:
            name (str): the name of the new generator to add
            generator_config: the configuration parameters to add to the datasource
            kwargs: additional keyword arguments will be passed directly to the new generator's constructor

        Returns:
             generator (Generator)
        """
        if isinstance(generator_config, string_types):
            warnings.warn("Configuring generators with a type name is no longer supported. Please update to new-style "
                          "configuration.")
            generator_config = {
                "type": generator_config
            }
        generator_config.update(kwargs)
        generator = self._build_generator_from_config(**generator_config)
        self._datasource_config["generators"][name] = generator_config

        return generator

    def get_generator(self, generator_name="default"):
        """Get the (named) generator from a datasource)

        Args:
            generator_name (str): name of generator (default value is 'default')

        Returns:
            generator (Generator)
        """
        if generator_name in self._generators:
            return self._generators[generator_name]
        elif generator_name in self._datasource_config["generators"]:
            generator_config = copy.deepcopy(self._datasource_config["generators"][generator_name])
        else:
            raise ValueError(
                "Unable to load generator %s -- no configuration found or invalid configuration." % generator_name
            )
        generator = self._build_generator_from_config(**generator_config)
        self._generators[generator_name] = generator
        return generator

    def list_generators(self):
        """List currently-configured generators for this datasource.

        Returns:
            List(dict): each dictionary includes "name" and "type" keys
        """
        generators = []
        # NOTE: 20190916 - JPC - Upon deprecation of support for type: configuration, this can be simplified
        for key, value in self._datasource_config["generators"].items():
            if "type" in value:
                logger.warning("Generator %s configured using type. Please use class_name instead." % key)
                generators.append({
                    "name": key,
                    "type": value["type"],
                    "class_name": self._get_generator_class_from_type(value["type"]).__name__
                })
            else:
                generators.append({
                    "name": key,
                    "class_name": value["class_name"]
                })
        return generators

    def get_batch(self, data_asset_name, expectation_suite_name, batch_kwargs, **kwargs):
        """
        Get a batch of data from the datasource.

        If a DataContext is attached, then expectation_suite_name can be used to define an expectation suite to
        attach to the data_asset being fetched. Otherwise, the expectation suite will be empty.

        If no batch_kwargs are specified, the next kwargs for the named data_asset will be fetched from the generator
        first.

        Specific datasource types implement the internal _get_data_asset method to use appropriate batch_kwargs to
        construct and return GE data_asset objects.

        Args:
            data_asset_name: the name of the data asset for which to fetch data.
            expectation_suite_name: the name of the expectation suite to attach to the batch
            batch_kwargs: dictionary of key-value pairs describing the batch to get, or a single identifier if \
            that can be unambiguously translated to batch_kwargs
            **kwargs: Additional key-value pairs to pass to the datasource, such as reader parameters

        Returns:
            A data_asset consisting of the specified batch of data with the named expectation suite connected.

        """
        if isinstance(data_asset_name, NormalizedDataAssetName):  # this richer type can include more metadata
            if self._data_context is not None:
                expectation_suite = self._data_context.get_expectation_suite(
                    data_asset_name,
                    expectation_suite_name
                )
            else:
                expectation_suite = None
                # If data_context is not set, we cannot definitely use a fully normalized data_asset reference.
                # This would mean someone got a normalized name without a data context which is unusual
                logger.warning(
                    "Using NormalizedDataAssetName type without a data_context could result in unexpected behavior: "
                    "using '/' as a default delimiter."
                )
        else:
            expectation_suite = get_empty_expectation_suite(data_asset_name=data_asset_name,
                                                            expectation_suite_name=expectation_suite_name)

        # Support partition_id or other mechanisms of building batch_kwargs
        if not isinstance(batch_kwargs, dict):
            batch_kwargs = self.build_batch_kwargs(data_asset_name, batch_kwargs)

        return self._get_data_asset(batch_kwargs, expectation_suite, **kwargs)

    def get_data_asset(self,
                       generator_asset,
                       generator_name=None,
                       expectation_suite=None,
                       batch_kwargs=None,
                       **kwargs):
        """
        Get a DataAsset using a datasource. generator_asset and generator_name are required.

        Args:
            generator_asset: The name of the asset as identified by the generator to return.
            generator_name: The name of the configured generator to use.
            expectation_suite: The expectation suite to attach to the data_asset
            batch_kwargs: Additional batch_kwargs that can
            **kwargs: Additional kwargs that can be used to supplement batch_kwargs

        Returns:
            DataAsset
        """
        if batch_kwargs is None:
            # noinspection PyUnboundLocalVariable
            generator = self.get_generator(generator_name)
            if generator is not None:
                batch_kwargs = generator.yield_batch_kwargs(generator_asset, **kwargs)

        return self._get_data_asset(batch_kwargs, expectation_suite, **kwargs)

    def _get_data_asset(self, batch_kwargs, expectation_suite, **kwargs):
        """
        Internal implementation of batch fetch logic. Note that this must be overridden by datasource implementations.

        Args:
            batch_kwargs: the identifying information to use to fetch the batch.
            expectation_suite: the expectation suite to attach to the batch.
            **kwargs: additional key-value pairs to use when fetching the batch of data

        Returns:
            A data_asset consisting of the specified batch of data with the named expectation suite connected.

        """
        raise NotImplementedError

    def get_available_data_asset_names(self, generator_names=None):
        """Returns a dictionary of data_asset_names that the specified generator can provide. Note that some generators,
        such as the "no-op" in-memory generator may not be capable of describing specific named data assets, and some
        generators (such as filesystem glob generators) require the user to configure data asset names.

        Args:
            generator_names: the generators for which to fetch available data asset names.

        Returns:
            dictionary consisting of sets of generator assets available for the specified generators:
            ::

                {
                  generator_name: [ data_asset_1, data_asset_2, ... ]
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

    def build_batch_kwargs(self, data_asset_name, *args, **kwargs):
        """
        Build batch kwargs for a requested data_asset. Try to use a generator where possible to support partitioning,
        but fall back to datasource-default behavior if the generator cannot be identified.

        Args:
            data_asset_name: the data asset for which to build batch_kwargs; if a normalized name is provided,
                use the named generator.
            *args: at most exactly one positional argument can be provided from which to build kwargs
            **kwargs: additional keyword arguments to be used to build the batch_kwargs

        Returns:
            A PandasDatasourceBatchKwargs object suitable for building a batch of data from this datasource

        """
        if isinstance(data_asset_name, (NormalizedDataAssetName, DataAssetIdentifier)):
            generator_name = data_asset_name.generator
            generator_asset = data_asset_name.generator_asset
        elif len(self._datasource_config["generators"]) == 1:
            logger.warning("Falling back to only configured generator to build batch_kwargs; consider explicitly "
                           "declaring the generator using named_generator_build_batch_kwargs or a DataAssetIdentifier.")
            generator_name = list(self._datasource_config["generators"].keys())[0]
            generator_asset = data_asset_name
        else:
            raise BatchKwargsError(
                "Unable to determine generator. Consider using named_generator_build_batch_kwargs or a "
                "DataAssetIdentifier.",
                {"args": args,
                 "kwargs": kwargs}
            )

        return self.named_generator_build_batch_kwargs(
            generator_name,
            generator_asset,
            *args,
            **kwargs
        )

    def named_generator_build_batch_kwargs(self, generator_name, generator_asset, *args, **kwargs):
        """Use the named generator to build batch_kwargs"""
        generator = self.get_generator(generator_name=generator_name)
        if len(args) == 1:  # We interpret a single argument as a partition_id
            batch_kwargs = generator.build_batch_kwargs_from_partition_id(
                generator_asset=generator_asset,
                partition_id=args[0],
                batch_kwargs=kwargs
            )
        elif len(args) > 0:
            raise BatchKwargsError("Multiple positional arguments were provided to build_batch_kwargs, but only"
                                   "one is supported. Please provide named arguments to build_batch_kwargs.")
        elif "partition_id" in kwargs:
            batch_kwargs = generator.build_batch_kwargs_from_partition_id(
                generator_asset=generator_asset,
                partition_id=kwargs["partition_id"],
                batch_kwargs=kwargs
            )
        else:
            if len(kwargs) > 0:
                batch_kwargs = generator.yield_batch_kwargs(generator_asset, kwargs)
            else:
                raise BatchKwargsError(
                    "Unable to build batch_kwargs: no partition_id or base kwargs found to pass to generator.",
                    batch_kwargs=kwargs
                )

        return batch_kwargs

    def get_data_context(self):
        """Getter for the currently-configured data context."""
        return self._data_context

    @staticmethod
    def _guess_reader_method_from_path(path):
        """Static helper for parsing reader types from file path extensions.

        Args:
            path (str): the to use to guess

        Returns:
            ReaderMethod to use for the filepath

        """
        if path.endswith(".csv") or path.endswith(".tsv"):
            return ReaderMethods.CSV
        elif path.endswith(".parquet"):
            return ReaderMethods.parquet
        elif path.endswith(".xlsx") or path.endswith(".xls"):
            return ReaderMethods.excel
        elif path.endswith(".json"):
            return ReaderMethods.JSON
        elif path.endswith(".csv.gz") or path.endswith(".csv.gz"):
            return ReaderMethods.CSV_GZ
        else:
            return None

    def _get_generator_class_from_type(self, type_):
        """DEPRECATED.

        This method can be used to support legacy-style type-only declaration of generators."""
        raise NotImplementedError

    def _get_data_asset_class(self, data_asset_type):
        """Returns the class to be used to generate a data_asset from this datasource"""
        if isinstance(data_asset_type, string_types):
            # We have a custom type, but it is defined with only a string
            try:
                logger.warning("Use of custom_data_assets module is deprecated. Please define data_asset_type"
                               "using a module_name and class_name.")
                # FOR LEGACY REASONS support the fixed "custom_data_assets" name
                # FIXME: this option should be removed in a future release
                custom_data_assets_module = __import__("custom_data_assets", fromlist=["custom_data_assets"])
                data_asset_type_class = getattr(custom_data_assets_module, data_asset_type)
                return data_asset_type_class
            except ImportError:
                logger.error(
                    "Unable to import custom_data_asset module. "
                    "Check the plugins directory for 'custom_data_assets'."
                )
                raise InvalidConfigError(
                    "Unable to import custom_data_asset module. "
                    "Check the plugins directory for 'custom_data_assets'."
                )
            except AttributeError:
                logger.error(
                    "Unable to find data_asset_type: '%s'." % data_asset_type
                )
                raise InvalidConfigError("Unable to find data_asset_type: '%s'." % data_asset_type)
        elif isinstance(data_asset_type, ClassConfig):
            try:
                if data_asset_type.module_name is None:
                    data_asset_type.module_name = "great_expectations.dataset"

                loaded_module = import_module(data_asset_type.module_name)
                data_asset_type_class = getattr(loaded_module, data_asset_type.class_name)
                return data_asset_type_class
            except ImportError:
                logger.error(
                    "Unable to find module '%s'." % data_asset_type.module_name
                )
                raise InvalidConfigError("Unable to find module '%s'." % data_asset_type.module_name)
            except AttributeError:
                logger.error(
                    "Unable to find data_asset_type: '%s' in module '%s'."
                    % (data_asset_type.class_name, data_asset_type.module_name)
                )
                raise InvalidConfigError(
                    "Unable to find data_asset_type: '%s' in module '%s'."
                    % (data_asset_type.class_name, data_asset_type.module_name)
                )
        else:
            raise InvalidConfigError("Invalid configuration for data_asset_type")
