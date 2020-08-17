# -*- coding: utf-8 -*-

import logging
import warnings

from great_expectations.core.id_dict import BatchKwargs

logger = logging.getLogger(__name__)


class DataConnector(object):
    r"""
    DataConnectors produce identifying information, called "batch_kwargs" that ExecutionEnvironments
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, downsampling, or other techniques appropriate
    for the datasource.

    For example, a DataConnector could produce a SQL query that logically represents "rows in
    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource
    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and
    ready for validation.

    A batch is a sample from a data asset, sliced according to a particular rule. For
    example, an hourly slide of the Events table or “most recent `users` records.”

    A Batch is the primary unit of validation in the Great Expectations DataContext.
    Batches include metadata that identifies how they were constructed--the same “batch_kwargs”
    assembled by the batch kwargs generator, While not every datasource will enable re-fetching a
    specific batch of data, GE can store snapshots of batches or store metadata from an
    external data version control system.
    """

    _batch_kwargs_type = BatchKwargs
    recognized_batch_parameters = set()

    def __init__(self, name, execution_engine):
        self._name = name
        self._data_connector_config = {"class_name": self.__class__.__name__}
        self._data_asset_iterators = {}
        if execution_engine is None:
            raise ValueError("execution engine must be provided for a DataConnector")
        self._execution_engine = execution_engine

    @property
    def name(self):
        return self._name

    def _get_iterator(self, data_asset_name, **kwargs):
        raise NotImplementedError

    def get_available_data_asset_names(self):
        """Return the list of asset names known by this batch kwargs generator.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    # TODO: deprecate generator_asset argument
    def get_available_partition_ids(self, generator_asset=None, data_asset_name=None):
        """
        Applies the current _partitioner to the batches available on data_asset_name and returns a list of valid
        partition_id strings that can be used to identify batches of data.

        Args:
            data_asset_name: the data asset whose partitions should be returned.

        Returns:
            A list of partition_id strings
        """
        raise NotImplementedError

    def get_config(self):
        return self._generator_config

    # TODO: deprecate generator_asset argument
    def reset_iterator(self, generator_asset=None, data_asset_name=None, **kwargs):
        assert (generator_asset and not data_asset_name) or (
            not generator_asset and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument will be deprecated and renamed to 'data_asset_name'. "
                "Please update code accordingly.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset

        self._data_asset_iterators[data_asset_name] = (
            self._get_iterator(data_asset_name=data_asset_name, **kwargs),
            kwargs,
        )

    # TODO: deprecate generator_asset argument
    def get_iterator(self, generator_asset=None, data_asset_name=None, **kwargs):
        assert (generator_asset and not data_asset_name) or (
            not generator_asset and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument will be deprecated and renamed to 'data_asset_name'. "
                "Please update code accordingly.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset

        if data_asset_name in self._data_asset_iterators:
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[
                data_asset_name
            ]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_kwargs using different supplemental kwargs. Please reset iterator to "
                    "use different supplemental kwargs."
                )
            return data_asset_iterator
        else:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            return self._data_asset_iterators[data_asset_name][0]

    def build_batch_kwargs(self, data_asset_name=None, partition_id=None, **kwargs):
        if (not kwargs.get("name") and not data_asset_name) or (
            kwargs.get("name") and data_asset_name
        ):
            raise ValueError("Please provide either name or data_asset_name.")
        if kwargs.get("name"):
            warnings.warn(
                "The 'name' argument will be deprecated and renamed to 'data_asset_name'. "
                "Please update code accordingly.",
                DeprecationWarning,
            )
            data_asset_name = kwargs.pop("name")

        """The key workhorse. Docs forthcoming."""
        if data_asset_name is not None:
            batch_parameters = {"data_asset_name": data_asset_name}
        else:
            batch_parameters = dict()
        if partition_id is not None:
            batch_parameters["partition_id"] = partition_id
        batch_parameters.update(kwargs)
        param_keys = set(batch_parameters.keys())
        recognized_params = (
            self.recognized_batch_parameters
            | self._datasource.recognized_batch_parameters
        )
        if not param_keys <= recognized_params:
            logger.warning(
                "Unrecognized batch_parameter(s): %s"
                % str(param_keys - recognized_params)
            )

        batch_kwargs = self._build_batch_kwargs(batch_parameters)
        batch_kwargs["data_asset_name"] = data_asset_name
        # Track the datasource *in batch_kwargs* when building from a context so that the context can easily reuse them.
        batch_kwargs["datasource"] = self._datasource.name
        return batch_kwargs

    def _build_batch_kwargs(self, batch_parameters):
        raise NotImplementedError

    # TODO: deprecate generator_asset argument
    def yield_batch_kwargs(self, data_asset_name=None, generator_asset=None, **kwargs):
        assert (generator_asset and not data_asset_name) or (
            not generator_asset and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument will be deprecated and renamed to 'data_asset_name'. "
                "Please update code accordingly.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset

        if data_asset_name not in self._data_asset_iterators:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
        data_asset_iterator, passed_kwargs = self._data_asset_iterators[data_asset_name]
        if passed_kwargs != kwargs:
            logger.warning(
                "Asked to yield batch_kwargs using different supplemental kwargs. Resetting iterator to "
                "use new supplemental kwargs."
            )
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[
                data_asset_name
            ]
        try:
            batch_kwargs = next(data_asset_iterator)
            batch_kwargs["datasource"] = self._datasource.name
            return batch_kwargs
        except StopIteration:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[
                data_asset_name
            ]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_kwargs using different batch parameters. Resetting iterator to "
                    "use different batch parameters."
                )
                self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
                data_asset_iterator, passed_kwargs = self._data_asset_iterators[
                    data_asset_name
                ]
            try:
                batch_kwargs = next(data_asset_iterator)
                batch_kwargs["datasource"] = self._datasource.name
                return batch_kwargs
            except StopIteration:
                # This is a degenerate case in which no kwargs are actually being generated
                logger.warning(
                    "No batch_kwargs found for data_asset_name %s" % data_asset_name
                )
                return {}
        except TypeError:
            # If we don't actually have an iterator we can generate, even after resetting, just return empty
            logger.warning(
                "Unable to generate batch_kwargs for data_asset_name %s"
                % data_asset_name
            )
            return {}

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

    def build_batch_kwargs(
        self, batch_kwargs_generator, data_asset_name=None, partition_id=None, **kwargs
    ):
        if kwargs.get("name"):
            if data_asset_name:
                raise ValueError(
                    "Cannot provide both 'name' and 'data_asset_name'. Please use 'data_asset_name' only."
                )
            warnings.warn(
                "name is being deprecated as a batch_parameter. Please use data_asset_name instead.",
                DeprecationWarning,
            )
            data_asset_name = kwargs.pop("name")
        generator_obj = self.get_batch_kwargs_generator(batch_kwargs_generator)
        if partition_id is not None:
            kwargs["partition_id"] = partition_id
        return generator_obj.build_batch_kwargs(
            data_asset_name=data_asset_name, **kwargs
        )
