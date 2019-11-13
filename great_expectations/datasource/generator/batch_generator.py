# -*- coding: utf-8 -*-

import logging

from great_expectations.datasource.types import BatchKwargs

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

    A batch is a sample from a data asset, sliced according to a particular rule. For 
    example, an hourly slide of the Events table or “most recent `users` records.” 
    
    A Batch is the primary unit of validation in the Great Expectations DataContext. 
    Batches include metadata that identifies how they were constructed--the same “batch_kwargs”
    assembled by the generator, While not every datasource will enable re-fetching a
    specific batch of data, GE can store snapshots of batches or store metadata from an
    external data version control system.

    Example Generator Configurations follow::

        my_datasource_1:
          class_name: PandasDatasource
          generators:
            # This generator will provide two data assets, corresponding to the globs defined under the "file_logs"
            # and "data_asset_2" keys. The file_logs asset will be partitioned according to the match group
            # defined in partition_regex
            default:
              class_name: GlobReaderGenerator
              base_directory: /var/logs
              reader_options:
                sep: "
              globs:
                file_logs:
                  glob: logs/*.gz
                  partition_regex: logs/file_(\d{0,4})_\.log\.gz
                data_asset_2:
                  glob: data/*.csv

        my_datasource_2:
          class_name: PandasDatasource
          generators:
            # This generator will create one data asset per subdirectory in /data
            # Each asset will have partitions corresponding to the filenames in that subdirectory
            default:
              class_name: SubdirReaderGenerator
              reader_options:
                sep: "
              base_directory: /data

        my_datasource_3:
          class_name: SqlalchemyDatasource
          generators:
            # This generator will search for a file named with the name of the requested generator asset and the
            # .sql suffix to open with a query to use to generate data
             default:
                class_name: QueryGenerator


    """

    _batch_kwargs_type = BatchKwargs

    def __init__(self, name, datasource=None):
        self._name = name
        self._generator_config = {
            "class_name": self.__class__.__name__
        }
        self._data_asset_iterators = {}
        self._datasource = datasource

    def _get_iterator(self, generator_asset, **kwargs):
        raise NotImplementedError

    def get_available_data_asset_names(self):
        """Return the list of asset names known by this generator.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_available_partition_ids(self, generator_asset):
        """
        Applies the current _partitioner to the batches available on generator_asset and returns a list of valid
        partition_id strings that can be used to identify batches of data.

        Args:
            generator_asset: the generator asset whose partitions should be returned.

        Returns:
            A list of partition_id strings
        """
        raise NotImplementedError

    def get_config(self):
        return self._generator_config

    def reset_iterator(self, generator_asset, **kwargs):
        self._data_asset_iterators[generator_asset] = self._get_iterator(generator_asset, **kwargs), kwargs

    def get_iterator(self, generator_asset, **kwargs):
        if generator_asset in self._data_asset_iterators:
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[generator_asset]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_kwargs using different supplemental kwargs. Please reset iterator to "
                    "use different supplemental kwargs.")
            return data_asset_iterator
        else:
            self.reset_iterator(generator_asset, **kwargs)
            return self._data_asset_iterators[generator_asset][0]

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, batch_kwargs=None, **kwargs):
        """
        Build batch kwargs for the named generator_asset based on partition_id and optionally existing batch_kwargs.
        Args:
            generator_asset: the generator_asset for which to build batch_kwargs
            partition_id: the partition id
            batch_kwargs: any existing batch_kwargs object to use. Will be supplemented with configured information.
            **kwargs: any addition kwargs to use. Will be added to returned batch_kwargs

        Returns: BatchKwargs object

        """
        raise NotImplementedError

    def yield_batch_kwargs(self, generator_asset, **kwargs):
        if generator_asset not in self._data_asset_iterators:
            self.reset_iterator(generator_asset, **kwargs)
        data_asset_iterator, passed_kwargs = self._data_asset_iterators[generator_asset]
        if passed_kwargs != kwargs:
            logger.warning("Asked to yield batch_kwargs using different supplemental kwargs. Resetting iterator to "
                           "use new supplemental kwargs.")
            self.reset_iterator(generator_asset, **kwargs)
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[generator_asset]
        try:
            return next(data_asset_iterator)
        except StopIteration:
            self.reset_iterator(generator_asset, **kwargs)
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[generator_asset]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_kwargs using different supplemental kwargs. Resetting iterator to "
                    "use different supplemental kwargs.")
                self.reset_iterator(generator_asset, **kwargs)
                data_asset_iterator, passed_kwargs = self._data_asset_iterators[generator_asset]
            try:
                return next(data_asset_iterator)
            except StopIteration:
                # This is a degenerate case in which no kwargs are actually being generated
                logger.warning("No batch_kwargs found for generator_asset %s" % generator_asset)
                return {}
        except TypeError:
            # If we don't actually have an iterator we can generate, even after resetting, just return empty
            logger.warning("Unable to generate batch_kwargs for generator_asset %s" % generator_asset)
            return {}
