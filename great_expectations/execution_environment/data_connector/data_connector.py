# -*- coding: utf-8 -*-

import logging
import warnings
from copy import deepcopy

from great_expectations.core import nested_update
from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)


class DataConnector(object):
    r"""
    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, downsampling, or other techniques appropriate
    for the ExecutionEnvironment.

    For example, a DataConnector could produce a SQL query that logically represents "rows in
    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyExecutionEnvironment
    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and
    ready for validation.

    A batch is a sample from a data asset, sliced according to a particular rule. For
    example, an hourly slide of the Events table or “most recent `users` records.”

    A Batch is the primary unit of validation in the Great Expectations DataContext.
    Batches include metadata that identifies how they were constructed--the same “batch_spec”
    assembled by the data connector, While not every ExecutionEnvironment will enable re-fetching a
    specific batch of data, GE can store snapshots of batches or store metadata from an
    external data version control system.
    """

    _batch_spec_type = BatchSpec
    recognized_batch_definition_keys = {
        "data_asset_name",
        "partition_id",
        "execution_environment",
        "data_connector",
        "batch_spec_passthrough",
        "limit"
    }

    def __init__(self, name, execution_environment, batch_definition_defaults=None):
        self._name = name
        self._data_connector_config = {"class_name": self.__class__.__name__}
        self._data_asset_iterators = {}

        batch_definition_defaults = batch_definition_defaults or {}
        batch_definition_defaults_keys = set(batch_definition_defaults.keys())
        if not batch_definition_defaults_keys <= self.recognized_batch_definition_keys:
            logger.warning(
                "Unrecognized batch_definition key(s): %s"
                % str(batch_definition_defaults_keys - self.recognized_batch_definition_keys)
            )

        self._batch_definition_defaults = {
            key: value for key, value in batch_definition_defaults.items() if key in self.recognized_batch_definition_keys
        }
        if execution_environment is None:
            raise ValueError(
                "execution environment must be provided for a DataConnector"
            )
        self._execution_environment = execution_environment

    @property
    def batch_definition_defaults(self):
        return self._batch_definition_defaults

    @property
    def name(self):
        return self._name

    def _get_iterator(
        self,
        data_asset_name,
        batch_definition,
        batch_spec
    ):
        raise NotImplementedError

    def get_available_data_asset_names(self):
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_available_partition_ids(self, data_asset_name=None):
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
        return self._data_connector_config

    def reset_iterator(self, data_asset_name, batch_definition, batch_spec):

        self._data_asset_iterators[data_asset_name] = (
            self._get_iterator(
                data_asset_name=data_asset_name,
                batch_definition=batch_definition,
                batch_spec=batch_spec
            ),
            batch_definition,
        )

    def get_iterator(self, data_asset_name=None, **kwargs):
        if not data_asset_name:
            raise ValueError("Please provide data_asset_name.")

        if data_asset_name in self._data_asset_iterators:
            data_asset_iterator, passed_kwargs = self._data_asset_iterators[
                data_asset_name
            ]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_spec using different supplemental kwargs. Please reset iterator to "
                    "use different supplemental kwargs."
                )
            return data_asset_iterator
        else:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            return self._data_asset_iterators[data_asset_name][0]

    def build_batch_spec(self, batch_definition):
        # data_asset_name=None, partition_id=None, **kwargs
        # TODO: The logic before raised an error here, but we also check for this below - which should it be?

        if "data_asset_name" not in batch_definition:
            raise ValueError("Batch definition must have a data_asset_name.")

        batch_definition_keys = set(batch_definition.keys())
        recognized_batch_definition_keys = (
            self.recognized_batch_definition_keys
            | self._execution_environment.execution_engine.recognized_batch_definition_keys
        )
        if not batch_definition_keys <= recognized_batch_definition_keys:
            logger.warning(
                "Unrecognized batch_parameter(s): %s"
                % str(batch_definition_keys - recognized_batch_definition_keys)
            )

        batch_definition_defaults = deepcopy(self.batch_definition_defaults)
        batch_definition = {
            key: value for key, value in batch_definition.items() if key in recognized_batch_definition_keys
        }
        batch_definition = nested_update(batch_definition_defaults, batch_definition)

        batch_spec_defaults = deepcopy(self._execution_environment.execution_engine.batch_spec_defaults)
        batch_spec_passthrough = batch_definition.get("batch_spec_passthrough", {})
        batch_spec_scaffold = nested_update(batch_spec_defaults, batch_spec_passthrough)

        batch_spec_scaffold["data_asset_name"] = batch_definition.get("data_asset_name")
        # Track the execution_environment *in batch_spec* when building from a context so that the context can easily
        # reuse
        # them.
        batch_spec_scaffold["execution_environment"] = self._execution_environment.name

        batch_spec = self._build_batch_spec(
            batch_definition=batch_definition,
            batch_spec=batch_spec_scaffold
        )

        return batch_spec

    # TODO: will need to handle partition_definition for in-memory df case
    def _build_batch_spec(self, batch_definition, batch_spec):
        return BatchSpec(batch_spec)

    def yield_batch_spec(self, data_asset_name, batch_definition, batch_spec):

        if data_asset_name not in self._data_asset_iterators:
            self.reset_iterator(
                data_asset_name=data_asset_name,
                batch_definition=batch_definition,
                batch_spec=batch_spec
            )
        data_asset_iterator, passed_batch_definition = self._data_asset_iterators[data_asset_name]
        if passed_batch_definition != batch_definition:
            logger.warning(
                "Asked to yield batch_spec using different supplemental batch_definition. Resetting iterator to "
                "use new supplemental batch_definition."
            )
            self.reset_iterator(
                data_asset_name=data_asset_name,
                batch_definition=batch_definition,
                batch_spec=batch_spec
            )
            data_asset_iterator, passed_batch_definition = self._data_asset_iterators[
                data_asset_name
            ]
        try:
            batch_spec = next(data_asset_iterator)
            return batch_spec
        except StopIteration:
            self.reset_iterator(
                data_asset_name=data_asset_name,
                batch_definition=batch_definition,
                batch_spec=batch_spec
            )
            data_asset_iterator, passed_batch_definition = self._data_asset_iterators[
                data_asset_name
            ]
            if passed_batch_definition != batch_definition:
                logger.warning(
                    "Asked to yield batch_spec using different batch parameters. Resetting iterator to "
                    "use different batch parameters."
                )
                self.reset_iterator(
                    data_asset_name=data_asset_name,
                    batch_definition=batch_definition,
                    batch_spec=batch_spec
                )
                data_asset_iterator, passed_batch_definition = self._data_asset_iterators[
                    data_asset_name
                ]
            try:
                batch_spec = next(data_asset_iterator)
                return batch_spec
            except StopIteration:
                # This is a degenerate case in which no batch_definition are actually being generated
                logger.warning(
                    "No batch_spec found for data_asset_name %s" % data_asset_name
                )
                return {}
        except TypeError:
            # If we don't actually have an iterator we can generate, even after resetting, just return empty
            logger.warning(
                "Unable to generate batch_spec for data_asset_name %s"
                % data_asset_name
            )
            return {}
