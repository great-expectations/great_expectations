import logging
import random
from typing import Any, List, Optional, Tuple

from great_expectations.core.batch import BatchDefinition, BatchMarkers, BatchRequest
from great_expectations.core.id_dict import BatchSpec
from great_expectations.datasource.data_connector.util import (
    fetch_batch_data_as_pandas_df,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class DataConnector:
    """
    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, downsampling, or other techniques appropriate
    for the Datasource.

    For example, a DataConnector could produce a SQL query that logically represents "rows in
    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource
    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and
    ready for validation.

    A batch is a sample from a data asset, sliced according to a particular rule. For
    example, an hourly slide of the Events table or “most recent `users` records.”

    A Batch is the primary unit of validation in the Great Expectations DataContext.
    Batches include metadata that identifies how they were constructed--the same “batch_spec”
    assembled by the data connector, While not every Datasource will enable re-fetching a
    specific batch of data, GE can store snapshots of batches or store metadata from an
    external data version control system.
    """

    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        self._name = name
        self._datasource_name = datasource_name
        self._execution_engine = execution_engine

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache = None

        self._data_context_root_directory = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_context_root_directory(self) -> str:
        return self._data_context_root_directory

    @data_context_root_directory.setter
    def data_context_root_directory(self, data_context_root_directory: str):
        self._data_context_root_directory = data_context_root_directory

    def get_batch_data_and_metadata(
        self, batch_definition: BatchDefinition,
    ) -> Tuple[
        Any, BatchSpec, BatchMarkers,  # batch_data
    ]:
        batch_spec: BatchSpec = self.build_batch_spec(batch_definition=batch_definition)
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        return (
            batch_data,
            batch_spec,
            batch_markers,
        )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> BatchSpec:
        batch_spec_params: dict = self._generate_batch_spec_parameters_from_batch_definition(
            batch_definition=batch_definition
        )
        batch_spec_passthrough: dict = batch_definition.batch_spec_passthrough
        if isinstance(batch_spec_passthrough, dict):
            batch_spec_params.update(batch_spec_passthrough)
        batch_spec: BatchSpec = BatchSpec(**batch_spec_params)
        return batch_spec

    def _refresh_data_references_cache(self,):
        raise NotImplementedError

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.
        This method is used to refresh the cache.
        """
        raise NotImplementedError

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[Any]:
        """
        Fetch data_references corresponding to data_asset_name from the cache.
        """
        raise NotImplementedError

    def get_data_reference_list_count(self) -> int:
        raise NotImplementedError

    def get_unmatched_data_references(self) -> List[Any]:
        raise NotImplementedError

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        raise NotImplementedError

    def _map_data_reference_to_batch_definition_list(
        self, data_reference: Any, data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        raise NotImplementedError

    def _map_batch_definition_to_data_reference(
        self, batch_definition: BatchDefinition
    ) -> Any:
        raise NotImplementedError

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        raise NotImplementedError

    def self_check(self, pretty_print=True, max_examples=3):
        if self._data_references_cache is None:
            self._refresh_data_references_cache()

        if pretty_print:
            print("\t" + self.name, ":", self.__class__.__name__)
            print()

        asset_names = self.get_available_data_asset_names()
        # <WILL> : do these need to be sorted using sorter? Sorters currently act on BatchDefinitions, and this returns a list of strings
        asset_names.sort()
        len_asset_names = len(asset_names)

        report_obj = {
            "class_name": self.__class__.__name__,
            "data_asset_count": len_asset_names,
            "example_data_asset_names": asset_names[:max_examples],
            "data_assets": {}
            # "data_reference_count": self.
        }

        if pretty_print:
            print(
                f"\tAvailable data_asset_names ({min(len_asset_names, max_examples)} of {len_asset_names}):"
            )

        for asset_name in asset_names[:max_examples]:
            data_reference_list = self._get_data_reference_list_from_cache_by_data_asset_name(
                data_asset_name=asset_name
            )
            len_batch_definition_list = len(data_reference_list)
            example_data_references = data_reference_list[:max_examples]

            if pretty_print:
                print(
                    f"\t\t{asset_name} ({min(len_batch_definition_list, max_examples)} of {len_batch_definition_list}):",
                    example_data_references,
                )

            report_obj["data_assets"][asset_name] = {
                "batch_definition_count": len_batch_definition_list,
                "example_data_references": example_data_references,
            }

        unmatched_data_references = self.get_unmatched_data_references()
        len_unmatched_data_references = len(unmatched_data_references)
        if pretty_print:
            print(
                f"\n\tUnmatched data_references ({min(len_unmatched_data_references, max_examples)} of {len_unmatched_data_references}):",
                unmatched_data_references[:max_examples],
            )

        report_obj["unmatched_data_reference_count"] = len_unmatched_data_references
        report_obj["example_unmatched_data_references"] = unmatched_data_references[
            :max_examples
        ]

        # Choose an example data_reference
        if pretty_print:
            print("\n\tChoosing an example data reference...")

        example_data_reference = None

        available_references = report_obj["data_assets"].items()
        if len(available_references) == 0:
            if pretty_print:
                print(f"\t\tNo references available.")
            return report_obj

        for data_asset_name, data_asset_return_obj in available_references:
            if data_asset_return_obj["batch_definition_count"] > 0:
                example_data_reference = random.choice(
                    data_asset_return_obj["example_data_references"]
                )
                break

        if example_data_reference is not None:
            if pretty_print:
                print(f"\t\tReference chosen: {example_data_reference}")

            # ...and fetch it.
            report_obj["example_data_reference"] = self._self_check_fetch_batch(
                pretty_print=pretty_print,
                example_data_reference=example_data_reference,
                data_asset_name=data_asset_name,
            )
        else:
            report_obj["example_data_reference"] = {}

        return report_obj

    def _self_check_fetch_batch(
        self, pretty_print: bool, example_data_reference, data_asset_name: str,
    ):
        if pretty_print:
            print(f"\n\t\tFetching batch data...")

        batch_definition_list = self._map_data_reference_to_batch_definition_list(
            data_reference=example_data_reference, data_asset_name=data_asset_name,
        )
        assert len(batch_definition_list) == 1
        batch_definition = batch_definition_list[0]

        # _execution_engine might be None for some tests
        if self._execution_engine is None:
            return {}
        batch_data, batch_spec, _ = self.get_batch_data_and_metadata(batch_definition)

        df = batch_data.head(n=5)
        n_rows = batch_data.row_count()

        if pretty_print and df is not None:
            print(f"\n\t\tShowing 5 rows")
            print(df)

        return {
            "batch_spec": batch_spec,
            "n_rows": n_rows,
        }

    def _validate_batch_request(self, batch_request: BatchRequest):
        if not (
            batch_request.datasource_name is None
            or batch_request.datasource_name == self.datasource_name
        ):
            raise ValueError(
                f"""execution_envrironment_name in BatchRequest: "{batch_request.datasource_name}" does not
match DataConnector datasource_name: "{self.datasource_name}".
                """
            )
        if not (
            batch_request.data_connector_name is None
            or batch_request.data_connector_name == self.name
        ):
            raise ValueError(
                f"""data_connector_name in BatchRequest: "{batch_request.data_connector_name}" does not match
DataConnector name: "{self.name}".
                """
            )
