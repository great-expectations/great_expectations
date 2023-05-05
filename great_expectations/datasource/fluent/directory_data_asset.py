from __future__ import annotations

import copy
import logging
from collections import Counter
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Pattern,
    Set,
    cast,
)

import pydantic

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.id_dict import BatchSpec
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.datasource.fluent.constants import MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FILE_PATH_BATCH_SPEC_KEY,
)
from great_expectations.datasource.fluent.data_asset.data_connector.file_path_data_connector import (
    get_batch_definition_for_splitter,
)
from great_expectations.datasource.fluent.data_asset.data_connector.regex_parser import (
    RegExParser,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_generic_splitters import (
    Splitter,
    SplitterColumnValue,
    SplitterDatetimePart,
    SplitterYear,
    SplitterYearAndMonth,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from great_expectations.core.batch import BatchDefinition, BatchMarkers
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
    )
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )
from great_expectations.datasource.fluent.file_path_data_asset import _FilePathDataAsset

logger = logging.getLogger(__name__)


class _DirectoryDataAsset(_FilePathDataAsset):


    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        self._validate_batch_request(batch_request)

        execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )

        if self.splitter:
            batch_definition_list: List[
                BatchDefinition
            ] = get_batch_definition_for_splitter(
                data_connector=self._data_connector,
                batch_request=batch_request,
                path=self._get_path_for_batch_definition(),
            )
        else:
            batch_definition_list: List[
                BatchDefinition
            ] = self._data_connector.get_batch_definition_list(
                batch_request=batch_request
            )

        batch_list: List[Batch] = []

        batch_spec: BatchSpec
        batch_spec_options: dict
        batch_data: Any
        batch_markers: BatchMarkers
        batch_metadata: BatchMetadata
        batch: Batch

        for batch_definition in batch_definition_list:
            batch_spec = self._data_connector.build_batch_spec(
                batch_definition=batch_definition
            )
            batch_spec_options = self._batch_spec_options_from_batch_request(batch_request)
            batch_spec.update(batch_spec_options)

            batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            fully_specified_batch_request = copy.deepcopy(batch_request)
            fully_specified_batch_request.options.update(
                batch_definition.batch_identifiers
            )
            batch_metadata = self._get_batch_metadata_from_batch_request(
                batch_request=fully_specified_batch_request
            )

            # Some pydantic annotations are postponed due to circular imports.
            # Batch.update_forward_refs() will set the annotations before we
            # instantiate the Batch class since we can import them in this scope.
            Batch.update_forward_refs()

            batch = Batch(
                datasource=self.datasource,
                data_asset=self,
                batch_request=fully_specified_batch_request,
                data=batch_data,
                metadata=batch_metadata,
                legacy_batch_markers=batch_markers,
                legacy_batch_spec=batch_spec,
                legacy_batch_definition=batch_definition,
            )
            batch_list.append(batch)

        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX-MOVE_SORTING_INTO_FILE_PATH_DATA_CONNECTOR_ON_BATCH_DEFINITION_OBJECTS</Alex>
        self.sort_batches(batch_list)

        return batch_list