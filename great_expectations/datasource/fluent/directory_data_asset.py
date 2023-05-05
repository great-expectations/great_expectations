from __future__ import annotations

import copy
import logging
import pathlib
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
from great_expectations.core.id_dict import BatchSpec, IDDict
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.datasource.fluent.constants import MATCH_ALL_PATTERN, _DATA_CONNECTOR_NAME
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FILE_PATH_BATCH_SPEC_KEY,
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
from great_expectations.core.batch import BatchDefinition

if TYPE_CHECKING:
    from typing_extensions import Self

    from great_expectations.core.batch import BatchMarkers
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

    data_directory: pathlib.Path

    def _get_batch_definition_list(self, batch_request: BatchRequest) -> list[BatchDefinition]:
        if self.splitter:
            # Currently non-sql asset splitters do not introspect the datasource for available
            # batches and only return a single batch based on specified batch_identifiers.
            batch_identifiers = batch_request.options
            if not batch_identifiers.get("path"):
                batch_identifiers["path"] = self.data_directory

            batch_definition = BatchDefinition(
                datasource_name=self._data_connector.datasource_name,
                data_connector_name=_DATA_CONNECTOR_NAME,
                data_asset_name=self._data_connector.data_asset_name,
                batch_identifiers=IDDict(batch_identifiers),
            )
            batch_definition_list = [batch_definition]
        else:
            batch_definition_list = self._data_connector.get_batch_definition_list(
                batch_request=batch_request
            )
        return batch_definition_list
