from __future__ import annotations

import pathlib
import re
from logging import Logger
from typing import TYPE_CHECKING, ClassVar, Optional, Type

from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import BatchMetadata
    from great_expectations.datasource.fluent.interfaces import (
        SortersDefinition,
    )
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        CSVAsset,
        DirectoryCSVAsset,
        ORCAsset,
        ParquetAsset,
        JSONAsset,
    )

logger: Logger

class SparkFilesystemDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[FilesystemDataConnector]] = ...

    # instance attributes
    type: Literal["spark_filesystem"] = "spark_filesystem"

    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path] = None
    def add_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = ...,
        modified_before: str = ...,
        modified_after: str = ...,
        # Spark Generic File Reader Options ^^^
        # CSV Specific Options vvv
        delimiter: str = ...,
        sep: str = ...,
        encoding: str = ...,
        quote: str = ...,
        escape: str = ...,
        comment: str = ...,
        header: bool = False,
        infer_schema: bool = False,
        prefer_date: bool = True,
        enforce_schema: bool = True,
        ignore_leading_white_space: bool = False,
        ignore_trailing_white_space: bool = False,
        null_value: str = ...,
        nan_value: str = ...,
        positive_inf: str = ...,
        negative_inf: str = ...,
        date_format: str = "yyyy-MM-dd",
        timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]",
        timestamp_ntz_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
        enable_date_time_parsing_fallback: bool = ...,
        max_columns: int = 20480,
        max_chars_per_column: int = -1,
        mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] = "PERMISSIVE",
        column_name_of_corrupt_record: str = ...,
        multi_line: bool = False,
        char_to_escape_quote_escaping: str = ...,
        sampling_ratio: float = 1.0,
        empty_value: str = ...,
        locale: str = ...,
        line_sep: str = ...,
        unescaped_quote_handling: Literal[
            "STOP_AT_CLOSING_QUOTE",
            "BACK_TO_DELIMITER",
            "STOP_AT_DELIMITER",
            "SKIP_VALUE",
            "RAISE_ERROR",
        ] = "STOP_AT_DELIMITER",
        # CSV Specific Options ^^^
        order_by: Optional[SortersDefinition] = ...,
    ) -> CSVAsset: ...
    def add_directory_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        glob_directive: str = "**/*",
        data_directory: str | pathlib.Path = ...,
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = ...,
        modified_before: str = ...,
        modified_after: str = ...,
        # Spark Generic File Reader Options ^^^
        # CSV Specific Options vvv
        delimiter: str = ...,
        sep: str = ...,
        encoding: str = ...,
        quote: str = ...,
        escape: str = ...,
        comment: str = ...,
        header: bool = False,
        infer_schema: bool = False,
        prefer_date: bool = True,
        enforce_schema: bool = True,
        ignore_leading_white_space: bool = False,
        ignore_trailing_white_space: bool = False,
        null_value: str = ...,
        nan_value: str = ...,
        positive_inf: str = ...,
        negative_inf: str = ...,
        date_format: str = "yyyy-MM-dd",
        timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]",
        timestamp_ntz_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
        enable_date_time_parsing_fallback: bool = ...,
        max_columns: int = 20480,
        max_chars_per_column: int = -1,
        mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] = "PERMISSIVE",
        column_name_of_corrupt_record: str = ...,
        multi_line: bool = False,
        char_to_escape_quote_escaping: str = ...,
        sampling_ratio: float = 1.0,
        empty_value: str = ...,
        locale: str = ...,
        line_sep: str = ...,
        unescaped_quote_handling: Literal[
            "STOP_AT_CLOSING_QUOTE",
            "BACK_TO_DELIMITER",
            "STOP_AT_DELIMITER",
            "SKIP_VALUE",
            "RAISE_ERROR",
        ] = "STOP_AT_DELIMITER",
        # CSV Specific Options ^^^
        order_by: Optional[SortersDefinition] = ...,
    ) -> DirectoryCSVAsset: ...
    def add_parquet_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"],
        int_96_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"],
        merge_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> ParquetAsset: ...
    def add_orc_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        merge_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> ORCAsset: ...
    def add_json_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        merge_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> JSONAsset: ...

    # TODO: Add JSONasset
    # TODO: Add TextAsset
    # TODO: Add AvroAsset
    # TODO: Add BinaryFileAsset
    # TODO: Add missing fields
    # TODO: Add missing tests
