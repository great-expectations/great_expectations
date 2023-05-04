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
        TextAsset,
        AvroAsset,
        BinaryFileAsset,
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
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = False,
        modified_before: str = "",
        modified_after: str = "",
        # Spark Generic File Reader Options ^^^
        # CSV Specific Options vvv
        sep: str = ...,
        encoding: str = ...,
        quote: str = ...,
        escape: str = ...,
        comment: str = "",
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
    ) -> CSVAsset: ...
    def add_directory_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = False,
        modified_before: str = "",
        modified_after: str = "",
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
    ) -> DirectoryCSVAsset: ...
    def add_parquet_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = False,
        modified_before: str = "",
        modified_after: str = "",
        # Spark Generic File Reader Options ^^^
        # Parquet Specific Options vvv
        datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"],
        int_96_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"],
        merge_schema: bool = ...,
        # Parquet Specific Options ^^^
    ) -> ParquetAsset: ...
    def add_orc_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = False,
        modified_before: str = "",
        modified_after: str = "",
        # Spark Generic File Reader Options ^^^
        # ORC Specific Options vvv
        merge_schema: bool = ...,
        # ORC Specific Options ^^^
    ) -> ORCAsset: ...
    def add_json_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = False,
        modified_before: str = "",
        modified_after: str = "",
        # Spark Generic File Reader Options ^^^
        # JSON Specific Options vvv
        timezone: str = ...,
        primitives_as_string: bool = False,
        prefers_decimal: bool = False,
        allow_comments: bool = False,
        allow_unquoted_field_names: bool = False,
        allow_single_quotes: bool = True,
        allow_numeric_leading_zeros: bool = False,
        allow_backslash_escaping_any_character: bool = False,
        mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] = "PERMISSIVE",
        column_name_of_corrupt_record: str = ...,
        date_format: str = "yyyy-MM-dd",
        timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]",
        timestamp_ntz_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
        enable_date_time_parsing_fallback: bool = ...,
        multi_line: bool = False,
        allow_unquoted_control_chars: bool = False,
        encoding: str = ...,
        line_sep: str = ...,
        sampling_ratio: float = 1.0,
        drop_field_if_all_null: bool = False,
        locale: str = ...,
        allow_non_numeric_numbers: bool = True,
        merge_schema: bool = False,
        # JSON Specific Options ^^^
    ) -> JSONAsset: ...
    def add_text_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        ignore_corrupt_files: bool = ...,
        ignore_missing_files: bool = ...,
        path_glob_filter: str = ...,
        recursive_file_lookup: bool = False,
        modified_before: str = "",
        modified_after: str = "",
        # Spark Generic File Reader Options ^^^
        # Text Specific Options vvv
        wholetext: bool = ...,
        line_sep: str = ...,
        # Text Specific Options ^^^
    ) -> TextAsset: ...
    def add_avro_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        # Avro does not support generic file reader options.
        # Spark Generic File Reader Options ^^^
        # Avro Specific Options vvv
        avro_schema: str = None,
        ignore_extension: bool = True,
        datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = ...,
        positional_field_matching: bool = False,
        # Avro Specific Options ^^^
    ) -> AvroAsset: ...
    def add_binary_file_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        # BinaryFileAsset does not support generic file reader options.
        # Spark Generic File Reader Options ^^^
        # BinaryFileAsset Specific Options vvv
        path_glob_filter: str = ...,
        # BinaryFileAsset Specific Options ^^^
    ) -> BinaryFileAsset: ...

    # TODO: Auto generate pyi content from pydantic model that includes default
