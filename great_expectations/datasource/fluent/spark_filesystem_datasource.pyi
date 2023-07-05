import pathlib
import re
from logging import Logger
from typing import ClassVar, Literal, Optional, Type, Union

from great_expectations.compatibility.pyspark import (
    types as pyspark_types,
)
from great_expectations.datasource.fluent import BatchMetadata, _SparkFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
    DeltaAsset,
    DirectoryCSVAsset,
    DirectoryDeltaAsset,
    DirectoryJSONAsset,
    DirectoryORCAsset,
    DirectoryParquetAsset,
    DirectoryTextAsset,
    JSONAsset,
    ORCAsset,
    ParquetAsset,
    TextAsset,
)

logger: Logger

class SparkFilesystemDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[FilesystemDataConnector]] = ...

    # instance attributes
    type: Literal["spark_filesystem"] = "spark_filesystem"

    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path] = None
    def add_csv_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # vvv spark parameters for pyspark.sql.DataFrameReader.csv() (ordered as in pyspark v3.4.0)
        # path: PathOrPaths,
        # NA - path determined by asset
        # schema: Optional[Union[StructType, str]] = None,
        spark_schema: Optional[Union[pyspark_types.StructType, str]] = None,
        # sep: Optional[str] = None,
        sep: Optional[str] = None,
        # encoding: Optional[str] = None,
        encoding: Optional[str] = None,
        # quote: Optional[str] = None,
        quote: Optional[str] = None,
        # escape: Optional[str] = None,
        escape: Optional[str] = None,
        # comment: Optional[str] = None,
        comment: Optional[str] = None,
        # header: Optional[Union[bool, str]] = None,
        header: Optional[Union[bool, str]] = None,
        # inferSchema: Optional[Union[bool, str]] = None,
        infer_schema: Optional[Union[bool, str]] = None,
        # ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignore_leading_white_space: Optional[Union[bool, str]] = None,
        # ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        ignore_trailing_white_space: Optional[Union[bool, str]] = None,
        # nullValue: Optional[str] = None,
        null_value: Optional[str] = None,
        # nanValue: Optional[str] = None,
        nan_value: Optional[str] = None,
        # positiveInf: Optional[str] = None,
        positive_inf: Optional[str] = None,
        # negativeInf: Optional[str] = None,
        negative_inf: Optional[str] = None,
        # dateFormat: Optional[str] = None,
        date_format: Optional[str] = None,
        # timestampFormat: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        # maxColumns: Optional[Union[int, str]] = None,
        max_columns: Optional[Union[int, str]] = None,
        # maxCharsPerColumn: Optional[Union[int, str]] = None,
        max_chars_per_column: Optional[Union[int, str]] = None,
        # maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        max_malformed_log_per_partition: Optional[Union[int, str]] = None,
        # mode: Optional[str] = None,
        mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None,
        # columnNameOfCorruptRecord: Optional[str] = None,
        column_name_of_corrupt_record: Optional[str] = None,
        # multiLine: Optional[Union[bool, str]] = None,
        multi_line: Optional[Union[bool, str]] = None,
        # charToEscapeQuoteEscaping: Optional[str] = None,
        char_to_escape_quote_escaping: Optional[str] = None,
        # samplingRatio: Optional[Union[float, str]] = None,
        sampling_ratio: Optional[Union[float, str]] = None,
        # enforceSchema: Optional[Union[bool, str]] = None,
        enforce_schema: Optional[Union[bool, str]] = None,
        # emptyValue: Optional[str] = None,
        empty_value: Optional[str] = None,
        # locale: Optional[str] = None,
        locale: Optional[str] = None,
        # lineSep: Optional[str] = None,
        line_sep: Optional[str] = None,
        # pathGlobFilter: Optional[Union[bool, str]] = None,
        path_glob_filter: Optional[Union[bool, str]] = None,
        # recursiveFileLookup: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # modifiedBefore: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        # modifiedAfter: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        # unescapedQuoteHandling: Optional[str] = None,
        unescaped_quote_handling: Optional[
            Literal[
                "STOP_AT_CLOSING_QUOTE",
                "BACK_TO_DELIMITER",
                "STOP_AT_DELIMITER",
                "SKIP_VALUE",
                "RAISE_ERROR",
            ]
        ] = None,
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-csv.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604
        # CSV Specific Options vvv
        # prefer_date: Optional[bool] = None,
        # timestamp_ntz_format: Optional[str] = None,
        # enable_date_time_parsing_fallback: Optional[bool] = None,
        # CSV Specific Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> CSVAsset: ...
    def add_directory_csv_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # vvv spark parameters for pyspark.sql.DataFrameReader.csv() (ordered as in pyspark v3.4.0)
        # path: PathOrPaths,
        # NA - path determined by asset
        # schema: Optional[Union[StructType, str]] = None,
        spark_schema: Optional[Union[pyspark_types.StructType, str]] = None,
        # sep: Optional[str] = None,
        sep: Optional[str] = None,
        # encoding: Optional[str] = None,
        encoding: Optional[str] = None,
        # quote: Optional[str] = None,
        quote: Optional[str] = None,
        # escape: Optional[str] = None,
        escape: Optional[str] = None,
        # comment: Optional[str] = None,
        comment: Optional[str] = None,
        # header: Optional[Union[bool, str]] = None,
        header: Optional[Union[bool, str]] = None,
        # inferSchema: Optional[Union[bool, str]] = None,
        infer_schema: Optional[Union[bool, str]] = None,
        # ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignore_leading_white_space: Optional[Union[bool, str]] = None,
        # ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        ignore_trailing_white_space: Optional[Union[bool, str]] = None,
        # nullValue: Optional[str] = None,
        null_value: Optional[str] = None,
        # nanValue: Optional[str] = None,
        nan_value: Optional[str] = None,
        # positiveInf: Optional[str] = None,
        positive_inf: Optional[str] = None,
        # negativeInf: Optional[str] = None,
        negative_inf: Optional[str] = None,
        # dateFormat: Optional[str] = None,
        date_format: Optional[str] = None,
        # timestampFormat: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        # maxColumns: Optional[Union[int, str]] = None,
        max_columns: Optional[Union[int, str]] = None,
        # maxCharsPerColumn: Optional[Union[int, str]] = None,
        max_chars_per_column: Optional[Union[int, str]] = None,
        # maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        max_malformed_log_per_partition: Optional[Union[int, str]] = None,
        # mode: Optional[str] = None,
        mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None,
        # columnNameOfCorruptRecord: Optional[str] = None,
        column_name_of_corrupt_record: Optional[str] = None,
        # multiLine: Optional[Union[bool, str]] = None,
        multi_line: Optional[Union[bool, str]] = None,
        # charToEscapeQuoteEscaping: Optional[str] = None,
        char_to_escape_quote_escaping: Optional[str] = None,
        # samplingRatio: Optional[Union[float, str]] = None,
        sampling_ratio: Optional[Union[float, str]] = None,
        # enforceSchema: Optional[Union[bool, str]] = None,
        enforce_schema: Optional[Union[bool, str]] = None,
        # emptyValue: Optional[str] = None,
        empty_value: Optional[str] = None,
        # locale: Optional[str] = None,
        locale: Optional[str] = None,
        # lineSep: Optional[str] = None,
        line_sep: Optional[str] = None,
        # pathGlobFilter: Optional[Union[bool, str]] = None,
        path_glob_filter: Optional[Union[bool, str]] = None,
        # recursiveFileLookup: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # modifiedBefore: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        # modifiedAfter: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        # unescapedQuoteHandling: Optional[str] = None,
        unescaped_quote_handling: Optional[
            Literal[
                "STOP_AT_CLOSING_QUOTE",
                "BACK_TO_DELIMITER",
                "STOP_AT_DELIMITER",
                "SKIP_VALUE",
                "RAISE_ERROR",
            ]
        ] = None,
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-csv.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604
        # CSV Specific Options vvv
        # prefer_date: Optional[bool] = None,
        # timestamp_ntz_format: Optional[str] = None,
        # enable_date_time_parsing_fallback: Optional[bool] = None,
        # CSV Specific Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> DirectoryCSVAsset: ...
    def add_parquet_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        path_glob_filter: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # Spark Generic File Reader Options ^^^
        # vvv spark parameters for pyspark.sql.DataFrameReader.parquet() (ordered as in pyspark v3.4.0)
        # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        # Parquet Specific Options vvv
        merge_schema: Optional[Union[bool, str]] = None,
        datetime_rebase_mode: Optional[
            Literal["EXCEPTION", "CORRECTED", "LEGACY"]
        ] = None,
        int_96_rebase_mode: Optional[
            Literal["EXCEPTION", "CORRECTED", "LEGACY"]
        ] = None,
        # Parquet Specific Options ^^^
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L473
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> ParquetAsset: ...
    def add_directory_parquet_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # Spark Generic File Reader Options vvv
        path_glob_filter: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # Spark Generic File Reader Options ^^^
        # vvv spark parameters for pyspark.sql.DataFrameReader.parquet() (ordered as in pyspark v3.4.0)
        # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        # Parquet Specific Options vvv
        merge_schema: Optional[Union[bool, str]] = None,
        datetime_rebase_mode: Optional[
            Literal["EXCEPTION", "CORRECTED", "LEGACY"]
        ] = None,
        int_96_rebase_mode: Optional[
            Literal["EXCEPTION", "CORRECTED", "LEGACY"]
        ] = None,
        # Parquet Specific Options ^^^
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L473
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> DirectoryParquetAsset: ...
    def add_orc_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        path_glob_filter: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # Spark Generic File Reader Options ^^^
        # ORC Specific Options vvv
        merge_schema: Optional[Union[bool, str]] = None,
        # ORC Specific Options ^^^
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L473
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> ORCAsset: ...
    def add_directory_orc_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # Spark Generic File Reader Options vvv
        path_glob_filter: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # Spark Generic File Reader Options ^^^
        # ORC Specific Options vvv
        merge_schema: Optional[Union[bool, str]] = None,
        # ORC Specific Options ^^^
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L473
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> DirectoryORCAsset: ...
    def add_json_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # vvv spark parameters for pyspark.sql.DataFrameReader.json() (ordered as in pyspark v3.4.0)
        # path: Union[str, List[str], RDD[str]],
        # NA - path determined by asset
        # schema: Optional[Union[StructType, str]] = None,
        spark_schema: Optional[Union[pyspark_types.StructType, str]] = None,
        # primitivesAsString: Optional[Union[bool, str]] = None,
        primitives_as_string: Optional[Union[bool, str]] = None,
        # prefersDecimal: Optional[Union[bool, str]] = None,
        prefers_decimal: Optional[Union[bool, str]] = None,
        # allowComments: Optional[Union[bool, str]] = None,
        allow_comments: Optional[Union[bool, str]] = None,
        # allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
        allow_unquoted_field_names: Optional[Union[bool, str]] = None,
        # allowSingleQuotes: Optional[Union[bool, str]] = None,
        allow_single_quotes: Optional[Union[bool, str]] = None,
        # allowNumericLeadingZero: Optional[Union[bool, str]] = None,
        allow_numeric_leading_zero: Optional[Union[bool, str]] = None,
        # allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
        allow_backslash_escaping_any_character: Optional[Union[bool, str]] = None,
        # mode: Optional[str] = None,
        mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None,
        # columnNameOfCorruptRecord: Optional[str] = None,
        column_name_of_corrupt_record: Optional[str] = None,
        # dateFormat: Optional[str] = None,
        date_format: Optional[str] = None,
        # timestampFormat: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        # multiLine: Optional[Union[bool, str]] = None,
        multi_line: Optional[Union[bool, str]] = None,
        # allowUnquotedControlChars: Optional[Union[bool, str]] = None,
        allow_unquoted_control_chars: Optional[Union[bool, str]] = None,
        # lineSep: Optional[str] = None,
        line_sep: Optional[str] = None,
        # samplingRatio: Optional[Union[float, str]] = None,
        sampling_ratio: Optional[Union[float, str]] = None,
        # dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        drop_field_if_all_null: Optional[Union[bool, str]] = None,
        # encoding: Optional[str] = None,
        encoding: Optional[str] = None,
        # locale: Optional[str] = None,
        locale: Optional[str] = None,
        # pathGlobFilter: Optional[Union[bool, str]] = None,
        path_glob_filter: Optional[Union[bool, str]] = None,
        # recursiveFileLookup: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # modifiedBefore: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        # modifiedAfter: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        # allowNonNumericNumbers: Optional[Union[bool, str]] = None,
        allow_non_numeric_numbers: Optional[Union[bool, str]] = None,
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-json.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # JSON Specific Options vvv
        # timezone: str = ...,
        # timestamp_ntz_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
        # enable_date_time_parsing_fallback: bool = ...,
        # JSON Specific Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> JSONAsset: ...
    def add_directory_json_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # vvv spark parameters for pyspark.sql.DataFrameReader.json() (ordered as in pyspark v3.4.0)
        # path: Union[str, List[str], RDD[str]],
        # NA - path determined by asset
        # schema: Optional[Union[StructType, str]] = None,
        spark_schema: Optional[Union[pyspark_types.StructType, str]] = None,
        # primitivesAsString: Optional[Union[bool, str]] = None,
        primitives_as_string: Optional[Union[bool, str]] = None,
        # prefersDecimal: Optional[Union[bool, str]] = None,
        prefers_decimal: Optional[Union[bool, str]] = None,
        # allowComments: Optional[Union[bool, str]] = None,
        allow_comments: Optional[Union[bool, str]] = None,
        # allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
        allow_unquoted_field_names: Optional[Union[bool, str]] = None,
        # allowSingleQuotes: Optional[Union[bool, str]] = None,
        allow_single_quotes: Optional[Union[bool, str]] = None,
        # allowNumericLeadingZero: Optional[Union[bool, str]] = None,
        allow_numeric_leading_zero: Optional[Union[bool, str]] = None,
        # allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
        allow_backslash_escaping_any_character: Optional[Union[bool, str]] = None,
        # mode: Optional[str] = None,
        mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None,
        # columnNameOfCorruptRecord: Optional[str] = None,
        column_name_of_corrupt_record: Optional[str] = None,
        # dateFormat: Optional[str] = None,
        date_format: Optional[str] = None,
        # timestampFormat: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        # multiLine: Optional[Union[bool, str]] = None,
        multi_line: Optional[Union[bool, str]] = None,
        # allowUnquotedControlChars: Optional[Union[bool, str]] = None,
        allow_unquoted_control_chars: Optional[Union[bool, str]] = None,
        # lineSep: Optional[str] = None,
        line_sep: Optional[str] = None,
        # samplingRatio: Optional[Union[float, str]] = None,
        sampling_ratio: Optional[Union[float, str]] = None,
        # dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        drop_field_if_all_null: Optional[Union[bool, str]] = None,
        # encoding: Optional[str] = None,
        encoding: Optional[str] = None,
        # locale: Optional[str] = None,
        locale: Optional[str] = None,
        # pathGlobFilter: Optional[Union[bool, str]] = None,
        path_glob_filter: Optional[Union[bool, str]] = None,
        # recursiveFileLookup: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # modifiedBefore: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        # modifiedAfter: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        # allowNonNumericNumbers: Optional[Union[bool, str]] = None,
        allow_non_numeric_numbers: Optional[Union[bool, str]] = None,
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-json.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # JSON Specific Options vvv
        # timezone: str = ...,
        # timestamp_ntz_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
        # enable_date_time_parsing_fallback: bool = ...,
        # JSON Specific Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> DirectoryJSONAsset: ...
    def add_text_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Generic File Reader Options vvv
        path_glob_filter: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # Spark Generic File Reader Options ^^^
        # Text Specific Options vvv
        # wholetext: bool = False,
        wholetext: bool = False,
        # lineSep: Optional[str] = None,
        line_sep: Optional[str] = None,
        # Text Specific Options ^^^
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> TextAsset: ...
    def add_directory_text_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # Spark Generic File Reader Options vvv
        path_glob_filter: Optional[Union[bool, str]] = None,
        modified_before: Optional[Union[bool, str]] = None,
        modified_after: Optional[Union[bool, str]] = None,
        recursive_file_lookup: Optional[Union[bool, str]] = None,
        # Spark Generic File Reader Options ^^^
        # Text Specific Options vvv
        # wholetext: bool = False,
        wholetext: bool = False,
        # lineSep: Optional[str] = None,
        line_sep: Optional[str] = None,
        # Text Specific Options ^^^
        # vvv pyspark Docs <> Source Code mismatch
        # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
        # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
        # Spark Generic File Reader Options vvv
        # ignore_corrupt_files: bool = ...,
        # ignore_missing_files: bool = ...,
        # Spark Generic File Reader Options ^^^
        # ^^^ pyspark Docs <> Source Code mismatch
    ) -> DirectoryTextAsset: ...
    def add_delta_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Delta Specific Options vvv
        timestamp_as_of: Optional[str] = None,
        version_as_of: Optional[str] = None,
        # Delta Specific Options ^^^
    ) -> DeltaAsset: ...
    def add_delta_directory_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = ...,
        # Spark Directory Reader Options vvv
        data_directory: str | pathlib.Path = ...,
        # Spark Directory Reader Options ^^^
        # Delta Specific Options vvv
        timestamp_as_of: Optional[str] = None,
        version_as_of: Optional[str] = None,
        # Delta Specific Options ^^^
    ) -> DirectoryDeltaAsset: ...
