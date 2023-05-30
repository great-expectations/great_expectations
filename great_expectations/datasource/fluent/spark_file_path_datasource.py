from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    List,
    Literal,
    Optional,
    Sequence,
    Type,
    Union,
)

import pydantic
from pydantic import Field

from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.directory_data_asset import (
    _DirectoryDataAssetMixin,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.serializable_types.pyspark import (
    SerializableStructType,  # noqa: TCH001
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import DataAsset

logger = logging.getLogger(__name__)


class _SparkGenericFilePathAssetMixin(_FilePathDataAsset):
    # vvv Docs <> Source Code mismatch
    # ignoreCorruptFiles and ignoreMissingFiles appear in the docs https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
    # but not in any reader method signatures (e.g. https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604)
    # ignore_corrupt_files: bool = Field(alias="ignoreCorruptFiles")
    # ignore_missing_files: bool = Field(alias="ignoreMissingFiles")
    # ^^^ Docs <> Source Code mismatch

    path_glob_filter: Optional[Union[bool, str]] = Field(None, alias="pathGlobFilter")
    recursive_file_lookup: Optional[Union[bool, str]] = Field(
        None, alias="recursiveFileLookup"
    )
    modified_before: Optional[Union[bool, str]] = Field(None, alias="modifiedBefore")
    modified_after: Optional[Union[bool, str]] = Field(None, alias="modifiedAfter")

    def _get_reader_options_include(self) -> set[str]:
        return {
            "path_glob_filter",
            "recursive_file_lookup",
            "modified_before",
            "modified_after",
            # vvv Missing from method signatures but appear in documentation:
            # "ignoreCorruptFiles",
            # "ignore_missing_files",
            # ^^^ Missing from method signatures but appear in documentation:
        }


class CSVAsset(_SparkGenericFilePathAssetMixin):
    type: Literal["csv"] = "csv"

    # vvv spark parameters for pyspark.sql.DataFrameReader.csv() (ordered as in pyspark v3.4.0) appear in comment above
    # parameter for reference (from https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604)
    # See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
    # path: PathOrPaths,
    # NA - path determined by asset
    # schema: Optional[Union[StructType, str]] = None,
    # schema shadows pydantic BaseModel attribute
    spark_schema: Optional[Union[SerializableStructType, str]] = Field(
        None, alias="schema"
    )
    # sep: Optional[str] = None,
    sep: Union[str, None] = None
    # encoding: Optional[str] = None,
    encoding: Optional[str] = None
    # quote: Optional[str] = None,
    quote: Optional[str] = None
    # escape: Optional[str] = None,
    escape: Optional[str] = None
    # comment: Optional[str] = None,
    comment: Optional[str] = None
    # header: Optional[Union[bool, str]] = None,
    header: Optional[Union[bool, str]] = None
    # inferSchema: Optional[Union[bool, str]] = None,
    infer_schema: Optional[Union[bool, str]] = Field(None, alias="inferSchema")
    # ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
    ignore_leading_white_space: Optional[Union[bool, str]] = Field(
        None, alias="ignoreLeadingWhiteSpace"
    )
    # ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
    ignore_trailing_white_space: Optional[Union[bool, str]] = Field(
        None, alias="ignoreTrailingWhiteSpace"
    )
    # nullValue: Optional[str] = None,
    null_value: Optional[str] = Field(None, alias="nullValue")
    # nanValue: Optional[str] = None,
    nan_value: Optional[str] = Field(None, alias="nanValue")
    # positiveInf: Optional[str] = None,
    positive_inf: Optional[str] = Field(None, alias="positiveInf")
    # negativeInf: Optional[str] = None,
    negative_inf: Optional[str] = Field(None, alias="negativeInf")
    # dateFormat: Optional[str] = None,
    date_format: Optional[str] = Field(None, alias="dateFormat")
    # timestampFormat: Optional[str] = None,
    timestamp_format: Optional[str] = Field(None, alias="timestampFormat")
    # maxColumns: Optional[Union[int, str]] = None,
    max_columns: Optional[Union[int, str]] = Field(None, alias="maxColumns")
    # maxCharsPerColumn: Optional[Union[int, str]] = None,
    max_chars_per_column: Optional[Union[int, str]] = Field(
        None, alias="maxCharsPerColumn"
    )
    # maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
    max_malformed_log_per_partition: Optional[Union[int, str]] = Field(
        None, alias="maxMalformedLogPerPartition"
    )
    # mode: Optional[str] = None,
    mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None
    # columnNameOfCorruptRecord: Optional[str] = None,
    column_name_of_corrupt_record: Optional[str] = Field(
        None, alias="columnNameOfCorruptRecord"
    )
    # multiLine: Optional[Union[bool, str]] = None,
    multi_line: Optional[Union[bool, str]] = Field(None, alias="multiLine")
    # charToEscapeQuoteEscaping: Optional[str] = None,
    char_to_escape_quote_escaping: Optional[str] = Field(
        None, alias="charToEscapeQuoteEscaping"
    )
    # samplingRatio: Optional[Union[float, str]] = None,
    sampling_ratio: Optional[Union[float, str]] = Field(None, alias="samplingRatio")
    # enforceSchema: Optional[Union[bool, str]] = None,
    enforce_schema: Optional[Union[bool, str]] = Field(None, alias="enforceSchema")
    # emptyValue: Optional[str] = None,
    empty_value: Optional[str] = Field(None, alias="emptyValue")
    # locale: Optional[str] = None,
    locale: Optional[str] = None
    # lineSep: Optional[str] = None,
    line_sep: Optional[str] = Field(None, alias="lineSep")
    # pathGlobFilter: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # recursiveFileLookup: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # modifiedBefore: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # modifiedAfter: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # unescapedQuoteHandling: Optional[str] = None,
    unescaped_quote_handling: Optional[
        Literal[
            "STOP_AT_CLOSING_QUOTE",
            "BACK_TO_DELIMITER",
            "STOP_AT_DELIMITER",
            "SKIP_VALUE",
            "RAISE_ERROR",
        ]
    ] = Field(None, alias="unescapedQuoteHandling")

    # vvv Docs <> Source Code mismatch
    # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-csv.html
    # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604
    # prefer_date: bool = Field(True, alias="preferDate")
    # timestamp_ntz_format: str = Field(
    #     "yyyy-MM-dd'T'HH:mm:ss[.SSS]", alias="timestampNTZFormat"
    # )
    # enable_date_time_parsing_fallback: bool = Field(
    #     alias="enableDateTimeParsingFallback"
    # )
    # ^^^ Docs <> Source Code mismatch

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        return "csv"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        parent_reader_options = super()._get_reader_options_include()
        reader_options = {
            "spark_schema",
            "sep",
            "encoding",
            "quote",
            "escape",
            "comment",
            "header",
            "infer_schema",
            "ignore_leading_white_space",
            "ignore_trailing_white_space",
            "null_value",
            "nan_value",
            "positive_inf",
            "negative_inf",
            "date_format",
            "timestamp_format",
            "max_columns",
            "max_chars_per_column",
            "max_malformed_log_per_partition",
            "mode",
            "column_name_of_corrupt_record",
            "multi_line",
            "char_to_escape_quote_escaping",
            "sampling_ratio",
            "enforce_schema",
            "empty_value",
            "locale",
            "line_sep",
            "unescaped_quote_handling",
            # Inherited vvv
            # "ignore_missing_files",
            # "path_glob_filter",
            # "modified_before",
            # "modified_after",
            # Inherited ^^^
            # vvv Docs <> Source Code mismatch
            # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-csv.html
            # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604
            # "preferDate",
            # "timestampNTZFormat",
            # "enableDateTimeParsingFallback",
            # ^^^ Docs <> Source Code mismatch
        }
        return parent_reader_options.union(reader_options)


class DirectoryCSVAsset(_DirectoryDataAssetMixin, CSVAsset):
    type: Literal["directory_csv"] = "directory_csv"  # type: ignore[assignment]

    @classmethod
    def _get_reader_method(cls) -> str:
        return "csv"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
        )


class ParquetAsset(_SparkGenericFilePathAssetMixin):
    type: Literal["parquet"] = "parquet"
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
    merge_schema: Optional[Union[bool, str]] = Field(None, alias="mergeSchema")
    datetime_rebase_mode: Optional[Literal["EXCEPTION", "CORRECTED", "LEGACY"]] = Field(
        None, alias="datetimeRebaseMode"
    )
    int_96_rebase_mode: Optional[Literal["EXCEPTION", "CORRECTED", "LEGACY"]] = Field(
        None, alias="int96RebaseMode"
    )

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        return "parquet"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "datetime_rebase_mode",
                    "int_96_rebase_mode",
                    "merge_schema",
                }
            )
        )


class DirectoryParquetAsset(_DirectoryDataAssetMixin, ParquetAsset):
    type: Literal["directory_parquet"] = "directory_parquet"  # type: ignore[assignment]

    @classmethod
    def _get_reader_method(cls) -> str:
        return "parquet"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
        )


class ORCAsset(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
    type: Literal["orc"] = "orc"
    merge_schema: Optional[Union[bool, str]] = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        return "orc"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
        """
        return super()._get_reader_options_include().union({"merge_schema"})


class DirectoryORCAsset(_DirectoryDataAssetMixin, ORCAsset):
    type: Literal["directory_orc"] = "directory_orc"  # type: ignore[assignment]

    @classmethod
    def _get_reader_method(cls) -> str:
        return "orc"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
        )


class JSONAsset(_SparkGenericFilePathAssetMixin):
    type: Literal["json"] = "json"

    # vvv spark parameters for pyspark.sql.DataFrameReader.json() (ordered as in pyspark v3.4.0) appear in comment above
    # parameter for reference (from https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309)
    # path: Union[str, List[str], RDD[str]],
    # NA - path determined by asset
    # schema: Optional[Union[StructType, str]] = None,
    # schema shadows pydantic BaseModel attribute
    spark_schema: Optional[Union[SerializableStructType, str]] = Field(
        None, alias="schema"
    )
    # primitivesAsString: Optional[Union[bool, str]] = None,
    primitives_as_string: Optional[Union[bool, str]] = Field(
        None, alias="primitivesAsString"
    )
    # prefersDecimal: Optional[Union[bool, str]] = None,
    prefers_decimal: Optional[Union[bool, str]] = Field(None, alias="prefersDecimal")
    # allowComments: Optional[Union[bool, str]] = None,
    allow_comments: Optional[Union[bool, str]] = Field(None, alias="allowComments")
    # allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
    allow_unquoted_field_names: Optional[Union[bool, str]] = Field(
        None, alias="allowUnquotedFieldNames"
    )
    # allowSingleQuotes: Optional[Union[bool, str]] = None,
    allow_single_quotes: Optional[Union[bool, str]] = Field(
        None, alias="allowSingleQuotes"
    )
    # allowNumericLeadingZero: Optional[Union[bool, str]] = None,
    allow_numeric_leading_zero: Optional[Union[bool, str]] = Field(
        None, alias="allowNumericLeadingZero"
    )
    # allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
    allow_backslash_escaping_any_character: Optional[Union[bool, str]] = Field(
        None, alias="allowBackslashEscapingAnyCharacter"
    )
    # mode: Optional[str] = None,
    mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None
    # columnNameOfCorruptRecord: Optional[str] = None,
    column_name_of_corrupt_record: Optional[str] = Field(
        None, alias="columnNameOfCorruptRecord"
    )
    # dateFormat: Optional[str] = None,
    date_format: Optional[str] = Field(None, alias="dateFormat")
    # timestampFormat: Optional[str] = None,
    timestamp_format: Optional[str] = Field(None, alias="timestampFormat")
    # multiLine: Optional[Union[bool, str]] = None,
    multi_line: Optional[Union[bool, str]] = Field(None, alias="multiLine")
    # allowUnquotedControlChars: Optional[Union[bool, str]] = None,
    allow_unquoted_control_chars: Optional[Union[bool, str]] = Field(
        None, alias="allowUnquotedControlChars"
    )
    # lineSep: Optional[str] = None,
    line_sep: Optional[str] = Field(None, alias="lineSep")
    # samplingRatio: Optional[Union[float, str]] = None,
    sampling_ratio: Optional[Union[float, str]] = Field(None, alias="samplingRatio")
    # dropFieldIfAllNull: Optional[Union[bool, str]] = None,
    drop_field_if_all_null: Optional[Union[bool, str]] = Field(
        None, alias="dropFieldIfAllNull"
    )
    # encoding: Optional[str] = None,
    encoding: Optional[str] = None
    # locale: Optional[str] = None,
    locale: Optional[str] = None
    # pathGlobFilter: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # recursiveFileLookup: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # modifiedBefore: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # modifiedAfter: Optional[Union[bool, str]] = None,
    # Inherited from _SparkGenericFilePathAssetMixin
    # allowNonNumericNumbers: Optional[Union[bool, str]] = None,
    allow_non_numeric_numbers: Optional[Union[bool, str]] = Field(
        None, alias="allowNonNumericNumbers"
    )
    # ^^^ spark parameters for pyspark.sql.DataFrameReader.json() (ordered as in pyspark v3.4.0)

    # vvv Docs <> Source Code mismatch
    # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-json.html
    # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
    # timezone: str = Field(alias="timeZone")
    # timestamp_ntz_format: str = Field(
    #     "yyyy-MM-dd'T'HH:mm:ss[.SSS]", alias="timestampNTZFormat"
    # )
    # enable_date_time_parsing_fallback: bool = Field(
    #     alias="enableDateTimeParsingFallback"
    # )
    # ^^^ Docs <> Source Code mismatch

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        return "json"

    def _get_reader_options_include(self) -> set[str]:
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "primitives_as_string",
                    "prefers_decimal",
                    "allow_comments",
                    "allow_unquoted_field_names",
                    "allow_single_quotes",
                    "allow_numeric_leading_zero",
                    "allow_backslash_escaping_any_character",
                    "mode",
                    "column_name_of_corrupt_record",
                    "date_format",
                    "timestamp_format",
                    "multi_line",
                    "allow_unquoted_control_chars",
                    "line_sep",
                    "sampling_ratio",
                    "drop_field_if_all_null",
                    "encoding",
                    "locale",
                    "allow_non_numeric_numbers",
                    # Inherited vvv
                    # "pathGlobFilter",
                    # "recursiveFileLookup",
                    # "modifiedBefore",
                    # "modifiedAfter",
                    # Inherited ^^^
                    # vvv Docs <> Source Code mismatch
                    # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-json.html
                    # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309
                    # "enableDateTimeParsingFallback",
                    # "timeZone",
                    # "timestampNTZFormat",
                    # ^^^ Docs <> Source Code mismatch
                }
            )
        )


class DirectoryJSONAsset(_DirectoryDataAssetMixin, JSONAsset):
    type: Literal["directory_json"] = "directory_json"  # type: ignore[assignment]

    @classmethod
    def _get_reader_method(cls) -> str:
        return "json"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-json.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
        )


class TextAsset(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
    type: Literal["text"] = "text"
    wholetext: bool = Field(False)
    line_sep: Optional[str] = Field(None, alias="lineSep")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        return "text"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
        """
        return super()._get_reader_options_include().union({"wholetext", "line_sep"})


class DirectoryTextAsset(_DirectoryDataAssetMixin, TextAsset):
    type: Literal["directory_text"] = "directory_text"  # type: ignore[assignment]

    @classmethod
    def _get_reader_method(cls) -> str:
        return "text"

    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
        )


class DeltaAsset(_FilePathDataAsset):
    # The options below are available as of 2023-05-12
    # See https://docs.databricks.com/delta/tutorial.html for more info.
    type: Literal["delta"] = "delta"

    timestamp_as_of: Optional[str] = Field(None, alias="timestampAsOf")
    version_as_of: Optional[str] = Field(None, alias="versionAsOf")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        return "delta"

    def _get_reader_options_include(self) -> set[str]:
        """The options below are available as of 2023-05-12

        See https://docs.databricks.com/delta/tutorial.html for more info.
        """
        return {"timestamp_as_of", "version_as_of"}


class DirectoryDeltaAsset(_DirectoryDataAssetMixin, DeltaAsset):
    type: Literal["directory_delta"] = "directory_delta"  # type: ignore[assignment]

    @classmethod
    def _get_reader_method(cls) -> str:
        return "delta"

    def _get_reader_options_include(self) -> set[str]:
        """The options below are available as of 2023-05-12

        See https://docs.databricks.com/delta/tutorial.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
        )


# New asset types should be added to the _SPARK_FILE_PATH_ASSET_TYPES tuple,
# and to _SPARK_FILE_PATH_ASSET_TYPES_UNION
# so that the schemas are generated and the assets are registered.
_SPARK_FILE_PATH_ASSET_TYPES = (
    CSVAsset,
    DirectoryCSVAsset,
    ParquetAsset,
    DirectoryParquetAsset,
    ORCAsset,
    DirectoryORCAsset,
    JSONAsset,
    DirectoryJSONAsset,
    TextAsset,
    DirectoryTextAsset,
    DeltaAsset,
    DirectoryDeltaAsset,
)
_SPARK_FILE_PATH_ASSET_TYPES_UNION = Union[
    CSVAsset,
    DirectoryCSVAsset,
    ParquetAsset,
    DirectoryParquetAsset,
    ORCAsset,
    DirectoryORCAsset,
    JSONAsset,
    DirectoryJSONAsset,
    TextAsset,
    DirectoryTextAsset,
    DeltaAsset,
    DirectoryDeltaAsset,
]


class _SparkFilePathDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = _SPARK_FILE_PATH_ASSET_TYPES

    # instance attributes
    assets: List[_SPARK_FILE_PATH_ASSET_TYPES_UNION] = []  # type: ignore[assignment]
