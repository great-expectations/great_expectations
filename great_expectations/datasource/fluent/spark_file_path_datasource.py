from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING, ClassVar, List, Sequence, Type, Union

import pydantic
from pydantic import Field
from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import DataAsset


logger = logging.getLogger(__name__)


class _SparkGenericFilePathAsset(_FilePathDataAsset):
    ignore_corrupt_files: bool = Field(alias="ignoreCorruptFiles")
    ignore_missing_files: bool = Field(alias="ignoreMissingFiles")
    path_glob_filter: str = Field(alias="pathGlobFilter")
    recursive_file_lookup: bool = Field(alias="recursiveFileLookup")
    modified_before: str = Field(alias="modifiedBefore")
    modified_after: str = Field(alias="modifiedAfter")

    def _get_reader_options_include(self) -> set[str] | None:
        return {
            "ignoreCorruptFiles",
            "ignoreMissingFiles",
            "pathGlobFilter",
            "recursiveFileLookup",
            "modifiedBefore",
            "modifiedAfter",
        }


class CSVAsset(_SparkGenericFilePathAsset):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
    type: Literal["csv"] = "csv"
    delimiter: str
    sep: str
    encoding: str = Field("UTF-8")
    quote: str = Field('"')
    escape: str
    comment: str
    header: bool = False
    infer_schema: bool = Field(False, alias="inferSchema")
    prefer_date: bool = Field(True, alias="preferDate")
    enforce_schema: bool = Field(True, alias="enforceSchema")
    ignore_leading_white_space: bool = Field(False, alias="ignoreLeadingWhiteSpace")
    ignore_trailing_white_space: bool = Field(False, alias="ignoreTrailingWhiteSpace")
    null_value: str = Field(alias="nullValue")
    nan_value: str = Field(alias="nanValue")
    positive_inf: str = Field(alias="positiveInf")
    negative_inf: str = Field(alias="negativeInf")
    date_format: str = Field("yyyy-MM-dd", alias="dateFormat")
    timestamp_format: str = Field(
        "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]", alias="timestampFormat"
    )
    timestamp_ntz_format: str = Field(
        "yyyy-MM-dd'T'HH:mm:ss[.SSS]", alias="timestampNTZFormat"
    )
    enable_date_time_parsing_fallback: bool = Field(
        alias="enableDateTimeParsingFallback"
    )
    max_columns: int = Field(20480, alias="maxColumns")
    max_chars_per_column: int = Field(-1, alias="maxCharsPerColumn")
    mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] = Field("PERMISSIVE")
    column_name_of_corrupt_record: str = Field(alias="columnNameOfCorruptRecord")
    multi_line: bool = Field(False, alias="multiLine")
    char_to_escape_quote_escaping: str = Field(alias="charToEscapeQuoteEscaping")
    sampling_ratio: float = Field(1.0, alias="samplingRatio")
    empty_value: str = Field(alias="emptyValue")
    locale: str
    lineSep: str = Field(alias="lineSep")
    unescaped_quote_handling: Literal[
        "STOP_AT_CLOSING_QUOTE",
        "BACK_TO_DELIMITER",
        "STOP_AT_DELIMITER",
        "SKIP_VALUE",
        "RAISE_ERROR",
    ] = Field("STOP_AT_DELIMITER", alias="unescapedQuoteHandling")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "delimiter",
                    "sep",
                    "encoding",
                    "quote",
                    "escape",
                    "comment",
                    "header",
                    "inferSchema",
                    "preferDate",
                    "enforceSchema",
                    "ignoreLeadingWhiteSpace",
                    "ignoreTrailingWhiteSpace",
                    "nullValue",
                    "nanValue",
                    "positiveInf",
                    "negativeInf",
                    "dateFormat",
                    "timestampFormat",
                    "timestampNTZFormat",
                    "enableDateTimeParsingFallback",
                    "maxColumns",
                    "maxCharsPerColumn",
                    "mode",
                    "columnNameOfCorruptRecord",
                    "multiLine",
                    "charToEscapeQuoteEscaping",
                    "samplingRatio",
                    "emptyValue",
                    "locale",
                    "lineSep",
                    "unescapedQuoteHandling",
                }
            )
        )


class DirectoryCSVAsset(CSVAsset):
    # Overridden inherited instance fields
    type: Literal["directory_csv"] = "directory_csv"
    data_directory: pathlib.Path

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        # Reader method is still "csv"
        return self.type.replace("directory_", "")

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "data_directory",
                }
            )
        )


class ParquetAsset(_SparkGenericFilePathAsset):
    type: Literal["parquet"] = "parquet"
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
    datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = Field(
        alias="datetimeRebaseMode"
    )
    int_96_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = Field(
        alias="int96RebaseMode"
    )
    merge_schema: bool = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union({"datetimeRebaseMode", "int96RebaseMode", "mergeSchema"})
        )


class ORCAsset(_SparkGenericFilePathAsset):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
    type: Literal["orc"] = "orc"
    merge_schema: bool = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
        """
        return super()._get_reader_options_include().union({"mergeSchema"})


class JSONAsset(_SparkGenericFilePathAsset):
    # Overridden inherited instance fields
    type: Literal["json"] = "json"
    timezone: str = Field(alias="timeZone")
    primitives_as_string: bool = Field(False, alias="primitivesAsString")
    prefers_decimal: bool = Field(False, alias="prefersDecimal")
    allow_comments: bool = Field(False, alias="allowComments")

    allow_unquoted_field_names: bool = Field(False, alias="allowUnquotedFieldNames")
    allow_single_quotes: bool = Field(True, alias="allowSingleQuotes")
    allow_numeric_leading_zeros: bool = Field(False, alias="allowNumericLeadingZeros")
    allow_backslash_escaping_any_character: bool = Field(
        False, alias="allowBackslashEscapingAnyCharacter"
    )
    mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] = Field("PERMISSIVE")
    column_name_of_corrupt_record: str = Field(alias="columnNameOfCorruptRecord")
    date_format: str = Field("yyyy-MM-dd", alias="dateFormat")
    timestamp_format: str = Field(
        "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]", alias="timestampFormat"
    )
    timestamp_ntz_format: str = Field(
        "yyyy-MM-dd'T'HH:mm:ss[.SSS]", alias="timestampNTZFormat"
    )
    enable_date_time_parsing_fallback: bool = Field(
        alias="enableDateTimeParsingFallback"
    )
    multi_line: bool = Field(False, alias="multiLine")
    allow_unquoted_control_chars: bool = Field(False, alias="allowUnquotedControlChars")
    encoding: str
    line_sep: str = Field(alias="lineSep")
    sampling_ratio: float = Field(1.0, alias="samplingRatio")
    drop_field_if_all_null: bool = Field(False, alias="dropFieldIfAllNull")
    locale: str
    allow_non_numeric_numbers: bool = Field(True, alias="allowNonNumericNumbers")
    merge_schema: bool = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "timeZone",
                    "primitivesAsString",
                    "prefersDecimal",
                    "allowComments",
                    "allowUnquotedFieldNames",
                    "allowSingleQuotes",
                    "allowNumericLeadingZeros",
                    "allowBackslashEscapingAnyCharacter",
                    "mode",
                    "columnNameOfCorruptRecord",
                    "dateFormat",
                    "timestampFormat",
                    "timestampNTZFormat",
                    "enableDateTimeParsingFallback",
                    "multiLine",
                    "allowUnquotedControlChars",
                    "encoding",
                    "lineSep",
                    "samplingRatio",
                    "dropFieldIfAllNull",
                    "locale",
                    "allowNonNumericNumbers",
                    "mergeSchema",
                }
            )
        )


class TextAsset(_SparkGenericFilePathAsset):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
    type: Literal["text"] = "text"
    wholetext: bool = Field(False)
    line_sep: str = Field(alias="lineSep")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
        """
        return super()._get_reader_options_include().union({"wholetext", "lineSep"})


class AvroAsset(_FilePathDataAsset):
    # Note: AvroAsset does not support generic options, so it does not inherit from _SparkGenericFilePathAsset
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-avro.html for more info.
    type: Literal["avro"] = "avro"
    avro_schema: str = Field(None, alias="avroSchema")
    ignore_extension: bool = Field(True, alias="ignoreExtension")
    datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = Field(
        alias="datetimeRebaseMode"
    )
    positional_field_matching: bool = Field(False, alias="positionalFieldMatching")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-avro.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "avroSchema",
                    "ignoreExtension",
                    "datetimeRebaseMode",
                    "positionalFieldMatching",
                }
            )
        )


# New asset types should be added to the _SPARK_FILE_PATH_ASSET_TYPES tuple,
# and to _SPARK_FILE_PATH_ASSET_TYPES_UNION
# so that the schemas are generated and the assets are registered.
_SPARK_FILE_PATH_ASSET_TYPES = (
    CSVAsset,
    DirectoryCSVAsset,
    ParquetAsset,
    ORCAsset,
    JSONAsset,
    TextAsset,
    AvroAsset,
)
_SPARK_FILE_PATH_ASSET_TYPES_UNION = Union[
    CSVAsset, DirectoryCSVAsset, ParquetAsset, ORCAsset, JSONAsset, TextAsset, AvroAsset
]
# Directory asset classes should be added to the _SPARK_DIRECTORY_ASSET_CLASSES
# tuple so that the appropriate directory related methods are called.
_SPARK_DIRECTORY_ASSET_CLASSES = (DirectoryCSVAsset,)


class _SparkFilePathDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = _SPARK_FILE_PATH_ASSET_TYPES

    # instance attributes
    assets: List[_SPARK_FILE_PATH_ASSET_TYPES_UNION] = []  # type: ignore[assignment]
