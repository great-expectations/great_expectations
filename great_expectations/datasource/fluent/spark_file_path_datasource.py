from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    List,
    Optional,
    Sequence,
    Type,
    Union,
)

import pydantic
from pydantic import Field
from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.directory_data_asset import (
    _DirectoryDataAssetMixin,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)

if TYPE_CHECKING:
    from great_expectations.compatibility.pyspark import types as pyspark_types
    from great_expectations.datasource.fluent.interfaces import DataAsset


logger = logging.getLogger(__name__)


class _SparkGenericFilePathAssetMixin(_FilePathDataAsset):
    # TODO: ignoreCorruptFiles and ignoreMissingFiles appear in the docs https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html but not in the reader method signatures (e.g. https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604)
    # ignore_corrupt_files: bool = Field(alias="ignoreCorruptFiles")
    # ignore_missing_files: bool = Field(alias="ignoreMissingFiles")

    path_glob_filter: Optional[Union[bool, str]] = Field(None, alias="pathGlobFilter")
    recursive_file_lookup: Optional[Union[bool, str]] = Field(
        None, alias="recursiveFileLookup"
    )
    modified_before: Optional[Union[bool, str]] = Field(None, alias="modifiedBefore")
    modified_after: Optional[Union[bool, str]] = Field(None, alias="modifiedAfter")

    def _get_reader_options_include(self) -> set[str] | None:
        return {
            "ignoreMissingFiles",
            "pathGlobFilter",
            "modifiedBefore",
            "modifiedAfter",
            # vvv Missing from method signatures but appear in documentation:
            # "ignoreCorruptFiles",
            # "recursiveFileLookup",
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
    spark_schema: Optional[Union[pyspark_types.StructType, str]] = Field(
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
        # return cls.type
        return "csv"

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "schema",
                    "sep",
                    "encoding",
                    "quote",
                    "escape",
                    "comment",
                    "header",
                    "inferSchema",
                    "ignoreLeadingWhiteSpace",
                    "ignoreTrailingWhiteSpace",
                    "nullValue",
                    "nanValue",
                    "positiveInf",
                    "negativeInf",
                    "dateFormat",
                    "timestampFormat",
                    "maxColumns",
                    "maxCharsPerColumn",
                    "maxMalformedLogPerPartition",
                    "mode",
                    "columnNameOfCorruptRecord",
                    "multiLine",
                    "charToEscapeQuoteEscaping",
                    "samplingRatio",
                    "enforceSchema",
                    "emptyValue",
                    "locale",
                    "lineSep",
                    "unescapedQuoteHandling",
                    # Inherited vvv
                    # "pathGlobFilter",
                    # "recursiveFileLookup",
                    # "modifiedBefore",
                    # "modifiedAfter",
                    # Inherited ^^^
                    # vvv Docs <> Source Code mismatch
                    # The following parameters are mentioned in https://spark.apache.org/docs/latest/sql-data-sources-csv.html
                    # however do not appear in the source code https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604
                    # "preferDate",
                    # "timestampNTZFormat",
                    # "enableDateTimeParsingFallback",
                    # ^^^ Docs <> Source Code mismatch
                }
            )
        )


class DirectoryCSVAsset(
    _SparkGenericFilePathAssetMixin, _DirectoryDataAssetMixin, CSVAsset
):
    # Overridden inherited instance fields
    type: Literal["directory_csv"] = "directory_csv"

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        # Reader method is still "csv"
        return "csv"

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        return (
            super(_SparkGenericFilePathAssetMixin, self)._get_reader_options_include()
            | super(_DirectoryDataAssetMixin, self)._get_reader_options_include()
            | super(CSVAsset, self)._get_reader_options_include()
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
        # return cls.type
        return "parquet"

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union({"datetimeRebaseMode", "int96RebaseMode", "mergeSchema"})
        )


class ORCAsset(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
    type: Literal["orc"] = "orc"
    merge_schema: Union[bool, None] = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        # return cls.type
        return "orc"

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
        """
        return super()._get_reader_options_include().union({"mergeSchema"})


class JSONAsset(_SparkGenericFilePathAssetMixin):
    # Overridden inherited instance fields
    type: Literal["json"] = "json"

    # vvv spark parameters for pyspark.sql.DataFrameReader.json() (ordered as in pyspark v3.4.0) appear in comment above
    # parameter for reference (from https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309)
    # path: Union[str, List[str], RDD[str]],
    # NA - path determined by asset
    # schema: Optional[Union[StructType, str]] = None,
    # schema shadows pydantic BaseModel attribute
    spark_schema: Optional[Union[pyspark_types.StructType, str]] = Field(
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
        # return cls.type
        return "json"

    def _get_reader_options_include(self) -> set[str] | None:
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "primitivesAsString",
                    "prefersDecimal",
                    "allowComments",
                    "allowUnquotedFieldNames",
                    "allowSingleQuotes",
                    "allowNumericLeadingZero",
                    "allowBackslashEscapingAnyCharacter",
                    "mode",
                    "columnNameOfCorruptRecord",
                    "dateFormat",
                    "timestampFormat",
                    "multiLine",
                    "allowUnquotedControlChars",
                    "lineSep",
                    "samplingRatio",
                    "dropFieldIfAllNull",
                    "encoding",
                    "locale",
                    "allowNonNumericNumbers",
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


class TextAsset(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
    type: Literal["text"] = "text"
    wholetext: bool = Field(False)
    line_sep: str = Field(alias="lineSep")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    @classmethod
    def _get_reader_method(cls) -> str:
        # return cls.type
        return "text"

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
        """
        return super()._get_reader_options_include().union({"wholetext", "lineSep"})


# TODO: Enable using "format" instead of method with reader name:
#  df = spark.read.format("avro").load("examples/src/main/resources/users.avro")
# class AvroAsset(_FilePathDataAsset):
#     # Note: AvroAsset does not support generic options, so it does not inherit from _SparkGenericFilePathAssetMixin
#     # The options below are available as of spark v3.4.0
#     # See https://spark.apache.org/docs/latest/sql-data-sources-avro.html for more info.
#     type: Literal["avro"] = "avro"
#     avro_schema: str = Field(None, alias="avroSchema")
#     ignore_extension: bool = Field(True, alias="ignoreExtension")
#     datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = Field(
#         alias="datetimeRebaseMode"
#     )
#     positional_field_matching: bool = Field(False, alias="positionalFieldMatching")
#
#     class Config:
#         extra = pydantic.Extra.forbid
#         allow_population_by_field_name = True
#
#     @classmethod
#     def _get_reader_method(cls) -> str:
#         # return cls.type
#         return "avro"
#
#     def _get_reader_options_include(self) -> set[str] | None:
#         """These options are available as of spark v3.4.0
#
#         See https://spark.apache.org/docs/latest/sql-data-sources-avro.html for more info.
#         """
#         return {
#             "avroSchema",
#             "ignoreExtension",
#             "datetimeRebaseMode",
#             "positionalFieldMatching",
#         }


# TODO: Enable using "format" instead of method with reader name:
#  spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load("/path/to/data")
# class BinaryFileAsset(_FilePathDataAsset):
#     # Note: BinaryFileAsset does not support generic options, so it does not inherit from _SparkGenericFilePathAssetMixin
#     # The options below are available as of spark v3.4.0
#     # See https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html for more info.
#     type: Literal["binary_file"] = "binary_file"
#     path_glob_filter: str = Field(alias="pathGlobFilter")
#
#     class Config:
#         extra = pydantic.Extra.forbid
#         allow_population_by_field_name = True
#
#     @classmethod
#     def _get_reader_method(cls) -> str:
#         return cls.type
#
#     def _get_reader_options_include(self) -> set[str] | None:
#         """These options are available as of spark v3.4.0
#
#         See https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html for more info.
#         """
#         return {
#             "pathGlobFilter",
#         }


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
    # TODO: Fix AvroAsset
    # AvroAsset,
    # TODO: Fix BinaryFileAsset
    # BinaryFileAsset,
)
_SPARK_FILE_PATH_ASSET_TYPES_UNION = Union[
    CSVAsset,
    DirectoryCSVAsset,
    ParquetAsset,
    ORCAsset,
    JSONAsset,
    TextAsset,
    # TODO: Fix AvroAsset
    # AvroAsset,
    # TODO: Fix BinaryFileAsset
    # BinaryFileAsset,
]
# Directory asset classes should be added to the _SPARK_DIRECTORY_ASSET_CLASSES
# tuple so that the appropriate directory related methods are called.
_SPARK_DIRECTORY_ASSET_CLASSES = (DirectoryCSVAsset,)


class _SparkFilePathDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = _SPARK_FILE_PATH_ASSET_TYPES

    # instance attributes
    assets: List[_SPARK_FILE_PATH_ASSET_TYPES_UNION] = []  # type: ignore[assignment]


def _get_mismatches(
    asset_fields, method_annotations, method_param_names: list[str], verbose=False
):
    """

    Args:
        asset_fields: Fields from the asset e.g. CSVAsset
        method_annotations: Fields from the method e.g. pyspark.sql.DataFrameReader.csv
        method_param_names: Names of the param names of the method e.g. pyspark.sql.DataFrameReader.csv
        verbose: True for more detailed output.

    Returns:
        Nothing, just prints.
    """

    # TODO: Warn instead of bad annotation for literal strings vs str e.g.:
    """
    BAD ANNOTATION: mode
    For field mode asset_field.annotation typing.Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] should == annotation typing.Union[str, NoneType] but doesn't
    BAD ANNOTATION: unescapedQuoteHandling
    For field unescapedQuoteHandling asset_field.annotation typing.Literal['STOP_AT_CLOSING_QUOTE', 'BACK_TO_DELIMITER', 'STOP_AT_DELIMITER', 'SKIP_VALUE', 'RAISE_ERROR'] should == annotation typing.Union[str, NoneType] but doesn't
    """

    def print_red(text: str) -> None:
        print(colored(text, "red"))

    aliases = {af.alias for name, af in asset_fields.items()}
    aliases_to_remove = {"id", "type", "name", "batch_metadata", "batching_regex"}
    aliases_to_check = aliases - aliases_to_remove
    for alias in aliases_to_check:
        if alias not in method_param_names:
            print_red(
                f"FIELD NOT FOUND: param alias: {alias} not found in method param names: {method_param_names}"
            )
    # TODO: Remove non spark fields e..g `id`, `batch_metadata`
    print_green(f"Aliases (for _get_reader_options_include()): {aliases}")
    for param, annotation in method_annotations.items():
        # print(param, annotation)

        asset_field = asset_fields.get(param) or asset_fields.get(camel_to_snake(param))

        if asset_field:
            # if asset_field.type_ == annotation:
            #     print(
            #         f"Success!! For param {param} the asset_field type {asset_field.type_} == method annotation {annotation}"
            #     )
            if asset_field.annotation == annotation:
                if verbose:
                    print(
                        f"Success!! For param {param} the asset_field annotation {asset_field.annotation} == method annotation {annotation}"
                    )
                else:
                    pass

            else:
                # print(
                #     f"For field {param} asset_field.type_ {asset_field.type_} should == annotation {annotation} but doesn't"
                # )
                print_red(f"BAD ANNOTATION: {param}")
                print(
                    f"For field {param} asset_field.annotation {asset_field.annotation} should == annotation {annotation} but doesn't"
                )

            # print(f"asset_field.annotation {asset_field.annotation}")
            # print(f"asset_field.default {asset_field.default}")

        elif param in aliases:
            print(f"param {param} is in aliases")
        else:
            print_red(f"MISSING: {param}")
            print(
                f"asset_field {param} is missing, it exists in the method params. Its annotation is {annotation}. Add using:"
            )
            # max_malformedLogPerPartition: Union[int, str, None] = Field(
            #     None, alias="maxMalformedLogPerPartition"
            # )
            annotation_for_printing = (
                str(annotation).replace("NoneType", "None").replace("typing.", "")
            )
            if param == camel_to_snake(param):
                print(
                    f"{param}: {annotation_for_printing} = insert_default_here_if_it_exists"
                )
            else:
                print(
                    f'{camel_to_snake(param)}: {annotation_for_printing} = Field(insert_default_here_if_it_exists, alias="{param}")'
                )


if __name__ == "__main__":
    verbose = False
    import re

    from termcolor import colored

    p1 = re.compile(r"(.)([A-Z][a-z]+)")
    p2 = re.compile(r"([a-z0-9])([A-Z])")

    def camel_to_snake(name: str) -> str:
        name = p1.sub(r"\1_\2", name)
        return p2.sub(r"\1_\2", name).lower()

    def print_green(text: str) -> None:
        print(colored(text, "green"))

    def print_red(text: str) -> None:
        print(colored(text, "red"))

    import inspect

    from great_expectations.compatibility.pyspark import pyspark

    for asset in _SPARK_FILE_PATH_ASSET_TYPES:
        print_green(f"================= Checking Spark Asset: {asset.__name__}")
        # asset_instance = asset(name=f"instance_of_{asset.__name__}")
        # reader_method_name = asset_instance._get_reader_method()
        reader_method_name = asset._get_reader_method()
        print(f"asset reader method: {reader_method_name}")

        spark_method = getattr(pyspark.sql.DataFrameReader, reader_method_name)
        # csv_def = inspect.getsource(spark_method)

        # type_hints = get_type_hints(spark_method)
        # print(type_hints)
        # print("Building field list from model")
        # print(CSVAsset.__fields__)
        #     asset_fields = CSVAsset.__fields__
        asset_fields = asset.__fields__

        # Example:
        # {'name': ModelField(name='name', type=str, required=True),
        #  'type': ModelField(name='type', type=Literal['csv'], required=False, default='csv'),
        #  'id': ModelField(name='id', type=Optional[UUID], required=False, default=None),
        #  'order_by': ModelField(name='order_by', type=List[Sorter], required=False, default_factory='<function list>'),
        #  'batch_metadata': ModelField(name='batch_metadata', type=Mapping[str, Any], required=False,
        #                               default_factory='<function dict>'),
        #  'batching_regex': ModelField(name='batching_regex', type=Pattern, required=False, default=re.compile('.*')),
        #  'connect_options': ModelField(name='connect_options', type=Mapping[Any, VT_co], required=False,
        #                                default_factory='<function dict>'),
        #  'path_glob_filter': ModelField(name='path_glob_filter', type=Union[bool, str, NoneType], required=False,
        #                                 default=None, alias='pathGlobFilter'),
        #  'recursive_file_lookup': ModelField(name='recursive_file_lookup', type=Union[bool, str, NoneType], required=False,
        #                                      default=None, alias='recursiveFileLookup'),
        #  'modified_before': ModelField(name='modified_before', type=Union[bool, str, NoneType], required=False,
        #                                default=None, alias='modifiedBefore'),
        #  'modified_after': ModelField(name='modified_after', type=Union[bool, str, NoneType], required=False, default=None,
        #                               alias='modifiedAfter'),
        #  'sep': ModelField(name='sep', type=Optional[str], required=False, default=None),
        #  'encoding': ModelField(name='encoding', type=Optional[str], required=False, default=None),
        #  'quote': ModelField(name='quote', type=Optional[str], required=False, default=None),
        #  'escape': ModelField(name='escape', type=Optional[str], required=False, default=None),
        #  'comment': ModelField(name='comment', type=Optional[str], required=False, default=None),
        #  'header': ModelField(name='header', type=Union[bool, str, NoneType], required=False, default=None),
        #  'infer_schema': ModelField(name='infer_schema', type=Union[bool, str, NoneType], required=False, default=None,
        #                             alias='inferSchema'),
        #  'prefer_date': ModelField(name='prefer_date', type=bool, required=False, default=True, alias='preferDate'),
        #  'enforce_schema': ModelField(name='enforce_schema', type=bool, required=False, default=True,
        #                               alias='enforceSchema'),
        #  'ignore_leading_white_space': ModelField(name='ignore_leading_white_space', type=Union[bool, str, NoneType],
        #                                           required=False, default=None, alias='ignoreLeadingWhiteSpace'),
        #  'ignore_trailing_white_space': ModelField(name='ignore_trailing_white_space', type=Union[bool, str, NoneType],
        #                                            required=False, default=None, alias='ignoreTrailingWhiteSpace'),
        #  'null_value': ModelField(name='null_value', type=Optional[str], required=False, default=None, alias='nullValue'),
        #  'nan_value': ModelField(name='nan_value', type=Optional[str], required=False, default=None, alias='nanValue'),
        #  'positive_inf': ModelField(name='positive_inf', type=Optional[str], required=False, default=None,
        #                             alias='positiveInf'),
        #  'negative_inf': ModelField(name='negative_inf', type=Optional[str], required=False, default=None,
        #                             alias='negativeInf'),
        #  'date_format': ModelField(name='date_format', type=Optional[str], required=False, default=None,
        #                            alias='dateFormat'),
        #  'timestamp_format': ModelField(name='timestamp_format', type=Optional[str], required=False, default=None,
        #                                 alias='timestampFormat'),
        #  'timestamp_ntz_format': ModelField(name='timestamp_ntz_format', type=str, required=False,
        #                                     default="yyyy-MM-dd'T'HH:mm:ss[.SSS]", alias='timestampNTZFormat'),
        #  'enable_date_time_parsing_fallback': ModelField(name='enable_date_time_parsing_fallback', type=bool, required=True,
        #                                                  alias='enableDateTimeParsingFallback'),
        #  'max_columns': ModelField(name='max_columns', type=Union[int, str, NoneType], required=False, default=None,
        #                            alias='maxColumns'),
        #  'max_chars_per_column': ModelField(name='max_chars_per_column', type=Union[int, str, NoneType], required=False,
        #                                     default=None, alias='maxCharsPerColumn'),
        #  'mode': ModelField(name='mode', type=Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'], required=False,
        #                     default='PERMISSIVE'),
        #  'column_name_of_corrupt_record': ModelField(name='column_name_of_corrupt_record', type=Optional[str],
        #                                              required=False, default=None, alias='columnNameOfCorruptRecord'),
        #  'multi_line': ModelField(name='multi_line', type=Union[bool, str, NoneType], required=False, default=None,
        #                           alias='multiLine'),
        #  'char_to_escape_quote_escaping': ModelField(name='char_to_escape_quote_escaping', type=Optional[str],
        #                                              required=False, default=None, alias='charToEscapeQuoteEscaping'),
        #  'sampling_ratio': ModelField(name='sampling_ratio', type=Union[str, float, NoneType], required=False, default=None,
        #                               alias='samplingRatio'),
        #  'empty_value': ModelField(name='empty_value', type=Optional[str], required=False, default=None,
        #                            alias='emptyValue'),
        #  'locale': ModelField(name='locale', type=Optional[str], required=False, default=None),
        #  'line_sep': ModelField(name='line_sep', type=Optional[str], required=False, default=None, alias='lineSep'),
        #  'unescaped_quote_handling': ModelField(name='unescaped_quote_handling', type=Literal[
        #      'STOP_AT_CLOSING_QUOTE', 'BACK_TO_DELIMITER', 'STOP_AT_DELIMITER', 'SKIP_VALUE', 'RAISE_ERROR'],
        #                                         required=False, default='STOP_AT_DELIMITER',
        #                                         alias='unescapedQuoteHandling')}
        # print("Building field list from spark function def")
        method_param_names = [
            n for n in inspect.signature(spark_method).parameters.keys()
        ]
        method_annotations = spark_method.__annotations__
        if method_annotations.get("return"):
            method_annotations.pop("return")
        # Example:
        # {'path': typing.Union[str, typing.List[str]], 'schema': typing.Union[pyspark.sql.types.StructType, str, NoneType],
        #  'sep': typing.Union[str, NoneType], 'encoding': typing.Union[str, NoneType], 'quote': typing.Union[str, NoneType],
        #  'escape': typing.Union[str, NoneType], 'comment': typing.Union[str, NoneType],
        #  'header': typing.Union[bool, str, NoneType], 'inferSchema': typing.Union[bool, str, NoneType],
        #  'ignoreLeadingWhiteSpace': typing.Union[bool, str, NoneType],
        #  'ignoreTrailingWhiteSpace': typing.Union[bool, str, NoneType], 'nullValue': typing.Union[str, NoneType],
        #  'nanValue': typing.Union[str, NoneType], 'positiveInf': typing.Union[str, NoneType],
        #  'negativeInf': typing.Union[str, NoneType], 'dateFormat': typing.Union[str, NoneType],
        #  'timestampFormat': typing.Union[str, NoneType], 'maxColumns': typing.Union[int, str, NoneType],
        #  'maxCharsPerColumn': typing.Union[int, str, NoneType],
        #  'maxMalformedLogPerPartition': typing.Union[int, str, NoneType], 'mode': typing.Union[str, NoneType],
        #  'columnNameOfCorruptRecord': typing.Union[str, NoneType], 'multiLine': typing.Union[bool, str, NoneType],
        #  'charToEscapeQuoteEscaping': typing.Union[str, NoneType], 'samplingRatio': typing.Union[str, float, NoneType],
        #  'enforceSchema': typing.Union[bool, str, NoneType], 'emptyValue': typing.Union[str, NoneType],
        #  'locale': typing.Union[str, NoneType], 'lineSep': typing.Union[str, NoneType],
        #  'pathGlobFilter': typing.Union[bool, str, NoneType], 'recursiveFileLookup': typing.Union[bool, str, NoneType],
        #  'modifiedBefore': typing.Union[bool, str, NoneType], 'modifiedAfter': typing.Union[bool, str, NoneType],
        #  'unescapedQuoteHandling': typing.Union[str, NoneType], 'return': 'DataFrame'}

        _get_mismatches(
            asset_fields=asset_fields,
            method_param_names=method_param_names,
            method_annotations=method_annotations,
            verbose=verbose,
        )

    # aliases = {af.alias for name, af in asset_fields.items()}
    # for param, annotation in method_annotations.items():
    #     # print(param, annotation)
    #     asset_field = asset_fields.get(param)
    #     if not asset_field:
    #         # Try the snake version
    #         if verbose:
    #             print(
    #                 f"trying snake version for param {param} (snake version: {camel_to_snake(param)})"
    #             )
    #         asset_field = asset_fields.get(camel_to_snake(param))
    #         if asset_field:
    #             if verbose:
    #                 print(
    #                     f"needed snake version for param: {param} for asset_field: {asset_field}"
    #                 )
    #
    #     if asset_field:
    #         # if asset_field.type_ == annotation:
    #         #     print(
    #         #         f"Success!! For param {param} the asset_field type {asset_field.type_} == method annotation {annotation}"
    #         #     )
    #         if asset_field.annotation == annotation:
    #             if verbose:
    #                 print(
    #                     f"Success!! For param {param} the asset_field annotation {asset_field.annotation} == method annotation {annotation}"
    #                 )
    #             else:
    #                 pass
    #
    #         else:
    #             # print(
    #             #     f"For field {param} asset_field.type_ {asset_field.type_} should == annotation {annotation} but doesn't"
    #             # )
    #             print(
    #                 f"For field {param} asset_field.annotation {asset_field.annotation} should == annotation {annotation} but doesn't"
    #             )
    #         # print(f"asset_field.annotation {asset_field.annotation}")
    #         # print(f"asset_field.default {asset_field.default}")
    #
    #     elif param in aliases:
    #         print(f"param {param} is in aliases")
    #     else:
    #         print(
    #             f"asset_field {param} is missing, it exists in the method params. Its annotation is {annotation}"
    #         )

    # breakpoint()
    # root = ast.parse(csv_def)
    # print(root)
    # for node in ast.walk(root):
    #     if isinstance(node, ast.arguments):
    #         for arg in node.args:
    #             print(arg.arg)
    #             print(arg.annotation)
    #             print(arg.type_comment)
    #             print("end")

# TODO: Generate pyi content from pydantic model that includes default
