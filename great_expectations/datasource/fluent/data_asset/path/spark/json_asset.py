from __future__ import annotations

from typing import Literal, Optional, Union

from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_asset.path.directory_asset import (
    DirectoryDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.file_asset import FileDataAsset
from great_expectations.datasource.fluent.data_asset.path.spark.spark_generic import (
    _SparkGenericFilePathAssetMixin,
)
from great_expectations.datasource.fluent.serializable_types.pyspark import (
    SerializableStructType,  # noqa: TCH001  # pydantic uses type at runtime
)


class JSONAssetBase(_SparkGenericFilePathAssetMixin):
    # vvv spark parameters for pyspark.sql.DataFrameReader.json() (ordered as in pyspark v3.4.0) appear in comment above  # noqa: E501
    # parameter for reference (from https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L309)
    # path: Union[str, List[str], RDD[str]],
    # NA - path determined by asset
    # schema: Optional[Union[StructType, str]] = None,
    # schema shadows pydantic BaseModel attribute
    spark_schema: Optional[Union[SerializableStructType, str]] = Field(None, alias="schema")
    # primitivesAsString: Optional[Union[bool, str]] = None,
    primitives_as_string: Optional[Union[bool, str]] = Field(None, alias="primitivesAsString")
    # prefersDecimal: Optional[Union[bool, str]] = None,
    prefers_decimal: Optional[Union[bool, str]] = Field(None, alias="prefersDecimal")
    # allowComments: Optional[Union[bool, str]] = None,
    allow_comments: Optional[Union[bool, str]] = Field(None, alias="allowComments")
    # allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
    allow_unquoted_field_names: Optional[Union[bool, str]] = Field(
        None, alias="allowUnquotedFieldNames"
    )
    # allowSingleQuotes: Optional[Union[bool, str]] = None,
    allow_single_quotes: Optional[Union[bool, str]] = Field(None, alias="allowSingleQuotes")
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
    column_name_of_corrupt_record: Optional[str] = Field(None, alias="columnNameOfCorruptRecord")
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
    drop_field_if_all_null: Optional[Union[bool, str]] = Field(None, alias="dropFieldIfAllNull")
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
        extra = "forbid"

        allow_population_by_field_name = True

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "json"

    @override
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


class JSONAsset(FileDataAsset, JSONAssetBase):
    type: Literal["json"] = "json"


class DirectoryJSONAsset(DirectoryDataAsset, JSONAssetBase):
    type: Literal["directory_json"] = "directory_json"

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "json"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-json.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(DirectoryDataAsset, self)._get_reader_options_include()
        )
