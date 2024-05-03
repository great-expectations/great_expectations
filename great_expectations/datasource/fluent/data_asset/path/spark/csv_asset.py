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


class CSVAssetBase(_SparkGenericFilePathAssetMixin):
    # vvv spark parameters for pyspark.sql.DataFrameReader.csv() (ordered as in pyspark v3.4.0) appear in comment above  # noqa: E501
    # parameter for reference (from https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604)
    # See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
    # path: PathOrPaths,
    # NA - path determined by asset
    # schema: Optional[Union[StructType, str]] = None,
    # schema shadows pydantic BaseModel attribute
    spark_schema: Optional[Union[SerializableStructType, str]] = Field(None, alias="schema")
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
    max_chars_per_column: Optional[Union[int, str]] = Field(None, alias="maxCharsPerColumn")
    # maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
    max_malformed_log_per_partition: Optional[Union[int, str]] = Field(
        None, alias="maxMalformedLogPerPartition"
    )
    # mode: Optional[str] = None,
    mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None
    # columnNameOfCorruptRecord: Optional[str] = None,
    column_name_of_corrupt_record: Optional[str] = Field(None, alias="columnNameOfCorruptRecord")
    # multiLine: Optional[Union[bool, str]] = None,
    multi_line: Optional[Union[bool, str]] = Field(None, alias="multiLine")
    # charToEscapeQuoteEscaping: Optional[str] = None,
    char_to_escape_quote_escaping: Optional[str] = Field(None, alias="charToEscapeQuoteEscaping")
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
        extra = "forbid"
        allow_population_by_field_name = True

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "csv"

    @override
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


class CSVAsset(FileDataAsset, CSVAssetBase):
    type: Literal["csv"] = "csv"


class DirectoryCSVAsset(DirectoryDataAsset, CSVAssetBase):
    type: Literal["directory_csv"] = "directory_csv"

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "csv"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-csv.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(DirectoryDataAsset, self)._get_reader_options_include()
        )
