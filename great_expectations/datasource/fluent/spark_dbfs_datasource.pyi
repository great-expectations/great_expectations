import re
from logging import Logger
from typing import Literal, Optional, Union

from great_expectations.compatibility.pyspark import (
    types as pyspark_types,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core._docs_decorators import public_api as public_api
from great_expectations.datasource.fluent import SparkFilesystemDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DBFSDataConnector as DBFSDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    BatchMetadata,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition as SortersDefinition,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError as TestConnectionError,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)

logger: Logger

class SparkDBFSDatasource(SparkFilesystemDatasource):
    type: Literal["spark_dbfs"]  # type: ignore[assignment]

    @override
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
