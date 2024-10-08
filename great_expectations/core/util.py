from __future__ import annotations

import datetime
import logging
import re
import warnings
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, Mapping, MutableMapping, Optional, TypeVar, Union
from urllib.parse import urlparse

import dateutil.parser
import numpy as np

from great_expectations import exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy import SQLALCHEMY_NOT_IMPORTED, LegacyRow, Row

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark


logger = logging.getLogger(__name__)

try:
    from shapely.geometry import LineString, MultiPolygon, Point, Polygon
except ImportError:
    Point = None
    Polygon = None
    MultiPolygon = None
    LineString = None


if not LegacyRow:
    LegacyRow = SQLALCHEMY_NOT_IMPORTED

if not Row:  # type: ignore[truthy-function]
    Row = SQLALCHEMY_NOT_IMPORTED  # type: ignore[misc]

SCHEMAS = {
    "api_np": {
        "NegativeInfinity": -np.inf,
        "PositiveInfinity": np.inf,
    },
    "api_cast": {
        "NegativeInfinity": -float("inf"),
        "PositiveInfinity": float("inf"),
    },
    "mysql": {
        "NegativeInfinity": -1.79e308,
        "PositiveInfinity": 1.79e308,
    },
    "mssql": {
        "NegativeInfinity": -1.79e308,
        "PositiveInfinity": 1.79e308,
    },
}


_SUFFIX_TO_PD_KWARG = {"gz": "gzip", "zip": "zip", "bz2": "bz2", "xz": "xz"}

M = TypeVar("M", bound=MutableMapping)


def nested_update(
    d: M,
    u: Mapping,
    dedup: bool = False,
    concat_lists: bool = True,
) -> M:
    """
    Update d with items from u, recursively and joining elements. By default, list values are
    concatenated without de-duplication. If concat_lists is set to False, lists in u (new dict)
    will replace those in d (base dict).
    """
    for k, v in u.items():
        if isinstance(v, Mapping):
            d[k] = nested_update(d.get(k, {}), v, dedup=dedup)
        elif isinstance(v, set) or (k in d and isinstance(d[k], set)):
            s1 = d.get(k, set())
            s2 = v or set()

            if concat_lists:
                d[k] = s1 | s2
            else:
                d[k] = s2
        elif isinstance(v, list) or (k in d and isinstance(d[k], list)):
            l1 = d.get(k, [])
            l2 = v or []
            if concat_lists:
                if dedup:
                    d[k] = list(set(l1 + l2))
                else:
                    d[k] = l1 + l2
            else:
                d[k] = l2
        else:
            d[k] = v
    return d


def in_jupyter_notebook():
    try:
        from IPython import get_ipython  # type: ignore[import-not-found]

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except (NameError, ImportError):
        return False  # Probably standard Python interpreter


def determine_progress_bar_method_by_environment() -> Callable:
    """
    As tqdm has specific methods for progress bar creation and iteration,
    we require a utility to determine which method to use.

    If in a Jupyter notebook, we want to use `tqdm.notebook.tqdm`. Otherwise,
    we default to the standard `tqdm.tqdm`. Please see the docs for more information: https://tqdm.github.io/

    Returns:
        The appropriate tqdm method for the environment in question.
    """
    from tqdm import tqdm
    from tqdm.notebook import tqdm as tqdm_notebook

    if in_jupyter_notebook():
        return tqdm_notebook
    return tqdm


def substitute_all_strftime_format_strings(
    data: Union[dict, list, str, Any], datetime_obj: Optional[datetime.datetime] = None
) -> Union[str, Any]:
    """
    This utility function will iterate over input data and for all strings, replace any strftime format
    elements using either the provided datetime_obj or the current datetime
    """  # noqa: E501

    datetime_obj = datetime_obj or datetime.datetime.now()  # noqa: DTZ005
    if isinstance(data, (dict, OrderedDict)):
        return {
            k: substitute_all_strftime_format_strings(v, datetime_obj=datetime_obj)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [
            substitute_all_strftime_format_strings(el, datetime_obj=datetime_obj) for el in data
        ]
    elif isinstance(data, str):
        return datetime_obj.strftime(data)
    else:
        return data


def parse_string_to_datetime(
    datetime_string: str, datetime_format_string: Optional[str] = None
) -> datetime.datetime:
    if not isinstance(datetime_string, str):
        raise gx_exceptions.SorterError(  # noqa: TRY003
            f"""Source "datetime_string" must have string type (actual type is "{type(datetime_string)!s}").
            """  # noqa: E501
        )

    if not datetime_format_string:
        return dateutil.parser.parse(timestr=datetime_string)

    if datetime_format_string and not isinstance(datetime_format_string, str):
        raise gx_exceptions.SorterError(  # noqa: TRY003
            f"""DateTime parsing formatter "datetime_format_string" must have string type (actual type is
"{type(datetime_format_string)!s}").
            """  # noqa: E501
        )

    return datetime.datetime.strptime(  # noqa: DTZ007
        datetime_string, datetime_format_string
    )


def datetime_to_int(dt: datetime.date) -> int:
    return int(dt.strftime("%Y%m%d%H%M%S"))


# noinspection SpellCheckingInspection
class AzureUrl:
    """
    Parses an Azure Blob Storage URL into its separate components.
    Formats:
        WASBS (for Spark): "wasbs://<CONTAINER>@<ACCOUNT_NAME>.blob.core.windows.net/<BLOB>"
        HTTP(S) (for Pandas) "<ACCOUNT_NAME>.blob.core.windows.net/<CONTAINER>/<BLOB>"

        Reference: WASBS -- Windows Azure Storage Blob (https://datacadamia.com/azure/wasb).
    """

    AZURE_BLOB_STORAGE_PROTOCOL_DETECTION_REGEX_PATTERN: str = (
        r"^[^@]+@.+\.blob\.core\.windows\.net\/.+$"
    )

    AZURE_BLOB_STORAGE_HTTPS_URL_REGEX_PATTERN: str = (
        r"^(https?:\/\/)?(.+?)\.blob\.core\.windows\.net/([^/]+)/(.+)$"
    )
    AZURE_BLOB_STORAGE_HTTPS_URL_TEMPLATE: str = (
        "{account_name}.blob.core.windows.net/{container}/{path}"
    )

    AZURE_BLOB_STORAGE_WASBS_URL_REGEX_PATTERN: str = (
        r"^(wasbs?:\/\/)?([^/]+)@(.+?)\.blob\.core\.windows\.net/(.+)$"
    )
    AZURE_BLOB_STORAGE_WASBS_URL_TEMPLATE: str = (
        "wasbs://{container}@{account_name}.blob.core.windows.net/{path}"
    )

    def __init__(self, url: str) -> None:
        search = re.search(AzureUrl.AZURE_BLOB_STORAGE_PROTOCOL_DETECTION_REGEX_PATTERN, url)
        if search is None:
            search = re.search(AzureUrl.AZURE_BLOB_STORAGE_HTTPS_URL_REGEX_PATTERN, url)
            assert (
                search is not None
            ), "The provided URL does not adhere to the format specified by the Azure SDK (<ACCOUNT_NAME>.blob.core.windows.net/<CONTAINER>/<BLOB>)"  # noqa: E501
            self._protocol = search.group(1)
            self._account_name = search.group(2)
            self._container = search.group(3)
            self._blob = search.group(4)
        else:
            search = re.search(AzureUrl.AZURE_BLOB_STORAGE_WASBS_URL_REGEX_PATTERN, url)
            assert (
                search is not None
            ), "The provided URL does not adhere to the format specified by the Azure SDK (wasbs://<CONTAINER>@<ACCOUNT_NAME>.blob.core.windows.net/<BLOB>)"
            self._protocol = search.group(1)
            self._container = search.group(2)
            self._account_name = search.group(3)
            self._blob = search.group(4)

    @property
    def protocol(self):
        return self._protocol

    @property
    def account_name(self):
        return self._account_name

    @property
    def account_url(self):
        return f"{self.account_name}.blob.core.windows.net"

    @property
    def container(self):
        return self._container

    @property
    def blob(self):
        return self._blob


class GCSUrl:
    """
    Parses a Google Cloud Storage URL into its separate components
    Format: gs://<BUCKET_OR_NAME>/<BLOB>
    """

    URL_REGEX_PATTERN: str = r"^gs://([^/]+)/(.+)$"

    OBJECT_URL_TEMPLATE: str = "gs://{bucket_or_name}/{path}"

    def __init__(self, url: str) -> None:
        search = re.search(GCSUrl.URL_REGEX_PATTERN, url)
        assert (
            search is not None
        ), "The provided URL does not adhere to the format specified by the GCS SDK (gs://<BUCKET_OR_NAME>/<BLOB>)"
        self._bucket = search.group(1)
        self._blob = search.group(2)

    @property
    def bucket(self):
        return self._bucket

    @property
    def blob(self):
        return self._blob


# S3Url class courtesy: https://stackoverflow.com/questions/42641315/s3-urls-get-bucket-name-and-path
class S3Url:
    OBJECT_URL_TEMPLATE: str = "s3a://{bucket}/{path}"

    """
    >>> s = S3Url("s3://bucket/hello/world")
    >>> s.bucket
    'bucket'
    >>> s.key
    'hello/world'
    >>> s.url
    's3://bucket/hello/world'

    >>> s = S3Url("s3://bucket/hello/world?qwe1=3#ddd")
    >>> s.bucket
    'bucket'
    >>> s.key
    'hello/world?qwe1=3#ddd'
    >>> s.url
    's3://bucket/hello/world?qwe1=3#ddd'

    >>> s = S3Url("s3://bucket/hello/world#foo?bar=2")
    >>> s.key
    'hello/world#foo?bar=2'
    >>> s.url
    's3://bucket/hello/world#foo?bar=2'
    """

    def __init__(self, url) -> None:
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return f"{self._parsed.path.lstrip('/')}?{self._parsed.query}"
        else:
            return self._parsed.path.lstrip("/")

    @property
    def suffix(self) -> Optional[str]:
        """
        Attempts to get a file suffix from the S3 key.
        If can't find one returns `None`.
        """
        splits = self._parsed.path.rsplit(".", 1)
        _suffix = splits[-1]
        if len(_suffix) > 0 and len(splits) > 1:
            return str(_suffix)
        return None

    @property
    def url(self):
        return self._parsed.geturl()


class DBFSPath:
    """
    Methods for converting Databricks Filesystem (DBFS) paths
    """

    @staticmethod
    def convert_to_file_semantics_version(path: str) -> str:
        if re.search(r"^dbfs:", path):
            return path.replace("dbfs:", "/dbfs", 1)

        if re.search("^/dbfs", path):
            return path

        raise ValueError("Path should start with either /dbfs or dbfs:")  # noqa: TRY003

    @staticmethod
    def convert_to_protocol_version(path: str) -> str:
        if re.search(r"^\/dbfs", path):
            candidate = path.replace("/dbfs", "dbfs:", 1)
            if candidate == "dbfs:":
                # Must add trailing slash
                return "dbfs:/"

            return candidate

        if re.search(r"^dbfs:", path):
            if path == "dbfs:":
                # Must add trailing slash
                return "dbfs:/"

            return path

        raise ValueError("Path should start with either /dbfs or dbfs:")  # noqa: TRY003


def sniff_s3_compression(s3_url: S3Url) -> Union[str, None]:
    """Attempts to get read_csv compression from s3_url"""
    return _SUFFIX_TO_PD_KWARG.get(s3_url.suffix) if s3_url.suffix else None


def get_or_create_spark_application(
    spark_config: Optional[dict[str, str]] = None,
    force_reuse_spark_context: Optional[bool] = None,
) -> pyspark.SparkSession:
    from great_expectations.execution_engine import SparkDFExecutionEngine

    # deprecated-v1.0.0
    warnings.warn(
        "Utility method get_or_create_spark_application() is deprecated and will be removed in v1.0.0. "  # noqa: E501
        "Please pass your spark_config to the relevant Spark Datasource, or create your Spark Session outside of GX.",  # noqa: E501
        category=DeprecationWarning,
    )
    if force_reuse_spark_context is not None:
        # deprecated-v1.0.0
        warnings.warn(
            "force_reuse_spark_context is deprecated and will be removed in version 1.0. "
            "In environments that allow it, the existing Spark context will be reused, adding the "
            "spark_config options that have been passed. If the Spark context cannot be updated with "  # noqa: E501
            "the spark_config, the context will be stopped and restarted with the new spark_config.",  # noqa: E501
            category=DeprecationWarning,
        )
    return SparkDFExecutionEngine.get_or_create_spark_session(
        spark_config=spark_config  # type:ignore[arg-type]
    )


def get_or_create_spark_session(
    spark_config: Optional[dict[str, str]] = None,
) -> pyspark.SparkSession:
    """Obtains Spark session if it already exists; otherwise creates Spark session and returns it to caller.

    Args:
        spark_config: Dictionary containing Spark configuration (string-valued keys mapped to string-valued properties).

    Returns:
        SparkSession
    """  # noqa: E501
    from great_expectations.execution_engine import SparkDFExecutionEngine

    # deprecated-v1.0.0
    warnings.warn(
        "Utility method get_or_create_spark_session() is deprecated and will be removed in v1.0.0. "
        "Please pass your spark_config to the relevant Spark Datasource, or create your Spark Session outside of GX.",  # noqa: E501
        category=DeprecationWarning,
    )

    return SparkDFExecutionEngine.get_or_create_spark_session(
        spark_config=spark_config or {},  # type: ignore[arg-type]
    )


def get_sql_dialect_floating_point_infinity_value(schema: str, negative: bool = False) -> float:
    res: Optional[dict] = SCHEMAS.get(schema)
    if res is None:
        if negative:
            return -np.inf
        else:
            return np.inf
    else:  # noqa: PLR5501
        if negative:
            return res["NegativeInfinity"]
        else:
            return res["PositiveInfinity"]
