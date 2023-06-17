from __future__ import annotations

import copy
import datetime
import decimal
import json
import logging
import os
import pathlib
import re
import sys
import uuid
from collections import OrderedDict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)
from urllib.parse import urlparse

import dateutil.parser
import numpy as np
import pandas as pd
import pydantic
from IPython import get_ipython

from great_expectations import exceptions as gx_exceptions
from great_expectations.compatibility import pyspark, sqlalchemy
from great_expectations.compatibility.sqlalchemy import (
    SQLALCHEMY_NOT_IMPORTED,
    LegacyRow,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.types import SerializableDictDot
from great_expectations.types.base import SerializableDotDict

# Updated from the stack overflow version below to concatenate lists
# https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
from great_expectations.util import convert_decimal_to_float

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.alias_types import JSONValues

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

if TYPE_CHECKING:
    import numpy.typing as npt
    from ruamel.yaml.comments import CommentedMap

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
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def in_databricks() -> bool:
    """
    Tests whether we are in a Databricks environment.

    Returns:
        bool
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


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


ToBool: TypeAlias = bool
ToFloat: TypeAlias = Union[float, np.floating]
ToInt: TypeAlias = Union[int, np.integer]
ToStr: TypeAlias = Union[
    str, bytes, slice, uuid.UUID, datetime.date, datetime.datetime, np.datetime64
]

ToList: TypeAlias = Union[list, set, tuple, "npt.NDArray", pd.Index, pd.Series]
ToDict: TypeAlias = Union[
    dict,
    "CommentedMap",
    pd.DataFrame,
    SerializableDictDot,
    SerializableDotDict,
    pydantic.BaseModel,
]

JSONConvertable: TypeAlias = Union[
    ToDict, ToList, ToStr, ToInt, ToFloat, ToBool, ToBool, None
]


@overload
def convert_to_json_serializable(  # type: ignore[misc] # overlap with `ToList`?
    data: ToDict,
) -> dict:
    ...


@overload
def convert_to_json_serializable(  # type: ignore[misc] # overlap with `ToDict`?
    data: ToList,
) -> list:
    ...


@overload
def convert_to_json_serializable(
    data: ToBool,
) -> bool:
    ...


@overload
def convert_to_json_serializable(
    data: ToFloat,
) -> float:
    ...


@overload
def convert_to_json_serializable(
    data: ToInt,
) -> int:
    ...


@overload
def convert_to_json_serializable(
    data: ToStr,
) -> str:
    ...


@overload
def convert_to_json_serializable(
    data: None,
) -> None:
    ...


@public_api  # - complexity 32
def convert_to_json_serializable(  # noqa: C901, PLR0911, PLR0912
    data: JSONConvertable,
) -> JSONValues:
    """Converts an object to one that is JSON-serializable.

    WARNING, data may be converted in place.

    Args:
        data: an object to convert to a JSON-serializable object

    Returns:
        A JSON-serializable object. For example:

        >>> convert_to_json_serializable(1)
        1

        >>> convert_to_json_serializable("hello")
        "hello"

        >>> convert_to_json_serializable(Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]))
        "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"

    Raises:
        TypeError: A non-JSON-serializable field was found.
    """
    if isinstance(data, pydantic.BaseModel):
        return json.loads(data.json())

    if isinstance(data, (SerializableDictDot, SerializableDotDict)):
        return data.to_json_dict()

    # Handling "float(nan)" separately is required by Python-3.6 and Pandas-0.23 versions.
    if isinstance(data, float) and np.isnan(data):
        return None

    if isinstance(data, (str, int, float, bool)):
        # No problem to encode json
        return data

    if isinstance(data, range):
        return list(data)

    if isinstance(data, dict):
        new_dict = {}
        for key in data:
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
            new_dict[str(key)] = convert_to_json_serializable(data[key])

        return new_dict

    if isinstance(data, (list, tuple, set)):
        new_list: List[JSONValues] = []
        for val in data:
            new_list.append(convert_to_json_serializable(val))

        return new_list

    if isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        return [convert_to_json_serializable(x) for x in data.tolist()]

    if isinstance(data, np.int64):
        return int(data)

    if isinstance(data, np.float64):
        return float(data)

    if isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()

    if isinstance(data, (np.datetime64)):
        return np.datetime_as_string(data)

    if isinstance(data, uuid.UUID):
        return str(data)

    if isinstance(data, bytes):
        return str(data)

    if isinstance(data, slice):
        return str(data)

    if isinstance(data, pathlib.PurePath):
        return str(data)

    # noinspection PyTypeChecker
    if Polygon and isinstance(data, (Point, Polygon, MultiPolygon, LineString)):
        return str(data)

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    if np.issubdtype(type(data), np.bool_):
        return bool(data)

    if np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return int(data)  # type: ignore[arg-type] # could be None

    if np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(data, sys.float_info.dig))  # type: ignore[arg-type] # could be None

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    if data is None:
        # No problem to encode json
        return data

    try:
        if not isinstance(data, list) and pd.isna(data):
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list)`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        return [
            {
                index_name: convert_to_json_serializable(idx),
                value_name: convert_to_json_serializable(val),
            }
            for idx, val in data.items()
        ]

    if isinstance(data, pd.DataFrame):
        return convert_to_json_serializable(data.to_dict(orient="records"))

    if pyspark.DataFrame and isinstance(data, pyspark.DataFrame):  # type: ignore[truthy-function]
        # using StackOverflow suggestion for converting pyspark df into dictionary
        # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
        return convert_to_json_serializable(
            dict(zip(data.schema.names, zip(*data.collect())))
        )

    # SQLAlchemy serialization
    if LegacyRow and isinstance(data, LegacyRow):
        return dict(data)

    # sqlalchemy text for SqlAlchemy 2 compatibility
    if sqlalchemy.TextClause and isinstance(data, sqlalchemy.TextClause):
        return str(data)

    if isinstance(data, decimal.Decimal):
        return convert_decimal_to_float(d=data)

    if isinstance(data, RunIdentifier):
        return data.to_json_dict()

    # PySpark schema serialization
    if pyspark.types and isinstance(data, pyspark.types.StructType):
        return dict(data.jsonValue())

    if sqlalchemy.Connection and isinstance(data, sqlalchemy.Connection):
        # Connection is a module, which is non-serializable. Return module name instead.
        return "sqlalchemy.engine.base.Connection"

    raise TypeError(
        f"{str(data)} is of type {type(data).__name__} which cannot be serialized."
    )


def ensure_json_serializable(data):  # noqa: C901, PLR0911, PLR0912
    """
    Helper function to convert an object to one that is json serializable
    Args:
        data: an object to attempt to convert a corresponding json-serializable object
    Returns:
        (dict) A converted test_object
    Warning:
        test_obj may also be converted in place.
    """

    if isinstance(data, (SerializableDictDot, SerializableDotDict)):
        return

    if isinstance(data, ((str,), (int,), float, bool)):
        # No problem to encode json
        return

    if isinstance(data, dict):
        for key in data:
            str(key)  # key must be cast-able to string
            ensure_json_serializable(data[key])

        return

    if isinstance(data, (list, tuple, set)):
        for val in data:
            ensure_json_serializable(val)
        return

    if isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        _ = [ensure_json_serializable(x) for x in data.tolist()]
        return

    if isinstance(data, (datetime.datetime, datetime.date)):
        return

    if isinstance(data, pathlib.PurePath):
        return

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    if np.issubdtype(type(data), np.bool_):
        return

    if np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return

    if np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    if data is None:
        # No problem to encode json
        return

    try:
        if not isinstance(data, list) and pd.isna(data):
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        _ = [
            {
                index_name: ensure_json_serializable(idx),
                value_name: ensure_json_serializable(val),
            }
            for idx, val in data.items()
        ]
        return

    if pyspark.DataFrame and isinstance(data, pyspark.DataFrame):  # type: ignore[truthy-function]
        # using StackOverflow suggestion for converting pyspark df into dictionary
        # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
        return ensure_json_serializable(
            dict(zip(data.schema.names, zip(*data.collect())))
        )

    if isinstance(data, pd.DataFrame):
        return ensure_json_serializable(data.to_dict(orient="records"))

    if isinstance(data, decimal.Decimal):
        return

    if isinstance(data, RunIdentifier):
        return

    if sqlalchemy.TextClause and isinstance(data, sqlalchemy.TextClause):
        # TextClause is handled manually by convert_to_json_serializable()
        return

    if sqlalchemy.Connection and isinstance(data, sqlalchemy.Connection):
        # Connection module is handled manually by convert_to_json_serializable()
        return

    raise InvalidExpectationConfigurationError(
        f"{str(data)} is of type {type(data).__name__} which cannot be serialized to json"
    )


def substitute_all_strftime_format_strings(
    data: Union[dict, list, str, Any], datetime_obj: Optional[datetime.datetime] = None
) -> Union[str, Any]:
    """
    This utility function will iterate over input data and for all strings, replace any strftime format
    elements using either the provided datetime_obj or the current datetime
    """

    datetime_obj = datetime_obj or datetime.datetime.now()  # noqa: DTZ005
    if isinstance(data, dict) or isinstance(data, OrderedDict):  # noqa: PLR1701
        return {
            k: substitute_all_strftime_format_strings(v, datetime_obj=datetime_obj)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [
            substitute_all_strftime_format_strings(el, datetime_obj=datetime_obj)
            for el in data
        ]
    elif isinstance(data, str):
        return datetime_obj.strftime(data)
    else:
        return data


def parse_string_to_datetime(
    datetime_string: str, datetime_format_string: Optional[str] = None
) -> datetime.datetime:
    if not isinstance(datetime_string, str):
        raise gx_exceptions.SorterError(
            f"""Source "datetime_string" must have string type (actual type is "{str(type(datetime_string))}").
            """
        )

    if not datetime_format_string:
        return dateutil.parser.parse(timestr=datetime_string)

    if datetime_format_string and not isinstance(datetime_format_string, str):
        raise gx_exceptions.SorterError(
            f"""DateTime parsing formatter "datetime_format_string" must have string type (actual type is
"{str(type(datetime_format_string))}").
            """
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
        search = re.search(
            AzureUrl.AZURE_BLOB_STORAGE_PROTOCOL_DETECTION_REGEX_PATTERN, url
        )
        if search is None:
            search = re.search(AzureUrl.AZURE_BLOB_STORAGE_HTTPS_URL_REGEX_PATTERN, url)
            assert (
                search is not None
            ), "The provided URL does not adhere to the format specified by the Azure SDK (<ACCOUNT_NAME>.blob.core.windows.net/<CONTAINER>/<BLOB>)"
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

        raise ValueError("Path should start with either /dbfs or dbfs:")

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

        raise ValueError("Path should start with either /dbfs or dbfs:")


def sniff_s3_compression(s3_url: S3Url) -> Union[str, None]:
    """Attempts to get read_csv compression from s3_url"""
    return _SUFFIX_TO_PD_KWARG.get(s3_url.suffix) if s3_url.suffix else None


# noinspection PyPep8Naming
def get_or_create_spark_application(
    spark_config: Optional[Dict[str, Any]] = None,
    force_reuse_spark_context: bool = True,
) -> pyspark.SparkSession:
    """Obtains configured Spark session if it has already been initialized; otherwise creates Spark session, configures it, and returns it to caller.

    Due to the uniqueness of SparkContext per JVM, it is impossible to change SparkSession configuration dynamically.
    Attempts to circumvent this constraint cause "ValueError: Cannot run multiple SparkContexts at once" to be thrown.
    Hence, SparkSession with SparkConf acceptable for all tests must be established at "pytest" collection time.
    This is preferred to calling "return SparkSession.builder.getOrCreate()", which will result in the setting
    ("spark.app.name", "pyspark-shell") remaining in SparkConf statically for the entire duration of the "pytest" run.

    Args:
        spark_config: Dictionary containing Spark configuration (string-valued keys mapped to string-valued properties).
        force_reuse_spark_context: Boolean flag indicating (if True) that creating new Spark context is forbidden.

    Returns: SparkSession (new or existing as per "isStopped()" status).
    """
    if spark_config is None:
        spark_config = {}
    else:
        spark_config = copy.deepcopy(spark_config)

    name: Optional[str] = spark_config.get("spark.app.name")
    if not name:
        name = "default_great_expectations_spark_application"

    spark_config.update({"spark.app.name": name})

    spark_session: Optional[pyspark.SparkSession] = get_or_create_spark_session(
        spark_config=spark_config
    )
    if spark_session is None:
        raise ValueError("SparkContext could not be started.")

    # noinspection PyUnresolvedReferences
    sc_stopped: bool = spark_session.sparkContext._jsc.sc().isStopped()
    if not force_reuse_spark_context and spark_restart_required(
        current_spark_config=spark_session.sparkContext.getConf().getAll(),
        desired_spark_config=spark_config,
    ):
        if not sc_stopped:
            try:
                # We need to stop the old/default Spark session in order to reconfigure it with the desired options.
                logger.info("Stopping existing spark context to reconfigure.")
                spark_session.sparkContext.stop()
            except AttributeError:
                logger.error(
                    "Unable to load spark context; install optional spark dependency for support."
                )
        spark_session = get_or_create_spark_session(spark_config=spark_config)
        if spark_session is None:
            raise ValueError("SparkContext could not be started.")
        # noinspection PyProtectedMember,PyUnresolvedReferences
        sc_stopped = spark_session.sparkContext._jsc.sc().isStopped()

    if sc_stopped:
        raise ValueError("SparkContext stopped unexpectedly.")

    return spark_session


# noinspection PyPep8Naming
def get_or_create_spark_session(
    spark_config: Optional[Dict[str, str]] = None,
) -> pyspark.SparkSession | None:
    """Obtains Spark session if it already exists; otherwise creates Spark session and returns it to caller.

    Due to the uniqueness of SparkContext per JVM, it is impossible to change SparkSession configuration dynamically.
    Attempts to circumvent this constraint cause "ValueError: Cannot run multiple SparkContexts at once" to be thrown.
    Hence, SparkSession with SparkConf acceptable for all tests must be established at "pytest" collection time.
    This is preferred to calling "return SparkSession.builder.getOrCreate()", which will result in the setting
    ("spark.app.name", "pyspark-shell") remaining in SparkConf statically for the entire duration of the "pytest" run.

    Args:
        spark_config: Dictionary containing Spark configuration (string-valued keys mapped to string-valued properties).

    Returns:

    """
    spark_session: Optional[pyspark.SparkSession]
    try:
        if spark_config is None:
            spark_config = {}
        else:
            spark_config = copy.deepcopy(spark_config)

        builder = pyspark.SparkSession.builder

        app_name: Optional[str] = spark_config.get("spark.app.name")
        if app_name:
            builder.appName(app_name)

        for k, v in spark_config.items():
            if k != "spark.app.name":
                builder.config(k, v)

        spark_session = builder.getOrCreate()
        # noinspection PyProtectedMember,PyUnresolvedReferences
        if spark_session.sparkContext._jsc.sc().isStopped():
            raise ValueError("SparkContext stopped unexpectedly.")

    except AttributeError:
        logger.error(
            "Unable to load spark context; install optional spark dependency for support."
        )
        spark_session = None

    return spark_session


def spark_restart_required(
    current_spark_config: List[Tuple[str, Any]], desired_spark_config: dict
) -> bool:
    """Determines whether or not Spark session should be restarted, based on supplied current and desired configuration.

    Either new "App" name or configuration change necessitates Spark session restart.

    Args:
        current_spark_config: List of tuples containing Spark configuration string-valued key/property pairs.
        desired_spark_config: List of tuples containing Spark configuration string-valued key/property pairs.

    Returns: Boolean flag indicating (if True) that Spark session restart is required.
    """

    # we can't change spark context config values within databricks runtimes
    if in_databricks():
        return False

    current_spark_config_dict: dict = {k: v for (k, v) in current_spark_config}
    if desired_spark_config.get("spark.app.name") != current_spark_config_dict.get(
        "spark.app.name"
    ):
        return True

    if not {(k, v) for k, v in desired_spark_config.items()}.issubset(
        current_spark_config
    ):
        return True

    return False


def get_sql_dialect_floating_point_infinity_value(
    schema: str, negative: bool = False
) -> float:
    res: Optional[dict] = SCHEMAS.get(schema)
    if res is None:
        if negative:
            return -np.inf
        else:
            return np.inf
    else:
        if negative:  # noqa: PLR5501
            return res["NegativeInfinity"]
        else:
            return res["PositiveInfinity"]
