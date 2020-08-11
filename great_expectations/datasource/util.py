import hashlib
import logging
import pickle
from urllib.parse import urlparse

import pandas as pd

logger = logging.getLogger(__name__)

try:
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from great_expectations.datasource.sparkdf_datasource import SparkDFDatasource
except ImportError:
    SparkContext = None
    SparkSession = None
    SparkDFDatasource = None
    # TODO: review logging more detail here
    logger.debug(
        "Unable to load pyspark; install optional spark dependency for support."
    )


# S3Url class courtesy: https://stackoverflow.com/questions/42641315/s3-urls-get-bucket-name-and-path
class S3Url(object):
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

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip("/") + "?" + self._parsed.query
        else:
            return self._parsed.path.lstrip("/")

    @property
    def url(self):
        return self._parsed.geturl()


def hash_pandas_dataframe(df):
    try:
        obj = pd.util.hash_pandas_object(df, index=True).values
    except TypeError:
        # In case of facing unhashable objects (like dict), use pickle
        obj = pickle.dumps(df, pickle.HIGHEST_PROTOCOL)

    return hashlib.md5(obj).hexdigest()


def get_or_create_spark_session(
    name: str = "default_great_expectations_spark_dataframe_datasource",
):
    # Due to the uniqueness of SparkContext per JVM, it is impossible to change SparkSession configuration dynamically.
    # Attempts to circumvent this constraint cause "ValueError: Cannot run multiple SparkContexts at once" to be thrown.
    # Hence, SparkSession with SparkConf acceptable for all tests must be established at "pytest" collection time.
    # This is preferred to calling "return SparkSession.builder.getOrCreate()", which will result in the setting
    # ("spark.app.name", "pyspark-shell") remaining in SparkConf statically for the entire duration of the "pytest" run.
    source: SparkDFDatasource = SparkDFDatasource(
        name=name,
        spark_config={
            "spark.app.name": "great_expectations",
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "450m",
        },
    )
    sess: SparkSession = source.spark
    sc: SparkContext = sess.sparkContext
    # Calling "sc.stop()" after all tests have run is not easy under "pytest".
    # Thus, will make sure that during testing, SparkContext is not stopped.
    # noinspection PyProtectedMember
    sc_stopped: bool = sc._jsc.sc().isStopped()
    if sc_stopped:
        raise ValueError("SparkContext stopped unexpectedly.")

    return sess
