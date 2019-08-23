from hashlib import md5
import datetime

import pandas as pd
from six import string_types
from great_expectations.types import RequiredKeysDotDict, ClassConfig

try:
    import pyspark
except ImportError:
    pyspark = None


class BatchKwargs(RequiredKeysDotDict):
    """BatchKwargs represent uniquely identifying information for a Batch of data.

    BatchKwargs are generated by BatchGenerators and are interpreted by datasources.

    Note that by default, the partition_id *will* be included both in the partition_id portion and in the batch_kwargs
    hash portion of the batch_id.

    """
    _required_keys = set()
    # FIXME: Need discussion about whether we want to explicitly ignore, explicitly include, or just use whatever
    # is present

    _partition_id_key = "partition_id"
    _batch_id_ignored_keys = {
        _partition_id_key,
        "data_asset_type"
    }
    _key_types = {
        "data_asset_type": ClassConfig
    }

    _partition_id_delimiter = "::"

    @property
    def batch_id(self):
        partition_id_key = self.get(self._partition_id_key, None)
        # We do not allow a "None" partition_id, even if it's explicitly present as such in batch_kwargs
        if partition_id_key is None:
            partition_id_key = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
        id_keys = set(self.keys()) - set(self._batch_id_ignored_keys)
        if len(id_keys) == 1:
            key = list(id_keys)[0]
            hash_ = key + ":" + self[key]
        else:
            hash_dict = {k: self[k] for k in set(self.keys()) - set(self._batch_id_ignored_keys)}
            hash_ = md5(str(sorted(hash_dict.items())).encode("utf-8")).hexdigest()

        return partition_id_key + self._partition_id_delimiter + hash_


class PandasDatasourceBatchKwargs(BatchKwargs):
    """This is an abstract class and should not be instantiated. It's relevant for testing whether
    a subclass is allowed
    """
    pass


class PandasDatasourcePathBatchKwargs(PandasDatasourceBatchKwargs):
    """PandasPathBatchKwargs represents kwargs suitable for reading a file from a given path."""
    _required_keys = {
        "path"
    }
    # NOTE: JPC - 20190821: Eventually, we will probably want to have some logic that decides to use, say,
    # an md5 hash of a file instead of a path to decide when it's the same.
    _batch_id_ignored_keys = {
    }
    _key_types = {
        "path": string_types,
        "reader_method": string_types
    }


class PandasDatasourceMemoryBatchKwargs(PandasDatasourceBatchKwargs):
    _required_keys = {
        "df"
    }
    _key_types = {
        "path": pd.DataFrame
    }


class SqlAlchemyDatasourceBatchKwargs(BatchKwargs):
    pass


class SqlAlchemyDatasourceTableBatchKwargs(SqlAlchemyDatasourceBatchKwargs):
    _required_keys = {
        "query"
        "timestamp"
    }
    _key_types = {
        "query": string_types,
        "timestamp": float
    }


class SqlAlchemyDatasourceQueryBatchKwargs(SqlAlchemyDatasourceBatchKwargs):
    _required_keys = {
        "table"
        "timestamp"
    }
    _key_types = {
        "table": string_types,
        "timestamp": float
    }


class SparkDFDatasourceBatchKwargs(BatchKwargs):
    pass


class SparkDFDatasourceMemoryBatchKwargs(SparkDFDatasourceBatchKwargs):
    _required_keys = {
        "df"
    }
    _key_types = {
        "df": pyspark.sql.DataFrame,
    }


class SparkDFDatasourceQueryBatchKwargs(SparkDFDatasourceBatchKwargs):
    _required_keys = {
        "query",
        "timestamp"
    }
    _key_types = {
        "query": string_types,
        "timestamp": float
    }


class SparkDFDatasourcePathBatchKwargs(SparkDFDatasourceBatchKwargs):
    _required_keys = {
        "path",
    }
    # NOTE: JPC - 20190821: Eventually, we will probably want to have some logic that decides to use, say,
    # an md5 hash of a file instead of a path to decide when it's the same.
    _key_types = {
        "path": string_types,
    }
