import logging
from abc import ABCMeta

from great_expectations.core.id_dict import BatchSpec
from great_expectations.exceptions import (InvalidBatchIdError,
                                           InvalidBatchSpecError)

logger = logging.getLogger(__name__)


class BatchMarkers(BatchSpec):
    """A BatchMarkers is a special type of BatchSpec (so that it has a batch_fingerprint) but it generally does
    NOT require specific keys and instead captures information about the OUTPUT of a datasource's fetch
    process, such as the timestamp at which a query was executed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "ge_load_time" not in self:
            raise InvalidBatchIdError("BatchMarkers requires a ge_load_time")

    @property
    def ge_load_time(self):
        return self.get("ge_load_time")


class PandasDatasourceBatchSpec(BatchSpec, metaclass=ABCMeta):
    """This is an abstract class and should not be instantiated. It's relevant for testing whether
    a subclass is allowed
    """

    pass


class SparkDFDatasourceBatchSpec(BatchSpec, metaclass=ABCMeta):
    """This is an abstract class and should not be instantiated. It's relevant for testing whether
    a subclass is allowed
    """

    pass


class SqlAlchemyDatasourceBatchSpec(BatchSpec, metaclass=ABCMeta):
    """This is an abstract class and should not be instantiated. It's relevant for testing whether
    a subclass is allowed
    """

    @property
    def limit(self):
        return self.get("limit")

    @property
    def schema(self):
        return self.get("schema")


class PathBatchSpec(PandasDatasourceBatchSpec, SparkDFDatasourceBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "path" not in self:
            raise InvalidBatchSpecError("PathBatchSpec requires a path element")

    @property
    def path(self):
        return self.get("path")

    @property
    def reader_method(self):
        return self.get("reader_method")


class S3BatchSpec(PandasDatasourceBatchSpec, SparkDFDatasourceBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "s3" not in self:
            raise InvalidBatchSpecError("S3BatchSpec requires a path element")

    @property
    def s3(self):
        return self.get("s3")

    @property
    def reader_method(self):
        return self.get("reader_method")


class InMemoryBatchSpec(PandasDatasourceBatchSpec, SparkDFDatasourceBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "dataset" not in self:
            raise InvalidBatchSpecError(
                "InMemoryBatchSpec requires a 'dataset' element"
            )

    @property
    def dataset(self):
        return self.get("dataset")


class PandasDatasourceInMemoryBatchSpec(InMemoryBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import pandas as pd

        if not isinstance(self["dataset"], pd.DataFrame):
            raise InvalidBatchSpecError(
                "PandasDatasourceInMemoryBatchSpec 'dataset' must be a pandas DataFrame"
            )


class SparkDFDatasourceInMemoryBatchSpec(InMemoryBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            import pyspark
        except ImportError:
            raise InvalidBatchSpecError(
                "SparkDFDatasourceInMemoryBatchSpec requires a valid pyspark installation, but pyspark import failed."
            )
        if not isinstance(self["dataset"], pyspark.sql.DataFrame):
            raise InvalidBatchSpecError(
                "SparkDFDatasourceInMemoryBatchSpec 'dataset' must be a spark DataFrame"
            )


class SqlAlchemyDatasourceTableBatchSpec(SqlAlchemyDatasourceBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "table" not in self:
            raise InvalidBatchSpecError(
                "SqlAlchemyDatasourceTableBatchSpec requires a 'table' element"
            )

    @property
    def table(self):
        return self.get("table")


class SqlAlchemyDatasourceQueryBatchSpec(SqlAlchemyDatasourceBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "query" not in self:
            raise InvalidBatchSpecError(
                "SqlAlchemyDatasourceQueryBatchSpec requires a 'query' element"
            )

    @property
    def query(self):
        return self.get("query")

    @property
    def query_parameters(self):
        return self.get("query_parameters")


class SparkDFDatasourceQueryBatchSpec(SparkDFDatasourceBatchSpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "query" not in self:
            raise InvalidBatchSpecError(
                "SparkDFDatasourceQueryBatchSpec requires a 'query' element"
            )

    @property
    def query(self):
        return self.get("query")
