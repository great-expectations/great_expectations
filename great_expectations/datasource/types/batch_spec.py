import logging
from abc import ABCMeta

from great_expectations.core.id_dict import BatchSpec
from great_expectations.exceptions import InvalidBatchIdError, InvalidBatchSpecError

logger = logging.getLogger(__name__)


# TODO: <Alex>This module needs to be cleaned up.
#  We have Batch used for the legacy design, and we also need Batch for the new design.
#  However, right now, the Batch from the legacy design is imported into execution engines of the new design.
#  As a result, we have multiple, inconsistent versions of BatchMarkers, extending legacy/new classes.</Alex>
# TODO: <Alex>See also "great_expectations/core/batch.py".</Alex>
# TODO: <Alex>The following class is part of the new design.</Alex>
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
            raise InvalidBatchSpecError("S3BatchSpec requires an S3 path element")

    @property
    def s3(self):
        return self.get("s3")

    @property
    def reader_method(self):
        return self.get("reader_method")


class RuntimeDataBatchSpec(BatchSpec):
    _id_ignore_keys = set("batch_data")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.batch_data is None:
            raise InvalidBatchSpecError(
                "RuntimeDataBatchSpec batch_data cannot be None"
            )

    @property
    def batch_data(self):
        return self.get("batch_data")

    @batch_data.setter
    def batch_data(self, batch_data):
        self["batch_data"] = batch_data
