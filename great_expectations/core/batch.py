import datetime

from great_expectations.core.id_dict import BatchKwargs, BatchSpec
from great_expectations.exceptions import InvalidBatchIdError
from great_expectations.types import DictDot


class Batch(DictDot):
    def __init__(
        self,
        data,
        batch_spec=None,
        batch_markers=None,
        # The remaining parameters are for backward compatibility.
        data_context=None,
        datasource_name=None,
        batch_parameters=None,
        batch_kwargs=None,
    ):
        self._data = data
        self._batch_spec = batch_spec or BatchSpec()

        if not batch_markers:
            batch_markers = BatchMarkers(
                {
                    "ge_load_time": datetime.datetime.now(
                        datetime.timezone.utc
                    ).strftime("%Y%m%dT%H%M%S.%fZ")
                }
            )
        self._batch_markers = batch_markers

        self._data_context = data_context
        self._datasource_name = datasource_name
        self._batch_parameters = batch_parameters
        self._batch_kwargs = batch_kwargs or BatchKwargs()

    @property
    def data(self):
        return self._data

    @property
    def batch_spec(self):
        return self._batch_spec

    @property
    def batch_markers(self):
        return self._batch_markers

    @property
    def datasource_name(self):
        return self._datasource_name

    @property
    def data_context(self):
        return self._data_context

    @property
    def batch_parameters(self):
        return self._batch_parameters

    @property
    def batch_kwargs(self):
        return self._batch_kwargs


class BatchMarkers(BatchKwargs):
    """A BatchMarkers is a special type of BatchKwargs (so that it has a batch_fingerprint) but it generally does
    NOT require specific keys and instead captures information about the OUTPUT of a datasource's fetch
    process, such as the timestamp at which a query was executed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "ge_load_time" not in self:
            raise InvalidBatchIdError("BatchMarkers requires a ge_load_time")

    @property
    def ge_load_time(self):
        return self.get("ge_load_time")
