from great_expectations.types import DictDot


class Batch(DictDot):
    def __init__(
        self,
        datasource_name,
        batch_kwargs,
        data,
        batch_parameters,
        batch_markers,
        data_context,
    ):
        self._datasource_name = datasource_name
        self._batch_kwargs = batch_kwargs
        self._data = data
        self._batch_parameters = batch_parameters
        self._batch_markers = batch_markers
        self._data_context = data_context

    @property
    def datasource_name(self):
        return self._datasource_name

    @property
    def batch_kwargs(self):
        return self._batch_kwargs

    @property
    def data(self):
        return self._data

    @property
    def batch_parameters(self):
        return self._batch_parameters

    @property
    def batch_markers(self):
        return self._batch_markers

    @property
    def data_context(self):
        return self._data_context
