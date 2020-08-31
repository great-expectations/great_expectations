from great_expectations.types import DictDot


class Batch(DictDot):
    def __init__(
        self,
        data,
        batch_markers,
        data_context,
        batch_parameters=None,
        batch_definition=None,
        batch_kwargs=None,
        batch_spec=None,
        datasource_name=None,
        execution_environment_name=None
    ):
        self._datasource_name = datasource_name
        self._batch_kwargs = batch_kwargs
        self._batch_spec = batch_spec
        self._data = data
        self._batch_parameters = batch_parameters
        self._batch_definition = batch_definition
        self._batch_markers = batch_markers
        self._data_context = data_context
        self._execution_environment_name = execution_environment_name

    @property
    def datasource_name(self):
        return self._datasource_name

    @property
    def execution_environment_name(self):
        return self._execution_environment_name

    @property
    def batch_kwargs(self):
        return self._batch_kwargs

    @property
    def batch_spec(self):
        return self._batch_spec

    @property
    def batch_definition(self):
        return self._batch_definition

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
