from great_expectations.execution_engine import ExecutionEngine


class FakeSqlAlchemyExecutionEngine(ExecutionEngine):
    def __init__(
        self,
        name=None,
        caching=True,
        batch_spec_defaults=None,
        batch_data_dict=None,
        validator=None,
        connection_str=None,
    ) -> None:
        print(f"{self.__class__.__name__} - __init__")
        super().__init__(name, caching, batch_spec_defaults, batch_data_dict, validator)

    def get_batch_data_and_markers(self, batch_spec):
        return super().get_batch_data_and_markers(batch_spec)
