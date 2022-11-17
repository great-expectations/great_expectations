from great_expectations.core.batch import BatchData


class SparkDFBatchData(BatchData):
    def __init__(self, execution_engine, dataframe) -> None:
        super().__init__(execution_engine=execution_engine)
        self._dataframe = dataframe

    @property
    def dataframe(self):
        return self._dataframe
