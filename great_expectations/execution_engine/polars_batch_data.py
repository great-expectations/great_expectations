try:
    from polars import DataFrame as pl_DataFrame

except ImportError:
    pl_DataFrame = None

from great_expectations.core.batch import BatchData


class PolarsBatchData(BatchData):
    def __init__(self, execution_engine, dataframe: pl_DataFrame) -> None:
        super().__init__(execution_engine=execution_engine)
        self._dataframe = dataframe

    @property
    def dataframe(self):
        return self._dataframe
