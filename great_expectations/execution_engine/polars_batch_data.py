import polars as pl

from great_expectations.execution_engine.execution_engine import BatchData


class PolarsBatchData(BatchData):
    def __init__(self, execution_engine, dataframe: pl.DataFrame) -> None:
        super().__init__(execution_engine=execution_engine)
        self._dataframe = dataframe

    @property
    def dataframe(self):
        return self._dataframe
