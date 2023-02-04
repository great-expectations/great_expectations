from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.core.batch import BatchData

if TYPE_CHECKING:
    import pandas as pd


class PandasBatchData(BatchData):
    def __init__(self, execution_engine, dataframe: pd.DataFrame) -> None:
        super().__init__(execution_engine=execution_engine)
        self._dataframe = dataframe

    @property
    def dataframe(self):
        return self._dataframe
