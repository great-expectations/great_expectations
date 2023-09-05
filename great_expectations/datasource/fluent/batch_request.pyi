from typing import Any, Dict, Optional

from typing_extensions import TypeAlias

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import StrictStr
from great_expectations.datasource.data_connector.batch_filter import BatchSlice

BatchRequestOptions: TypeAlias = Dict[StrictStr, Any]

class BatchRequest(pydantic.BaseModel):
    datasource_name: StrictStr
    data_asset_name: StrictStr
    options: BatchRequestOptions

    def __init__(
        self,
        datasource_name: StrictStr,
        data_asset_name: StrictStr,
        options: Optional[BatchRequestOptions] = None,
        batch_slice: Optional[BatchSlice] = None,
    ) -> None: ...
    @property
    def batch_slice(self) -> slice: ...

    # MyPy doesn't like setters/getters with different types (for now)
    # Type ignores can be avoided by using BatchRequest.update_batch_slice().
    # https://github.com/python/mypy/issues/3004
    @batch_slice.setter
    def batch_slice(self, value: Optional[BatchSlice]) -> None: ...
    def update_batch_slice(self, value: Optional[BatchSlice] = None) -> None: ...
