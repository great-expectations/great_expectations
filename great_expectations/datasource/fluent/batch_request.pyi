from typing import Any, Dict, Generic, Optional

from typing_extensions import TypeAlias

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import StrictStr
from great_expectations.core.batch_definition import PartitionerT
from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice

BatchParameters: TypeAlias = Dict[StrictStr, Any]

class BatchRequest(pydantic.GenericModel, Generic[PartitionerT]):
    datasource_name: StrictStr
    data_asset_name: StrictStr
    options: BatchParameters
    partitioner: Optional[PartitionerT] = None

    def __init__(
        self,
        datasource_name: StrictStr,
        data_asset_name: StrictStr,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[PartitionerT] = None,
    ) -> None: ...
    @property
    def batch_slice(self) -> slice: ...

    # MyPy doesn't like setters/getters with different types (for now)
    # Type ignores can be avoided by using BatchRequest.update_batch_slice().
    # https://github.com/python/mypy/issues/3004
    @batch_slice.setter
    def batch_slice(self, value: Optional[BatchSlice]) -> None: ...
    def update_batch_slice(self, value: Optional[BatchSlice] = None) -> None: ...
