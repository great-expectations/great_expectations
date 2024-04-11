import re
from typing import Any, Dict, Optional

from typing_extensions import TypeAlias

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import StrictStr
from great_expectations.core.partitioners import Partitioner
from great_expectations.datasource.data_connector.batch_filter import BatchSlice

BatchParameters: TypeAlias = Dict[StrictStr, Any]

class BatchRequest(pydantic.BaseModel):
    datasource_name: StrictStr
    data_asset_name: StrictStr
    options: BatchParameters
    partitioner: Optional[Partitioner] = None
    batching_regex: Optional[re.Pattern] = None

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: StrictStr,
        data_asset_name: StrictStr,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[Partitioner] = None,
        batching_regex: Optional[re.Pattern] = None,
    ) -> None: ...
    @property
    def batch_slice(self) -> slice: ...

    # MyPy doesn't like setters/getters with different types (for now)
    # Type ignores can be avoided by using BatchRequest.update_batch_slice().
    # https://github.com/python/mypy/issues/3004
    @batch_slice.setter
    def batch_slice(self, value: Optional[BatchSlice]) -> None: ...
    def update_batch_slice(self, value: Optional[BatchSlice] = None) -> None: ...
