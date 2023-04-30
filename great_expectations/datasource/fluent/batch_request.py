from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Union,
)

import pydantic
from pydantic import StrictStr
from typing_extensions import TypeAlias

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector.batch_filter import (
    BatchSlice,
    parse_batch_slice,
)

if TYPE_CHECKING:
    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]


# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants as returned. The keys represent dimensions one can filter the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter, and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
BatchRequestOptions: TypeAlias = Dict[StrictStr, Any]


@public_api
class BatchRequest(pydantic.BaseModel):
    """A BatchRequest is the way to specify which data Great Expectations will validate.

    A Batch Request is provided to a Data Asset in order to create one or more Batches.

    Args:
        datasource_name: The name of the Datasource used to connect to the data.
        data_asset_name: The name of the Data Asset used to connect to the data.
        options: A dict that can be used to filter the batch groups associated with the Data Asset.
            The dict structure depends on the asset type. The available keys for dict can be obtained by
            calling DataAsset.batch_request_options.
        batch_slice: A python slice that can be used to filter the sorted batches by index.
            e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.

    Returns:
        BatchRequest
    """

    datasource_name: StrictStr = pydantic.Field(
        ...,
        allow_mutation=False,
        description="The name of the Datasource used to connect to the data.",
    )
    data_asset_name: StrictStr = pydantic.Field(
        ...,
        allow_mutation=False,
        description="The name of the Data Asset used to connect to the data.",
    )
    options: BatchRequestOptions = pydantic.Field(
        default_factory=dict,
        allow_mutation=True,
        description=(
            "A map that can be used to filter the batch groups associated with the Data Asset. "
            "The structure and types depends on the asset type."
        ),
    )
    batch_slice_input: Optional[BatchSlice] = pydantic.Field(
        default=None,
        allow_mutation=True,
        alias="batch_slice",
    )

    @property
    def batch_slice(self) -> slice:
        """A built-in slice that can be used to filter a list of batches by index."""
        return parse_batch_slice(batch_slice=self.batch_slice_input)

    class Config:
        # ignore extra params, otherwise pycharm will not recognize "batch_slice" alias and show an error
        extra = pydantic.Extra.ignore
        validate_assignment = True

    @pydantic.validator("options", pre=True)
    def _validate_options(cls, options) -> BatchRequestOptions:
        if not isinstance(options, dict):
            raise TypeError("BatchRequestOptions must take the form of a dictionary.")
        if any(not isinstance(key, str) for key in options):
            raise TypeError("BatchRequestOptions keys must all be strings.")
        return options

    @pydantic.validator("batch_slice_input")
    def _validate_batch_slice_input(cls, batch_slice_input) -> Optional[BatchSlice]:
        try:
            parse_batch_slice(batch_slice=batch_slice_input)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to parse BatchSlice to slice: {e}")
        return batch_slice_input

    @public_api
    def json(
        self,
        *,
        include: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        exclude: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        # Default to True to serialize the batch_slice_input with the name batch_slice
        by_alias: bool = True,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        """
        Generate a json representation of the BatchRequest, optionally specifying which
        fields to include or exclude.

        Deviates from pydantic
          - `by_alias` `True` by default instead of `False` by default.
        """
        return super().json(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
            **dumps_kwargs,
        )

    @public_api
    def dict(
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        # Default to True to serialize the batch_slice_input with the name batch_slice
        by_alias: bool = True,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        # deprecated - use exclude_unset instead
        skip_defaults: bool | None = None,
    ) -> dict[str, Any]:
        """
        Generate a dictionary representation of the BatchRequest, optionally specifying which
        fields to include or exclude.

        Deviates from pydantic
          - `by_alias` `True` by default instead of `False` by default.
        """
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            skip_defaults=skip_defaults,
        )
