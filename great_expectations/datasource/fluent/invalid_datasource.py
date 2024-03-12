from __future__ import annotations

import warnings
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Final,
    List,
    NoReturn,
    Type,
    Union,
    overload,
)

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent import (
    DataAsset,
    Datasource,
    GxDatasourceWarning,
    TestConnectionError,
)
from great_expectations.datasource.fluent.type_lookup import TypeLookup, ValidTypes

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.datasource.fluent.interfaces import (
        Batch,
        BatchRequestOptions,
        BatchSlice,
    )


# Controls which methods should raise an error when called on an InvalidDatasource
METHOD_SHOULD_RAISE_ERROR: Final[set] = {
    "get_batch_list_from_batch_request",
    "add_batch_config",
}


class GxInvalidDatasourceWarning(GxDatasourceWarning):
    """
    A warning that the Datasource configuration is invalid and will must be updated before it can used.
    """


class InvalidAsset(DataAsset):
    """
    A DataAsset that is invalid.
    The DataAsset itself may be valid, but it is classified as invalid because its parent Datasource or sibling assets are invalid.
    """

    type: str = "invalid"
    name: str = "invalid"

    class Config:
        extra = "ignore"

    def _raise_type_error(self) -> NoReturn:
        """
        Raise a TypeError indicating that the Asset is invalid.
        If available, raise from the original config error that caused the Datasource to be invalid.
        """
        error = TypeError(f"{self.name} Asset is invalid")
        if datasource := getattr(self, "datasource", None):
            raise error from datasource.config_error
        raise error

    @override
    def test_connection(self) -> None:
        if datasource := getattr(self, "datasource", None):
            raise TestConnectionError(
                f"The Datasource configuration for {self.name} is invalid and cannot be used. Please fix the error and try again"
            ) from datasource.config_error
        # the asset should always have a datasource, but if it doesn't, we should still raise an error
        raise TestConnectionError(
            "This Asset configuration is invalid and cannot be used. Please fix the error and try again"
        )

    @property
    @override
    def batch_request_options(self) -> tuple[str, ...]:
        self._raise_type_error()

    @override
    def build_batch_request(
        self,
        options: BatchRequestOptions | None = None,
        batch_slice: BatchSlice | None = None,
    ) -> BatchRequest:
        self._raise_type_error()

    @override
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        self._raise_type_error()


class InvalidAssetTypeLookup(TypeLookup):
    """A TypeLookup that always returns InvalidAsset for any type."""

    @overload
    def __getitem__(self, key: str) -> Type:
        ...

    @overload
    def __getitem__(self, key: Type) -> str:
        ...

    @override
    def __getitem__(self, key: ValidTypes) -> ValidTypes:
        if isinstance(key, str):
            return InvalidAsset
        # if a type is passed, normally we would return the type name but that doesn't make sense here
        # for an InvalidAsset
        raise NotImplementedError(
            f"Looking up the `type` name for {InvalidAsset.__name__} is not supported"
        )


class InvalidDatasource(Datasource):
    """
    A Datasource that is invalid.

    This is used to represent a Datasource that is invalid and cannot be used.

    This class should override all methods that would commonly be called when a user intends to use the Datasource.
    The overridden methods should indicate to the user that the Datasource configuration is invalid and provide details about
    why it was considered invalid.

    Any errors raised should raise `from self.config_error`.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [InvalidAsset]
    _type_lookup: ClassVar[TypeLookup] = InvalidAssetTypeLookup()

    type: str = "invalid"
    config_error: Union[pydantic.ValidationError, LookupError] = Field(
        ..., description="The error that caused the Datasource to be invalid."
    )
    assets: List[InvalidAsset] = []

    class Config:
        extra = "ignore"
        arbitrary_types_allowed = True
        json_encoders = {
            pydantic.ValidationError: lambda v: v.errors(),
            LookupError: lambda v: repr(v),
        }

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        raise TestConnectionError(
            "This Datasource configuration is invalid and cannot be used. Please fix the error and try again"
        ) from self.config_error

    @override
    def get_asset(self, asset_name: str) -> InvalidAsset:
        """
        Always raise a warning and return an InvalidAsset.
        Don't raise an error because the users may want to inspect the asset config.
        """
        warnings.warn(
            f"The {self.name} Datasource configuration is invalid and cannot be used. Please fix the error and try again",
            GxInvalidDatasourceWarning,
        )
        return super().get_asset(asset_name)

    def _raise_type_error(self, *args, **kwargs) -> NoReturn:
        """
        Raise a TypeError indicating that the Datasource is invalid.
        Raise from the original config error that caused the Datasource to be invalid.
        """
        error = TypeError(
            f"{self.name} Datasource is configuration is invalid and cannot be used. Please fix the error and try again"
        )
        raise error from self.config_error

    @override
    def __getattribute__(self, attr: str):
        """
        Dynamically raise a TypeError with details of the original config error for
        any methods and attributes that do not make sense for an InvalidDatasource.
        """
        if attr in METHOD_SHOULD_RAISE_ERROR:
            raise AttributeError  # this causes __getattr__ to be called
        return super().__getattribute__(attr)

    def __getattr__(self, attr: str):
        # __getattr__ is only called if the attribute is not found by __getattribute__
        if attr in ("add_dataframe_asset", "__deepcopy__"):
            # these methods are part of protocol checks and should return None
            return None
        return self._raise_type_error()
