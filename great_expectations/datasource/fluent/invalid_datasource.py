from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, ClassVar, List, NoReturn, Type, overload

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
    from great_expectations.core.batch_config import BatchConfig
    from great_expectations.datasource.fluent.batch_request import BatchRequest


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
        extra = "allow"
        arbitrary_types_allowed = True


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
        return "invalid"


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
    config_error: pydantic.ValidationError = Field(
        ..., description="The error that caused the Datasource to be invalid."
    )
    assets: List[InvalidAsset] = []

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True
        json_encoders = {
            pydantic.ValidationError: lambda v: v.errors(),
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

    @override
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> NoReturn:
        raise TypeError(f"{self.name} Datasource is invalid") from self.config_error

    @override
    def add_batch_config(self, batch_config: BatchConfig) -> NoReturn:
        raise TypeError(f"{self.name} Datasource is invalid") from self.config_error
