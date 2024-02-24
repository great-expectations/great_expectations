from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, List, NoReturn, Type, overload

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
    from great_expectations.datasource.fluent.interfaces import Batch


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

    @override
    def add_batch_config(self, name: str, partitioner: Any | None = None) -> NoReturn:
        self._raise_type_error()

    @override
    def add_sorters(self, sorters: List[Any]) -> NoReturn:
        self._raise_type_error()

    @override
    def build_batch_request(
        self,
        options: dict | None = None,
        batch_slice: Any = None,
        partitioner: Any = None,
    ) -> NoReturn:
        self._raise_type_error()

    @override
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> NoReturn:
        self._raise_type_error()

    @override
    def sort_batches(self, batch_list: List[Batch]) -> None:
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
    config_error: pydantic.ValidationError = Field(
        ..., description="The error that caused the Datasource to be invalid."
    )
    assets: List[InvalidAsset] = []

    class Config:
        extra = "ignore"
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
