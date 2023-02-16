from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    NamedTuple,
    Type,
    Union,
)

from great_expectations.experimental.datasources.signatures import _merge_signatures
from great_expectations.experimental.datasources.type_lookup import TypeLookup

if TYPE_CHECKING:
    import pydantic

    from great_expectations.data_context import AbstractDataContext as GXDataContext
    from great_expectations.experimental.context import DataContext
    from great_expectations.experimental.datasources.interfaces import (
        DataAsset,
        Datasource,
    )

SourceFactoryFn = Callable[..., "Datasource"]

logger = logging.getLogger(__name__)


class TypeRegistrationError(TypeError):
    pass


class _FieldDetails(NamedTuple):
    default_value: Any
    type_annotation: Type


def _get_field_details(
    model: Type[pydantic.BaseModel], field_name: str
) -> _FieldDetails:
    """Get the default value of the requested field and its type annotation."""
    return _FieldDetails(
        default_value=model.__fields__[field_name].default,
        type_annotation=model.__fields__[field_name].type_,
    )


class _SourceFactories:
    """
    Contains a collection of datasource factory methods in the format `.add_<TYPE_NAME>()`

    Contains a `.type_lookup` dict-like two way mapping between previously registered `Datasource`
    or `DataAsset` types and a simplified name for those types.
    """

    # TODO (kilo59): split DataAsset & Datasource lookups
    type_lookup: ClassVar = TypeLookup()
    __source_factories: ClassVar[Dict[str, SourceFactoryFn]] = {}

    _data_context: Union[DataContext, GXDataContext]

    def __init__(self, data_context: Union[DataContext, GXDataContext]):
        self._data_context = data_context

    @classmethod
    def register_types_and_ds_factory(
        cls,
        ds_type: Type[Datasource],
        factory_fn: SourceFactoryFn,
    ) -> None:
        """
        Add/Register a datasource factory function and all related `Datasource`,
        `DataAsset` and `ExecutionEngine` types.

        Creates mapping table between the `DataSource`/`DataAsset` classes and their
        declared `type` string.


        Example
        -------

        An `.add_pandas_filesystem()` pandas_filesystem factory method will be added to `context.sources`.

        >>> class PandasFilesystemDatasource(_PandasDatasource):
        >>>     type: str = 'pandas_filesystem'
        >>>     asset_types = [FileAsset]
        >>>     execution_engine: PandasExecutionEngine
        """

        # TODO: check that the name is a valid python identifier (and maybe that it is snake_case?)
        ds_type_name = _get_field_details(ds_type, "type").default_value
        if not ds_type_name:
            raise TypeRegistrationError(
                f"`{ds_type.__name__}` is missing a `type` attribute with an assigned string value"
            )

        # rollback type registrations if exception occurs
        with cls.type_lookup.transaction() as type_lookup:

            # TODO: We should namespace the asset type to the datasource so different datasources can reuse asset types.
            cls._register_assets(ds_type, asset_type_lookup=type_lookup)

            cls._register_datasource_and_factory_method(
                ds_type,
                factory_fn=factory_fn,
                ds_type_name=ds_type_name,
                datasource_type_lookup=type_lookup,
            )

    @classmethod
    def _register_datasource_and_factory_method(
        cls,
        ds_type: Type[Datasource],
        factory_fn: SourceFactoryFn,
        ds_type_name: str,
        datasource_type_lookup: TypeLookup,
    ) -> str:
        """
        Register the `Datasource` class and add a factory method for the class on `sources`.
        The method name is pulled from the `Datasource.type` attribute.
        """
        method_name = f"add_{ds_type_name}"
        logger.debug(
            f"2a. Registering {ds_type.__name__} as {ds_type_name} with {method_name}() factory"
        )

        pre_existing = cls.__source_factories.get(method_name)
        if pre_existing:
            raise TypeRegistrationError(
                f"'{ds_type_name}' - `sources.{method_name}()` factory already exists",
            )

        datasource_type_lookup[ds_type] = ds_type_name
        logger.debug(f"'{ds_type_name}' added to `type_lookup`")
        cls.__source_factories[method_name] = factory_fn
        return ds_type_name

    @classmethod
    def _register_assets(cls, ds_type: Type[Datasource], asset_type_lookup: TypeLookup):

        asset_types: List[Type[DataAsset]] = ds_type.asset_types

        if not asset_types:
            logger.warning(
                f"No `{ds_type.__name__}.asset_types` have be declared for the `Datasource`"
            )

        for t in asset_types:
            if t.__name__.startswith("_"):
                logger.debug(
                    f"{t} is private, assuming not intended as a public concrete type. Skipping registration"
                )
                continue
            try:
                asset_type_name = _get_field_details(t, "type").default_value
                if asset_type_name is None:
                    raise TypeError(
                        f"{t.__name__} `type` field must be assigned and cannot be `None`"
                    )
                logger.debug(
                    f"2b. Registering `DataAsset` `{t.__name__}` as {asset_type_name}"
                )
                asset_type_lookup[t] = asset_type_name
            except (AttributeError, KeyError, TypeError) as bad_field_exc:
                raise TypeRegistrationError(
                    f"No `type` field found for `{ds_type.__name__}.asset_types` -> `{t.__name__}` unable to register asset type",
                ) from bad_field_exc

            cls._bind_asset_factory_method_if_not_present(ds_type, t, asset_type_name)

    @classmethod
    def _bind_asset_factory_method_if_not_present(
        cls,
        ds_type: Type[Datasource],
        asset_type: Type[DataAsset],
        asset_type_name: str,
    ):
        asset_factory_method_name = f"add_{asset_type_name}_asset"
        asset_factory_defined: bool = asset_factory_method_name in ds_type.__dict__

        if not asset_factory_defined:
            logger.debug(
                f"No `{asset_factory_method_name}()` method found for `{ds_type.__name__}` generating the method..."
            )

            def _add_asset_factory(
                self: Datasource, name: str, **kwargs
            ) -> pydantic.BaseModel:
                asset = asset_type(name=name, **kwargs)
                return self.add_asset(asset)

            # attr-defined issue
            # https://github.com/python/mypy/issues/12472
            _add_asset_factory.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
                _add_asset_factory, asset_type, exclude={"type"}
            )
            setattr(ds_type, asset_factory_method_name, _add_asset_factory)
        else:
            logger.debug(
                f"`{asset_factory_method_name}()` already defined `{ds_type.__name__}`"
            )

    @property
    def factories(self) -> List[str]:
        return list(self.__source_factories.keys())

    def __getattr__(self, attr_name: str):
        try:
            ds_constructor = self.__source_factories[attr_name]

            def wrapped(name: str, **kwargs):
                datasource = ds_constructor(name=name, **kwargs)
                # TODO (bdirks): _attach_datasource_to_context to the AbstractDataContext class
                self._data_context._attach_datasource_to_context(datasource)
                return datasource

            wrapped.__doc__ = ds_constructor.__doc__
            return wrapped
        except KeyError:
            raise AttributeError(f"No factory {attr_name} in {self.factories}")

    def __dir__(self) -> List[str]:
        """Preserves autocompletion for dynamic attributes."""
        return [*self.factories, *super().__dir__()]
