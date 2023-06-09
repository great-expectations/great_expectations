from __future__ import annotations

import inspect
import logging
import uuid
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.constants import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
)
from great_expectations.datasource.fluent.signatures import _merge_signatures
from great_expectations.datasource.fluent.type_lookup import TypeLookup

if TYPE_CHECKING:
    import pydantic
    from typing_extensions import TypeAlias

    from great_expectations.data_context import AbstractDataContext as GXDataContext
    from great_expectations.datasource import BaseDatasource, LegacyDatasource
    from great_expectations.datasource.fluent import PandasDatasource
    from great_expectations.datasource.fluent.interfaces import (
        DataAsset,
        Datasource,
    )
    from great_expectations.validator.validator import Validator

SourceFactoryFn: TypeAlias = Callable[..., "Datasource"]

logger = logging.getLogger(__name__)


class DefaultPandasDatasourceError(Exception):
    pass


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


class CrudMethodType(str, Enum):
    ADD = "ADD"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    ADD_OR_UPDATE = "ADD_OR_UPDATE"


CrudMethodInfoFn: TypeAlias = Callable[..., Tuple[CrudMethodType, Type["Datasource"]]]


class _SourceFactories:
    """
    Contains a collection of datasource factory methods in the format `.add_<TYPE_NAME>()`

    Contains a `.type_lookup` dict-like two way mapping between previously registered `Datasource`
    or `DataAsset` types and a simplified name for those types.
    """

    type_lookup: ClassVar = TypeLookup()
    __crud_registry: ClassVar[Dict[str, CrudMethodInfoFn]] = {}

    _data_context: GXDataContext

    def __init__(self, data_context: GXDataContext):
        self._data_context = data_context

    @classmethod
    def register_datasource(cls, ds_type: Type[Datasource]) -> None:
        """
        Add/Register a datasource. This registers all the crud datasource methods:
        add_<datasource>, delete_<datasource>, update_<datasource>, add_or_update_<datasource>
        and all related `Datasource`, `DataAsset` and `ExecutionEngine` types.

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
        with cls.type_lookup.transaction() as ds_type_lookup, ds_type._type_lookup.transaction() as asset_type_lookup:
            cls._register_assets(ds_type, asset_type_lookup=asset_type_lookup)

            cls._register_datasource(
                ds_type,
                ds_type_name=ds_type_name,
                datasource_type_lookup=ds_type_lookup,
            )

    @classmethod
    def _register_datasource(
        cls,
        ds_type: Type[Datasource],
        ds_type_name: str,
        datasource_type_lookup: TypeLookup,
    ) -> str:
        """
        Register the `Datasource` class and add a factory method for the class on `sources`.
        The method name is pulled from the `Datasource.type` attribute.
        """
        if ds_type in datasource_type_lookup:
            raise TypeRegistrationError(
                f"'{ds_type_name}' is already a registered typed and there can only be 1 type "
                "for a given name."
            )
        datasource_type_lookup[ds_type] = ds_type_name
        logger.debug(f"'{ds_type_name}' added to `type_lookup`")

        cls._register_add_datasource(ds_type, ds_type_name)
        cls._register_update_datasource(ds_type, ds_type_name)
        cls._register_add_or_update_datasource(ds_type, ds_type_name)
        cls._register_delete_datasource(ds_type, ds_type_name)

        return ds_type_name

    @classmethod
    def _register_add_datasource(cls, ds_type: Type[Datasource], ds_type_name: str):
        method_name = f"add_{ds_type_name}"

        def crud_method_info() -> tuple[CrudMethodType, Type[Datasource]]:
            return CrudMethodType.ADD, ds_type

        cls._register_crud_method(method_name, cls.__doc__, crud_method_info)

    @classmethod
    def _register_update_datasource(cls, ds_type: Type[Datasource], ds_type_name: str):
        method_name = f"update_{ds_type_name}"

        def crud_method_info() -> tuple[CrudMethodType, Type[Datasource]]:
            return CrudMethodType.UPDATE, ds_type

        cls._register_crud_method(
            method_name, f"Updates a {ds_type_name} data source.", crud_method_info
        )

    @classmethod
    def _register_add_or_update_datasource(
        cls, ds_type: Type[Datasource], ds_type_name: str
    ):
        method_name = f"add_or_update_{ds_type_name}"

        def crud_method_info() -> tuple[CrudMethodType, Type[Datasource]]:
            return CrudMethodType.ADD_OR_UPDATE, ds_type

        cls._register_crud_method(
            method_name,
            f"Adds or updates a {ds_type_name} data source.",
            crud_method_info,
        )

    @classmethod
    def _register_delete_datasource(cls, ds_type: Type[Datasource], ds_type_name: str):
        method_name = f"delete_{ds_type_name}"

        def crud_method_info() -> tuple[CrudMethodType, Type[Datasource]]:
            return CrudMethodType.DELETE, ds_type

        cls._register_crud_method(
            method_name, f"Deletes a {ds_type_name} data source.", crud_method_info
        )

    @classmethod
    def _register_crud_method(
        cls,
        crud_fn_name: str,
        crud_fn_doc: str | None,
        crud_method_info: CrudMethodInfoFn,
    ):
        # We set the name and doc and the crud_method_info because that will be used as a proxy
        # for the real crud method so we can call public_api to generate docs for these dynamically
        # generated methods.
        crud_method_info.__name__ = crud_fn_name
        crud_method_info.__doc__ = crud_fn_doc
        if crud_fn_name in cls.__crud_registry:
            raise TypeRegistrationError(
                f"'`sources.{crud_fn_name}()` already exists",
            )
        logger.debug(f"Registering data_context.source.{crud_fn_name}()")
        public_api(crud_method_info)
        cls.__crud_registry[crud_fn_name] = crud_method_info

    @classmethod
    def _register_assets(cls, ds_type: Type[Datasource], asset_type_lookup: TypeLookup):
        asset_types: Sequence[Type[DataAsset]] = ds_type.asset_types

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
                    f"Registering `{ds_type.__name__}` `DataAsset` `{t.__name__}` as '{asset_type_name}'"
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
        add_asset_factory_method_name = f"add_{asset_type_name}_asset"
        asset_factory_defined: bool = hasattr(ds_type, add_asset_factory_method_name)

        if not asset_factory_defined:
            logger.debug(
                f"No `{add_asset_factory_method_name}()` method found for `{ds_type.__name__}` generating the method..."
            )

            def _add_asset_factory(
                self: Datasource, name: str, **kwargs
            ) -> pydantic.BaseModel:
                # if the Datasource uses a data_connector we need to identify the
                # asset level attributes needed by the data_connector
                # push them to `connect_options` field
                if self.data_connector_type:
                    logger.info(
                        f"'{self.name}' {type(self).__name__} uses {self.data_connector_type.__name__}"
                    )
                    connect_options = {
                        k: v
                        for (k, v) in kwargs.items()
                        if k in self.data_connector_type.asset_level_option_keys
                    }
                    if connect_options:
                        logger.info(
                            f"{self.data_connector_type.__name__} connect_options provided -> {list(connect_options.keys())}"
                        )
                        for k in connect_options:  # TODO: avoid this extra loop
                            kwargs.pop(k)
                        kwargs["connect_options"] = connect_options
                        # asset_options_type should raise an error if the options are invalid
                        self.data_connector_type.asset_options_type(**connect_options)
                else:
                    connect_options = {}

                asset = asset_type(name=name, **kwargs)
                return self._add_asset(asset, connect_options=connect_options)

            # attr-defined issue
            # https://github.com/python/mypy/issues/12472
            _add_asset_factory.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
                _add_asset_factory, asset_type, exclude={"type"}
            )
            _add_asset_factory.__name__ = add_asset_factory_method_name
            setattr(ds_type, add_asset_factory_method_name, _add_asset_factory)

            # add the public api decorator
            public_api(getattr(ds_type, add_asset_factory_method_name))

            def _read_asset_factory(
                self: Datasource, asset_name: str | None = None, **kwargs
            ) -> Validator:
                name = asset_name or DEFAULT_PANDAS_DATA_ASSET_NAME
                asset = asset_type(name=name, **kwargs)
                self._add_asset(asset)
                batch_request = asset.build_batch_request()
                # TODO: raise error if `_data_context` not set
                return self._data_context.get_validator(batch_request=batch_request)  # type: ignore[union-attr] # self._data_context must be set

            _read_asset_factory.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
                _read_asset_factory, asset_type, exclude={"type"}
            )
            read_asset_factory_method_name = f"read_{asset_type_name}"
            setattr(ds_type, read_asset_factory_method_name, _read_asset_factory)

        else:
            logger.debug(
                f"`{add_asset_factory_method_name}()` already defined `{ds_type.__name__}`"
            )

    @property
    def pandas_default(self) -> PandasDatasource:
        from great_expectations.datasource.fluent import PandasDatasource

        datasources: dict[
            str, LegacyDatasource | BaseDatasource | Datasource | PandasDatasource
        ] = self._data_context.datasources

        # if a legacy datasource with this name already exists, we try a different name
        existing_datasource = datasources.get(DEFAULT_PANDAS_DATASOURCE_NAME)

        if not existing_datasource:
            return self._data_context.sources.add_pandas(
                name=DEFAULT_PANDAS_DATASOURCE_NAME
            )

        if isinstance(existing_datasource, PandasDatasource):
            return existing_datasource

        raise DefaultPandasDatasourceError(
            f'A datasource with a legacy type already exists with the name: "{DEFAULT_PANDAS_DATASOURCE_NAME}". '
            "Please rename this datasources if you wish to use the pandas_default `PandasDatasource`."
        )

    @property
    def factories(self) -> List[str]:
        return list(self.__crud_registry.keys())

    def _validate_current_datasource_type(
        self, name: str, datasource_type: Type[Datasource], raise_if_none: bool = True
    ) -> None:
        try:
            current_datasource = self._data_context.get_datasource(name)
        except ValueError as e:
            if raise_if_none:
                raise ValueError(
                    f"There is no datasource {name} in the data context."
                ) from e
            current_datasource = None
        if current_datasource and not isinstance(current_datasource, datasource_type):
            raise ValueError(
                f"Trying to update datasource {name} but it is not the correct type. "
                f"Expected {datasource_type.__name__} but got {type(current_datasource).__name__}"
            )

    def _datasource_passed_in_as_only_argument(
        self,
        datasource_type: Type[Datasource],
        name_or_datasource: Optional[Union[str, Datasource]],
        **kwargs,
    ) -> Optional[Datasource]:
        """Returns a datasource if one is passed in, otherwise None."""
        from great_expectations.datasource.fluent.interfaces import Datasource

        datasource: Optional[Datasource] = None
        if name_or_datasource and isinstance(name_or_datasource, Datasource):
            if len(kwargs) != 0:
                raise ValueError(
                    f"The datasource must be the sole argument. We also received: {kwargs}"
                )
            datasource = name_or_datasource
        elif name_or_datasource is None and "datasource" in kwargs:
            if len(kwargs) != 1:
                raise ValueError(
                    f"The datasource must be the sole argument. We received: {kwargs}"
                )
            datasource = kwargs["datasource"]
        if datasource and not isinstance(datasource, datasource_type):
            raise ValueError(
                f"Trying to modify datasource {datasource.name} but it is not the correct type. "
                f"Expected {datasource_type} but got {type(datasource)}"
            )
        return datasource

    def _datasource_passed_in(
        self,
        datasource_type: Type[Datasource],
        name_or_datasource: Optional[Union[str, Datasource]],
        **kwargs,
    ) -> Optional[Datasource]:
        """Validates the input is a datasource or a set of constructor parameters

        The first argument can be a non-keyword argument. If present it is a datasource
        or the name. If it is None, we expect to find either datasource or name as a kwarg.

        Args:
             datasource_type: The expected type of datasource
             name_or_datasource: Either the datasource or the name of the datasource.

        Returns:
            The passed in datasource or None.

        Raises:
            ValueError: This is raised if a datasource is passed in with additional arguments or
                        a datasource is not passed in and no name argument is present.
        """
        new_datasource = self._datasource_passed_in_as_only_argument(
            datasource_type, name_or_datasource, **kwargs
        )
        if new_datasource:
            return new_datasource
        if (
            name_or_datasource
            and isinstance(name_or_datasource, str)
            and "name" not in "kwargs"  # noqa: PLR0133
        ) or (
            name_or_datasource is None
            and "name" in kwargs
            and isinstance(kwargs["name"], str)
        ):
            return None
        raise ValueError(
            "A datasource object or a name string must be present. The datasource or "
            "name can be passed in as the first and only positional argument or can be"
            "can be passed in as keyword arguments. The arguments we received were: "
            f"positional argument: {name_or_datasource}, kwargs: {kwargs}"
        )

    def create_add_crud_method(
        self,
        datasource_type: Type[Datasource],
        doc_string: str = "",
    ) -> SourceFactoryFn:
        def add_datasource(
            name_or_datasource: Optional[Union[str, Datasource]] = None, **kwargs
        ) -> Datasource:
            # Because of the precedence of `or` and `if`, these grouping paranthesis are necessary.
            datasource = (
                self._datasource_passed_in(
                    datasource_type, name_or_datasource, **kwargs
                )
            ) or (
                datasource_type(name=name_or_datasource, **kwargs)  # type: ignore[arg-type] # could be Datasource - expect str
                if name_or_datasource
                else datasource_type(**kwargs)
            )
            logger.debug(f"Adding {datasource_type} with {datasource.name}")
            datasource._data_context = self._data_context
            datasource.test_connection()
            self._data_context._add_fluent_datasource(datasource)
            self._data_context._save_project_config()
            return datasource

        add_datasource.__doc__ = doc_string
        # attr-defined issue https://github.com/python/mypy/issues/12472
        add_datasource.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
            add_datasource,
            datasource_type,
            exclude={"type", "assets"},
            return_type=datasource_type,
        )
        return add_datasource

    def create_update_crud_method(
        self,
        datasource_type: Type[Datasource],
        doc_string: str = "",
    ) -> SourceFactoryFn:
        def update_datasource(
            name_or_datasource: Optional[Union[str, Datasource]] = None, **kwargs
        ) -> Datasource:
            new_datasource: Optional[Datasource] = self._datasource_passed_in(
                datasource_type, name_or_datasource, **kwargs
            )
            # if new_datasource is None that means name is defined as name_or_datasource or as a kwarg
            datasource_name: str = (
                new_datasource.name  # type: ignore[assignment] # always a str
                if new_datasource
                else name_or_datasource or kwargs["name"]
            )
            logger.debug(f"Updating {datasource_type} with {datasource_name}")
            self._validate_current_datasource_type(
                datasource_name,
                datasource_type,
            )

            # preserve any pre-existing id for usage with cloud
            id_: uuid.UUID | None = getattr(
                self._data_context.datasources.get(datasource_name), "id", None
            )
            if id_:
                # if not a str `name_or_datasource` is a datasource and `id` can be directly attached
                if name_or_datasource and not isinstance(name_or_datasource, str):
                    name_or_datasource.id = id_
                else:
                    kwargs["id"] = id_

            # local delete only, don't update the persisted store entry
            self._data_context._delete_fluent_datasource(
                datasource_name=datasource_name, _call_store=False
            )
            # Now that the input is validated and the old datasource is deleted we pass the
            # original arguments to the add method (ie name and not datasource_name).
            return self.create_add_crud_method(datasource_type)(
                name_or_datasource, **kwargs
            )

        update_datasource.__doc__ = doc_string
        # attr-defined issue https://github.com/python/mypy/issues/12472
        update_datasource.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
            update_datasource,
            datasource_type,
            exclude={"type", "assets"},
            return_type=datasource_type,
        )
        return update_datasource

    def create_add_or_update_crud_method(
        self,
        datasource_type: Type[Datasource],
        doc_string: str = "",
    ) -> SourceFactoryFn:
        def add_or_update_datasource(
            name_or_datasource: Optional[Union[str, Datasource]] = None, **kwargs
        ) -> Datasource:
            new_datasource: Optional[Datasource] = self._datasource_passed_in(
                datasource_type, name_or_datasource, **kwargs
            )
            # if new_datasource is None that means name is defined as name_or_datasource or as a kwarg
            datasource_name: str = (
                new_datasource.name  # type: ignore[assignment] # will be a str
                if new_datasource
                else name_or_datasource or kwargs["name"]
            )
            logger.debug(
                f"Adding or updating {datasource_type.__name__} with '{datasource_name}'"
            )
            self._validate_current_datasource_type(
                datasource_name, datasource_type, raise_if_none=False
            )

            # preserve any pre-existing id for usage with cloud
            id_: uuid.UUID | None = getattr(
                self._data_context.datasources.get(datasource_name), "id", None
            )
            if id_:
                # if not a str `name_or_datasource` is a datasource and `id` can be directly attached
                if name_or_datasource and not isinstance(name_or_datasource, str):
                    name_or_datasource.id = id_
                else:
                    kwargs["id"] = id_

            # local delete only, don't update the persisted store entry
            self._data_context._delete_fluent_datasource(
                datasource_name=datasource_name, _call_store=False
            )
            # Now that the input is validated and the old datasource is deleted we pass the
            # original arguments to the add method (ie name and not datasource_name).
            return self.create_add_crud_method(datasource_type)(
                name_or_datasource, **kwargs
            )

        add_or_update_datasource.__doc__ = doc_string
        # attr-defined issue https://github.com/python/mypy/issues/12472
        add_or_update_datasource.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
            add_or_update_datasource,
            datasource_type,
            exclude={"type", "assets"},
            return_type=datasource_type,
        )
        return add_or_update_datasource

    def create_delete_crud_method(
        self,
        datasource_type: Type[Datasource],
        doc_string: str = "",
    ) -> Callable[[str], None]:
        def delete_datasource(name: str) -> None:
            logger.debug(f"Delete {datasource_type} with {name}")
            self._validate_current_datasource_type(name, datasource_type)
            self._data_context._delete_fluent_datasource(datasource_name=name)
            self._data_context._save_project_config()

        delete_datasource.__doc__ = doc_string
        # attr-defined issue https://github.com/python/mypy/issues/12472
        delete_datasource.__signature__ = inspect.signature(delete_datasource)  # type: ignore[attr-defined]
        return delete_datasource

    def __getattr__(self, attr_name: str):
        try:
            crud_method_info = self.__crud_registry[attr_name]
            crud_method_type, datasource_type = crud_method_info()
            docstring = crud_method_info.__doc__ or ""
            if crud_method_type == CrudMethodType.ADD:
                return self.create_add_crud_method(datasource_type, docstring)
            elif crud_method_type == CrudMethodType.UPDATE:
                return self.create_update_crud_method(datasource_type, docstring)
            elif crud_method_type == CrudMethodType.ADD_OR_UPDATE:
                return self.create_add_or_update_crud_method(datasource_type, docstring)
            elif crud_method_type == CrudMethodType.DELETE:
                return self.create_delete_crud_method(datasource_type, docstring)
            else:
                raise TypeRegistrationError(
                    f"Unknown crud method registered for {attr_name} with type {crud_method_type}"
                )
        except KeyError as e:
            raise AttributeError(
                f"No crud method '{attr_name}' in {self.factories}"
            ) from e

    def __dir__(self) -> List[str]:
        """Preserves autocompletion for dynamic attributes."""
        return [*self.factories, *super().__dir__()]


def _iter_all_registered_types(
    include_datasource: bool = True, include_data_asset: bool = True
) -> Generator[tuple[str, Type[Datasource] | Type[DataAsset]], None, None]:
    """
    Iterate through all registered Datasource and DataAsset types.
    Returns tuples of the registered type name and the actual type/class.
    """
    for ds_name in _SourceFactories.type_lookup.type_names():
        ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_name]
        if include_datasource:
            yield ds_name, ds_type

        if include_data_asset:
            for asset_name in ds_type._type_lookup.type_names():
                asset_type: Type[DataAsset] = ds_type._type_lookup[asset_name]
                yield asset_name, asset_type
