from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypeVar, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_suite import ExpectationSuiteSchema
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.database_store_backend import (
    DatabaseStoreBackend,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
)
from great_expectations.util import (
    filter_properties_dict,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.expectations.expectation import Expectation

    _TExpectation = TypeVar("_TExpectation", bound=Expectation)


class ExpectationConfigurationDTO(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.ignore

    id: str
    type: str
    rendered_content: List[dict] = pydantic.Field(default_factory=list)
    kwargs: dict
    meta: Union[dict, None]
    expectation_context: Union[dict, None]


class ExpectationSuiteDTO(pydantic.BaseModel):
    """Capture known fields from a serialized ExpectationSuite."""

    class Config:
        extra = pydantic.Extra.ignore

    name: str
    id: str
    expectations: List[ExpectationConfigurationDTO]
    meta: Union[dict, None]
    notes: Union[str, None]


class ExpectationsStore(Store):
    """
    An Expectations Store provides a way to store Expectation Suites accessible to a Data Context.
    """

    _key_class = ExpectationSuiteIdentifier

    def __init__(
        self,
        store_backend: dict | None = None,
        runtime_environment: dict | None = None,
        store_name: str = "no_store_name",
        data_context: AbstractDataContext | None = None,
    ) -> None:
        self._expectationSuiteSchema = ExpectationSuiteSchema()
        self._data_context = data_context

        store_backend_class = self._determine_store_backend_class(store_backend)
        # Store Backend Class was loaded successfully; verify that it is of a correct subclass.
        if store_backend:
            if issubclass(store_backend_class, TupleStoreBackend):
                # Provide defaults for this common case
                store_backend["filepath_suffix"] = store_backend.get("filepath_suffix", ".json")
            elif issubclass(store_backend_class, DatabaseStoreBackend):
                # Provide defaults for this common case
                store_backend["table_name"] = store_backend.get(
                    "table_name", "ge_expectations_store"
                )
                store_backend["key_columns"] = store_backend.get(
                    "key_columns", ["expectation_suite_name"]
                )

        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter  # noqa: E501
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.  # noqa: E501
        self._config = {
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    @override
    @classmethod
    def gx_cloud_response_json_to_object_dict(cls, response_json: dict) -> dict:
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        suite_data: Dict
        # if only the expectation_suite_name is passed, a list will be returned
        if isinstance(response_json["data"], list):
            if len(response_json["data"]) == 1:
                suite_data = response_json["data"][0]
            else:
                raise ValueError(  # noqa: TRY003
                    "More than one Expectation Suite was found with the expectation_suite_name."
                )
        else:
            suite_data = response_json["data"]

        return cls._convert_raw_json_to_object_dict(suite_data)

    @override
    @staticmethod
    def _convert_raw_json_to_object_dict(data: dict[str, Any]) -> dict[str, Any]:
        # Cloud backend adds a default result format type of None, so ensure we remove it:
        for expectation in data.get("expectations", []):
            kwargs = expectation["kwargs"]
            if "result_format" in kwargs and kwargs["result_format"] is None:
                kwargs.pop("result_format")

        suite_dto = ExpectationSuiteDTO.parse_obj(data)
        return suite_dto.dict()

    def add_expectation(self, suite: ExpectationSuite, expectation: _TExpectation) -> _TExpectation:
        suite_identifier, fetched_suite = self._refresh_suite(suite)

        # we need to find which ID has been added by the backend
        old_ids = {exp.id for exp in fetched_suite.expectations}

        if self.cloud_mode:
            expectation.id = None  # flag this expectation as new for the backend
        else:
            expectation.id = str(uuid.uuid4())
        fetched_suite.expectations.append(expectation)

        self.update(key=suite_identifier, value=fetched_suite)
        if self.cloud_mode:
            # since update doesn't return the object we need (here), we refetch the suite
            suite_identifier, fetched_suite = self._refresh_suite(suite)
            new_ids = [exp.id for exp in fetched_suite.expectations if exp.id not in old_ids]
            if len(new_ids) > 1:
                # edge case: suite has been changed remotely, and one or more new expectations
                #            have been added. Since the store doesn't return the updated object,
                #            we have no reliable way to know which new ID belongs to this expectation,  # noqa: E501
                #            so we raise an exception and ask the user to refresh their suite.
                #            The Expectation should have been successfully added to the suite.
                raise RuntimeError(  # noqa: TRY003
                    "Expectation was added, however this ExpectationSuite is out of sync with the Cloud backend. "  # noqa: E501
                    f'Please fetch the latest state of this suite by calling `context.suites.get(name="{suite.name}")`.'  # noqa: E501
                )
            elif len(new_ids) == 0:
                # edge case: this is an unexpected state - if the cloud backend failed to add the expectation,  # noqa: E501
                #            it should have already raised an exception.
                raise RuntimeError("Unknown error occurred and Expectation was not added.")  # noqa: TRY003
            else:
                new_id = new_ids[0]
            expectation.id = new_id
        return expectation

    def update_expectation(self, suite: ExpectationSuite, expectation: Expectation) -> Expectation:
        suite_identifier, fetched_suite = self._refresh_suite(suite)

        if expectation.id not in {exp.id for exp in fetched_suite.expectations}:
            raise KeyError("Cannot update Expectation because it was not found.")  # noqa: TRY003

        for i, old_expectation in enumerate(fetched_suite.expectations):
            if old_expectation.id == expectation.id:
                fetched_suite.expectations[i] = expectation
                break

        self.update(key=suite_identifier, value=fetched_suite)
        # we don't expect the backend to have made changes to the Expectation,
        # so we don't update its in-memory reference.

        return expectation

    def delete_expectation(self, suite: ExpectationSuite, expectation: Expectation) -> Expectation:
        suite_identifier, suite = self._refresh_suite(suite)

        if expectation.id not in {exp.id for exp in suite.expectations}:
            raise KeyError("Cannot delete Expectation because it was not found.")  # noqa: TRY003

        for i, old_expectation in enumerate(suite.expectations):
            if old_expectation.id == expectation.id:
                del suite.expectations[i]
                break

        self.update(key=suite_identifier, value=suite)
        return expectation

    def _refresh_suite(
        self, suite
    ) -> tuple[Union[GXCloudIdentifier, ExpectationSuiteIdentifier], ExpectationSuite]:
        """Get the latest state of an ExpectationSuite from the backend."""
        suite_identifier = self.get_key(name=suite.name, id=suite.id)
        suite_dict = self.get(key=suite_identifier)
        suite = ExpectationSuite(**suite_dict)
        return suite_identifier, suite

    def _add(self, key, value, **kwargs):  # type: ignore[explicit-override] # FIXME
        if not self.cloud_mode:
            # this logic should move to the store backend, but is implemented here for now
            value: ExpectationSuite = self._add_ids_on_create(value)
        try:
            result = super()._add(key=key, value=value, **kwargs)
            if self.cloud_mode:
                # cloud backend has added IDs, so we update our local state to be in sync
                assert isinstance(result, GXCloudResourceRef)
                suite_kwargs = self.deserialize(
                    self.gx_cloud_response_json_to_object_dict(result.response)
                )
                cloud_suite = ExpectationSuite(**suite_kwargs)
                value = self._add_cloud_ids_to_local_suite_and_expectations(
                    local_suite=value,
                    cloud_suite=cloud_suite,
                )
            return result
        except gx_exceptions.StoreBackendError as exc:
            raise gx_exceptions.ExpectationSuiteError(  # noqa: TRY003
                f"An ExpectationSuite named {value.name} already exists."
            ) from exc

    def _update(self, key, value, **kwargs):  # type: ignore[explicit-override] # FIXME
        if not self.cloud_mode:
            # this logic should move to the store backend, but is implemented here for now
            value: ExpectationSuite = self._add_ids_on_update(value)
        try:
            result = super()._update(key=key, value=value, **kwargs)

            if self.cloud_mode:
                # cloud backend has added IDs, so we update our local state to be in sync
                assert isinstance(result, GXCloudResourceRef)
                suite_kwargs = self.deserialize(
                    self.gx_cloud_response_json_to_object_dict(result.response)
                )
                cloud_suite = ExpectationSuite(**suite_kwargs)
                value = self._add_cloud_ids_to_local_suite_and_expectations(
                    local_suite=value,
                    cloud_suite=cloud_suite,
                )
        except gx_exceptions.StoreBackendError as e:
            # todo: this generic error clobbers more informative errors coming from the store

            raise gx_exceptions.ExpectationSuiteNotAddedError(name=value.name) from e

    def _add_ids_on_create(self, suite: ExpectationSuite) -> ExpectationSuite:
        """This method handles adding IDs to suites and expectations for non-cloud backends.
        In the future, this logic should be the responsibility of each non-cloud backend.
        """
        suite["id"] = str(uuid.uuid4())
        if isinstance(suite, ExpectationSuite):
            key = "expectation_configurations"
        else:
            # this will be true if a serialized suite is provided
            key = "expectations"
        for expectation_configuration in suite[key]:
            expectation_configuration["id"] = str(uuid.uuid4())
        return suite

    def _add_ids_on_update(self, suite: ExpectationSuite) -> ExpectationSuite:
        """This method handles adding IDs to suites and expectations for non-cloud backends.
        In the future, this logic should be the responsibility of each non-cloud backend.
        """

        if not suite.id:
            suite.id = str(uuid.uuid4())

        # enforce that every ID in this suite is unique
        expectation_ids = [exp.id for exp in suite.expectations if exp.id]
        if len(expectation_ids) != len(set(expectation_ids)):
            raise RuntimeError("Expectation IDs must be unique within a suite.")  # noqa: TRY003

        for expectation in suite.expectations:
            if not expectation.id:
                expectation.id = str(uuid.uuid4())
        return suite

    def _add_cloud_ids_to_local_suite_and_expectations(
        self, local_suite: ExpectationSuite, cloud_suite: ExpectationSuite
    ) -> ExpectationSuite:
        if not local_suite.id:
            local_suite.id = cloud_suite.id
        # We replace local expectations with those returned from the backend
        # so remote changes are reflected in the in-memory ExpectationSuite.
        # Note that the parent Suite of these Expectations is actually `cloud_suite`,
        # since we aren't using the public ExpectationSuite API to add the Expectations.
        # This means that `Expectation._save_callback` is provided by a different copy of the
        # same ExpectationSuite.
        local_suite.expectations = [expectation for expectation in cloud_suite.expectations]
        return local_suite

    @override
    def get(self, key) -> dict:
        return super().get(key)  # type: ignore[return-value]

    @override
    def _validate_key(  # type: ignore[override]
        self, key: ExpectationSuiteIdentifier | GXCloudIdentifier
    ) -> None:
        if isinstance(key, GXCloudIdentifier) and not key.id and not key.resource_name:
            raise ValueError(  # noqa: TRY003
                "GXCloudIdentifier for ExpectationsStore must contain either "
                "an id or a resource_name, but neither are present."
            )
        return super()._validate_key(key=key)

    def serialize(self, value):  # type: ignore[explicit-override] # FIXME
        if self.cloud_mode:
            # GXCloudStoreBackend expects a json str
            val = self._expectationSuiteSchema.dump(value)
            return val
        return self._expectationSuiteSchema.dumps(value, indent=2, sort_keys=True)

    def deserialize(self, value):  # type: ignore[explicit-override] # FIXME
        if isinstance(value, dict):
            return self._expectationSuiteSchema.load(value)
        elif isinstance(value, str):
            return self._expectationSuiteSchema.loads(value)
        else:
            raise TypeError(f"Cannot deserialize value of unknown type: {type(value)}")  # noqa: TRY003

    def deserialize_suite_dict(self, suite_dict: dict) -> ExpectationSuite:
        suite = ExpectationSuite(**suite_dict)
        if suite._include_rendered_content:
            suite.render()
        return suite

    def get_key(
        self, name: str, id: Optional[str] = None
    ) -> GXCloudIdentifier | ExpectationSuiteIdentifier:
        """Given a name and optional ID, build the correct key for use in the ExpectationsStore."""
        key: GXCloudIdentifier | ExpectationSuiteIdentifier
        if self.cloud_mode:
            key = GXCloudIdentifier(
                resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                id=id,
                resource_name=name,
            )
        else:
            key = ExpectationSuiteIdentifier(name=name)
        return key
