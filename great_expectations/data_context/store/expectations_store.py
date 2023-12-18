from __future__ import annotations

import random
import uuid
from typing import Dict, cast

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
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
from great_expectations.data_context.util import load_class
from great_expectations.expectation_suite import ExpectationSuite
from great_expectations.expectation_suite.expectation_suite import (
    ExpectationSuiteSchema,
)
from great_expectations.util import (
    filter_properties_dict,
    verify_dynamic_loading_support,
)


class ExpectationsStore(Store):
    """
    An Expectations Store provides a way to store Expectation Suites accessible to a Data Context.
    """

    _key_class = ExpectationSuiteIdentifier

    def __init__(
        self,
        store_backend=None,
        runtime_environment=None,
        store_name=None,
        data_context=None,
    ) -> None:
        self._expectationSuiteSchema = ExpectationSuiteSchema()
        # TODO: refactor so ExpectationStore can have access to DataContext. Currently used by usage_stats messages.
        self._data_context = data_context
        if store_backend is not None:
            store_backend_module_name = store_backend.get(
                "module_name", "great_expectations.data_context.store"
            )
            store_backend_class_name = store_backend.get(
                "class_name", "InMemoryStoreBackend"
            )
            verify_dynamic_loading_support(module_name=store_backend_module_name)
            store_backend_class = load_class(
                store_backend_class_name, store_backend_module_name
            )

            # Store Backend Class was loaded successfully; verify that it is of a correct subclass.
            if issubclass(store_backend_class, TupleStoreBackend):
                # Provide defaults for this common case
                store_backend["filepath_suffix"] = store_backend.get(
                    "filepath_suffix", ".json"
                )
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

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(response_json: Dict) -> Dict:
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
                raise ValueError(
                    "More than one Expectation Suite was found with the expectation_suite_name."
                )
        else:
            suite_data = response_json["data"]
        ge_cloud_suite_id: str = suite_data["id"]
        suite_dict: Dict = suite_data["attributes"]["suite"]
        suite_dict["ge_cloud_id"] = ge_cloud_suite_id

        return suite_dict

    def _add(self, key, value, **kwargs):
        if not self.cloud_mode:
            # this logic should move to the store backend, but is implemented here for now
            value = self._add_ids_on_create(value)
        try:
            result = super()._add(key=key, value=value, **kwargs)
            if self.cloud_mode:
                # cloud backend has added IDs, so we update our local state to be in sync
                result = cast(GXCloudResourceRef, result)
                value = self._add_cloud_ids_to_local_suite_and_expectations(
                    local_suite=value,
                    cloud_suite=result.response["data"]["attributes"]["suite"],
                )
            return result
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.ExpectationSuiteError(
                f"An ExpectationSuite named {value.expectation_suite_name} already exists."
            )

    def _update(self, key, value, **kwargs):
        if not self.cloud_mode:
            # this logic should move to the store backend, but is implemented here for now
            value = self._add_ids_on_update(value)
        try:
            result = super()._update(key=key, value=value, **kwargs)
            if self.cloud_mode:
                # cloud backend has added IDs, so we update our local state to be in sync
                result = cast(GXCloudResourceRef, result)
                value = self._add_cloud_ids_to_local_suite_and_expectations(
                    local_suite=value,
                    cloud_suite=result.response["data"]["attributes"]["suite"],
                )
        except gx_exceptions.StoreBackendError as exc:
            # todo: this generic error clobbers more informative errors coming from the store
            raise gx_exceptions.ExpectationSuiteError(
                f"Could not find an existing ExpectationSuite named {value.expectation_suite_name}."
            ) from exc

    def _add_ids_on_create(self, suite: ExpectationSuite) -> ExpectationSuite:
        """This method handles adding IDs to suites and expectations for non-cloud backends.
        In the future, this logic should be the responsibility of each non-cloud backend.
        """
        suite["ge_cloud_id"] = str(uuid.uuid4())
        if isinstance(suite, ExpectationSuite):
            key = "expectation_configurations"
        else:
            # this will be true if a serialized suite is provided
            key = "expectations"
        for expectation_configuration in suite[key]:
            expectation_configuration["ge_cloud_id"] = str(uuid.uuid4())
        return suite

    def _add_ids_on_update(self, suite: ExpectationSuite) -> ExpectationSuite:
        """This method handles adding IDs to suites and expectations for non-cloud backends.
        In the future, this logic should be the responsibility of each non-cloud backend.
        """
        if not suite["ge_cloud_id"]:
            suite["ge_cloud_id"] = str(uuid.uuid4())
        if isinstance(suite, ExpectationSuite):
            key = "expectation_configurations"
        else:
            # this will be true if a serialized suite is provided
            key = "expectations"

        # enforce that every ID in this suite is unique
        expectation_ids = [
            exp["ge_cloud_id"] for exp in suite[key] if exp["ge_cloud_id"]
        ]
        if len(expectation_ids) != len(set(expectation_ids)):
            raise RuntimeError("Expectation IDs must be unique within a suite.")

        for expectation_configuration in suite[key]:
            if not expectation_configuration["ge_cloud_id"]:
                expectation_configuration["ge_cloud_id"] = str(uuid.uuid4())
        return suite

    def _add_cloud_ids_to_local_suite_and_expectations(
        self, local_suite: ExpectationSuite, cloud_suite: dict
    ) -> ExpectationSuite:
        if not local_suite.ge_cloud_id:
            local_suite.ge_cloud_id = cloud_suite["ge_cloud_id"]
        # replace local expectations with those returned from the backend
        expectations = []
        from great_expectations.expectations.registry import get_expectation_impl

        for expectation_dict in cloud_suite["expectations"]:
            class_ = get_expectation_impl(expectation_dict["expectation_type"])
            expectation = class_(
                meta=expectation_dict["meta"],
                id=expectation_dict["ge_cloud_id"],
                **expectation_dict["kwargs"],
            )
            expectations.append(expectation)
        # todo: update when configurations are no longer source of truth on suite
        local_suite.expectation_configurations = [
            expectation.configuration for expectation in expectations
        ]
        return local_suite

    @override
    def get(self, key) -> ExpectationSuite:
        return super().get(key)  # type: ignore[return-value]

    @override
    def _validate_key(  # type: ignore[override]
        self, key: ExpectationSuiteIdentifier | GXCloudIdentifier
    ) -> None:
        if isinstance(key, GXCloudIdentifier) and not key.id and not key.resource_name:
            raise ValueError(
                "GXCloudIdentifier for ExpectationsStore must contain either "
                "an id or a resource_name, but neither are present."
            )
        return super()._validate_key(key=key)

    def remove_key(self, key):
        return self.store_backend.remove_key(key)

    def serialize(self, value):
        if self.cloud_mode:
            # GXCloudStoreBackend expects a json str
            return self._expectationSuiteSchema.dump(value)
        return self._expectationSuiteSchema.dumps(value, indent=2, sort_keys=True)

    def deserialize(self, value):
        if isinstance(value, dict):
            return self._expectationSuiteSchema.load(value)
        else:
            return self._expectationSuiteSchema.loads(value)

    def get_key(
        self, suite: ExpectationSuite
    ) -> GXCloudIdentifier | ExpectationSuiteIdentifier:
        """Given an ExpectationSuite, build the correct key for use in the ExpectationsStore."""
        key: GXCloudIdentifier | ExpectationSuiteIdentifier
        if self.cloud_mode:
            key = GXCloudIdentifier(
                resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                id=suite.ge_cloud_id,
                resource_name=suite.expectation_suite_name,
            )
        else:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=suite.expectation_suite_name
            )
        return key

    def self_check(self, pretty_print):  # noqa: PLR0912
        return_obj = {}

        if pretty_print:
            print("Checking for existing keys...")

        return_obj["keys"] = self.list_keys()
        return_obj["len_keys"] = len(return_obj["keys"])
        len_keys = return_obj["len_keys"]

        if pretty_print:
            if return_obj["len_keys"] == 0:
                print(f"\t{len_keys} keys found")
            else:
                print(f"\t{len_keys} keys found:")
                for key in return_obj["keys"][:10]:
                    print(f"		{key!s}")
            if len_keys > 10:  # noqa: PLR2004
                print("\t\t...")
            print()

        test_key_name = "test-key-" + "".join(
            [random.choice(list("0123456789ABCDEF")) for i in range(20)]
        )
        if self.cloud_mode:
            test_key: GXCloudIdentifier = self.key_class(
                resource_type=GXCloudRESTResource.CHECKPOINT,
                ge_cloud_id=str(uuid.uuid4()),
            )
        else:
            test_key: ExpectationSuiteIdentifier = self.key_class(test_key_name)
        test_value = ExpectationSuite(
            expectation_suite_name=test_key_name, data_context=self._data_context
        )

        if pretty_print:
            print(f"Attempting to add a new test key: {test_key}...")
        self.set(key=test_key, value=test_value)
        if pretty_print:
            print("\tTest key successfully added.")
            print()

        if pretty_print:
            print(
                f"Attempting to retrieve the test value associated with key: {test_key}..."
            )
        test_value = self.get(
            key=test_key,
        )
        if pretty_print:
            print("\tTest value successfully retrieved.")
            print()

        if pretty_print:
            print(f"Cleaning up test key and value: {test_key}...")

        test_value = self.remove_key(
            # key=self.key_to_tuple(test_key),
            key=self.key_to_tuple(test_key),
        )
        if pretty_print:
            print("\tTest key and value successfully removed.")
            print()

        return return_obj
