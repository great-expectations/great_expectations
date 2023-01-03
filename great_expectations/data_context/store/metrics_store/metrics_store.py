import logging
from typing import Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import InMemoryStoreBackend
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
    MetricIdentifier,
)
from great_expectations.data_context.util import load_class
from great_expectations.metric_computations.metric_computation import (
    MetricComputation,
    metricComputationSchema,
)
from great_expectations.util import (
    filter_properties_dict,
    verify_dynamic_loading_support,
)

logger = logging.getLogger(__name__)


class MetricsStore(Store):

    """
    Batch-Metric Store (BMS) provides a way to store Marshmallow Schema validated BMS computation records.
    """

    _key_class = MetricIdentifier

    def __init__(
        self,
        store_name: str,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
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
            if not issubclass(store_backend_class, InMemoryStoreBackend):
                raise ge_exceptions.DataContextError(
                    "Invalid StoreBackend configuration: expected an InMemoryStoreBackend instance."
                )

        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "store_name": store_name,
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def remove_key(self, key) -> None:
        # TODO: <Alex>ALEX</Alex>
        pass
        # TODO: <Alex>ALEX</Alex>

    def serialize(self, value: MetricComputation) -> dict:
        return metricComputationSchema.dump(value)

    def deserialize(self, value: dict) -> MetricComputation:
        return metricComputationSchema.load(value)

    @property
    def config(self) -> dict:
        return self._config

    # TODO: <Alex>ALEX</Alex>
    # def self_check(self, pretty_print: bool = True) -> dict:  # type: ignore[override]
    #     # Provide visibility into parameters that ConfigurationStore was instantiated with.
    #     report_object: dict = {"config": self.config}
    #
    #     if pretty_print:
    #         print("Checking for existing keys...")
    #
    #     report_object["keys"] = sorted(
    #         key.metric_computation_key for key in self.list_keys()  # type: ignore[attr-defined]
    #     )
    #
    #     report_object["len_keys"] = len(report_object["keys"])
    #     len_keys: int = report_object["len_keys"]
    #
    #     if pretty_print:
    #         print(f"\t{len_keys} keys found")
    #         if report_object["len_keys"] > 0:
    #             for key in report_object["keys"][:10]:
    #                 print(f"		{str(key)}")
    #         if len_keys > 10:
    #             print("\t\t...")
    #         print()
    #
    #     # TODO: <Alex>ALEX</Alex>
    #     # self.serialization_self_check(pretty_print=pretty_print)
    #     # TODO: <Alex>ALEX</Alex>
    #
    #     return report_object
    # TODO: <Alex>ALEX</Alex>

    # TODO: <Alex>ALEX</Alex>
    # def serialization_self_check(self, pretty_print: bool) -> None:
    #     raise NotImplementedError
    # TODO: <Alex>ALEX</Alex>

    @staticmethod
    def determine_key(
        name: Optional[str],
        ge_cloud_id: Optional[str] = None,
    ) -> Union[GXCloudIdentifier, MetricIdentifier]:
        assert bool(name) ^ bool(
            ge_cloud_id
        ), "Must provide either name or ge_cloud_id."

        key: Union[GXCloudIdentifier, MetricIdentifier]
        if ge_cloud_id:
            key = GXCloudIdentifier(
                resource_type=GXCloudRESTResource.CHECKPOINT, ge_cloud_id=ge_cloud_id
            )
        else:
            key = MetricIdentifier(metric_key=name)  # type: ignore[arg-type]

        return key
