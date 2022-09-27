"""TODO: Add docstring"""
import logging
from dataclasses import dataclass
from typing import List, Optional, cast

import requests

from great_expectations.core.http import create_session
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.migrator.configuration_bundle import (
    ConfigurationBundle,
    ConfigurationBundleJsonSerializer,
    ConfigurationBundleSchema,
)
from great_expectations.data_context.store.ge_cloud_store_backend import (
    AnyPayload,
    GeCloudRESTResource,
    GeCloudStoreBackend,
    construct_json_payload,
    construct_url,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

logger = logging.getLogger(__name__)


@dataclass
class SendValidationResultsErrorDetails:
    # TODO: Implementation
    pass


class CloudMigrator:
    def __init__(
        self,
        context: BaseDataContext,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> None:
        self._context = context

        cloud_config = CloudDataContext.get_ge_cloud_config(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_access_token=ge_cloud_access_token,
            ge_cloud_organization_id=ge_cloud_organization_id,
        )

        ge_cloud_base_url = cloud_config.base_url
        ge_cloud_access_token = cloud_config.access_token
        ge_cloud_organization_id = cloud_config.organization_id

        # Invariant due to `get_ge_cloud_config` raising an error if any config values are missing
        if not ge_cloud_organization_id:
            raise ValueError(
                "An organization id must be present when performing a migration"
            )

        self._ge_cloud_base_url = ge_cloud_base_url
        self._ge_cloud_access_token = ge_cloud_access_token
        self._ge_cloud_organization_id = ge_cloud_organization_id

        self._session = create_session(access_token=ge_cloud_access_token)

    @classmethod
    def migrate(
        cls,
        context: BaseDataContext,
        test_migrate: bool,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> None:
        """Migrate your Data Context to GX Cloud.

        Args:
            context: The Data Context you wish to migrate.
            test_migrate: True if this is a test, False if you want to perform
                the migration.
            ge_cloud_base_url: Optional, you may provide this alternatively via
                environment variable GE_CLOUD_BASE_URL
            ge_cloud_access_token: Optional, you may provide this alternatively
                via environment variable GE_CLOUD_ACCESS_TOKEN
            ge_cloud_organization_id: Optional, you may provide this alternatively
                via environment variable GE_CLOUD_ORGANIZATION_ID

        Returns:
            None
        """
        raise NotImplementedError("This will be implemented soon!")
        # This code will be uncommented when the migrator is implemented:
        # cloud_migrator: CloudMigrator = cls(
        #     context=context,
        #     ge_cloud_base_url=ge_cloud_base_url,
        #     ge_cloud_access_token=ge_cloud_access_token,
        #     ge_cloud_organization_id=ge_cloud_organization_id,
        # )
        # cloud_migrator._migrate_to_cloud(test_migrate)

    @classmethod
    def migrate_validation_result(
        cls,
        context: AbstractDataContext,
        validation_result_suite_identifier: ValidationResultIdentifier,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ):
        raise NotImplementedError("This will be implemented soon!")

    def _migrate_to_cloud(self, test_migrate: bool):
        """TODO: This is a rough outline of the steps to take during the migration, verify against the spec before release."""
        self._print_migration_introduction_message()

        configuration_bundle: ConfigurationBundle = ConfigurationBundle(
            context=self._context
        )
        self._warn_if_test_migrate()
        self._warn_if_usage_stats_disabled(
            is_usage_stats_enabled=configuration_bundle.is_usage_stats_enabled()
        )
        self._warn_if_bundle_contains_datasources(
            configuration_bundle=configuration_bundle
        )
        self._print_configuration_bundle(configuration_bundle=configuration_bundle)

        serialized_bundle = self._serialize_configuration_bundle(
            configuration_bundle=configuration_bundle
        )
        self._send_configuration_bundle(
            serialized_bundle=serialized_bundle, test_migrate=test_migrate
        )

        self._print_migration_conclusion_message()

    def _warn_if_test_migrate(self) -> None:
        pass

    def _warn_if_usage_stats_disabled(self, is_usage_stats_enabled: bool) -> None:
        pass

    def _warn_if_bundle_contains_datasources(
        self, configuration_bundle: ConfigurationBundle
    ) -> None:
        pass

    def _print_configuration_bundle(
        self, configuration_bundle: ConfigurationBundle
    ) -> None:
        pass

    def _serialize_configuration_bundle(
        self, configuration_bundle: ConfigurationBundle
    ) -> dict:
        serializer = ConfigurationBundleJsonSerializer(
            schema=ConfigurationBundleSchema()
        )
        serialized_bundle = serializer.serialize(configuration_bundle)
        return serialized_bundle

    def _send_configuration_bundle(
        self, serialized_bundle: dict, test_migrate: bool
    ) -> None:
        serialized_validation_results = serialized_bundle.pop("validation_results")

        if not test_migrate:
            self._send_bundle_to_cloud_backend(serialized_bundle=serialized_bundle)
            self._send_validation_results_to_cloud_backend(
                serialized_validation_results=serialized_validation_results,
            )

    def _send_bundle_to_cloud_backend(self, serialized_bundle: dict) -> None:
        self._post_to_cloud_backend(
            resource_name="migration",
            resource_type="migration",
            attributes_key="bundle",
            attributes_value=serialized_bundle,
        )

    def _send_validation_results_to_cloud_backend(
        self, serialized_validation_results: List[dict]
    ) -> None:
        resource_type = GeCloudRESTResource.EXPECTATION_VALIDATION_RESULT
        resource_name = GeCloudStoreBackend.RESOURCE_PLURALITY_LOOKUP_DICT[
            resource_type
        ]
        attributes_key = GeCloudStoreBackend.PAYLOAD_ATTRIBUTES_KEYS[resource_type]

        for validation_result in serialized_validation_results:
            self._post_to_cloud_backend(
                resource_name=resource_name,
                resource_type=resource_type,
                attributes_key=attributes_key,
                attributes_value=validation_result,
            )

    def _post_to_cloud_backend(
        self,
        resource_name: str,
        resource_type: str,
        attributes_key: str,
        attributes_value: dict,
    ) -> None:
        url = construct_url(
            base_url=self._ge_cloud_base_url,
            organization_id=self._ge_cloud_organization_id,
            resource_name=resource_name,
        )

        data = construct_json_payload(
            resource_type=resource_type,
            organization_id=self._ge_cloud_organization_id,
            attributes_key=attributes_key,
            attributes_value=attributes_value,
        )

        response = self._session.post(url, json=data)
        self._handle_cloud_response(response=response)

    def _handle_cloud_response(self, response: requests.Response) -> None:
        # print to stdout
        success = response.ok
        response_json: AnyPayload
        try:
            response_json = response.json()
        except:
            response_json = cast(AnyPayload, {})
            success = False

    def _print_send_configuration_bundle_error(self, http_response: AnyPayload) -> None:
        pass

    def _break_for_send_configuration_bundle_error(
        self, http_response: AnyPayload
    ) -> None:
        pass

    def _send_and_print_validation_results(
        self, test_migrate: bool
    ) -> List[SendValidationResultsErrorDetails]:
        # TODO: Uses migrate_validation_result in a loop. Only sends if not self.test_migrate
        pass

    def _print_validation_result_error_summary(
        self, errors: List[SendValidationResultsErrorDetails]
    ) -> None:
        pass

    def _print_migration_introduction_message(self) -> None:
        pass

    def _print_migration_conclusion_message(self) -> None:
        pass
