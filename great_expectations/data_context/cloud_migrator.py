"""TODO: Add docstring"""

import logging
from typing import List, Optional, Tuple

import requests

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.core.http import create_session
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.store.ge_cloud_store_backend import (
    AnyPayload,
    GeCloudStoreBackend,
    get_user_friendly_error_message,
)
from great_expectations.data_context.types.base import DatasourceConfig, GeCloudConfig
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.exceptions.exceptions import GeCloudError

logger = logging.getLogger(__name__)


class ConfigurationBundle:

    # TODO: Can we leverage DataContextVariables here?

    def __init__(self) -> None:
        self._datasource_configs: List[DatasourceConfig] = []
        self._validation_results: List[ExpectationSuiteValidationResult] = []

    def build_configuration_bundle(self, context: AbstractDataContext):
        self._datasource_configs = self._get_all_datasource_configs(context)
        self._validation_results = self._get_all_validation_results(context)
        # TODO: Add other methods to retrieve the rest of the configs

    def _get_all_datasource_configs(
        self,
        context: AbstractDataContext,
    ) -> List[DatasourceConfig]:
        return [
            DatasourceConfig(**datasource_config_dict)
            for datasource_config_dict in context.list_datasources()
        ]

    def _get_all_validation_results(
        self,
        context: AbstractDataContext,
    ) -> List[ExpectationSuiteValidationResult]:
        pass

    # TODO: Add other methods to retrieve the rest of the configs


class SendValidationResultsErrorDetails:
    # TODO: Implementation
    pass


class CloudMigrator:
    def __init__(
        self,
        context: AbstractDataContext,
        test_migrate: bool,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> None:
        self._context = context
        self._test_migrate = test_migrate

        cloud_config = CloudDataContext.get_ge_cloud_config(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_access_token=ge_cloud_access_token,
            ge_cloud_organization_id=ge_cloud_organization_id,
        )

        ge_cloud_base_url = cloud_config.base_url
        ge_cloud_access_token = cloud_config.access_token
        ge_cloud_organization_id = cloud_config.organization_id

        self._ge_cloud_base_url = ge_cloud_base_url
        self._ge_cloud_access_token = ge_cloud_access_token
        self._ge_cloud_organization_id = ge_cloud_organization_id

        self._session = create_session(access_token=ge_cloud_access_token)

    @property
    def test_migrate(self):
        return self._test_migrate

    @classmethod
    def migrate(
        cls,
        context: AbstractDataContext,
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
        #     test_migrate=test_migrate,
        #     ge_cloud_base_url=ge_cloud_base_url,
        #     ge_cloud_access_token=ge_cloud_access_token,
        #     ge_cloud_organization_id=ge_cloud_organization_id,
        # )
        # cloud_migrator._migrate_to_cloud()

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

    def _migrate_to_cloud(self):
        """TODO: This is a rough outline of the steps to take during the migration, verify against the spec before release."""
        self._warn_if_test_migrate()
        self._warn_if_usage_stats_disabled()
        configuration_bundle: ConfigurationBundle = self._build_configuration_bundle()
        self._print_configuration_bundle(configuration_bundle)
        if not self.test_migrate:
            configuration_bundle_response: AnyPayload = self._send_configuration_bundle(
                configuration_bundle
            )
            self._print_send_configuration_bundle_error(configuration_bundle_response)
            self._break_for_send_configuration_bundle_error(
                configuration_bundle_response
            )
        errors: List[
            SendValidationResultsErrorDetails
        ] = self._send_and_print_validation_results(self.test_migrate)
        self._print_validation_result_error_summary(errors)
        self._print_migration_conclusion_message()

    def _process_cloud_credential_overrides(
        self,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> GeCloudConfig:
        """Get cloud credentials from environment variables or parameters.

        Check first for ge_cloud_base_url, ge_cloud_access_token and
        ge_cloud_organization_id provided via params, if not then check
        for the corresponding environment variable.

        Args:
            ge_cloud_base_url: Optional, you may provide this alternatively via
                environment variable GE_CLOUD_BASE_URL
            ge_cloud_access_token: Optional, you may provide this alternatively
                via environment variable GE_CLOUD_ACCESS_TOKEN
            ge_cloud_organization_id: Optional, you may provide this alternatively
                via environment variable GE_CLOUD_ORGANIZATION_ID

        Returns:
            GeCloudConfig

        Raises:
            GeCloudError

        """
        # TODO: Use GECloudEnvironmentVariable enum for environment variables
        # TODO: Merge with existing logic in Data Context - could be static method on CloudDataContext or
        #  module level method in cloud_data_context.py Let's not duplicate this code.
        pass

    def _warn_if_test_migrate(self) -> None:
        pass

    def _warn_if_usage_stats_disabled(self) -> None:
        pass

    def _build_configuration_bundle(self) -> ConfigurationBundle:
        pass

    def _print_configuration_bundle(
        self, configuration_bundle: ConfigurationBundle
    ) -> None:
        pass

    def _send_configuration_bundle(
        self,
        configuration_bundle: ConfigurationBundle,
        serializer,
    ) -> AnyPayload:
        url = GeCloudStoreBackend.construct_url(
            base_url=self._ge_cloud_base_url,
            organization_id=self._ge_cloud_organization_id,
            resource_name="migration",
        )

        serialized_bundle = serializer.serialize(configuration_bundle)
        data = GeCloudStoreBackend.construct_json_payload(
            resource_type="migration",
            organization_id=self._ge_cloud_organization_id,
            attributes_key="bundle",
            attributes_value=serialized_bundle,
        )

        try:
            response = self._session.post(url, json=data)
            response.raise_for_status()
            return response.json()

        except requests.HTTPError as http_exc:
            raise GeCloudError(
                f"Unable to migrate config to Cloud: {get_user_friendly_error_message(http_exc)}"
            )
        except requests.Timeout as timeout_exc:
            logger.exception(timeout_exc)
            raise GeCloudError(
                "Unable to migrate config to Cloud: This is likely a transient error. Please try again."
            )
        except Exception as e:
            logger.debug(str(e))
            raise GeCloudError(f"Something went wrong while migrating to Cloud: {e}")

    def _print_send_configuration_bundle_error(self, http_response: AnyPayload) -> None:
        pass

    def _break_for_send_configuration_bundle_error(
        self, http_response: AnyPayload
    ) -> None:
        pass

    def _send_and_print_validation_results(
        self,
    ) -> List[SendValidationResultsErrorDetails]:
        # TODO: Uses migrate_validation_result in a loop. Only sends if not self.test_migrate
        pass

    def _print_validation_result_error_summary(
        self, errors: List[SendValidationResultsErrorDetails]
    ) -> None:
        pass

    def _print_migration_conclusion_message(self):
        pass
