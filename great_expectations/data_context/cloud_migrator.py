"""TODO: Add docstring"""

from typing import List, Optional

from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig

from great_expectations.core import ExpectationSuiteValidationResult, ExpectationSuite
from great_expectations.data_context import AbstractDataContext, BaseDataContext
from great_expectations.data_context.store.ge_cloud_store_backend import AnyPayload
from great_expectations.data_context.types.base import DatasourceConfig, GeCloudConfig, DataContextConfig, \
    CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)


class ConfigurationBundle:

    # TODO: Can we leverage DataContextVariables here?

    def __init__(self, context: BaseDataContext) -> None:

        self._data_context_config: DataContextConfig = context.project_config_with_variables_substituted

        self._expectation_suites: List[ExpectationSuite] = self._get_all_expectation_suites(context)
        self._checkpoints: List[CheckpointConfig] = self._get_all_checkpoints(context)
        self._profilers: List[RuleBasedProfilerConfig] = self._get_all_profilers(context)
        self._validation_results: List[ExpectationSuiteValidationResult] = self._get_all_validation_results(context)

    def is_usage_statistics_key_set(self, context: BaseDataContext) -> bool:
        # TODO: Is this needed and if so should it be a public method?
        return context.project_config_with_variables_substituted.anonymous_usage_statistics.enabled

    def _get_all_expectation_suites(self, context: BaseDataContext) -> List[ExpectationSuite]:
        return [context.get_expectation_suite(name) for name in context.list_expectation_suite_names()]

    def _get_all_checkpoints(self, context: BaseDataContext) -> List[CheckpointConfig]:
        return [context.checkpoint_store.get_checkpoint(name=checkpoint_name, ge_cloud_id=None) for checkpoint_name in context.list_checkpoints()]

    def _get_all_profilers(self, context: BaseDataContext) -> List[RuleBasedProfilerConfig]:
        return [context.get_profiler(name).config for name in context.list_profilers()]

    def _get_all_validation_results(
        self,
        context: BaseDataContext,
    ) -> List[ExpectationSuiteValidationResult]:
        return [context.validations_store.get(key) for key in context.validations_store.list_keys()]



class ConfigurationBundleSchema:
    """Marshmallow Schema for the Configuration Bundle."""
    pass


class ConfigurationBundleJsonSerializer:
    """Special handling for removing usage stats key."""
    pass


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
        self._ge_cloud_base_url = ge_cloud_base_url
        self._ge_cloud_access_token = ge_cloud_access_token
        self._ge_cloud_organization_id = ge_cloud_organization_id

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
        configuration_bundle: ConfigurationBundle = self._build_configuration_bundle()
        self._warn_if_usage_stats_disabled(configuration_bundle)
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

    def _warn_if_usage_stats_disabled(self, configuration_bundle: ConfigurationBundle) -> None:
        pass

    def _build_configuration_bundle(self) -> ConfigurationBundle:
        pass

    def _print_configuration_bundle(
        self, configuration_bundle: ConfigurationBundle
    ) -> None:
        pass

    def _send_configuration_bundle(
        self, configuration_bundle: ConfigurationBundle
    ) -> AnyPayload:
        pass

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
