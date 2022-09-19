from typing import Optional, List

from great_expectations.core import ExpectationSuiteValidationResult

from great_expectations.data_context.types.base import DatasourceConfig

from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier


class ConfigurationBundle:
    pass

class HTTPResponse:
    pass


class SendValidationResultsErrorDetails:
    pass

# TODO: Move builder to this module instead of in BaseDataContext? I think so.
# def build_data_context_cloud_migrator(context: BaseDataContext)
# class BuildDataContextCloudMigrator

class DataContextCloudMigrator:

    def __init__(
        self,
        datasources: List[DatasourceConfig],
        validation_results: List[ExpectationSuiteValidationResult],
        # TODO: Add checkpoints, expectation suites, profilers, etc. Or just full config
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ):
        pass


    def migrate_to_cloud(
        self,
        test_migrate: bool
    ):
        self._warn_if_test_migrate()
        self._warn_if_usage_stats_disabled()
        configuration_bundle: ConfigurationBundle = self._bundle_configuration()
        self._log_configuration_bundle(configuration_bundle)
        if not test_migrate:
            configuration_bundle_response: HTTPResponse = self._send_configuration_bundle(configuration_bundle)
            self._log_send_configuration_bundle_error(configuration_bundle_response)
        errors: List[SendValidationResultsErrorDetails] = self._send_and_log_validation_results(test_migrate)
        self._log_validation_result_error_summary(errors)



    def migrate_validation_result_to_cloud(
        self,
        validation_result_suite_identifier: ValidationResultIdentifier
    ):
        pass


    def _warn_if_test_migrate(self) -> None:
        pass

    def _warn_if_usage_stats_disabled(self) -> None:
        pass

    def _bundle_configuration(self) -> ConfigurationBundle:
        pass

    def _log_configuration_bundle(self, configuration_bundle: ConfigurationBundle) -> None:
        pass

    def _send_configuration_bundle(self, configuration_bundle: ConfigurationBundle) -> HTTPResponse:
        pass

    def _log_send_configuration_bundle_error(self, http_response: HTTPResponse) -> None:
        pass

    def _send_and_log_validation_results(self, test_migrate: bool) -> List[SendValidationResultsErrorDetails]:
        pass

    def _log_validation_result_error_summary(self, errors: List[SendValidationResultsErrorDetails]) -> None:
        pass

