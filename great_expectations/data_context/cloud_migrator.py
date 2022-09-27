"""TODO: Add docstring"""

import logging
from dataclasses import dataclass
from typing import List, Optional, cast

import requests
from marshmallow import Schema, fields, post_dump

from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    ExpectationSuiteSchema,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultSchema,
)
from great_expectations.core.http import create_session
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context_variables import DataContextVariables
from great_expectations.data_context.store.ge_cloud_store_backend import (
    AnyPayload,
    construct_json_payload,
    construct_url,
    get_user_friendly_error_message,
)
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    CheckpointConfigSchema,
    DataContextConfigSchema,
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.exceptions.exceptions import GeCloudError
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    RuleBasedProfilerConfigSchema,
    ruleBasedProfilerConfigSchema,
)

logger = logging.getLogger(__name__)


class ConfigurationBundle:
    def __init__(self, context: BaseDataContext) -> None:

        self._context = context
        self._context_id = context.data_context_id

        self._data_context_variables: DataContextVariables = context.variables

        self._datasources: List[DatasourceConfig] = self._get_all_datasources()
        self._expectation_suites: List[
            ExpectationSuite
        ] = self._get_all_expectation_suites()
        self._checkpoints: List[CheckpointConfig] = self._get_all_checkpoints()
        self._profilers: List[RuleBasedProfilerConfig] = self._get_all_profilers()
        self._validation_results: List[
            ExpectationSuiteValidationResult
        ] = self._get_all_validation_results()

    @property
    def data_context_id(self) -> str:
        return self._context_id

    def is_usage_stats_enabled(self) -> bool:
        """Determine whether usage stats are enabled.

        Also returns false if there are no usage stats settings provided.

        Returns: Boolean of whether the usage statistics are enabled.

        """
        if self._data_context_variables.anonymous_usage_statistics:
            return self._data_context_variables.anonymous_usage_statistics.enabled
        else:
            return False

    @property
    def data_context_variables(self) -> DataContextVariables:
        return self._data_context_variables

    @property
    def datasources(self) -> List[DatasourceConfig]:
        return self._datasources

    @property
    def expectation_suites(self) -> List[ExpectationSuite]:
        return self._expectation_suites

    @property
    def checkpoints(self) -> List[CheckpointConfig]:
        return self._checkpoints

    @property
    def profilers(self) -> List[RuleBasedProfilerConfig]:
        return self._profilers

    @property
    def validation_results(self) -> List[ExpectationSuiteValidationResult]:
        return self._validation_results

    def _get_all_datasources(self) -> List[DatasourceConfig]:

        datasource_names: List[str] = list(self._context.datasources.keys())

        # Note: we are accessing the protected _datasource_store to not add a public property
        # to all Data Contexts.
        datasource_configs: List[DatasourceConfig] = [
            self._context._datasource_store.retrieve_by_name(
                datasource_name=datasource_name
            )
            for datasource_name in datasource_names
        ]

        return datasource_configs

    def _get_all_expectation_suites(self) -> List[ExpectationSuite]:
        return [
            self._context.get_expectation_suite(name)
            for name in self._context.list_expectation_suite_names()
        ]

    def _get_all_checkpoints(self) -> List[CheckpointConfig]:
        return [self._context.checkpoint_store.get_checkpoint(name=checkpoint_name, ge_cloud_id=None) for checkpoint_name in self._context.list_checkpoints()]  # type: ignore[arg-type]

    def _get_all_profilers(self) -> List[RuleBasedProfilerConfig]:
        def round_trip_profiler_config(
            profiler_config: RuleBasedProfilerConfig,
        ) -> RuleBasedProfilerConfig:
            return ruleBasedProfilerConfigSchema.load(
                ruleBasedProfilerConfigSchema.dump(profiler_config)
            )

        return [
            round_trip_profiler_config(self._context.get_profiler(name).config)
            for name in self._context.list_profilers()
        ]

    def _get_all_validation_results(
        self,
    ) -> List[ExpectationSuiteValidationResult]:
        return [
            cast(
                ExpectationSuiteValidationResult,
                self._context.validations_store.get(key),
            )
            for key in self._context.validations_store.list_keys()
        ]


class ConfigurationBundleSchema(Schema):
    """Marshmallow Schema for the Configuration Bundle."""

    data_context_id = fields.String(allow_none=False, required=True)
    data_context_variables = fields.Nested(DataContextConfigSchema, allow_none=False)
    datasources = fields.List(
        fields.Nested(DatasourceConfigSchema, allow_none=True, required=True),
        required=True,
    )
    expectation_suites = fields.List(
        fields.Nested(ExpectationSuiteSchema, allow_none=True, required=True),
        required=True,
    )
    checkpoints = fields.List(
        fields.Nested(CheckpointConfigSchema, allow_none=True, required=True),
        required=True,
    )
    profilers = fields.List(
        fields.Nested(RuleBasedProfilerConfigSchema, allow_none=True, required=True),
        required=True,
    )
    validation_results = fields.List(
        fields.Nested(
            ExpectationSuiteValidationResultSchema, allow_none=True, required=True
        ),
        required=True,
    )

    @post_dump
    def clean_up(self, data, **kwargs) -> dict:
        data_context_variables = data.get("data_context_variables", {})
        data_context_variables.pop("anonymous_usage_statistics", None)
        return data


class ConfigurationBundleJsonSerializer:
    def __init__(self, schema: Schema) -> None:
        """

        Args:
            schema: Marshmallow schema defining raw serialized version of object.
        """
        self.schema = schema

    def serialize(self, obj: ConfigurationBundle) -> dict:
        """Serialize config to json dict.

        Args:
            obj: AbstractConfig object to serialize.

        Returns:
            Representation of object as a dict suitable for serializing to json.
        """
        config: dict = self.schema.dump(obj)

        json_serializable_dict: dict = convert_to_json_serializable(data=config)

        return json_serializable_dict


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
        self._warn_if_test_migrate()
        configuration_bundle: ConfigurationBundle = ConfigurationBundle(
            context=self._context
        )
        self._warn_if_usage_stats_disabled(
            configuration_bundle.is_usage_stats_enabled()
        )
        self._print_configuration_bundle(configuration_bundle)
        if not test_migrate:
            configuration_bundle_serializer = ConfigurationBundleJsonSerializer(
                schema=ConfigurationBundleSchema()
            )
            configuration_bundle_response: AnyPayload = self._send_configuration_bundle(
                configuration_bundle, configuration_bundle_serializer
            )
            self._print_send_configuration_bundle_error(configuration_bundle_response)
            self._break_for_send_configuration_bundle_error(
                configuration_bundle_response
            )
        errors: List[
            SendValidationResultsErrorDetails
        ] = self._send_and_print_validation_results(test_migrate)
        self._print_validation_result_error_summary(errors)
        self._print_migration_conclusion_message()

    def _warn_if_test_migrate(self) -> None:
        pass

    def _warn_if_usage_stats_disabled(self, is_usage_stats_enabled: bool) -> None:
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
        serializer: ConfigurationBundleJsonSerializer,
    ) -> AnyPayload:
        url = construct_url(
            base_url=self._ge_cloud_base_url,
            organization_id=self._ge_cloud_organization_id,
            resource_name="migration",
        )

        serialized_bundle = serializer.serialize(configuration_bundle)
        data = construct_json_payload(
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
            logger.warning(str(e))
            raise GeCloudError(f"Something went wrong while migrating to Cloud: {e}")

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

    def _print_migration_conclusion_message(self):
        pass
