"""
The CloudMigrator is reponsible for collecting elements of user's project config,
bundling them, and sending that bundle to the GX Cloud backend.

Upon successful migration, a user's config will be saved in Cloud and subsequent uses of
GX should be done through the Cloud UI.

Please see ConfigurationBundle for more information on the bundle's contents.

Usage:
```
# Test migration before actually going through the process
# Once tested, the boolean flag is to be flipped
migrator = gx.CloudMigrator.migrate(context=context, test_migrate=True)

# If the migrate process failed while processing validation results: (Optional)
migrator.retry_migrate_validation_results()
```
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional

import requests

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.configuration import AbstractConfig  # noqa: TCH001
from great_expectations.core.http import create_session
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.migrator.configuration_bundle import (
    ConfigurationBundle,
    ConfigurationBundleJsonSerializer,
    ConfigurationBundleSchema,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
    construct_json_payload,
    construct_url,
    get_user_friendly_error_message,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )

logger = logging.getLogger(__name__)


class MigrationResponse(NamedTuple):
    message: str
    status_code: int
    success: bool


class CloudMigrator:
    def __init__(
        self,
        context: AbstractDataContext,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
    ) -> None:
        self._context = context

        cloud_config = CloudDataContext.get_cloud_config(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )

        cloud_base_url = cloud_config.base_url
        cloud_access_token = cloud_config.access_token
        cloud_organization_id = cloud_config.organization_id

        # Invariant due to `get_cloud_config` raising an error if any config values are missing
        if not cloud_organization_id:
            raise ValueError(
                "An organization id must be present when performing a migration"
            )

        self._cloud_base_url = cloud_base_url
        self._cloud_access_token = cloud_access_token
        self._cloud_organization_id = cloud_organization_id

        self._session = create_session(access_token=cloud_access_token)

        self._unsuccessful_validations: Dict[str, dict] = {}

    @classmethod
    def migrate(  # noqa: PLR0913
        cls,
        context: AbstractDataContext,
        test_migrate: bool,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
    ) -> CloudMigrator:
        """Migrate your Data Context to GX Cloud.

        Args:
            context: The Data Context you wish to migrate.
            test_migrate: True if this is a test, False if you want to perform
                the migration.
            cloud_base_url: Optional, you may provide this alternatively via
                environment variable GX_CLOUD_BASE_URL
            cloud_access_token: Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ACCESS_TOKEN
            cloud_organization_id: Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ORGANIZATION_ID

        Returns:
            CloudMigrator instance
        """
        event = UsageStatsEvents.CLOUD_MIGRATE
        event_payload = {"organization_id": cloud_organization_id}
        try:
            cloud_migrator: CloudMigrator = cls(
                context=context,
                cloud_base_url=cloud_base_url,
                cloud_access_token=cloud_access_token,
                cloud_organization_id=cloud_organization_id,
            )
            cloud_migrator._migrate_to_cloud(test_migrate)
            if not test_migrate:  # Only send an event if this is not a test run.
                send_usage_message(
                    data_context=context,
                    event=event,
                    event_payload=event_payload,
                    success=True,
                )
            return cloud_migrator
        except Exception as e:
            # Note we send an event on any exception here
            if not test_migrate:
                send_usage_message(
                    data_context=context,
                    event=event,
                    event_payload=event_payload,
                    success=False,
                )
            raise gx_exceptions.MigrationError(
                "Migration failed. Please check the error message for more details."
            ) from e

    def retry_migrate_validation_results(self) -> None:
        if not self._unsuccessful_validations:
            print("No unsuccessful validations found!")
            return

        self._process_validation_results(
            serialized_validation_results=self._unsuccessful_validations,
            test_migrate=False,
        )
        if self._unsuccessful_validations:
            self._print_unsuccessful_validation_message()

    def _migrate_to_cloud(self, test_migrate: bool) -> None:
        self._print_migration_introduction_message()

        configuration_bundle: ConfigurationBundle = ConfigurationBundle(
            context=self._context
        )
        self._emit_log_stmts(
            configuration_bundle=configuration_bundle, test_migrate=test_migrate
        )
        self._print_configuration_bundle_summary(
            configuration_bundle=configuration_bundle
        )

        serialized_bundle = self._serialize_configuration_bundle(
            configuration_bundle=configuration_bundle
        )
        serialized_validation_results = self._prepare_validation_results(
            serialized_bundle=serialized_bundle
        )

        if not self._send_configuration_bundle(
            serialized_bundle=serialized_bundle, test_migrate=test_migrate
        ):
            return  # Exit early as validation results cannot be sent if the main payload fails

        self._send_validation_results(
            serialized_validation_results=serialized_validation_results,
            test_migrate=test_migrate,
        )

        self._print_migration_conclusion_message(test_migrate=test_migrate)

    def _emit_log_stmts(
        self, configuration_bundle: ConfigurationBundle, test_migrate: bool
    ) -> None:
        if test_migrate:
            self._log_about_test_migrate()
        if not configuration_bundle.is_usage_stats_enabled():
            self._log_about_usage_stats_disabled()
        if configuration_bundle.datasources:
            self._log_about_bundle_contains_datasources()

    def _log_about_test_migrate(self) -> None:
        logger.info(
            "This is a test run! Please pass `test_migrate=False` to begin the "
            "actual migration (e.g. `CloudMigrator.migrate(context=context, test_migrate=False)`).\n"
        )

    def _log_about_usage_stats_disabled(self) -> None:
        logger.info(
            "We noticed that you had disabled usage statistics tracking. "
            "Please note that by migrating your context to GX Cloud your new Cloud Data Context "
            "will emit usage statistics. These statistics help us understand how we can improve "
            "the product and we hope you don't mind!\n"
        )

    def _log_about_bundle_contains_datasources(self) -> None:
        logger.info(
            "Since your existing context includes one or more datasources, "
            "please note that if your credentials are included in the datasource config, "
            "they will be sent to the GX Cloud backend. We recommend storing your credentials "
            "locally in config_variables.yml or in environment variables referenced "
            "from your configuration rather than directly in your configuration. Please see "
            "our documentation for more details.\n"
        )

    def _print_configuration_bundle_summary(
        self, configuration_bundle: ConfigurationBundle
    ) -> None:
        # NOTE(cdkini): Datasources should be verbose, not just summary!
        to_print = (
            ("Datasource", configuration_bundle.datasources),
            ("Checkpoint", configuration_bundle.checkpoints),
            ("Expectation Suite", configuration_bundle.expectation_suites),
            ("Profiler", configuration_bundle.profilers),
        )

        print("[Step 1/4]: Bundling context configuration")
        for name, collection in to_print:
            # ExpectationSuite is not AbstractConfig but contains required `name`
            self._print_object_summary(obj_name=name, obj_collection=collection)  # type: ignore[arg-type]

    def _print_object_summary(
        self, obj_name: str, obj_collection: List[AbstractConfig]
    ) -> None:
        length = len(obj_collection)

        summary = f"  Bundled {length} {obj_name}(s)"
        if length:
            summary += ":"
        print(summary)

        for obj in obj_collection[:10]:
            print(f"    {obj['name']}")

        if length > 10:  # noqa: PLR2004
            extra = length - 10
            print(f"    ({extra} other {obj_name.lower()}(s) not displayed)")

    def _serialize_configuration_bundle(
        self, configuration_bundle: ConfigurationBundle
    ) -> dict:
        serializer = ConfigurationBundleJsonSerializer(
            schema=ConfigurationBundleSchema()
        )
        serialized_bundle = serializer.serialize(configuration_bundle)
        return serialized_bundle

    def _prepare_validation_results(self, serialized_bundle: dict) -> Dict[str, dict]:
        print("[Step 2/4]: Preparing validation results")
        return serialized_bundle.pop("validation_results")

    def _send_configuration_bundle(
        self, serialized_bundle: dict, test_migrate: bool
    ) -> bool:
        print("[Step 3/4]: Sending context configuration")
        if test_migrate:
            return True

        response = self._post_to_cloud_backend(
            resource_name="migration",
            resource_type="migration",
            attributes_key="bundle",
            attributes_value=serialized_bundle,
        )

        if not response.success:
            print(
                "\nThere was an error sending your configuration to GX Cloud!\n"
                "We have reverted your GX Cloud configuration to the state before the migration. "
                "Please check your configuration before re-attempting the migration.\n\n"
                "The server returned the following error:\n"
                f"  Code : {response.status_code}\n  Error: {response.message}"
            )

        return response.success

    def _send_validation_results(
        self,
        serialized_validation_results: Dict[str, dict],
        test_migrate: bool,
    ) -> None:
        print("[Step 4/4]: Sending validation results")
        self._process_validation_results(
            serialized_validation_results=serialized_validation_results,
            test_migrate=test_migrate,
        )

    def _process_validation_results(
        self, serialized_validation_results: Dict[str, dict], test_migrate: bool
    ) -> None:
        # 20220928 - Chetan - We want to use the static lookup tables in GXCloudStoreBackend
        # to ensure the appropriate URL and payload shape. This logic should be moved to
        # a more central location.
        resource_type = GXCloudRESTResource.EXPECTATION_VALIDATION_RESULT
        resource_name = GXCloudStoreBackend.RESOURCE_PLURALITY_LOOKUP_DICT[
            resource_type
        ]
        attributes_key = GXCloudStoreBackend.PAYLOAD_ATTRIBUTES_KEYS[resource_type]

        unsuccessful_validations = {}

        for i, (key, validation_result) in enumerate(
            serialized_validation_results.items()
        ):
            success: bool
            if test_migrate:
                success = True
            else:
                response = self._post_to_cloud_backend(
                    resource_name=resource_name,
                    resource_type=resource_type,
                    attributes_key=attributes_key,
                    attributes_value=validation_result,
                )
                success = response.success

            progress = f"({i+1}/{len(serialized_validation_results)})"

            if success:
                print(f"  Sent validation result {progress}")
            else:
                print(f"  Error sending validation result '{key}' {progress}")
                unsuccessful_validations[key] = validation_result

        self._unsuccessful_validations = unsuccessful_validations

    def _post_to_cloud_backend(
        self,
        resource_name: str,
        resource_type: str,
        attributes_key: str,
        attributes_value: dict,
    ) -> MigrationResponse:
        url = construct_url(
            base_url=self._cloud_base_url,
            organization_id=self._cloud_organization_id,
            resource_name=resource_name,
        )
        data = construct_json_payload(
            resource_type=resource_type,
            organization_id=self._cloud_organization_id,
            attributes_key=attributes_key,
            attributes_value=attributes_value,
        )
        response = self._session.post(url, json=data)

        message = ""
        try:
            response.raise_for_status()
        except requests.HTTPError as http_err:
            message = get_user_friendly_error_message(http_err, log_level=logging.INFO)

        status_code = response.status_code
        success = response.ok

        return MigrationResponse(
            message=message, status_code=status_code, success=success
        )

    def _print_unsuccessful_validation_message(self) -> None:
        print(
            f"\nPlease note that there were {len(self._unsuccessful_validations)} "
            "validation result(s) that were not successfully migrated:"
        )

        for key in self._unsuccessful_validations:
            print(f"  {key}")

        print(
            "\nTo retry uploading these validation results, you can use the following "
            "code snippet:\n"
            "  `migrator.retry_migrate_validation_results()`"
        )

    def _print_migration_introduction_message(self) -> None:
        print(
            "Thank you for using Great Expectations!\n\n"
            "We will now begin the migration process to GX Cloud. First we will bundle "
            "your existing context configuration and send it to the Cloud backend. Then "
            "we will send each of your validation results.\n"
        )

    def _print_migration_conclusion_message(self, test_migrate: bool) -> None:
        if test_migrate:
            print(
                "\nTest run completed! Please set `test_migrate=False` to perform an actual migration."
            )
            return

        if self._unsuccessful_validations:
            print("\nPartial success!")
        else:
            print("\nSuccess!")

        print(
            "Now that you have migrated your Data Context to GX Cloud, you should use your "
            "Cloud Data Context from now on to interact with Great Expectations. "
            "If you continue to use your existing Data Context your configurations could "
            "become out of sync. "
        )

        if self._unsuccessful_validations:
            self._print_unsuccessful_validation_message()
