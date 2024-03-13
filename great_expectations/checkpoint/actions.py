"""
An action is a way to take an arbitrary method and make it configurable and runnable within a Data Context.

The only requirement from an action is for it to have a take_action method.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

import requests

from great_expectations.compatibility.pydantic import (
    BaseModel,
    PrivateAttr,
    root_validator,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.types.refs import GXCloudResourceRef

try:
    import pypd
except ImportError:
    pypd = None

from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.util import (
    send_email,
    send_microsoft_teams_notifications,
    send_opsgenie_alert,
    send_slack_notification,
    send_sns_notification,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError, DataContextError

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.render.renderer.renderer import Renderer

logger = logging.getLogger(__name__)


@public_api
class ValidationAction(BaseModel):
    """
    ValidationActions define a set of steps to be run after a validation result is produced.

    Through a Checkpoint, one can orchestrate the validation of data and configure notifications, data documentation updates,
    and other actions to take place after the validation result is produced.
    """

    def __init__(self) -> None:
        from great_expectations import project_manager

        super().__init__()
        self._using_cloud_context = project_manager.is_using_cloud()

    @public_api
    def run(  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset,
        expectation_suite_identifier: Optional[ExpectationSuiteIdentifier] = None,
        checkpoint_identifier=None,
        **kwargs,
    ):
        """Public entrypoint GX uses to trigger a ValidationAction.

        When a ValidationAction is configured in a Checkpoint, this method gets called
        after the Checkpoint produces an ExpectationSuiteValidationResult.

        Args:
            validation_result_suite: An instance of the ExpectationSuiteValidationResult class.
            validation_result_suite_identifier: an instance of either the ValidationResultIdentifier class (for open source Great Expectations) or the GXCloudIdentifier (from Great Expectations Cloud).
            data_asset: An instance of the Validator class.
            expectation_suite_identifier: Optionally, an instance of the ExpectationSuiteIdentifier class.
            checkpoint_identifier: Optionally, an Identifier for the Checkpoint.
            kwargs: named parameters that are specific to a given Action, and need to be assigned a value in the Action's configuration in a Checkpoint's action_list.

        Returns:
            A Dict describing the result of the Action.
        """
        return self._run(
            validation_result_suite=validation_result_suite,
            validation_result_suite_identifier=validation_result_suite_identifier,
            data_asset=data_asset,
            expectation_suite_identifier=expectation_suite_identifier,
            checkpoint_identifier=checkpoint_identifier,
            **kwargs,
        )

    @public_api
    def _run(  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        """Private method containing the logic specific to a ValidationAction's implementation.

        The implementation details specific to this ValidationAction must live in this method.
        Additional context required by the ValidationAction may be specified in the Checkpoint's
        `action_list` under the `action` key. These arbitrary key/value pairs will be passed into
        the ValidationAction as keyword arguments.

        Args:
            validation_result_suite: An instance of the ExpectationSuiteValidationResult class.
            validation_result_suite_identifier: an instance of either the ValidationResultIdentifier
                class (for open source Great Expectations) or the GeCloudIdentifier (from Great Expectations Cloud).
            data_asset: An instance of the Validator class.
            expectation_suite_identifier:  Optionally, an instance of the ExpectationSuiteIdentifier class.
            checkpoint_identifier:  Optionally, an Identifier for the Checkpoints.

        Returns:
            A Dict describing the result of the Action.
        """
        return NotImplementedError

    @staticmethod
    def _build_renderer(config: dict) -> Renderer:
        renderer = instantiate_class_from_config(
            config=config,
            runtime_environment={},
            config_defaults={},
        )
        if not renderer:
            raise ClassInstantiationError(
                module_name=config.get("module_name"),
                package_name=None,
                class_name=config.get("class_name"),
            )
        return renderer


class DataDocsAction(ValidationAction):
    def __init__(self) -> None:
        from great_expectations import project_manager

        super().__init__()
        self._build_data_docs = project_manager.build_data_docs
        self._get_docs_sites_urls = project_manager.get_docs_sites_urls


@public_api
class SlackNotificationAction(DataDocsAction):
    """Sends a Slack notification to a given webhook.

    ```yaml
    - name: send_slack_notification_on_validation_result
    action:
      class_name: SlackNotificationAction
      # put the actual webhook URL in the uncommitted/config_variables.yml file
      # or pass in as environment variable
      # use slack_webhook when not using slack bot token
      slack_webhook: ${validation_notification_slack_webhook}
      slack_token:
      slack_channel:
      notify_on: all
      notify_with:
      renderer:
        # the class that implements the message to be sent
        # this is the default implementation, but you can
        # implement a custom one
        module_name: great_expectations.render.renderer.slack_renderer
        class_name: SlackRenderer
      show_failed_expectations: True
    ```

    Args:
        renderer: Specifies the Renderer used to generate a query consumable by Slack API, e.g.:
            ```python
            {
               "module_name": "great_expectations.render.renderer.slack_renderer",
               "class_name": "SlackRenderer",
           }
           ```
        slack_webhook: The incoming Slack webhook to which to send notification.
        slack_token: Token from Slack app. Used when not using slack_webhook.
        slack_channel: Slack channel to receive notification. Used when not using slack_webhook.
        notify_on: Specifies validation status that triggers notification. One of "all", "failure", "success".
        notify_with: List of DataDocs site names to display  in Slack messages. Defaults to all.
        show_failed_expectations: Shows a list of failed expectation types.
    """

    slack_webhook: Optional[str] = None
    slack_token: Optional[str] = None
    slack_channel: Optional[str] = None
    notify_on: Literal["all", "failure", "success"] = "all"
    notify_with: Optional[List[str]] = None
    show_failed_expectations: bool = False
    renderer: dict = {
        "class_name": "SlackRenderer",
        "module_name": "great_expectations.render.renderer.slack_renderer",
    }

    @root_validator
    def _root_validate_slack_params(cls, values: dict) -> dict:
        slack_token = values["slack_token"]
        slack_channel = values["slack_channel"]
        slack_webhook = values["slack_webhook"]

        try:
            if not slack_token and slack_channel:
                assert slack_webhook
            if not slack_webhook:
                assert slack_token and slack_channel
            assert not (slack_webhook and slack_channel and slack_token)
        except AssertionError:
            raise ValueError(
                "Please provide either slack_webhook or slack_token and slack_channel"
            )

        return values

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: C901, PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset=None,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("SlackNotificationAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, "
                f"not {type(validation_result_suite_identifier)}"
            )

        validation_success = validation_result_suite.success
        data_docs_pages = None
        if payload:
            # process the payload
            for action_names in payload.keys():
                if payload[action_names]["class"] == "UpdateDataDocsAction":
                    data_docs_pages = payload[action_names]

        # Assemble complete GX Cloud URL for a specific validation result
        data_docs_urls: list[dict[str, str]] = self._get_docs_sites_urls(
            resource_identifier=validation_result_suite_identifier
        )

        validation_result_urls: list[str] = [
            data_docs_url["site_url"]
            for data_docs_url in data_docs_urls
            if data_docs_url["site_url"]
        ]
        if (
            isinstance(validation_result_suite_identifier, GXCloudIdentifier)
            and validation_result_suite_identifier.id
        ):
            # To send a notification with a link to the validation result, we need to have created the validation
            # result in cloud. If the user has configured the store action after the notification action, they will
            # get a warning that no link will be provided. See the __init__ method for ActionListValidationOperator.
            if (
                "store_validation_result" in payload
                and "validation_result_url" in payload["store_validation_result"]
            ):
                validation_result_urls.append(
                    payload["store_validation_result"]["validation_result_url"]
                )
        result = {"slack_notification_result": "none required"}
        if (
            self.notify_on == "all"
            or self.notify_on == "success"
            and validation_success
            or self.notify_on == "failure"
            and not validation_success
        ):
            renderer = self._build_renderer(config=self.renderer)
            query: Dict = renderer.render(
                validation_result_suite,
                data_docs_pages,
                self.notify_with,
                self.show_failed_expectations,
                validation_result_urls,
            )

            blocks = query.get("blocks")
            if blocks:
                if len(blocks) >= 1:
                    if blocks[0].get("text"):
                        result = self._send_notifications_in_batches(
                            blocks, query, result
                        )
                    else:
                        result = self._get_slack_result(query)

        return result

    def _send_notifications_in_batches(self, blocks, query, result):
        text = blocks[0]["text"]["text"]
        chunks, chunk_size = len(text), len(text) // 4
        split_text = [
            text[position : position + chunk_size]
            for position in range(0, chunks, chunk_size)
        ]
        for batch in split_text:
            query["text"] = batch
            result = self._get_slack_result(query)
        return result

    def _get_slack_result(self, query):
        # this will actually send the POST request to the Slack webapp server
        slack_notif_result = send_slack_notification(
            query,
            slack_webhook=self.slack_webhook,
            slack_token=self.slack_token,
            slack_channel=self.slack_channel,
        )
        return {"slack_notification_result": slack_notif_result}


@public_api
class PagerdutyAlertAction(ValidationAction):
    """Sends a PagerDuty event.

    ```yaml
    - name: send_pagerduty_alert_on_validation_result
    action:
      class_name: PagerdutyAlertAction
      api_key: ${pagerduty_api_key}
      routing_key: ${pagerduty_routing_key}
      notify_on: failure
      severity: critical
    ```

    Args:
        api_key: Events API v2 key for pagerduty.
        routing_key: The 32 character Integration Key for an integration on a service or on a global ruleset.
        notify_on: Specifies validation status that triggers notification. One of "all", "failure", "success".
        severity: The PagerDuty severity levels determine the level of urgency. One of "critical", "error", "warning", or "info".
    """

    api_key: str
    routing_key: str
    notify_on: Literal["all", "failure", "success"] = "failure"
    severity: Literal["critical", "error", "warning", "info"] = "critical"

    @root_validator
    def _root_validate_deps(cls, values: dict) -> dict:
        if not pypd:
            raise DataContextError("ModuleNotFoundError: No module named 'pypd'")
        return values

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset=None,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("PagerdutyAlertAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, "
                f"not {type(validation_result_suite_identifier)}"
            )

        validation_success = validation_result_suite.success

        if (
            self.notify_on == "all"
            or self.notify_on == "success"
            and validation_success
            or self.notify_on == "failure"
            and not validation_success
        ):
            expectation_suite_name = validation_result_suite.meta.get(
                "expectation_suite_name", "__no_expectation_suite_name__"
            )
            pypd.api_key = self.api_key
            pypd.EventV2.create(
                data={
                    "routing_key": self.routing_key,
                    "dedup_key": expectation_suite_name,
                    "event_action": "trigger",
                    "payload": {
                        "summary": f"Great Expectations suite check {expectation_suite_name} has failed",
                        "severity": self.severity,
                        "source": "Great Expectations",
                    },
                }
            )

            return {"pagerduty_alert_result": "success"}
        return {"pagerduty_alert_result": "none sent"}


@public_api
class MicrosoftTeamsNotificationAction(ValidationAction):
    """Sends a Microsoft Teams notification to a given webhook.

    ```yaml
    - name: send_microsoft_teams_notification_on_validation_result
    action:
      class_name: MicrosoftTeamsNotificationAction
      # put the actual webhook URL in the uncommitted/config_variables.yml file
      # or pass in as environment variable
      microsoft_teams_webhook: ${validation_notification_microsoft_teams_webhook}
      notify_on: all
      renderer:
        # the class that implements the message to be sent
        # this is the default implementation, but you can
        # implement a custom one
        module_name: great_expectations.render.renderer.microsoft_teams_renderer
        class_name: MicrosoftTeamsRenderer
    ```

    Args:
        renderer: Specifies the renderer used to generate a query consumable by teams API, e.g.:
            ```python
            {
               "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
               "class_name": "MicrosoftTeamsRenderer",
            }
            ```
        microsoft_teams_webhook: Incoming Microsoft Teams webhook to which to send notifications.
        notify_on: Specifies validation status that triggers notification. One of "all", "failure", "success".
    """

    teams_webhook: str
    notify_on: Literal["all", "failure", "success"] = "all"
    renderer: dict = {
        "class_name": "MicrosoftTeamsRenderer",
        "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
    }

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset=None,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("MicrosoftTeamsNotificationAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, "
                f"not {type(validation_result_suite_identifier)}"
            )
        validation_success = validation_result_suite.success
        data_docs_pages = None

        if payload:
            # process the payload
            for action_names in payload.keys():
                if payload[action_names]["class"] == "UpdateDataDocsAction":
                    data_docs_pages = payload[action_names]

        if (
            self.notify_on == "all"
            or self.notify_on == "success"
            and validation_success
            or self.notify_on == "failure"
            and not validation_success
        ):
            renderer = self._build_renderer(config=self.renderer)
            query = renderer.render(
                validation_result_suite,
                validation_result_suite_identifier,
                data_docs_pages,
            )
            # this will actually sent the POST request to the Microsoft Teams webapp server
            teams_notif_result = send_microsoft_teams_notifications(
                query, microsoft_teams_webhook=self.teams_webhook
            )
            return {"microsoft_teams_notification_result": teams_notif_result}
        else:
            return {"microsoft_teams_notification_result": None}


@public_api
class OpsgenieAlertAction(ValidationAction):
    """Sends an Opsgenie alert.

    ```yaml
    - name: send_opsgenie_alert_on_validation_result
    action:
      class_name: OpsgenieAlertAction
      # put the actual webhook URL in the uncommitted/config_variables.yml file
      # or pass in as environment variable
      api_key: ${opsgenie_api_key}
      region:
      priority: P2
      notify_on: failure
    ```

    Args:
        api_key: Opsgenie API key.
        region: Specifies the Opsgenie region. Populate 'EU' for Europe otherwise do not set.
        priority: Specifies the priority of the alert (P1 - P5).
        notify_on: Specifies validation status that triggers notification. One of "all", "failure", "success".
        tags: Tags to include in the alert
    """

    api_key: str
    region: Optional[str] = None
    priority: Literal["P1", "P2", "P3", "P4", "P5"] = "P3"
    notify_on: Literal["all", "failure", "success"] = "failure"
    tags: Optional[List[str]] = None
    renderer: dict = {
        "class_name": "OpsgenieRenderer",
        "module_name": "great_expectations.render.renderer.opsgenie_renderer",
    }

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset=None,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("OpsgenieAlertAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, "
                f"not {type(validation_result_suite_identifier)}"
            )

        validation_success = validation_result_suite.success

        if (
            self.notify_on == "all"
            or self.notify_on == "success"
            and validation_success
            or self.notify_on == "failure"
            and not validation_success
        ):
            expectation_suite_name = validation_result_suite.meta.get(
                "expectation_suite_name", "__no_expectation_suite_name__"
            )

            settings = {
                "api_key": self.api_key,
                "region": self.region,
                "priority": self.priority,
                "tags": self.tags,
            }

            renderer = self._build_renderer(config=self.renderer)
            description = renderer.render(validation_result_suite, None, None)

            alert_result = send_opsgenie_alert(
                description, expectation_suite_name, settings
            )

            return {"opsgenie_alert_result": alert_result}
        else:
            return {"opsgenie_alert_result": ""}


@public_api
class EmailAction(ValidationAction):
    """Sends an email to a given list of email addresses.

    ```yaml
    - name: send_email_on_validation_result
    action:
      class_name: EmailAction
      notify_on: all # possible values: "all", "failure", "success"
      notify_with:
      renderer:
        # the class that implements the message to be sent
        # this is the default implementation, but you can
        # implement a custom one
        module_name: great_expectations.render.renderer.email_renderer
        class_name: EmailRenderer
      # put the actual following information in the uncommitted/config_variables.yml file
      # or pass in as environment variable
      smtp_address: ${smtp_address}
      smtp_port: ${smtp_port}
      sender_login: ${email_address}
      sender_password: ${sender_password}
      sender_alias: ${sender_alias} # useful to send an email as an alias
      receiver_emails: ${receiver_emails}
      use_tls: False
      use_ssl: True
    ```

    Args:
        renderer: Specifies the renderer used to generate an email, for example:
            ```python
            {
               "module_name": "great_expectations.render.renderer.email_renderer",
               "class_name": "EmailRenderer",
            }
            ```
        smtp_address: Address of the SMTP server used to send the email.
        smtp_address: Port of the SMTP server used to send the email.
        sender_login: Login used send the email.
        sender_password: Password used to send the email.
        sender_alias: Optional. Alias used to send the email (default = sender_login).
        receiver_emails: Email addresses that will receive the email (separated by commas).
        use_tls: Optional. Use of TLS to send the email (using either TLS or SSL is highly recommended).
        use_ssl: Optional. Use of SSL to send the email (using either TLS or SSL is highly recommended).
        notify_on: "Specifies validation status that triggers notification. One of "all", "failure", "success".
        notify_with: Optional list of DataDocs site names to display  in Slack messages. Defaults to all.
    """

    smtp_address: str
    smtp_port: str
    receiver_emails: str
    sender_login: Optional[str] = None
    sender_password: Optional[str] = None
    sender_alias: Optional[str] = None
    use_tls: Optional[bool] = None
    use_ssl: Optional[bool] = None
    notify_on: Literal["all", "failure", "success"] = "all"
    notify_with: Optional[List[str]] = None
    renderer: dict = {
        "class_name": "EmailRenderer",
        "module_name": "great_expectations.render.renderer.email_renderer",
    }

    _receiver_emails_list: List[str] = PrivateAttr()

    @root_validator
    def _root_validate_email_params(cls, values: dict) -> dict:
        if not values["sender_alias"]:
            values["sender_alias"] = values["sender_login"]

        values["_receiver_emails_list"] = list(
            map(lambda x: x.strip(), values["receiver_emails"].split(","))
        )

        if not values["sender_login"]:
            logger.warning(
                "No login found for sending the email in action config. "
                "This will only work for email server that does not require authentication."
            )
        if not values["sender_password"]:
            logger.warning(
                "No password found for sending the email in action config."
                "This will only work for email server that does not require authentication."
            )

        return values

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset=None,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("EmailAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, "
                f"not {type(validation_result_suite_identifier)}"
            )

        validation_success = validation_result_suite.success
        data_docs_pages = None

        if payload:
            # process the payload
            for action_names in payload.keys():
                if payload[action_names]["class"] == "UpdateDataDocsAction":
                    data_docs_pages = payload[action_names]

        if (
            (self.notify_on == "all")
            or (self.notify_on == "success" and validation_success)
            or (self.notify_on == "failure" and not validation_success)
        ):
            renderer = self._build_renderer(config=self.renderer)
            title, html = renderer.render(
                validation_result_suite, data_docs_pages, self.notify_with
            )
            # this will actually send the email
            email_result = send_email(
                title,
                html,
                self.smtp_address,
                self.smtp_port,
                self.sender_login,
                self.sender_password,
                self.sender_alias,
                self._receiver_emails_list,
                self.use_tls,
                self.use_ssl,
            )

            # sending payload back as dictionary
            return {"email_result": email_result}
        else:
            return {"email_result": ""}


@public_api
class StoreValidationResultAction(ValidationAction):
    """Store a validation result in the ValidationsStore.
    Typical usage example:
        ```yaml
        - name: store_validation_result
        action:
          class_name: StoreValidationResultAction
          # name of the store where the actions will store validation results
          # the name must refer to a store that is configured in the great_expectations.yml file
          target_store_name: validations_store
        ```
    Args:
        data_context: GX Data Context.
        target_store_name: The name of the store where the actions will store the validation result.
    Raises:
        TypeError: validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}.
    """

    def __init__(
        self,
        data_context: AbstractDataContext,
        target_store_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        if target_store_name is None:
            self.target_store = data_context.stores[data_context.validations_store_name]
        else:
            self.target_store = data_context.stores[target_store_name]

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier: Optional[GXCloudIdentifier] = None,
    ):
        logger.debug("StoreValidationResultAction.run")
        run_return_value = self._basic_run(
            validation_result_suite,
            validation_result_suite_identifier,
            expectation_suite_identifier,
            checkpoint_identifier,
        )
        if self._using_cloud_context and isinstance(
            run_return_value, GXCloudResourceRef
        ):
            return self._run_cloud_post_process_resource_ref(
                run_return_value, validation_result_suite_identifier
            )

    def _basic_run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        expectation_suite_identifier,
        checkpoint_identifier: Optional[GXCloudIdentifier],
    ) -> Union[bool, GXCloudResourceRef]:
        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}.".format(
                    type(validation_result_suite_identifier)
                )
            )

        checkpoint_id = None
        if self._using_cloud_context and checkpoint_identifier:
            checkpoint_id = checkpoint_identifier.id

        expectation_suite_id = None
        if self._using_cloud_context and expectation_suite_identifier:
            expectation_suite_id = expectation_suite_identifier.id

        return self.target_store.set(
            validation_result_suite_identifier,
            validation_result_suite,
            checkpoint_id=checkpoint_id,
            expectation_suite_id=expectation_suite_id,
        )

    def _run_cloud_post_process_resource_ref(
        self,
        gx_cloud_resource_ref: GXCloudResourceRef,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
    ):
        store_set_return_value: GXCloudResourceRef
        new_id = gx_cloud_resource_ref.id
        # ValidationResultIdentifier has no `.id`
        validation_result_suite_identifier.id = new_id  # type: ignore[union-attr]
        return gx_cloud_resource_ref


@public_api
class UpdateDataDocsAction(DataDocsAction):
    """Notify the site builders of all data docs sites of a Data Context that a validation result should be added to the data docs.

    YAML configuration example:

    ```yaml
    - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
    ```

    You can also instruct ``UpdateDataDocsAction`` to build only certain sites by providing a ``site_names`` key with a
    list of sites to update:

    ```yaml
    - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names:
        - local_site
    ```

    Args:
        site_names: Optional. A list of the names of sites to update.
    """

    site_names: List[str] = []

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("UpdateDataDocsAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
            )

        # TODO Update for RenderedDataDocs
        # build_data_docs will return the index page for the validation results, but we want to return the url for the validation result using the code below
        self._build_data_docs(
            site_names=self.site_names,
            resource_identifiers=[
                validation_result_suite_identifier,
                expectation_suite_identifier,
            ],
        )
        data_docs_validation_results: dict = {}
        if self._using_cloud_context:
            return data_docs_validation_results

        # get the URL for the validation result
        docs_site_urls_list = self._get_docs_sites_urls(
            resource_identifier=validation_result_suite_identifier,
            site_names=self.site_names,
        )
        # process payload
        for sites in docs_site_urls_list:
            data_docs_validation_results[sites["site_name"]] = sites["site_url"]
        return data_docs_validation_results


@public_api
class SNSNotificationAction(ValidationAction):
    """Action that pushes validations results to an SNS topic with a subject of passed or failed.

    YAML configuration example:

        ```yaml
        - name: send_sns_notification_on_validation_result
        action:
          class_name: SNSNotificationAction
          # put the actual SNS Arn in the uncommitted/config_variables.yml file
          # or pass in as environment variable
          sns_topic_arn:
          sns_subject:
        ```

    Args:
        sns_topic_arn: The SNS Arn to publish messages to.
        sns_subject: Optional. The SNS Message Subject - defaults to expectation_suite_identifier.name.
    """

    sns_topic_arn: str
    sns_message_subject: Optional[str]

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        data_asset=None,
        **kwargs,
    ) -> str:
        logger.debug("SNSNotificationAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action. "
            )

        if self.sns_message_subject is None:
            logger.warning(
                "No message subject was passed checking for expectation_suite_name"
            )
            if expectation_suite_identifier is None:
                subject = validation_result_suite_identifier.run_id
                logger.warning(
                    f"No expectation_suite_identifier was passed. Defaulting to validation run_id: {subject}."
                )
            else:
                subject = expectation_suite_identifier.name
                logger.info(f"Using expectation_suite_name: {subject}")
        else:
            subject = self.sns_message_subject

        return send_sns_notification(
            self.sns_topic_arn, subject, validation_result_suite.__str__(), **kwargs
        )


class APINotificationAction(ValidationAction):
    url: str

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        data_asset,
        expectation_suite_identifier: Optional[ExpectationSuiteIdentifier] = None,
        checkpoint_identifier=None,
        **kwargs,
    ):
        suite_name: str = validation_result_suite.meta["expectation_suite_name"]
        if "batch_kwargs" in validation_result_suite.meta:
            data_asset_name = validation_result_suite.meta["batch_kwargs"].get(
                "data_asset_name", "__no_data_asset_name__"
            )
        elif "active_batch_definition" in validation_result_suite.meta:
            data_asset_name = (
                validation_result_suite.meta["active_batch_definition"].data_asset_name
                if validation_result_suite.meta[
                    "active_batch_definition"
                ].data_asset_name
                else "__no_data_asset_name__"
            )
        else:
            data_asset_name = "__no_data_asset_name__"

        validation_results: list = validation_result_suite.get("results")
        validation_results_serializable: list = convert_to_json_serializable(
            validation_results
        )

        payload = self.create_payload(
            data_asset_name, suite_name, validation_results_serializable
        )

        response = self.send_results(payload)
        return (
            f"Successfully Posted results to API, status code - {response.status_code}"
        )

    def send_results(self, payload) -> requests.Response:
        try:
            headers = {"Content-Type": "application/json"}
            return requests.post(self.url, headers=headers, data=payload)
        except Exception as e:
            print(f"Exception when sending data to API - {e}")
            raise e

    @staticmethod
    def create_payload(
        data_asset_name, suite_name, validation_results_serializable
    ) -> str:
        payload = json.dumps(
            {
                "test_suite_name": suite_name,
                "data_asset_name": data_asset_name,
                "validation_results": validation_results_serializable,
            }
        )
        return payload
