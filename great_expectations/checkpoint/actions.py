"""
An action is a way to take an arbitrary method and make it configurable and runnable within a Data Context.

The only requirement from an action is for it to have a take_action method.
"""  # noqa: E501 # FIXME CoP

from __future__ import annotations

import json
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Literal,
    Optional,
    Type,
    Union,
)

import requests
from typing_extensions import dataclass_transform

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import aws
from great_expectations.compatibility.pydantic import (
    BaseModel,
    Extra,
    Field,
    ModelMetaclass,
    PrivateAttr,
    root_validator,
    validator,
)
from great_expectations.compatibility.pypd import pypd
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.owner_resolution import ResolvedContext, resolve_context
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.exceptions import ClassInstantiationError
from great_expectations.exceptions.exceptions import (
    ValidationActionAlreadyRegisteredError,
    ValidationActionRegistryRetrievalError,
)
from great_expectations.render.renderer import (
    EmailRenderer,
    MicrosoftTeamsRenderer,
    OpsgenieRenderer,
    SlackRenderer,
)
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.util import convert_to_json_serializable  # noqa: TID251 # FIXME CoP

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.expectations.metadata_types import FailureSeverity

logger = logging.getLogger(__name__)


NotifyOn = Literal["all", "success", "failure", "info", "warning", "critical"]


def _build_renderer(config: dict) -> Renderer:
    renderer = instantiate_class_from_config(
        config=config,
        runtime_environment={},
        config_defaults={"module_name": "great_expectations.render.renderer"},
    )
    if not renderer:
        raise ClassInstantiationError(
            module_name=config.get("module_name"),
            package_name=None,
            class_name=config.get("class_name"),
        )
    return renderer


@public_api
class ActionContext:
    """
    Shared context for all Actions in a Checkpoint run.
    Note that order matters in the Action list, as the context is updated with each Action's result.
    """

    def __init__(self) -> None:
        self._data: list[tuple[ValidationAction, dict]] = []

    @property
    def data(self) -> list[tuple[ValidationAction, dict]]:
        return self._data

    def update(self, action: ValidationAction, action_result: dict) -> None:
        self._data.append((action, action_result))

    @public_api
    def filter_results(self, class_: Type[ValidationAction]) -> list[dict]:
        """
        Filter the results of the actions in the context by class.

        Args:
            class_: The class to filter by.

        Returns:
            A list of action results.
        """
        return [action_result for action, action_result in self._data if isinstance(action, class_)]


class ValidationActionRegistry:
    """
    Registers ValidationActions to enable deserialization based on their configuration.

    Uses the `type` key from the action configuration to determine which registered class
    to instantiate.
    """

    def __init__(self):
        self._registered_actions: dict[str, Type[ValidationAction]] = {}

    def register(self, action_type: str, action_class: Type[ValidationAction]) -> None:
        """
        Register a ValidationAction class with the registry.

        Args:
            action_type: The type of the action to register.
            action_class: The ValidationAction class to register.

        Raises:
            ValidationActionAlreadyRegisteredError: If the action type is already registered.
        """
        if action_type in self._registered_actions:
            raise ValidationActionAlreadyRegisteredError(action_type)

        self._registered_actions[action_type] = action_class

    def get(self, action_type: str | None) -> Type[ValidationAction]:
        """
        Return a ValidationAction class based on its type.
        Used when instantiating actions from a checkpoint configuration.

        Args:
            action_type: The 'type' key from the action configuration.

        Returns:
            The ValidationAction class corresponding to the configuration.

        Raises:
            ValidationActionRegistryRetrievalError: If the action type is not registered.
        """
        if action_type not in self._registered_actions:
            raise ValidationActionRegistryRetrievalError(action_type)

        return self._registered_actions[action_type]


_VALIDATION_ACTION_REGISTRY = ValidationActionRegistry()


@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))  # Enables type hinting
class MetaValidationAction(ModelMetaclass):
    """MetaValidationAction registers ValidationAction as they are defined, adding them to
    the registry.

    Any class inheriting from ValidationAction will be registered based on the value of the
    "type" class attribute.
    """

    def __new__(cls, clsname, bases, attrs):
        newclass = super().__new__(cls, clsname, bases, attrs)

        action_type = newclass.__fields__.get("type")
        if action_type and action_type.default:  # Excludes base classes
            _VALIDATION_ACTION_REGISTRY.register(
                action_type=action_type.default, action_class=newclass
            )

        return newclass


@public_api
class ValidationAction(BaseModel, metaclass=MetaValidationAction):
    """
    Actions define a set of steps to run after a Validation Result is produced. Subclass `ValidationAction` to create a [custom Action](/docs/core/trigger_actions_based_on_results/create_a_custom_action).

    Through a Checkpoint, one can orchestrate the validation of data and configure notifications, data documentation updates,
    and other actions to take place after the Validation Result is produced.
    """  # noqa: E501 # FIXME CoP

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        # Due to legacy pattern of instantiate_class_from_config, we need a custom serializer
        json_encoders = {Renderer: lambda r: r.serialize()}

    type: str
    name: str

    # The Data Context this action runs against. A Checkpoint sets it on each of its actions
    # before running them; an action no Checkpoint has run has none and resolves through the
    # current Data Context.
    _data_context: Any = PrivateAttr(default=None)

    def _resolve_context(self) -> ResolvedContext:
        return resolve_context(self._data_context)

    @property
    def _using_cloud_context(self) -> bool:
        return project_manager.is_using_cloud()

    @public_api
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        """
        Run the action.

        Args:
            checkpoint_result: The result of the checkpoint run.
            action_context: The context in which the action is run.

        Returns:
            A dictionary containing the result of the action.
        """
        raise NotImplementedError

    def _get_data_docs_pages_from_prior_action(
        self, action_context: ActionContext | None
    ) -> dict[ValidationResultIdentifier, dict[str, str]] | None:
        if action_context:
            data_docs_results = action_context.filter_results(class_=UpdateDataDocsAction)
            data_docs_pages = {}
            for result in data_docs_results:
                data_docs_pages.update(result)
            return data_docs_pages

        return None

    @staticmethod
    def _substitute_config_str_if_needed(
        value: Union[str, ConfigStr, None],
        config_provider: _ConfigurationProvider | None = None,
    ) -> Optional[str]:
        if config_provider is None:
            config_provider = project_manager.get_config_provider()
        if isinstance(value, ConfigStr):
            return value.get_config_value(config_provider=config_provider)
        else:
            return value

    def _get_max_severity_failure_from_checkpoint_result(
        self, checkpoint_result: CheckpointResult
    ) -> Optional[FailureSeverity]:
        """Get the maximum severity failure across all validation results in a checkpoint result."""
        if not checkpoint_result.run_results:
            return None

        from great_expectations.expectations import metadata_types

        max_severity = None

        for validation_result in checkpoint_result.run_results.values():
            severity = validation_result.get_max_severity_failure()
            if severity is not None:
                # Short-circuit if we find CRITICAL (highest possible)
                if severity == metadata_types.FailureSeverity.CRITICAL:
                    return severity
                if max_severity is None or severity > max_severity:
                    max_severity = severity

        return max_severity


def should_notify(
    success: bool, notify_on: NotifyOn, max_severity: Optional[FailureSeverity] = None
) -> bool:
    if notify_on in {"all", "success", "failure"}:
        return (
            notify_on == "all"
            or (notify_on == "success" and success)
            or (notify_on == "failure" and not success)
        )
    if success is False and max_severity:
        return notify_on == max_severity
    return False


class DataDocsAction(ValidationAction):
    def _build_data_docs(
        self,
        site_names: list[str] | None = None,
        resource_identifiers: list | None = None,
    ) -> dict:
        return self._resolve_context().context.build_data_docs(
            site_names=site_names,
            resource_identifiers=resource_identifiers,
            dry_run=False,
            build_index=True,
        )

    def _get_docs_sites_urls(
        self,
        site_names: list[str] | None = None,
        resource_identifier: Any | None = None,
    ):
        return self._resolve_context().context.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=None,
            only_if_exists=True,
            site_names=site_names,
        )


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
        renderer: Specifies the Renderer used to generate a query consumable by Slack API.
        slack_webhook: The incoming Slack webhook to which to send notification.
        slack_token: Token from Slack app. Used when not using slack_webhook.
        slack_channel: Slack channel to receive notification. Used when not using slack_webhook.
        notify_on: Specifies validation status that triggers notification. One of "all", "failure", "success".
        notify_with: List of DataDocs site names to display  in Slack messages. Defaults to all.
        show_failed_expectations: Shows a list of failed expectation types.

    Examples:
        **renderer:**

            ```python
            {
               "module_name": "great_expectations.render.renderer.slack_renderer",
               "class_name": "SlackRenderer",
           }
           ```
    """  # noqa: E501 # FIXME CoP

    type: Literal["slack"] = "slack"

    slack_webhook: Optional[Union[ConfigStr, str]] = None
    slack_token: Optional[Union[ConfigStr, str]] = None
    slack_channel: Optional[Union[ConfigStr, str]] = None
    notify_on: NotifyOn = "all"
    notify_with: Optional[List[str]] = None
    show_failed_expectations: bool = False
    renderer: SlackRenderer = Field(default_factory=SlackRenderer)

    @validator("renderer", pre=True)
    def _validate_renderer(cls, renderer: dict | SlackRenderer) -> SlackRenderer:
        if isinstance(renderer, dict):
            _renderer = _build_renderer(config=renderer)
            if not isinstance(_renderer, SlackRenderer):
                raise ValueError(  # noqa: TRY003, TRY004 # FIXME CoP
                    "renderer must be a SlackRenderer or a valid configuration for one."
                )
            renderer = _renderer
        return renderer

    @root_validator
    def _root_validate_slack_params(cls, values: dict) -> dict:
        slack_webhook = values["slack_webhook"]
        slack_token = values["slack_token"]
        slack_channel = values["slack_channel"]
        try:
            if slack_webhook:
                assert not slack_token and not slack_channel
            else:
                assert slack_token and slack_channel
        except AssertionError:
            raise ValueError("Please provide either slack_webhook or slack_token and slack_channel")  # noqa: TRY003 # FIXME CoP

        return values

    @override
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        success = checkpoint_result.success or False
        checkpoint_name = checkpoint_result.checkpoint_config.name
        result = {"slack_notification_result": "none required"}
        max_severity = self._get_max_severity_failure_from_checkpoint_result(checkpoint_result)

        if not should_notify(success=success, notify_on=self.notify_on, max_severity=max_severity):
            return result

        checkpoint_text_blocks: list[dict] = []
        for (
            validation_result_suite_identifier,
            validation_result_suite,
        ) in checkpoint_result.run_results.items():
            validation_text_blocks = self._render_validation_result(
                result_identifier=validation_result_suite_identifier,
                result=validation_result_suite,
                action_context=action_context,
            )
            checkpoint_text_blocks.extend(validation_text_blocks)

        payload = self.renderer.concatenate_text_blocks(
            action_name=self.name,
            text_blocks=checkpoint_text_blocks,
            success=success,
            checkpoint_name=checkpoint_name,
            run_id=checkpoint_result.run_id,
        )

        return self._send_slack_notification(payload=payload)

    def _render_validation_result(
        self,
        result_identifier: ValidationResultIdentifier,
        result: ExpectationSuiteValidationResult,
        action_context: ActionContext | None = None,
    ) -> list[dict]:
        data_docs_pages = None
        if action_context:
            data_docs_pages = self._get_data_docs_pages_from_prior_action(
                action_context=action_context
            )

        # Assemble complete GX Cloud URL for a specific validation result
        data_docs_urls: list[dict[str, str]] = self._get_docs_sites_urls(
            resource_identifier=result_identifier
        )

        validation_result_urls: list[str] = [
            data_docs_url["site_url"]
            for data_docs_url in data_docs_urls
            if data_docs_url["site_url"]
        ]
        if result.result_url:
            result.result_url += "?slack=true"
            validation_result_urls.append(result.result_url)

        return self.renderer.render(
            validation_result=result,
            data_docs_pages=data_docs_pages,
            notify_with=self.notify_with,
            validation_result_urls=validation_result_urls,
        )

    def _send_slack_notification(self, payload: dict) -> dict:
        config_provider = self._resolve_context().context.config_provider
        slack_webhook = self._substitute_config_str_if_needed(self.slack_webhook, config_provider)
        slack_token = self._substitute_config_str_if_needed(self.slack_token, config_provider)
        slack_channel = self._substitute_config_str_if_needed(self.slack_channel, config_provider)

        session = requests.Session()
        url = slack_webhook
        headers = None

        # Slack doc about overwritting the channel when using the legacy Incoming Webhooks
        # https://api.slack.com/legacy/custom-integrations/messaging/webhooks
        # ** Since it is legacy, it could be deprecated or removed in the future **
        if slack_channel:
            payload["channel"] = slack_channel

        if not slack_webhook:
            url = "https://slack.com/api/chat.postMessage"
            headers = {"Authorization": f"Bearer {slack_token}"}

        if not url:
            raise ValueError("No Slack webhook URL provided.")  # noqa: TRY003 # FIXME CoP

        try:
            response = session.post(url=url, headers=headers, json=payload)
            response.raise_for_status()
        except requests.ConnectionError:
            logger.warning(f"Failed to connect to Slack webhook after {10} retries.")
            return {"slack_notification_result": None}
        except requests.HTTPError:
            logger.warning(
                f"Request to Slack webhook returned error {response.status_code}: {response.text}"  # type: ignore[possibly-undefined] # ok for httperror
            )
            return {"slack_notification_result": None}

        return {"slack_notification_result": "Slack notification succeeded."}


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
    """  # noqa: E501 # FIXME CoP

    type: Literal["pagerduty"] = "pagerduty"

    api_key: str
    routing_key: str
    notify_on: NotifyOn = "failure"
    severity: Literal["critical", "error", "warning", "info"] = "critical"

    @override
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        success = checkpoint_result.success or False
        checkpoint_name = checkpoint_result.checkpoint_config.name
        summary = f"Great Expectations Checkpoint {checkpoint_name} has "
        if success:
            summary += "succeeded"
        else:
            summary += "failed"
        max_severity = self._get_max_severity_failure_from_checkpoint_result(checkpoint_result)

        return self._run_pypd_alert(
            dedup_key=checkpoint_name, message=summary, success=success, max_severity=max_severity
        )

    def _run_pypd_alert(
        self,
        dedup_key: str,
        message: str,
        success: bool,
        max_severity: Optional[FailureSeverity] = None,
    ):
        if should_notify(success=success, notify_on=self.notify_on, max_severity=max_severity):
            pypd.api_key = self.api_key
            pypd.EventV2.create(
                data={
                    "routing_key": self.routing_key,
                    "dedup_key": dedup_key,
                    "event_action": "trigger",
                    "payload": {
                        "summary": message,
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

    Args:
        teams_webhook: Incoming Microsoft Teams webhook to which to send notifications.
        notify_on: Specifies validation status that triggers notification. One of "all", "failure", "success".
    """  # noqa: E501 # FIXME CoP

    type: Literal["microsoft"] = "microsoft"

    teams_webhook: Union[ConfigStr, str]
    notify_on: NotifyOn = "all"
    renderer: MicrosoftTeamsRenderer = Field(default_factory=MicrosoftTeamsRenderer)

    @validator("renderer", pre=True)
    def _validate_renderer(cls, renderer: dict | MicrosoftTeamsRenderer) -> MicrosoftTeamsRenderer:
        if isinstance(renderer, dict):
            _renderer = _build_renderer(config=renderer)
            if not isinstance(_renderer, MicrosoftTeamsRenderer):
                raise ValueError(  # noqa: TRY003, TRY004 # FIXME CoP
                    "renderer must be a MicrosoftTeamsRenderer or a valid configuration for one."
                )
            renderer = _renderer
        return renderer

    @override
    def run(self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None):
        success = checkpoint_result.success or False
        max_severity = self._get_max_severity_failure_from_checkpoint_result(checkpoint_result)

        if not should_notify(success=success, notify_on=self.notify_on, max_severity=max_severity):
            return {"microsoft_teams_notification_result": None}

        data_docs_pages = self._get_data_docs_pages_from_prior_action(action_context=action_context)

        payload = self.renderer.render(
            checkpoint_result=checkpoint_result,
            data_docs_pages=data_docs_pages,
        )

        # this will actually sent the POST request to the Microsoft Teams webapp server
        teams_notif_result = self._send_microsoft_teams_notifications(payload=payload)

        return {"microsoft_teams_notification_result": teams_notif_result}

    def _send_microsoft_teams_notifications(self, payload: dict) -> str | None:
        config_provider = self._resolve_context().context.config_provider
        webhook = self._substitute_config_str_if_needed(self.teams_webhook, config_provider)
        if not webhook:  # Necessary to appease mypy; this is guaranteed.
            raise ValueError("No Microsoft Teams webhook URL provided.")  # noqa: TRY003 # FIXME CoP

        session = requests.Session()
        try:
            response = session.post(url=webhook, json=payload)
            response.raise_for_status()
        except requests.ConnectionError:
            logger.warning("Failed to connect to Microsoft Teams webhook after 10 retries.")
            return None
        except requests.HTTPError as e:
            logger.warning(
                f"Request to Microsoft Teams API returned error {response.status_code}: {e}"  # type: ignore[possibly-undefined] # ok for httperror
            )
            return None

        return "Microsoft Teams notification succeeded."


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
    """  # noqa: E501 # FIXME CoP

    type: Literal["opsgenie"] = "opsgenie"

    api_key: str
    region: Optional[str] = None
    priority: Literal["P1", "P2", "P3", "P4", "P5"] = "P3"
    notify_on: NotifyOn = "failure"
    tags: Optional[List[str]] = None
    renderer: OpsgenieRenderer = Field(default_factory=OpsgenieRenderer)

    @validator("renderer", pre=True)
    def _validate_renderer(cls, renderer: dict | OpsgenieRenderer) -> OpsgenieRenderer:
        if isinstance(renderer, dict):
            _renderer = _build_renderer(config=renderer)
            if not isinstance(_renderer, OpsgenieRenderer):
                raise ValueError(  # noqa: TRY003, TRY004 # FIXME CoP
                    "renderer must be a OpsgenieRenderer or a valid configuration for one."
                )
            renderer = _renderer
        return renderer

    @override
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        validation_success = checkpoint_result.success or False
        checkpoint_name = checkpoint_result.checkpoint_config.name
        max_severity = self._get_max_severity_failure_from_checkpoint_result(checkpoint_result)

        if should_notify(
            success=validation_success, notify_on=self.notify_on, max_severity=max_severity
        ):
            settings = {
                "api_key": self.api_key,
                "region": self.region,
                "priority": self.priority,
                "tags": self.tags,
            }

            description = self.renderer.render(checkpoint_result=checkpoint_result)

            message = f"Great Expectations Checkpoint {checkpoint_name} "
            if checkpoint_result.success:
                message += "succeeded!"
            else:
                message += "failed!"

            alert_result = self._send_opsgenie_alert(
                query=description, message=message, settings=settings
            )

            return {"opsgenie_alert_result": alert_result}
        else:
            return {"opsgenie_alert_result": "No alert sent"}

    def _send_opsgenie_alert(self, query: str, message: str, settings: dict) -> bool:
        """Creates an alert in Opsgenie."""
        if settings["region"] is not None:
            # accommodate for Europeans
            url = f"https://api.{settings['region']}.opsgenie.com/v2/alerts"
        else:
            url = "https://api.opsgenie.com/v2/alerts"

        headers = {"Authorization": f"GenieKey {settings['api_key']}"}
        payload = {
            "message": message,
            "description": query,
            "priority": settings["priority"],  # allow this to be modified in settings
            "tags": settings["tags"],
        }

        session = requests.Session()

        try:
            response = session.post(url, headers=headers, json=payload)
            response.raise_for_status()
        except requests.ConnectionError as e:
            logger.warning(f"Failed to connect to Opsgenie: {e}")
            return False
        except requests.HTTPError as e:
            logger.warning(f"Request to Opsgenie API returned error {response.status_code}: {e}")  # type: ignore[possibly-undefined] # ok for httperror
            return False
        return True


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
        renderer: Specifies the renderer used to generate an email.
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

    Examples:
        **renderer:**

        ```python
        {
           "module_name": "great_expectations.render.renderer.email_renderer",
           "class_name": "EmailRenderer",
        }
        ```
    """  # noqa: E501 # FIXME CoP

    type: Literal["email"] = "email"

    smtp_address: Union[ConfigStr, str]
    smtp_port: Union[ConfigStr, str]
    receiver_emails: Union[ConfigStr, str]
    sender_login: Optional[Union[ConfigStr, str]] = None
    sender_password: Optional[Union[ConfigStr, str]] = None
    sender_alias: Optional[Union[ConfigStr, str]] = None
    use_tls: Optional[bool] = None
    use_ssl: Optional[bool] = None
    notify_on: NotifyOn = "all"
    notify_with: Optional[List[str]] = None
    renderer: EmailRenderer = Field(default_factory=EmailRenderer)

    @validator("renderer", pre=True)
    def _validate_renderer(cls, renderer: dict | EmailRenderer) -> EmailRenderer:
        if isinstance(renderer, dict):
            _renderer = _build_renderer(config=renderer)
            if not isinstance(_renderer, EmailRenderer):
                raise ValueError(  # noqa: TRY003, TRY004 # FIXME CoP
                    "renderer must be a EmailRenderer or a valid configuration for one."
                )
            renderer = _renderer
        return renderer

    @root_validator
    def _root_validate_email_params(cls, values: dict) -> dict:
        if not values["sender_alias"]:
            values["sender_alias"] = values["sender_login"]

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
    def run(
        self,
        checkpoint_result: CheckpointResult,
        action_context: ActionContext | None = None,
    ) -> dict:
        success = checkpoint_result.success or False
        max_severity = self._get_max_severity_failure_from_checkpoint_result(checkpoint_result)

        if not should_notify(success=success, notify_on=self.notify_on, max_severity=max_severity):
            return {"email_result": ""}

        title, html = self.renderer.render(checkpoint_result=checkpoint_result)
        config_provider = self._resolve_context().context.config_provider
        substituted_receiver_emails = (
            self._substitute_config_str_if_needed(self.receiver_emails, config_provider) or ""
        )

        receiver_emails_list = list(
            map(lambda x: x.strip(), substituted_receiver_emails.split(","))
        )

        # this will actually send the email
        email_result = self._send_email(
            title=title,
            html=html,
            receiver_emails_list=receiver_emails_list,
        )

        # sending payload back as dictionary
        return {"email_result": email_result}

    def _send_email(  # noqa: C901 # FIXME CoP
        self,
        title,
        html,
        receiver_emails_list,
    ):
        config_provider = self._resolve_context().context.config_provider
        smtp_address = self._substitute_config_str_if_needed(self.smtp_address, config_provider)
        smtp_port = self._substitute_config_str_if_needed(self.smtp_port, config_provider)
        sender_login = self._substitute_config_str_if_needed(self.sender_login, config_provider)
        sender_password = self._substitute_config_str_if_needed(
            self.sender_password, config_provider
        )
        sender_alias = self._substitute_config_str_if_needed(self.sender_alias, config_provider)

        msg = MIMEMultipart()
        msg["From"] = sender_alias
        msg["To"] = ", ".join(receiver_emails_list)
        msg["Subject"] = title
        msg.attach(MIMEText(html, "html"))
        try:
            if self.use_ssl:
                if self.use_tls:
                    logger.warning("Please choose between SSL or TLS, will default to SSL")
                context = ssl.create_default_context()
                mailserver = smtplib.SMTP_SSL(smtp_address, smtp_port, context=context)
            elif self.use_tls:
                mailserver = smtplib.SMTP(smtp_address, smtp_port)
                context = ssl.create_default_context()
                mailserver.starttls(context=context)
            else:
                logger.warning("Not using TLS or SSL to send an email is not secure")
                mailserver = smtplib.SMTP(smtp_address, smtp_port)
            if sender_login is not None and sender_password is not None:
                mailserver.login(sender_login, sender_password)
            elif not (sender_login is None and sender_password is None):
                logger.error(
                    "Please specify both sender_login and sender_password or specify both as None"
                )
            mailserver.sendmail(sender_alias, receiver_emails_list, msg.as_string())
            mailserver.quit()
        except smtplib.SMTPConnectError:
            logger.error(f"Failed to connect to the SMTP server at address: {smtp_address}")  # noqa: TRY400 # FIXME CoP
        except smtplib.SMTPAuthenticationError:
            logger.error(f"Failed to authenticate to the SMTP server at address: {smtp_address}")  # noqa: TRY400 # FIXME CoP
        except Exception as e:
            logger.error(str(e))  # noqa: TRY400 # FIXME CoP
        else:
            return "success"


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
    """  # noqa: E501 # FIXME CoP

    type: Literal["update_data_docs"] = "update_data_docs"

    site_names: List[str] = []

    @override
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        action_results: dict[ValidationResultIdentifier, dict[str, str]] = {}
        for result_identifier, result in checkpoint_result.run_results.items():
            suite_name = result.suite_name

            expectation_suite_identifier: ExpectationSuiteIdentifier | GXCloudIdentifier
            if self._using_cloud_context:
                expectation_suite_identifier = GXCloudIdentifier(
                    resource_type=GXCloudRESTResource.EXPECTATION_SUITE, resource_name=suite_name
                )
            else:
                expectation_suite_identifier = ExpectationSuiteIdentifier(name=suite_name)

            action_result = self._run(
                validation_result_suite=result,
                validation_result_suite_identifier=result_identifier,
                expectation_suite_identifier=expectation_suite_identifier,
            )
            action_results[result_identifier] = action_result

        return action_results

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[ValidationResultIdentifier, GXCloudIdentifier],
        action_context=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        logger.debug("UpdateDataDocsAction.run")

        if validation_result_suite is None:
            logger.warning(
                f"No validation_result_suite was passed to {type(self).__name__} action. Skipping action."  # noqa: E501 # FIXME CoP
            )
            return

        if not isinstance(
            validation_result_suite_identifier,
            (ValidationResultIdentifier, GXCloudIdentifier),
        ):
            raise TypeError(  # noqa: TRY003 # FIXME CoP
                "validation_result_id must be of type ValidationResultIdentifier or"
                f" GeCloudIdentifier, not {type(validation_result_suite_identifier)}"
            )

        # TODO Update for RenderedDataDocs
        # build_data_docs will return the index page for the validation results, but we want to return the url for the validation result using the code below  # noqa: E501 # FIXME CoP
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
    """  # noqa: E501 # FIXME CoP

    type: Literal["sns"] = "sns"

    sns_topic_arn: str
    sns_message_subject: Optional[str]

    @override
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        msg = self._send_sns_notification(
            sns_subject=self.sns_message_subject or checkpoint_result.name,
            validation_results=json.dumps(
                [result.to_json_dict() for result in checkpoint_result.run_results.values()],
                indent=4,
            ),
        )
        return {"result": msg}

    def _send_sns_notification(self, sns_subject: str, validation_results: str, **kwargs) -> str:
        """
        Send JSON results to an SNS topic with a schema of:

        :param sns_subject: : The SNS Message Subject - defaults to suite_identifier.name
        :param validation_results:  The results of the validation ran
        :param kwargs:  Keyword arguments to pass to the boto3 Session
        :return:  Message ID that was published or error message

        """
        if not aws.boto3:
            logger.warning("boto3 is not installed")
            return "boto3 is not installed"

        message_dict = {
            "TopicArn": self.sns_topic_arn,
            "Subject": sns_subject,
            "Message": json.dumps(validation_results),
            "MessageAttributes": {
                "String": {"DataType": "String.Array", "StringValue": "ValidationResults"},
            },
            "MessageStructure": "json",
        }
        session = aws.boto3.Session(**kwargs)
        sns = session.client("sns")
        try:
            response = sns.publish(**message_dict)
        except sns.exceptions.InvalidParameterException:
            error_msg = f"Received invalid for message: {validation_results}"
            logger.error(error_msg)  # noqa: TRY400 # FIXME CoP
            return error_msg
        else:
            return (
                f"Successfully posted results to {response['MessageId']} with Subject {sns_subject}"
            )


class APINotificationAction(ValidationAction):
    type: Literal["api"] = "api"

    url: str

    @override
    def run(
        self, checkpoint_result: CheckpointResult, action_context: ActionContext | None = None
    ) -> dict:
        aggregate_payload = []
        for run_id, run_result in checkpoint_result.run_results.items():
            suite_name = run_result.suite_name
            serializable_results = convert_to_json_serializable(run_result.results)
            batch_identifier = run_id.batch_identifier

            payload = self.create_payload(
                data_asset_name=batch_identifier,
                suite_name=suite_name,
                validation_results_serializable=serializable_results,
            )
            aggregate_payload.append(payload)

        response = self.send_results(aggregate_payload)
        return {"result": f"Posted results to API, status code - {response.status_code}"}

    def send_results(self, payload) -> requests.Response:
        try:
            headers = {"Content-Type": "application/json"}
            return requests.post(self.url, headers=headers, data=payload)
        except Exception as e:
            print(f"Exception when sending data to API - {e}")
            raise e  # noqa: TRY201 # FIXME CoP

    @staticmethod
    def create_payload(data_asset_name, suite_name, validation_results_serializable) -> dict:
        return {
            "test_suite_name": suite_name,
            "data_asset_name": data_asset_name,
            "validation_results": validation_results_serializable,
        }
