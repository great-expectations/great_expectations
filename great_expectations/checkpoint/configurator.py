import copy
import logging
from typing import Dict, List, Optional, Union

from ruamel.yaml.comments import CommentedMap

from great_expectations.checkpoint.util import (
    batch_request_in_validations_contains_batch_data,
    get_validations_with_batch_request_as_dict,
)
from great_expectations.core.batch import (
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
)
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    checkpointConfigSchema,
)
from great_expectations.util import is_list_of_strings, is_sane_slack_webhook

logger = logging.getLogger(__name__)


class ActionDicts:
    STORE_VALIDATION_RESULT = {
        "name": "store_validation_result",
        "action": {"class_name": "StoreValidationResultAction"},
    }
    STORE_EVALUATION_PARAMS = {
        "name": "store_evaluation_params",
        "action": {"class_name": "StoreEvaluationParametersAction"},
    }
    UPDATE_DATA_DOCS = {
        "name": "update_data_docs",
        "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
    }

    @staticmethod
    def build_slack_action(webhook, notify_on, notify_with):
        return {
            "name": "send_slack_notification",
            "action": {
                "class_name": "SlackNotificationAction",
                "slack_webhook": webhook,
                "notify_on": notify_on,
                "notify_with": notify_with,
                "renderer": {
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            },
        }


class SimpleCheckpointConfigurator:
    """
    SimpleCheckpointBuilder is a convenience class to easily configure a simple
    Checkpoint.
    """

    def __init__(
        self,
        name: str,
        data_context,
        site_names: Union[None, str, List[str]] = "all",
        slack_webhook: Optional[str] = None,
        notify_on: str = "all",
        notify_with: Union[str, List[str]] = "all",
        **kwargs,
    ):
        """
        After instantiation, call the .build() method to get a new Checkpoint.

        By default, the Checkpoint created will:
        1. store the validation result
        2. store evaluation parameters
        3. update all data docs sites

        When configured, this builds a Checkpoint that sends a slack message.

        Args:
            name: The Checkpoint name
            data_context: a valid DataContext
            site_names: Names of sites to update. Defaults to "all". Set to None to skip updating data docs.
            slack_webhook: If present, a sleck message will be sent.
            notify_on: When to send a slack notification. Defaults to "all". Possible values: "all", "failure", "success"
            notify_with: optional list of DataDocs site names to display in Slack message. Defaults to showing all

        Examples:

        Most simple usage:
        ```
        checkpoint = SimpleCheckpointBuilder("foo", data_context).build()
        ```

        A Checkpoint that sends a slack message on all validation events.
        ```
        checkpoint = SimpleCheckpointBuilder(
            "foo",
            data_context,
            slack_webhook="https://hooks.slack.com/foo/bar"
        ).build()
        ```

        A Checkpoint that does not update data docs.
        ```
        checkpoint = SimpleCheckpointBuilder("foo", data_context, site_names=None).build()
        ```
        """
        self.name = name
        self.data_context = data_context
        self.site_names = site_names
        self.notify_on = notify_on
        self.notify_with = notify_with
        self.slack_webhook = slack_webhook
        self.other_kwargs = kwargs

    def build(self) -> CheckpointConfig:
        """Build a Checkpoint."""
        self._validate_site_names(self.data_context)
        self._validate_notify_on()
        self._validate_notify_with()
        self._validate_slack_webhook()
        self._validate_slack_configuration()

        return self._build_checkpoint_config()

    def _build_checkpoint_config(self) -> CheckpointConfig:
        action_list = self._default_action_list()
        if self.site_names:
            action_list = self._add_update_data_docs_action(action_list)
        if self.slack_webhook:
            action_list = self._add_slack_action(action_list)

        config_kwargs: dict = self.other_kwargs or {}

        # DataFrames shouldn't be saved to CheckpointStore
        batch_request = config_kwargs.get("batch_request")
        if batch_request_contains_batch_data(batch_request=batch_request):
            config_kwargs.pop("batch_request", None)
        else:
            config_kwargs["batch_request"] = get_batch_request_as_dict(
                batch_request=batch_request
            )

        # DataFrames shouldn't be saved to CheckpointStore
        validations = config_kwargs.get("validations")
        if batch_request_in_validations_contains_batch_data(validations=validations):
            config_kwargs.pop("validations", [])
        else:
            config_kwargs["validations"] = get_validations_with_batch_request_as_dict(
                validations=validations
            )

        specific_config_kwargs_overrides: dict = {
            "config_version": 1.0,
            "name": self.name,
            "class_name": "Checkpoint",
            "action_list": action_list,
            "ge_cloud_id": self.other_kwargs.pop("ge_cloud_id", None),
        }
        config_kwargs.update(specific_config_kwargs_overrides)

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        checkpoint_config: dict = checkpointConfigSchema.load(
            CommentedMap(**config_kwargs)
        )
        config_kwargs = checkpointConfigSchema.dump(checkpoint_config)

        logger.debug(
            f"SimpleCheckpointConfigurator built this CheckpointConfig:"
            f"{checkpoint_config}"
        )
        return CheckpointConfig(**config_kwargs)

    @staticmethod
    def _default_action_list() -> List[Dict]:
        return [
            ActionDicts.STORE_VALIDATION_RESULT,
            ActionDicts.STORE_EVALUATION_PARAMS,
        ]

    def _add_update_data_docs_action(self, action_list) -> List[Dict]:
        update_docs_action = copy.deepcopy(ActionDicts.UPDATE_DATA_DOCS)
        if isinstance(self.site_names, list):
            update_docs_action["action"]["site_names"] = self.site_names
        action_list.append(update_docs_action)
        return action_list

    def _add_slack_action(self, action_list: List[Dict]) -> List[Dict]:
        """
        The underlying SlackNotificationAction and SlackRenderer default to
        including links to all sites if the key notify_with is not present. We
        are intentionally hiding this from users of SimpleCheckpoint by having a
        default of "all" that sets the configuration appropriately.
        """
        _notify_with = self.notify_with
        if self.notify_with == "all":
            _notify_with = None
        action_list.append(
            ActionDicts.build_slack_action(
                self.slack_webhook, self.notify_on, _notify_with
            )
        )
        return action_list

    def _validate_site_names(self, data_context):
        if not (
            self.site_names is None
            or self.site_names == "all"
            or is_list_of_strings(self.site_names)
            and set(self.site_names).issubset(set(data_context.get_site_names()))
        ):
            raise TypeError(
                "site_names must be one of: None, 'all', or a list of site names to update"
            )
        if self.site_names in ["all", None]:
            return

    def _validate_notify_on(self):
        if self.notify_on not in ["all", "success", "failure"]:
            raise ValueError("notify_on must be one of: 'all', 'failure', 'success'")

    def _validate_notify_with(self):
        if not self.notify_with:
            return
        if self.notify_with == "all":
            return
        if not is_list_of_strings(self.notify_with):
            raise ValueError("notify_with must be a list of site names")

    def _validate_slack_webhook(self):
        if self.slack_webhook and not is_sane_slack_webhook(self.slack_webhook):
            raise ValueError("Please provide a valid slack webhook")

    def _validate_slack_configuration(self):
        """Guide the user toward correct configuration."""
        if isinstance(self.notify_with, list) and self.slack_webhook is None:
            raise ValueError(
                "It appears you wish to send a slack message because you "
                "specified a list of sites in the notify_with parameter but "
                "you have not yet specified a slack_webhook. Please either "
                "specify a slack webhook or remove the notify_with parameter."
            )
        if self.notify_on != "all" and self.slack_webhook is None:
            raise ValueError(
                "It appears you wish to send a slack message because you "
                "specified a condition in the notify_on parameter but "
                "you have not yet specified a slack_webhook. Please either "
                "specify a slack webhook or remove the notify_on parameter."
            )
