"""
An action is a way to take an arbitrary method and make it configurable and runnable within a Data Context.

The only requirement from an action is for it to have a take_action method.
"""

import logging
import warnings

try:
    import pypd
except ImportError:
    pypd = None


from great_expectations.data_context.util import instantiate_class_from_config

from ..data_context.store.metric_store import MetricStore
from ..data_context.types.resource_identifiers import ValidationResultIdentifier
from ..exceptions import ClassInstantiationError, DataContextError
from .util import send_slack_notification, send_opsgenie_alert

logger = logging.getLogger(__name__)


class ValidationAction:
    """
    This is the base class for all actions that act on validation results
    and are aware of a Data Context namespace structure.

    The Data Context is passed to this class in its constructor.
    """

    def __init__(self, data_context):
        self.data_context = data_context

    def run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        **kwargs,
    ):
        """

        :param validation_result_suite:
        :param validation_result_suite_identifier:
        :param data_asset:
        :param: kwargs - any additional arguments the child might use
        :return:
        """
        return self._run(
            validation_result_suite,
            validation_result_suite_identifier,
            data_asset,
            **kwargs,
        )

    def _run(
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        return NotImplementedError


class NoOpAction(ValidationAction):
    def __init__(
        self,
        data_context,
    ):
        super().__init__(data_context)

    def _run(
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        print("Happily doing nothing")


class SlackNotificationAction(ValidationAction):
    """
SlackNotificationAction sends a Slack notification to a given webhook.

**Configuration**

.. code-block:: yaml

    - name: send_slack_notification_on_validation_result
    action:
      class_name: StoreValidationResultAction
      # put the actual webhook URL in the uncommitted/config_variables.yml file
      slack_webhook: ${validation_notification_slack_webhook}
      notify_on: all # possible values: "all", "failure", "success"
      notify_with: # optional list of DataDocs site names to display in Slack message. Defaults to showing all
      renderer:
        # the class that implements the message to be sent
        # this is the default implementation, but you can
        # implement a custom one
        module_name: great_expectations.render.renderer.slack_renderer
        class_name: SlackRenderer

    """

    def __init__(
        self, data_context, renderer, slack_webhook, notify_on="all", notify_with=None,
    ):
        """Construct a SlackNotificationAction

        Args:
            data_context:
            renderer: dictionary specifying the renderer used to generate a query consumable by Slack API, for example:
                {
                   "module_name": "great_expectations.render.renderer.slack_renderer",
                   "class_name": "SlackRenderer",
               }
            slack_webhook: incoming Slack webhook to which to send notification
            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification
            payload: *Optional* payload from other ValidationActions
        """
        super().__init__(data_context)
        self.renderer = instantiate_class_from_config(
            config=renderer, runtime_environment={}, config_defaults={},
        )
        module_name = renderer["module_name"]
        if not self.renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=renderer["class_name"],
            )
        self.slack_webhook = slack_webhook
        assert slack_webhook, "No Slack webhook found in action config."
        self.notify_on = notify_on
        self.notify_with = notify_with

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset=None,
        payload=None,
    ):
        logger.debug("SlackNotificationAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
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
            query = self.renderer.render(
                validation_result_suite, data_docs_pages, self.notify_with
            )
            # this will actually sent the POST request to the Slack webapp server
            slack_notif_result = send_slack_notification(
                query, slack_webhook=self.slack_webhook
            )

            # sending payload back as dictionary
            return {"slack_notification_result": slack_notif_result}
        else:
            return {"slack_notification_result": ""}


class PagerdutyAlertAction(ValidationAction):
    """
PagerdutyAlertAction sends a pagerduty event

**Configuration**

.. code-block:: yaml

    - name: send_pagerduty_alert_on_validation_result
    action:
      class_name: PagerdutyAlertAction
      api_key: ${pagerduty_api_key} # Events API v2 key
      routing_key: # The 32 character Integration Key for an integration on a service or on a global ruleset.
      notify_on: failure # possible values: "all", "failure", "success"

    """

    def __init__(
        self, data_context, api_key, routing_key, notify_on="failure",
    ):
        """Construct a PagerdutyAlertAction

        Args:
            data_context:
            api_key: Events API v2 key for pagerduty.
            routing_key: The 32 character Integration Key for an integration on a service or on a global ruleset.
            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification
        """
        super().__init__(data_context)
        if not pypd:
            raise DataContextError("ModuleNotFoundError: No module named 'pypd'")
        self.api_key = api_key
        assert api_key, "No Pagerduty api_key found in action config."
        self.routing_key = routing_key
        assert routing_key, "No Pagerduty routing_key found in action config."
        self.notify_on = notify_on

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset=None,
        payload=None,
    ):
        logger.debug("PagerdutyAlertAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
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
                        "severity": "critical",
                        "source": "Great Expectations",
                    },
                }
            )

            return {"pagerduty_alert_result": "success"}
        return {"pagerduty_alert_result": "none sent"}


class OpsgenieAlertAction(ValidationAction):
    """
    OpsgenieAlertAction creates and sends an Opsgenie alert

    **Configuration**

    .. code-block:: yaml

        - name: send_opsgenie_alert_on_validation_result
        action:
          class_name: OpsgenieAlertAction
          # put the actual webhook URL in the uncommitted/config_variables.yml file
          api_key: ${opsgenie_api_key} # Opsgenie API key
          region: specifies the Opsgenie region. Populate 'EU' for Europe otherwise leave empty
          priority: specify the priority of the alert (P1 - P5) defaults to P3
          notify_on: failure # possible values: "all", "failure", "success"

    """

    def __init__(
        self,
        data_context,
        renderer,
        api_key,
        region=None,
        priority="P3",
        notify_on="failure",
    ):
        """Construct a OpsgenieAlertAction

        Args:
            data_context:
            api_key: Opsgenie API key
            region: specifies the Opsgenie region. Populate 'EU' for Europe otherwise do not set
            priority: specify the priority of the alert (P1 - P5) defaults to P3
            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification
        """
        super().__init__(data_context)
        self.renderer = instantiate_class_from_config(
            config=renderer,
            runtime_environment={},
            config_defaults={},
        )
        module_name = renderer["module_name"]
        if not self.renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=renderer["class_name"],
            )

        self.api_key = api_key
        assert api_key, "opsgenie_api_key missing in config_variables.yml"
        self.region = region
        self.priority = priority
        self.notify_on = notify_on

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset=None,
        payload=None,
    ):
        logger.debug("OpsgenieAlertAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
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
            }

            description = self.renderer.render(validation_result_suite, None, None)

            alert_result = send_opsgenie_alert(
                description, expectation_suite_name, settings
            )

            return {"opsgenie_alert_result": alert_result}
        else:
            return {"opsgenie_alert_result": ""}


class StoreValidationResultAction(ValidationAction):
    """
    StoreValidationResultAction stores a validation result in the ValidationsStore.

**Configuration**

.. code-block:: yaml

    - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
      # name of the store where the actions will store validation results
      # the name must refer to a store that is configured in the great_expectations.yml file
      target_store_name: validations_store

    """

    def __init__(
        self, data_context, target_store_name=None,
    ):
        """

        :param data_context: Data Context
        :param target_store_name: the name of the param_store in the Data Context which
                should be used to param_store the validation result
        """

        super().__init__(data_context)
        if target_store_name is None:
            self.target_store = data_context.stores[data_context.validations_store_name]
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        payload=None,
    ):
        logger.debug("StoreValidationResultAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.target_store.set(
            validation_result_suite_identifier, validation_result_suite
        )


class StoreEvaluationParametersAction(ValidationAction):
    """
StoreEvaluationParametersAction extracts evaluation parameters from a validation result and stores them in the store
configured for this action.

Evaluation parameters allow expectations to refer to statistics/metrics computed
in the process of validating other prior expectations.

**Configuration**

.. code-block:: yaml

    - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
      # name of the store where the action will store the parameters
      # the name must refer to a store that is configured in the great_expectations.yml file
      target_store_name: evaluation_parameter_store

    """

    def __init__(self, data_context, target_store_name=None):
        """

        Args:
            data_context: Data Context
            target_store_name: the name of the store in the Data Context which
                should be used to store the evaluation parameters
        """
        super().__init__(data_context)

        if target_store_name is None:
            self.target_store = data_context.evaluation_parameter_store
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        payload=None,
    ):
        logger.debug("StoreEvaluationParametersAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.data_context.store_evaluation_parameters(validation_result_suite)


class StoreMetricsAction(ValidationAction):
    """
StoreMetricsAction extracts metrics from a Validation Result and stores them
in a metrics store.

**Configuration**

.. code-block:: yaml

    - name: store_evaluation_params
    action:
      class_name: StoreMetricsAction
      # name of the store where the action will store the metrics
      # the name must refer to a store that is configured in the great_expectations.yml file
      target_store_name: my_metrics_store

    """

    def __init__(
        self, data_context, requested_metrics, target_store_name="metrics_store"
    ):
        """

        Args:
            data_context: Data Context
            requested_metrics: dictionary of metrics to store. Dictionary should have the following structure:

                expectation_suite_name:
                    metric_name:
                        - metric_kwargs_id

                You may use "*" to denote that any expectation suite should match.
            target_store_name: the name of the store in the Data Context which
                should be used to store the metrics
        """
        super().__init__(data_context)
        self._requested_metrics = requested_metrics
        self._target_store_name = target_store_name
        try:
            store = data_context.stores[target_store_name]
        except KeyError:
            raise DataContextError(
                "Unable to find store {} in your DataContext configuration.".format(
                    target_store_name
                )
            )
        if not isinstance(store, MetricStore):
            raise DataContextError(
                "StoreMetricsAction must have a valid MetricsStore for its target store."
            )

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        payload=None,
    ):
        logger.debug("StoreMetricsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.data_context.store_validation_result_metrics(
            self._requested_metrics, validation_result_suite, self._target_store_name
        )


class UpdateDataDocsAction(ValidationAction):
    """
UpdateDataDocsAction is a validation action that
notifies the site builders of all the data docs sites of the Data Context
that a validation result should be added to the data docs.

**Configuration**

.. code-block:: yaml

    - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction

You can also instruct ``UpdateDataDocsAction`` to build only certain sites by providing a ``site_names`` key with a
list of sites to update:

    - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names:
        - production_site

    """

    def __init__(self, data_context, site_names=None, target_site_names=None):
        """
        :param data_context: Data Context
        :param site_names: *optional* List of site names for building data docs
        """
        super().__init__(data_context)
        if target_site_names:
            warnings.warn(
                "target_site_names is deprecated. Please use site_names instead.",
                DeprecationWarning,
            )
            if site_names:
                raise DataContextError(
                    "Invalid configuration: legacy key target_site_names and site_names key are "
                    "both present in UpdateDataDocsAction configuration"
                )
            site_names = target_site_names
        self._site_names = site_names

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        payload=None,
    ):
        logger.debug("UpdateDataDocsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {}".format(
                    type(validation_result_suite_identifier)
                )
            )

        # build_data_docs will return the index page for the validation results, but we want to return the url for the valiation result using the code below
        data_docs_index_pages = self.data_context.build_data_docs(
            site_names=self._site_names,
            resource_identifiers=[validation_result_suite_identifier],
        )

        # get the URL for the validation result
        docs_site_urls_list = self.data_context.get_docs_sites_urls(
            resource_identifier=validation_result_suite_identifier,
            site_names=self._site_names,
        )
        # process payload
        data_docs_validation_results = {}
        for sites in docs_site_urls_list:
            data_docs_validation_results[sites["site_name"]] = sites["site_url"]

        return data_docs_validation_results
