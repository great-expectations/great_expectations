import logging
import warnings

from great_expectations.data_context.util import instantiate_class_from_config

from ..data_context.store.metric_store import MetricStore
from ..data_context.types.resource_identifiers import ValidationResultIdentifier
from ..exceptions import ClassInstantiationError, DataContextError
from .util import send_slack_notification

logger = logging.getLogger(__name__)


class ValidationAction(object):
    """
    This is the base class for all actions that act on validation results
    and are aware of a data context namespace structure.

    The data context is passed to this class in its constructor.
    """

    def __init__(self, data_context):
        self.data_context = data_context

    def run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset,
        **kwargs
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
            **kwargs
        )

    def _run(
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        return NotImplementedError


class NoOpAction(ValidationAction):
    def __init__(
        self, data_context,
    ):
        super(NoOpAction, self).__init__(data_context)

    def _run(
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        print("Happily doing nothing")


class SlackNotificationAction(ValidationAction):
    """
    SlackNotificationAction sends a Slack notification to a given webhook.

    Example config:
    {
        "renderer": {
            "module_name": "great_expectations.render.renderer.slack_renderer",
            "class_name": "SlackRenderer",
        },
        "slack_webhook": "https://example_webhook",
        "notify_on": "all"
    }
    """

    def __init__(
        self, data_context, renderer, slack_webhook, notify_on="all",
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
        """
        super(SlackNotificationAction, self).__init__(data_context)
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

    def _run(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        data_asset=None,
    ):
        logger.debug("SlackNotificationAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_suite_id must be of type ValidationResultIdentifier, not {0}".format(
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
            query = self.renderer.render(validation_result_suite)
            return send_slack_notification(query, slack_webhook=self.slack_webhook)
        else:
            return


class StoreValidationResultAction(ValidationAction):
    """
    StoreValidationResultAction stores a validation result in the ValidationsStore.
    """

    def __init__(
        self, data_context, target_store_name=None,
    ):
        """

        :param data_context: data context
        :param target_store_name: the name of the param_store in the data context which
                should be used to param_store the validation result
        """

        super(StoreValidationResultAction, self).__init__(data_context)
        if target_store_name is None:
            self.target_store = data_context.stores[data_context.validations_store_name]
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        logger.debug("StoreValidationResultAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.target_store.set(
            validation_result_suite_identifier, validation_result_suite
        )


class StoreEvaluationParametersAction(ValidationAction):
    """
    StoreEvaluationParametersAction is a namespeace-aware validation action that
    extracts evaluation parameters from a validation result and stores them in the param_store
    configured for this action.

    Evaluation parameters allow expectations to refer to statistics/metrics computed
    in the process of validating other prior expectations.
    """

    def __init__(self, data_context, target_store_name=None):
        """

        Args:
            data_context: data context
            target_store_name: the name of the store in the data context which
                should be used to store the evaluation parameters
        """
        super(StoreEvaluationParametersAction, self).__init__(data_context)

        if target_store_name is None:
            self.target_store = data_context.evaluation_parameter_store
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        logger.debug("StoreEvaluationParametersAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.data_context.store_evaluation_parameters(validation_result_suite)


class StoreMetricsAction(ValidationAction):
    def __init__(
        self, data_context, requested_metrics, target_store_name="metrics_store"
    ):
        """

        Args:
            data_context: data context
            requested_metrics: dictionary of metrics to store. Dictionary should have the following structure:

                expectation_suite_name:
                    metric_name:
                        - metric_kwargs_id

                You may use "*" to denote that any expectation suite should match.
            target_store_name: the name of the store in the data context which
                should be used to store the metrics
        """
        super(StoreMetricsAction, self).__init__(data_context)
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
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        logger.debug("StoreMetricsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.data_context.store_validation_result_metrics(
            self._requested_metrics, validation_result_suite, self._target_store_name
        )


class UpdateDataDocsAction(ValidationAction):
    """
    UpdateDataDocsAction is a namespace-aware validation action that
    notifies the site builders of all the data docs sites of the data context
    that a validation result should be added to the data docs.
    """

    def __init__(self, data_context, site_names=None, target_site_names=None):
        """
        :param data_context: data context
        :param site_names: *optional* List of site names for building data docs
        """
        super(UpdateDataDocsAction, self).__init__(data_context)
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
        self, validation_result_suite, validation_result_suite_identifier, data_asset
    ):
        logger.debug("UpdateDataDocsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(
            validation_result_suite_identifier, ValidationResultIdentifier
        ):
            raise TypeError(
                "validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                    type(validation_result_suite_identifier)
                )
            )

        self.data_context.build_data_docs(
            site_names=self._site_names,
            resource_identifiers=[validation_result_suite_identifier],
        )
