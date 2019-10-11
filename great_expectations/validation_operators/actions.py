import logging
import requests
logger = logging.getLogger(__name__)


from great_expectations.data_context.util import (
    instantiate_class_from_config,
)

from ..data_context.types import (
    ValidationResultIdentifier
)

from .util import send_slack_notification

class BasicValidationAction(object):
    """
    The base class of all actions that act on validation results.

    It defines the signature of the public run method that take a validation result suite
    """

    def run(self, validation_result_suite):
        return NotImplementedError

class NamespacedValidationAction(BasicValidationAction):
    """
    This is the base class for all actions that act on validation results
    and are aware of a data context namespace structure.

    The data context is passed to this class in its constructor.
    """

    def __init__(self, data_context):
        self.data_context = data_context

    def run(self, validation_result_suite, validation_result_suite_identifier, data_asset, **kwargs):
        """

        :param validation_result_suite:
        :param validation_result_suite_identifier:
        :param data_asset:
        :param: kwargs - any additional arguments the child might use
        :return:
        """
        return self._run(validation_result_suite, validation_result_suite_identifier, data_asset, **kwargs)


    def _run(self, validation_result_suite, validation_result_suite_identifier, data_asset):
        return NotImplementedError


class NoOpAction(NamespacedValidationAction):

    def __init__(self, data_context,):
        super(NoOpAction, self).__init__(data_context)
    
    def _run(self, validation_result_suite, validation_result_suite_identifier, data_asset):
        print("Happily doing nothing")


class SlackNotificationAction(NamespacedValidationAction):
    """
    SlackNotificationAction is a namespeace-aware validation action that sends a Slack notification to a given webhook.
    
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
            self,
            data_context,
            renderer,
            slack_webhook,
            notify_on="all",
    ):
        """
        :param data_context: data context
        :param renderer: dictionary specifying the renderer used to generate a query consumable by Slack API
            e.g.:
                {
                   "module_name": "great_expectations.render.renderer.slack_renderer",
                   "class_name": "SlackRenderer",
               }
        :param slack_webhook: string - incoming Slack webhook to send notification to
        :param notify_on: string - "all", "failure", "success" - specifies validation status that will trigger notification
        """
        self.data_context = data_context
        self.renderer = instantiate_class_from_config(
            config=renderer,
            runtime_config={},
            config_defaults={},
        )
        self.slack_webhook = slack_webhook
        assert slack_webhook, "No Slack webhook found in action config."
        self.notify_on = notify_on
        
    def _run(self, validation_result_suite, validation_result_suite_identifier, data_asset=None):
        logger.debug("SlackNotificationAction.run")
    
        if validation_result_suite is None:
            return
        
        if not isinstance(validation_result_suite_identifier, ValidationResultIdentifier):
            raise TypeError("validation_result_suite_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_suite_identifier)
            ))

        validation_success = validation_result_suite["success"]
        
        if self.notify_on == "all" or \
                self.notify_on == "success" and validation_success or \
                self.notify_on == "failure" and not validation_success:
            query = self.renderer.render(validation_result_suite)
            return send_slack_notification(query, slack_webhook=self.slack_webhook)
        else:
            return


class StoreAction(NamespacedValidationAction):
    """
    StoreAction is a namespeace-aware validation action that stores a validation result
    in the store.
    """

    def __init__(self,
                 data_context,
                 target_store_name=None,
                 ):
        """

        :param data_context: data context
        :param target_store_name: the name of the store in the data context which
                should be used to store the validation result
        """

        super(StoreAction, self).__init__(data_context)

        # NOTE: Eventually, we probably need a check to verify that this store is compatible with validation_result_suite_identifiers.
        # Unless ALL stores are compatible...
        if target_store_name is None:
            self.target_store = data_context.stores[data_context.validations_store_name]
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(self, validation_result_suite, validation_result_suite_identifier, data_asset):
        logger.debug("StoreAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_suite_identifier, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_suite_identifier)
            ))


        self.target_store.set(validation_result_suite_identifier, validation_result_suite)


class ExtractAndStoreEvaluationParamsAction(NamespacedValidationAction):
    """
    ExtractAndStoreEvaluationParamsAction is a namespeace-aware validation action that
    extracts evaluation parameters from a validation result and stores them in the store
    configured for this action.

    Evaluation parameters allow expectations to refer to statistics/metrics computed
    in the process of validating other prior expectations.
    """

    def __init__(self,
                 data_context,
                 target_store_name=None,
                 ):
        """

        :param data_context: data context
        :param target_store_name: the name of the store in the data context which
                should be used to store the validation result
        """
        super(ExtractAndStoreEvaluationParamsAction, self).__init__(data_context)

        if target_store_name is None:
            self.target_store = data_context.stores[data_context.evaluation_parameter_store_name]
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(self, validation_result_suite, validation_result_suite_identifier, data_asset):
        logger.debug("ExtractAndStoreEvaluationParamsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_suite_identifier, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_suite_identifier)
            ))


        self.data_context._extract_and_store_parameters_from_validation_results(
            validation_result_suite,
            validation_result_suite_identifier.expectation_suite_identifier.data_asset_name,
            validation_result_suite_identifier.expectation_suite_identifier.expectation_suite_name,
            validation_result_suite_identifier.run_id,
        )

class UpdateDataDocsAction(NamespacedValidationAction):
    """
    UpdateDataDocsAction is a namespeace-aware validation action that
    notifies the site builders of all the data docs sites of the data context
    that a validation result should be added to the data docs.
    """

    def __init__(self, data_context):
        """
        :param data_context: data context
        """
        super(UpdateDataDocsAction, self).__init__(data_context)


    def _run(self, validation_result_suite, validation_result_suite_identifier, data_asset):
        logger.debug("UpdateDataDocsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_suite_identifier, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_suite_identifier)
            ))

        self.data_context.build_data_docs(
            resource_identifiers=[validation_result_suite_identifier]
        )
