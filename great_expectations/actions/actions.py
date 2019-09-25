import logging
import requests
logger = logging.getLogger(__name__)

from ..util import (
    get_class_from_module_name_and_class_name,
)

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
# from .types import (
#     ActionConfig,
#     ActionInternalConfig,
# )
from ..data_context.types import (
    ValidationResultIdentifier
)

# NOTE: Abe 2019/08/23 : This is first implementation of all these classes. Consider them UNSTABLE for now. 

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

    def run(self, validation_result_suite, validation_result_suite_identifier, data_asset):
        """

        :param validation_result_suite:
        :param validation_result_suite_identifier:
        :param data_asset:
        :return:
        """
        return NotImplementedError


class NoOpAction(NamespacedValidationAction):

    def __init__(self, data_context,):
        super(NoOpAction, self).__init__(data_context)
    
    def run(self, validation_result_id, validation_result_suite, data_asset):
        print("Happily doing nothing")


class SlackNotificationAction(NamespacedValidationAction):
    """
    SlackNotificationAction is a namespeace-aware validation action that sends a Slack notification to a given webhook.
    """
    
    def __init__(
            self,
            data_context,
            renderer,
            slack_webhook=None,
            notify_on=None,
    ):
        """
        :param data_context: data context
        :param renderer: dictionary specifying the renderer used to generate a query consumable by Slack API
        :param slack_webhook: string - incoming Slack webhook to send notification to
        :param notify_on: string - "all", "failure", "success" - specifies validation status that will trigger notification
        """
        self.data_context = data_context
        self.renderer = instantiate_class_from_config(
            config=renderer,
            runtime_config={},
            config_defaults={},
        )
        if slack_webhook:
            self.slack_webhook = slack_webhook
        else:
            slack_webhook = data_context.get_config_with_variables_substituted().get("notifications", {}).get("slack_webhook")
        assert slack_webhook, "No Slack webhook found in action or project configs."
        if notify_on:
            self.notify_on = notify_on
        elif data_context.get_config_with_variables_substituted().get("notifications", {}).get("notify_on"):
            self.notify_on = data_context.get_config_with_variables_substituted().get("notifications", {}).get("notify_on")
        else:
            self.notify_on = "all"
        
    def take_action(self, validation_result_id, validation_result_suite):
        logger.debug("SummarizeAndNotifySlackAction.take_action")
    
        if validation_result_suite is None:
            return
        
        if not isinstance(validation_result_id, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_id)
            ))

        validation_success = validation_result_suite["success"]
        
        if self.notify_on == "all" or \
                self.notify_on == "success" and validation_success or \
                self.notify_on == "failure" and not validation_success:
            query = self.renderer.render(validation_result_suite)
            self._send_slack_notification(query)
        else:
            return
                
    def _send_slack_notification(self, query):
        session = requests.Session()
    
        try:
            response = session.post(url=self.slack_webhook, json=query)
        except requests.ConnectionError:
            logger.warning(
                'Failed to connect to Slack webhook at {url} '
                'after {max_retries} retries.'.format(
                    url=self.slack_webhook, max_retries=10))
        except Exception as e:
            logger.error(str(e))
        else:
            if response.status_code != 200:
                logger.warning(
                    'Request to Slack webhook at {url} '
                    'returned error {status_code}: {text}'.format(
                        url=self.slack_webhook,
                        status_code=response.status_code,
                        text=response.text))
            else:
                return "Slack notification succeeded."


class StoreAction(NamespacedValidationAction):
    """
    StoreAction is a namespeace-aware validation action that stores a validation result
    in the store.
    """

    def __init__(self,
                 data_context,
                 target_store_name,
                 ):
        """

        :param data_context: data context
        :param target_store_name: the name of the store in the data context which
                should be used to store the validation result
        """

        super(StoreAction, self).__init__(data_context)

        # NOTE: Eventually, we probably need a check to verify that this store is compatible with validation_result_suite_identifiers.
        # Unless ALL stores are compatible...
        self.target_store = data_context.stores[target_store_name]

    def run(self, validation_result_suite_id, validation_result_suite, data_asset):
        logger.debug("StoreAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_suite_id, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_suite_id)
            ))


        self.target_store.set(validation_result_suite_id, validation_result_suite)


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
                 target_store_name,
                 ):
        """

        :param data_context: data context
        :param target_store_name: the name of the store in the data context which
                should be used to store the validation result
        """
        super(ExtractAndStoreEvaluationParamsAction, self).__init__(data_context)

        # NOTE: Eventually, we probably need a check to verify that this store is compatible with validation_result_suite_identifiers.
        # Unless ALL stores are compatible...
        self.target_store = data_context.stores[target_store_name]

    def run(self, validation_result_suite_id, validation_result_suite, data_asset):
        logger.debug("ExtractAndStoreEvaluationParamsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_suite_id, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ExtractAndStoreEvaluationParamsAction, not {0}".format(
                type(validation_result_suite_id)
            ))


        self.data_context._extract_and_store_parameters_from_validation_results(
            validation_result_suite,
            validation_result_suite_id.expectation_suite_identifier.data_asset_name,
            validation_result_suite_id.expectation_suite_identifier.expectation_suite_name,
            validation_result_suite_id.run_id,
        )

class StoreSnapshotOnFailAction(NamespacedValidationAction):
    """
    StoreSnapshotOnFailAction is a namespeace-aware validation action that
    stores the data asset to the snapshot store for a later review in case the
    validation of the data asset failed and the data asset was found to not meet
    the expectations.

    The snapshot store is configured in the data context and its name (as appears
    in the data context's configuration) is passed to the action in its config.
    configured for this action.
    """

    def __init__(self,
                 data_context,
                 target_store_name,
                 ):
        """

        :param data_context: data context
        :param target_store_name: the name of the store in the data context which
                should be used to store the validation result
        """
        super(ExtractAndStoreEvaluationParamsAction, self).__init__(data_context)

        # NOTE: Eventually, we probably need a check to verify that this store is compatible with validation_result_suite_identifiers.
        # Unless ALL stores are compatible...
        self.target_store = data_context.stores[target_store_name]

    def run(self, validation_result_suite_id, validation_result_suite, data_asset):
        logger.debug("ExtractAndStoreEvaluationParamsAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_suite_id, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ExtractAndStoreEvaluationParamsAction, not {0}".format(
                type(validation_result_suite_id)
            ))

        if validation_result_suite["success"] is False and "data_asset_snapshot_store" in self.stores:
            logging.debug("Storing validation results to data_asset_snapshot_store")
            self.stores.data_asset_snapshot_store.set(
                key=validation_result_suite_id,
                value=data_asset
            )

# NOTE: Eugene: 2019-09-23: Since actions are "stackable", it is better for each action to do one thing.
class SummarizeAndStoreAction(NamespacedValidationAction):

    def __init__(self,
        data_context,
        # name,
        # result_key,
        target_store_name,
        summarizer,
        # stores, # TODO: Migrate stores and services to a runtime_config object
        # services, # TODO: Migrate stores and services to a runtime_config object
    ):

        self.summarizer = instantiate_class_from_config(
            config = summarizer,
            runtime_config= {},
            config_defaults= {},
        )

        #??? Do we need a view_class as well?

        # NOTE: Eventually, we probably need a check to verify that this store is compatible with validation_result_suite_identifiers.
        # Unless ALL stores are compatible...
        self.target_store = data_context.stores[target_store_name]

    def run(self, validation_result_id, validation_result_suite ):
        logger.debug("SummarizeAndStoreAction.run")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_id, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_id)
            ))

        rendered_summary = self.summarizer.render(validation_result_suite)

        self.target_store.set(validation_result_id, rendered_summary)
    