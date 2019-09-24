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

    def take_action(self, validation_result_suite):
        return NotImplementedError

class NamespacedValidationAction(BasicValidationAction):

    def __init__(self, config, stores, services):
        # TODO: Switch to expressive inits.
        
        #Uses config to instantiate itself
        super(NamespacedValidationAction, self).__init__(config)

        #The config may include references to stores and services.
        #Both act like endpoints to which results can be sent.
        #Stores support both reading and writing, in key-value fashion.
        #Services only support writing.
        #Future versions of Services may get results returned as part of the call.
        #(Some) Stores and Services will need persistent connections, which are managed by the DataContext.

    def take_action(self, validation_result_suite, validation_result_suite_identifier):
        return NotImplementedError


class NoOpAction(NamespacedValidationAction):

    def __init__(self,
        data_context,
        # name,
    ):
        # self.name = name
        self.data_context = data_context
    
    def take_action(self, validation_result_id, validation_result_suite):
        print("Happily doing nothing")


class SlackNotificationAction(NamespacedValidationAction):
    
    def __init__(
            self,
            data_context,
            summarizer,
            webhook=None,
            notify_on="all", # "failure", "success"
            # result_key="failure",
            # name,
            # result_key,
            # target_store_name,
            # stores,
            # services
    ):
        self.data_context = data_context
        self.summarizer = instantiate_class_from_config(
            config=summarizer,
            runtime_config={},
            config_defaults={},
        )
        if webhook:
            self.webhook = webhook
        else:
            webhook = data_context.get_project_config().get("slack_webhook")
        assert webhook, "No Slack webhook found in action or project configs."
        self.notify_on = notify_on
        
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
            query = self.summarizer.render(validation_result_suite)
            self._send_slack_notification(query)
        else:
            return
                
    def _send_slack_notification(self, query):
        session = requests.Session()
    
        try:
            response = session.post(url=self.webhook, json=query)
        except requests.ConnectionError:
            logger.warning(
                'Failed to connect to Slack webhook at {url} '
                'after {max_retries} retries.'.format(
                    url=self.webhook, max_retries=10))
        except Exception as e:
            logger.error(str(e))
        else:
            if response.status_code != 200:
                logger.warning(
                    'Request to Slack webhook at {url} '
                    'returned error {status_code}: {text}'.format(
                        url=self.webhook,
                        status_code=response.status_code,
                        text=response.text))


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

    def take_action(self, validation_result_id, validation_result_suite ):
        logger.debug("SummarizeAndStoreAction.take_action")

        if validation_result_suite is None:
            return

        if not isinstance(validation_result_id, ValidationResultIdentifier):
            raise TypeError("validation_result_id must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_id)
            ))

        rendered_summary = self.summarizer.render(validation_result_suite)

        self.target_store.set(validation_result_id, rendered_summary)
    