import logging
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
        super(NameSpaceAwareValidationAction, self).__init__(config)

        #The config may include references to stores and services.
        #Both act like endpoints to which results can be sent.
        #Stores support both reading and writing, in key-value fashion.
        #Services only support writing.
        #Future versions of Services may get results returned as part of the call.
        #(Some) Stores and Services will need persistent connections, which are managed by the DataContext.

    def take_action(self, validation_result_suite, validation_result_suite_identifier):
        return NotImplementedError

# FIXME: This class is only here temporarily. It should be moved either to tests or renderers
class TemporaryNoOpSummarizer(object):

    def render(self, input):
        return input
    
# FIXME: This class is only here temporarily. It should be moved either to tests or renderers
class DropAllVowelsSummarizer(object):

    def render(self, input):
        return input

class SummarizeAndStoreAction(NamespacedValidationAction):

    def __init__(self,
        data_context,
        name,
        result_key,
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

    def take_action(self, validation_result_suite, validation_result_suite_identifier):
        logger.debug("SummarizeAndStoreAction.take_action")

        if not isinstance(validation_result_suite_identifier, ValidationResultIdentifier):
            raise TypeError("validation_result_suite_identifier must be of type ValidationResultIdentifier, not {0}".format(
                type(validation_result_suite_identifier)
            ))

        rendered_summary = self.summarizer.render(validation_result_suite)

        self.target_store.set(validation_result_suite_identifier, rendered_summary)
    