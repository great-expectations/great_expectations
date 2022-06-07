from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
)


class CloudDataContext(AbstractDataContext):
    """
    Subclass of AbstractDataContext that contains functionality necessary to hydrate state from cloud
    """

    def _init_variables(self) -> CloudDataContextVariables:
        raise NotImplementedError
