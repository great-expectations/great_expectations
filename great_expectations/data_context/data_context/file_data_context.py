from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.data_context_variables import (
    FileDataContextVariables,
)


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError
