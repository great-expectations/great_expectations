import logging

from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.data_context.data_context import DataContext

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class ExplorerDataContext(DataContext):
    def __init__(self, context_root_dir=None, expectation_explorer=True):
        """
            expectation_explorer: If True, load the expectation explorer manager, which will modify GE return objects \
            to include ipython notebook widgets.
        """

        super().__init__(context_root_dir)

        self._expectation_explorer = expectation_explorer
        if expectation_explorer:
            from great_expectations.jupyter_ux.expectation_explorer import (
                ExpectationExplorer,
            )

            self._expectation_explorer_manager = ExpectationExplorer()

    def update_return_obj(self, data_asset, return_obj):
        """Helper called by data_asset.

        Args:
            data_asset: The data_asset whose validation produced the current return object
            return_obj: the return object to update

        Returns:
            return_obj: the return object, potentially changed into a widget by the configured expectation explorer
        """
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(
                data_asset, return_obj
            )
        else:
            return return_obj
