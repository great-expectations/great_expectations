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


def _get_metric_configuration_tuples(metric_configuration, base_kwargs=None):
    if base_kwargs is None:
        base_kwargs = {}

    if isinstance(metric_configuration, str):
        return [(metric_configuration, base_kwargs)]

    metric_configurations_list = []
    for kwarg_name in metric_configuration.keys():
        if not isinstance(metric_configuration[kwarg_name], dict):
            raise ge_exceptions.DataContextError(
                "Invalid metric_configuration: each key must contain a " "dictionary."
            )
        if (
            kwarg_name == "metric_kwargs_id"
        ):  # this special case allows a hash of multiple kwargs
            for metric_kwargs_id in metric_configuration[kwarg_name].keys():
                if base_kwargs != {}:
                    raise ge_exceptions.DataContextError(
                        "Invalid metric_configuration: when specifying "
                        "metric_kwargs_id, no other keys or values may be defined."
                    )
                if not isinstance(
                    metric_configuration[kwarg_name][metric_kwargs_id], list
                ):
                    raise ge_exceptions.DataContextError(
                        "Invalid metric_configuration: each value must contain a "
                        "list."
                    )
                metric_configurations_list += [
                    (metric_name, {"metric_kwargs_id": metric_kwargs_id})
                    for metric_name in metric_configuration[kwarg_name][
                        metric_kwargs_id
                    ]
                ]
        else:
            for kwarg_value in metric_configuration[kwarg_name].keys():
                base_kwargs.update({kwarg_name: kwarg_value})
                if not isinstance(metric_configuration[kwarg_name][kwarg_value], list):
                    raise ge_exceptions.DataContextError(
                        "Invalid metric_configuration: each value must contain a "
                        "list."
                    )
                for nested_configuration in metric_configuration[kwarg_name][
                    kwarg_value
                ]:
                    metric_configurations_list += _get_metric_configuration_tuples(
                        nested_configuration, base_kwargs=base_kwargs
                    )

    return metric_configurations_list
