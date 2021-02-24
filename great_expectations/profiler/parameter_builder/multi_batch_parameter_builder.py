"""
Defines the abstract MultiBatchParameterBuilder class
"""

from abc import ABC
from typing import List

from ..exceptions import ProfilerExecutionError
from .parameter_builder import ParameterBuilder


class MultiBatchParameterBuilder(ParameterBuilder, ABC):
    """
    MultiBatchParameterBuilder checks that there are multiple batch ids passed to build_parameters,
    and uses a configured batch_request parameter to obtain them if they are not.
    """

    def __init__(self, *, parameter_id, batch_request, data_context):
        if data_context is None:
            raise ProfilerExecutionError(
                f"MultiBatchParameterBuilder requires a data_context, but none was provided."
            )
        super().__init__(parameter_id=parameter_id, data_context=data_context)
        self._batch_request = batch_request

    def _get_batch_ids(self, batch_request) -> List[str]:
        datasource_name = batch_request.datasource_name
        batch_definitions = self._data_context.get_datasource(
            datasource_name
        ).get_batch_definition_list_from_batch_request(batch_request)
        return [batch_definition.id for batch_definition in batch_definitions]

    def build_parameters(
        self, *, rule_state=None, validator=None, batch_ids=None, **kwargs
    ):
        """Build the parameters for the specified domain_kwargs."""
        if batch_ids is None:
            batch_ids = self._get_batch_ids(self._batch_request)

        return self._build_parameters(
            rule_state=rule_state, validator=validator, batch_ids=batch_ids, **kwargs
        )
