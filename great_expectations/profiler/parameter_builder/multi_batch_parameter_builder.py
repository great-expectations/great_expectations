from abc import ABC
from typing import List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.profiler.parameter_builder.parameter_builder import ParameterBuilder
from great_expectations.profiler.profiler_rule.rule_state import RuleState
from great_expectations.validator.validator import Validator
from great_expectations.profiler.parameter_builder.parameter import Parameter


# TODO: <Alex>ALEX -- If ParameterBuilder already extends ABC, why does this class need to do the same?</Alex>
class MultiBatchParameterBuilder(ParameterBuilder, ABC):
    """
    Defines the abstract MultiBatchParameterBuilder class

    MultiBatchParameterBuilder checks that there are multiple batch ids passed to build_parameters,
    and uses a configured batch_request parameter to obtain them if they are not.
    """

    def __init__(self, *, parameter_id, batch_request, data_context):
        if data_context is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"MultiBatchParameterBuilder requires a data_context, but none was provided."
            )
        super().__init__(parameter_id=parameter_id, data_context=data_context)
        self._batch_request = batch_request

    # TODO: <Alex>ALEX -- Add type hints (and possibly standardize method arguments usage).</Alex>
    def _get_batch_ids(self, batch_request) -> List[str]:
        datasource_name = batch_request.datasource_name
        batch_definitions = self._data_context.get_datasource(
            datasource_name
        ).get_batch_definition_list_from_batch_request(batch_request)
        return [batch_definition.id for batch_definition in batch_definitions]

    # TODO: <Alex>ALEX -- Add type hints (and possibly standardize method arguments usage).</Alex>
    def build_parameters(
        self, *, rule_state: Optional[RuleState] = None, validator: Optional[Validator] = None, batch_ids: Optional[List[str]] = None, **kwargs
    ) -> Parameter:
        """Build the parameters for the specified domain_kwargs."""
        if batch_ids is None:
            batch_ids = self._get_batch_ids(self._batch_request)

        return self._build_parameters(
            rule_state=rule_state, validator=validator, batch_ids=batch_ids, **kwargs
        )
