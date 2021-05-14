from abc import abstractmethod
from typing import Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.rule_based_profiler.util import get_batch_ids
from great_expectations.validator.validator import Validator


class MultiBatchParameterBuilder(ParameterBuilder):
    """
    Defines the abstract MultiBatchParameterBuilder class

    MultiBatchParameterBuilder checks that there are multiple batch ids passed to build_parameters,
    and uses a configured batch_request parameter to obtain them if they are not.
    """

    def __init__(
        self,
        parameter_name: str,
        batch_request: BatchRequest,
        data_context: Optional[DataContext] = None,
    ):
        """
        Args:
            parameter_name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            data_context: DataContext
        """
        if data_context is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"MultiBatchParameterBuilder requires a data_context, but none was provided."
            )

        super().__init__(
            parameter_name=parameter_name,
            data_context=data_context,
        )

        self._batch_request = batch_request
        self._batch_ids = get_batch_ids(
            data_context=self.data_context, batch_request=self._batch_request
        )

    @abstractmethod
    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_ids: Optional[List[str]] = None,
    ):
        pass

    @property
    def batch_ids(self) -> List[str]:
        return self._batch_ids
