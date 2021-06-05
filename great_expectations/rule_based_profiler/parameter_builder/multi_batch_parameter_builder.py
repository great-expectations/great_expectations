from abc import abstractmethod
from typing import Dict, List, Optional, Union

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
from great_expectations.rule_based_profiler.util import (
    get_batch_ids_from_batch_request,
    get_batch_ids_from_validator,
    get_parameter_argument,
)
from great_expectations.validator.validator import Validator


class MultiBatchParameterBuilder(ParameterBuilder):
    """
    Defines the abstract MultiBatchParameterBuilder class

    MultiBatchParameterBuilder checks that there are multiple batch ids passed to its "_build_parameters()" method,
    and uses a configured batch_request parameter to obtain them if they are not.

    This is an abstract class in the sense that instead of implementing the interface method, "_build_parameters()",
    this method will remain abstract (for subclasses to implement) and the present class will contain useful utilities
    for the subclasses.  For example, getting the effective list of batch IDs (whether or not it was passed as argument
    or obtained from the configured BatchRequest parameter).  The reason for this design is that parameter naming is
    specific to the convention between the ParameterBuilder and ExpectationConfigurationBuilder employed in a specific
    use-case (i.e., parameter names are created with the ultimate usage by the ExpectationConfigurationBuilder in mind).
    """

    def __init__(
        self,
        parameter_name: str,
        data_context: Optional[DataContext] = None,
        batch_request: Optional[Union[BatchRequest, dict, str]] = None,
    ):
        """
        Args:
            parameter_name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            data_context: DataContext
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
        """
        if data_context is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a data_context, but none was provided."
            )

        super().__init__(
            parameter_name=parameter_name,
            data_context=data_context,
        )

        self._batch_request = batch_request

    @abstractmethod
    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        pass

    def get_batch_ids(
        self,
        validator: Optional[Validator] = None,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional[List[str]]:
        batch_ids: Optional[List[str]]
        if self._batch_request is None:
            batch_ids = get_batch_ids_from_validator(validator=validator)
        else:
            # Obtain BatchRequest from rule state (i.e., variables and parameters); from instance variable otherwise.
            batch_request: Optional[Union[BatchRequest, str]] = get_parameter_argument(
                domain=domain,
                argument=self.batch_request,
                variables=variables,
                parameters=parameters,
            )
            if isinstance(batch_request, dict):
                batch_request = BatchRequest(**batch_request)
            batch_ids = get_batch_ids_from_batch_request(
                data_context=self.data_context, batch_request=batch_request
            )
        return batch_ids

    @property
    def batch_request(self) -> BatchRequest:
        return self._batch_request
