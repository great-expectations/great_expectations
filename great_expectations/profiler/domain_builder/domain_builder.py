from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.validator.validator import Validator


class DomainBuilder(ABC):
    """A DomainBuilder provides methods to get domains based on one or more batches of data.

    It may additionally accept other configuration.
    """

    # TODO: <Alex>ALEX -- We should be careful with **kwargs -- if there is no immediate use case for them, then we should only keep explicit arguments.</Alex>
    def get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = True,
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs
    ) -> List[Domain]:
        """
        Note: Please do not overwrite the public "get_domains()" method.  If a child class needs to check parameters,
        then please do so in its implementation of the (private) "_get_domains()" method, or in a utility method.
        """
        return self._get_domains(
            validator=validator,
            batch_ids=batch_ids,
            include_batch_id=include_batch_id,
            domain_type=domain_type,
            **kwargs
        )

    @abstractmethod
    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = False,
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs
    ) -> List[Domain]:
        """_get_domains is the primary workhorse for the DomainBuilder"""
        pass
