from abc import ABC, abstractmethod
from typing import List, Optional

from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.validator.validator import Validator


class DomainBuilder(ABC):
    """A DomainBuilder provides methods to get domains based on one or more batches of data.

    There is no default constructor for this class, and it may accept configuration as needed for the particular domain.
    """

    def get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> List[Domain]:
        """
        :param validator
        :param batch_ids: A list of batch_ids to use when profiling (e.g. can be a subset of batches provided via
        Validator, batch, batches, batch_request).  If not provided, all batches are used.  If a Validator is provided,
        Validator active batch id is used.

        Note: In this class, we do not verify that all of these batch_ids are accessible; this should be done elsewhere
        (with an error raised in the appropriate situations).

        Note: Please do not overwrite the public "get_domains()" method.  If a child class needs to check parameters,
        then please do so in its implementation of the (private) "_get_domains()" method, or in a utility method.
        """
        return self._get_domains(validator=validator, batch_ids=batch_ids)

    @abstractmethod
    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> List[Domain]:
        """_get_domains is the primary workhorse for the DomainBuilder"""
        pass
