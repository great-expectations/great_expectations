import enum
from typing import List, Optional

from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer


class CardinalityCategory(enum.Enum):
    """Used to determine appropriate Expectation configurations based on data.

    Defines relative and absolute number of records (table rows) that
    correspond to each cardinality category.

    """

    # TODO AJB 20220216: add implementation
    raise NotImplementedError


class CategoricalColumnDomainBuilder(DomainBuilder):
    """ """

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
        cardinality_category: Optional[CardinalityCategory] = None,
    ) -> List[Domain]:
        """Return domains matching the selected cardinality_category.

        Cardinality categories define a maximum number of unique items that
        can be contained in a given domain. If this number is exceeded, the
        domain is not included for the currently executing rule.
        This filter considers unique values across all supplied batches.

        Args:
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the desired cardinality.
        """

        # TODO AJB 20220216: add implementation
        raise NotImplementedError
