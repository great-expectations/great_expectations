from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional, Union

if TYPE_CHECKING:
    from great_expectations.core.domain import SemanticDomainTypes


class SemanticTypeFilter(ABC):
    @abstractmethod
    def parse_semantic_domain_type_argument(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
    ) -> List[SemanticDomainTypes]:
        pass

    @property
    @abstractmethod
    def table_column_name_to_inferred_semantic_domain_type_map(
        self,
    ) -> Dict[str, SemanticDomainTypes]:
        pass
