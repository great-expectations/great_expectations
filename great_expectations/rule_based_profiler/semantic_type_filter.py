from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Union

from great_expectations.core.domain import SemanticDomainTypes


class SemanticTypeFilter(ABC):
    @abstractmethod
    def parse_semantic_domain_type_argument(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, list[Union[str, SemanticDomainTypes]]]
        ] = None,
    ) -> list[SemanticDomainTypes]:
        pass

    @property
    @abstractmethod
    def table_column_name_to_inferred_semantic_domain_type_map(
        self,
    ) -> dict[str, SemanticDomainTypes]:
        pass
