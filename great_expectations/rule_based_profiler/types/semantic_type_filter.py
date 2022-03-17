from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from great_expectations.rule_based_profiler.types import (
    InferredSemanticDomainType,
    SemanticDomainTypes,
)


class SemanticTypeFilter(ABC):
    @abstractmethod
    def parse_semantic_domain_type_argument(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
    ) -> List[SemanticDomainTypes]:
        pass

    @abstractmethod
    def infer_semantic_domain_type_from_table_column_type(
        self,
        column_types_dict_list: List[Dict[str, Any]],
        column_name: str,
    ) -> InferredSemanticDomainType:
        pass
