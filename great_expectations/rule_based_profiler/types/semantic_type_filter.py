
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
from great_expectations.rule_based_profiler.types import SemanticDomainTypes

class SemanticTypeFilter(ABC):

    @abstractmethod
    def parse_semantic_domain_type_argument(self, semantic_types: Optional[Union[(str, SemanticDomainTypes, List[Union[(str, SemanticDomainTypes)]])]]=None) -> List[SemanticDomainTypes]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        pass

    @property
    @abstractmethod
    def table_column_name_to_inferred_semantic_domain_type_map(self) -> Dict[(str, SemanticDomainTypes)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        pass
