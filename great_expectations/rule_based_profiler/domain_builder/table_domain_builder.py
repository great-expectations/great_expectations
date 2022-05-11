
from typing import List, Optional
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer

class TableDomainBuilder(DomainBuilder):

    def __init__(self, data_context: Optional['BaseDataContext']=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            data_context: BaseDataContext associated with this DomainBuilder\n        '
        super().__init__(data_context=data_context)

    @property
    def domain_type(self) -> MetricDomainTypes:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return MetricDomainTypes.TABLE
    '\n    The interface method of TableDomainBuilder emits a single Domain object, corresponding to the implied Batch (table).\n\n    Note that for appropriate use-cases, it should be readily possible to build a multi-batch implementation, where a\n    separate Domain object is emitted for each individual Batch (using its respective batch_id).  (This is future work.)\n    '

    def _get_domains(self, rule_name: str, variables: Optional[ParameterContainer]=None) -> List[Domain]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        domains: List[Domain] = [Domain(domain_type=self.domain_type, rule_name=rule_name)]
        return domains
