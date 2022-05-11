from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_batch_ids as get_batch_ids_from_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_resolved_metrics_by_key,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator as get_validator_using_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.types import (
    Builder,
    Domain,
    ParameterContainer,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class DomainBuilder(ABC, Builder):
    "\n    A DomainBuilder provides methods to get domains based on one or more batches of data.\n"

    def __init__(self, data_context: Optional["BaseDataContext"] = None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Args:\n            data_context: BaseDataContext associated with DomainBuilder\n        "
        super().__init__(data_context=data_context)

    def get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
    ) -> List[Domain]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Args:\n            rule_name: name of Rule object, for which "Domain" objects are obtained.\n            variables: attribute name/value pairs\n            batch_list: Explicit list of Batch objects to supply data at runtime.\n            batch_request: Explicit batch_request used to supply data at runtime.\n\n        Returns:\n            List of Domain objects.\n\n        Note: Please do not overwrite the public "get_domains()" method.  If a child class needs to check parameters,\n        then please do so in its implementation of the (private) "_get_domains()" method, or in a utility method.\n        '
        self.set_batch_list_or_batch_request(
            batch_list=batch_list, batch_request=batch_request
        )
        return self._get_domains(rule_name=rule_name, variables=variables)

    @property
    @abstractmethod
    def domain_type(self) -> MetricDomainTypes:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        pass

    @abstractmethod
    def _get_domains(
        self, rule_name: str, variables: Optional[ParameterContainer] = None
    ) -> List[Domain]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        _get_domains is the primary workhorse for the DomainBuilder\n        "
        pass

    def get_table_row_counts(
        self,
        validator: Optional["Validator"] = None,
        batch_ids: Optional[List[str]] = None,
        variables: Optional[ParameterContainer] = None,
    ) -> Dict[(str, int)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if validator is None:
            validator = self.get_validator(variables=variables)
        if batch_ids is None:
            batch_ids = self.get_batch_ids(variables=variables)
        batch_id: str
        metric_configurations_by_batch_id: Dict[(str, List[MetricConfiguration])] = {
            batch_id: [
                MetricConfiguration(
                    metric_name="table.row_count",
                    metric_domain_kwargs={"batch_id": batch_id},
                    metric_value_kwargs={"include_nested": True},
                    metric_dependencies=None,
                )
            ]
            for batch_id in batch_ids
        }
        resolved_metrics_by_batch_id: Dict[
            (str, Dict[(Tuple[(str, str, str)], Any)])
        ] = get_resolved_metrics_by_key(
            validator=validator,
            metric_configurations_by_key=metric_configurations_by_batch_id,
        )
        batch_id: str
        resolved_metrics: Dict[(Tuple[(str, str, str)], Any)]
        metric_value: Any
        table_row_count_lists_by_batch_id: Dict[(str, List[int])] = {
            batch_id: [metric_value for metric_value in resolved_metrics.values()]
            for (batch_id, resolved_metrics) in resolved_metrics_by_batch_id.items()
        }
        table_row_counts_by_batch_id: Dict[(str, int)] = {
            batch_id: metric_value[0]
            for (batch_id, metric_value) in table_row_count_lists_by_batch_id.items()
        }
        return table_row_counts_by_batch_id

    def get_validator(
        self, variables: Optional[ParameterContainer] = None
    ) -> Optional["Validator"]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return get_validator_using_batch_list_or_batch_request(
            purpose="domain_builder",
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            domain=None,
            variables=variables,
            parameters=None,
        )

    def get_batch_ids(
        self, variables: Optional[ParameterContainer] = None
    ) -> Optional[List[str]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return get_batch_ids_from_batch_list_or_batch_request(
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            domain=None,
            variables=variables,
            parameters=None,
        )
