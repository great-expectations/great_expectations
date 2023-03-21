from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)


from dataprofiler import Profiler

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain, SemanticDomainTypes
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.semantic_type_filter import (
    SemanticTypeFilter,  # noqa: TCH001
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator


class DataProfilerColumnDomainBuilder(ColumnDomainBuilder):

    def __init__(
        self,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        exclude_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        include_column_name_suffixes: Optional[Union[str,
                                                     Iterable, List[str]]] = None,
        exclude_column_name_suffixes: Optional[Union[str,
                                                     Iterable, List[str]]] = None,
        