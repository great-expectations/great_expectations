from typing import List

import os

import pandas as pd
import pytest
from ruamel.yaml import YAML

from contrib.capitalone_dataprofiler_expectations.capitalone_dataprofiler_expectations.rule_based_profiler.domain_builder.data_profiler_column_domain_builder import DataProfilerColumnDomainBuilder

import great_expectations.exceptions as gx_exceptions
from great_expectations import DataContext
from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    ColumnPairDomainBuilder,
    DomainBuilder,
    MultiColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    build_parameter_container_for_variables,
)

yaml = YAML(typ="safe")


@pytest.mark.integration
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder(
):
