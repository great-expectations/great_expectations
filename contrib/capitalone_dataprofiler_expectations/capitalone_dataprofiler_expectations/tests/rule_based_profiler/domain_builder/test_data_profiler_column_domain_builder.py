from __future__ import annotations

import os
from typing import TYPE_CHECKING, List
from unittest import mock

import pytest
from capitalone_dataprofiler_expectations.metrics import *  # noqa: F403
from capitalone_dataprofiler_expectations.rule_based_profiler.domain_builder.data_profiler_column_domain_builder import (
    DataProfilerColumnDomainBuilder,
)

from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context import FileDataContext
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    build_parameter_container_for_variables,
)

if TYPE_CHECKING:
    from capitalone_dataprofiler_expectations.tests.conftest import (
        BaseProfiler,
    )


test_root_path: str = os.path.dirname(  # noqa: PTH120
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # noqa: PTH120
)


@pytest.mark.big
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder_with_profile_path_as_value(
    bobby_columnar_table_multi_batch_deterministic_data_context: FileDataContext,
):
    """
    This test verifies that "Domain" objects corresponding to full list of columns in Profiler Report (same as in Batch)
    are emitted when path to "profile.pkl" is specified explicitly.
    """
    data_context: FileDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    variables_configs: dict = {
        "strict_min": False,
        "strict_max": False,
        "profile_path": profile_path,
        "profile_report_filtering_key": "data_type",
        "profile_report_accepted_filtering_values": [
            "int",
            "float",
            "string",
            "datetime",
        ],
    }
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder(
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule",
        variables=variables,
        batch_request=batch_request,
    )

    assert len(domains) == 18
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "vendor_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "vendor_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "passenger_count",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "trip_distance",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "trip_distance": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "rate_code_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "rate_code_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "store_and_fwd_flag",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "store_and_fwd_flag": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_location_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_location_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_location_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_location_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "payment_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "payment_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "fare_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "fare_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "extra",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "extra": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "mta_tax",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "mta_tax": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tip_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tip_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tolls_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tolls_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "improvement_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "improvement_surcharge": SemanticDomainTypes.NUMERIC.value,
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "total_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "congestion_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "congestion_surcharge": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]


@pytest.mark.big
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder_with_profile_path_as_default_reference(
    bobby_columnar_table_multi_batch_deterministic_data_context: FileDataContext,
):
    """
    This test verifies that "Domain" objects corresponding to full list of columns in Profiler Report (same as in Batch)
    are emitted when path to "profile.pkl" is specified as Rule variable (implicitly).
    """
    data_context: FileDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    variables_configs: dict = {
        "profile_path": profile_path,
        "estimator": "quantiles",
        "false_positive_rate": 1.0e-2,
        "mostly": 1.0,
        "profile_report_filtering_key": "data_type",
        "profile_report_accepted_filtering_values": [
            "int",
            "float",
            "string",
            "datetime",
        ],
    }
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder(
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule",
        variables=variables,
        batch_request=batch_request,
    )

    assert len(domains) == 18
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "vendor_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "vendor_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "passenger_count",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "trip_distance",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "trip_distance": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "rate_code_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "rate_code_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "store_and_fwd_flag",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "store_and_fwd_flag": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_location_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_location_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_location_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_location_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "payment_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "payment_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "fare_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "fare_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "extra",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "extra": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "mta_tax",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "mta_tax": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tip_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tip_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tolls_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tolls_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "improvement_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "improvement_surcharge": SemanticDomainTypes.NUMERIC.value,
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "total_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "congestion_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "congestion_surcharge": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]


@pytest.mark.big
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder_with_profile_path_as_reference(
    bobby_columnar_table_multi_batch_deterministic_data_context: FileDataContext,
):
    """
    This test verifies that "Domain" objects corresponding to full list of columns in Profiler Report (same as in Batch)
    are emitted when path to "profile.pkl" is specified as Rule variable (implicitly).
    """
    data_context: FileDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    variables_configs: dict = {
        "profile_path": profile_path,
        "estimator": "quantiles",
        "false_positive_rate": 1.0e-2,
        "mostly": 1.0,
        "profile_report_filtering_key": "data_type",
        "profile_report_accepted_filtering_values": [
            "int",
            "float",
            "string",
            "datetime",
        ],
    }
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder(
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule",
        variables=variables,
        batch_request=batch_request,
    )

    assert len(domains) == 18
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "vendor_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "vendor_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "passenger_count",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "trip_distance",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "trip_distance": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "rate_code_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "rate_code_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "store_and_fwd_flag",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "store_and_fwd_flag": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_location_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_location_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_location_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_location_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "payment_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "payment_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "fare_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "fare_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "extra",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "extra": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "mta_tax",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "mta_tax": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tip_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tip_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tolls_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tolls_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "improvement_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "improvement_surcharge": SemanticDomainTypes.NUMERIC.value,
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "total_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "congestion_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "congestion_surcharge": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]


@pytest.mark.big
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder_with_profile_path_as_reference_with_exclude_column_names_with_exclude_column_name_suffixes(
    bobby_columnar_table_multi_batch_deterministic_data_context: FileDataContext,
):
    """
    This test verifies that "Domain" objects corresponding to partial list of columns in Profiler Report under exclusion
    directives (as subset of columns in Batch) are emitted when path to "profile.pkl" is specified as Rule variable (implicitly).
    """
    data_context: FileDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    variables_configs: dict = {
        "profile_path": profile_path,
        "estimator": "quantiles",
        "false_positive_rate": 1.0e-2,
        "mostly": 1.0,
        "profile_report_filtering_key": "data_type",
        "profile_report_accepted_filtering_values": [
            "int",
            "float",
            "string",
            "datetime",
        ],
    }
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder(
        exclude_column_names=[
            "store_and_fwd_flag",
            "congestion_surcharge",
        ],
        exclude_column_name_suffixes=[
            "_id",
        ],
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule",
        variables=variables,
        batch_request=batch_request,
    )

    assert len(domains) == 12
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "pickup_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "dropoff_datetime",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_datetime": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "passenger_count",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "trip_distance",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "trip_distance": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "payment_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "payment_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "fare_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "fare_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "extra",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "extra": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "mta_tax",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "mta_tax": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tip_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tip_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tolls_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tolls_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "improvement_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "improvement_surcharge": SemanticDomainTypes.NUMERIC.value,
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "total_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]


@pytest.mark.big
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder_with_profile_path_as_reference_with_partial_column_list_in_profiler_report(
    bobby_columnar_table_multi_batch_deterministic_data_context: FileDataContext,
    mock_base_data_profiler: BaseProfiler,
):
    """
    This test verifies that "Domain" objects corresponding to partial list of columns in Profiler Report (as subset of
    columns in Batch) are emitted when path to "profile.pkl" is specified as Rule variable (implicitly).
    """
    data_context: FileDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    variables_configs: dict = {
        "profile_path": profile_path,
        "estimator": "quantiles",
        "false_positive_rate": 1.0e-2,
        "mostly": 1.0,
        "profile_report_filtering_key": "data_type",
        "profile_report_accepted_filtering_values": [
            "int",
            "float",
            "string",
            "datetime",
        ],
    }
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder(
        data_context=data_context,
    )
    with mock.patch(
        "dataprofiler.profilers.profile_builder.BaseProfiler.load",
        return_value=mock_base_data_profiler,
    ):
        domains: List[Domain] = domain_builder.get_domains(
            rule_name="my_rule",
            variables=variables,
            batch_request=batch_request,
        )

    assert len(domains) == 4
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "vendor_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "vendor_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "passenger_count",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "total_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "congestion_surcharge",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "congestion_surcharge": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]
