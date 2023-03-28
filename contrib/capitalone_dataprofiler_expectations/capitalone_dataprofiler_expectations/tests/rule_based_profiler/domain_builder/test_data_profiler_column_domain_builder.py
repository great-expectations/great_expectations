import os
from typing import List

import pytest

# noinspection PyUnresolvedReferences
from contrib.capitalone_dataprofiler_expectations.capitalone_dataprofiler_expectations.rule_based_profiler.domain_builder.data_profiler_column_domain_builder import (
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

test_root_path: str = os.path.dirname(  # noqa: PTH120
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # noqa: PTH120
)


# TODO: <Alex>ALEX</Alex>
# @pytest.mark.integration
# @pytest.mark.slow  # 1.21s
# def test_data_profiler_column_domain_builder():
#
#     test_root_path = str(Path(__file__).parents[2])
#
#     profile_path = PurePath(
#         test_root_path,
#         "data_profiler_files",
#         "profile.pkl",
#     )
#
#     variables, a, b, c = dict(), dict(), dict(), dict()
#
#     b["variables"] = c
#
#     a["variables"] = b
#
#     variables["parameter_nodes"] = a
#
#     variables["parameter_nodes"]["variables"]["variables"][
#         "profile_path"
#     ] = profile_path
#
#
#     domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder()
#     text_column_names = domain_builder.get_effective_column_names(
#         rule_name="text_rule", variables=variables
#     )
#     numeric_column_names = domain_builder.get_effective_column_names(
#         rule_name="numeric_rule", variables=variables
#     )
#     assert text_column_names == ["store_and_fwd_flag"]
#     assert numeric_column_names == [
#         "VendorID",
#         "passenger_count",
#         "trip_distance",
#         "RatecodeID",
#         "PULocationID",
#         "DOLocationID",
#         "payment_type",
#         "fare_amount",
#         "extra",
#         "mta_tax",
#         "tip_amount",
#         "tolls_amount",
#         "improvement_surcharge",
#         "total_amount",
#         "congestion_surcharge",
#     ]
#
#     domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder()
#     text_domains = domain_builder._get_domains(
#         rule_name="text_rule", variables=variables
#     )
#     numeric_domains = domain_builder._get_domains(
#         rule_name="numeric_rule", variables=variables
#     )
#     assert text_domains == [
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "store_and_fwd_flag"},
#             "rule_name": "text_rule",
#         }
#     ]
#     assert numeric_domains == [
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "VendorID"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "passenger_count"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "trip_distance"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "RatecodeID"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "PULocationID"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "DOLocationID"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "payment_type"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "fare_amount"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "extra"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "mta_tax"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "tip_amount"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "tolls_amount"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "improvement_surcharge"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "total_amount"},
#             "rule_name": "numeric_rule",
#         },
#         {
#             "domain_type": "column",
#             "domain_kwargs": {"column": "congestion_surcharge"},
#             "rule_name": "numeric_rule",
#         },
#     ]
# TODO: <Alex>ALEX</Alex>


@pytest.mark.integration
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder(
    bobby_columnar_table_multi_batch_deterministic_data_context: FileDataContext,
):
    data_context: FileDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    variables_configs: dict = {
        "estimator": "quantiles",
        "false_positive_rate": 1.0e-2,
        "mostly": 1.0,
    }
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    # TODO: <Alex>ALEX</Alex>
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }
    # TODO: <Alex>ALEX</Alex>

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder(
        profile_path=profile_path,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule",
        variables=variables,
        # TODO: <Alex>ALEX</Alex>
        batch_request=batch_request,
        # TODO: <Alex>ALEX</Alex>
    )
    print(f"\n[ALEX_TEST] [WOUTPUT] WOUTPUT:\n{domains} ; TYPE: {str(type(domains))}")

    assert len(domains) == 18
    # TODO: <Alex>ALEX</Alex>
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "vendor_id"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "vendor_id": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "pickup_datetime"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_datetime": SemanticDomainTypes.TEXT.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "dropoff_datetime"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_datetime": SemanticDomainTypes.TEXT.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "passenger_count"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "trip_distance"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "trip_distance": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "rate_code_id"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "rate_code_id": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "store_and_fwd_flag"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "store_and_fwd_flag": SemanticDomainTypes.TEXT.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "pickup_location_id"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "pickup_location_id": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "dropoff_location_id"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "dropoff_location_id": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "payment_type"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "payment_type": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "fare_amount"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "fare_amount": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "extra"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {"extra": SemanticDomainTypes.NUMERIC.value}
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "mta_tax"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "mta_tax": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "tip_amount"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tip_amount": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "tolls_amount"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tolls_amount": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "improvement_surcharge"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "improvement_surcharge": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "total_amount"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {"column": "congestion_surcharge"},
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "congestion_surcharge": SemanticDomainTypes.NUMERIC.value
                }
            },
        },
    ]
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    # assert domains == [
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "id",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "id": SemanticDomainTypes.TEXT.value,
    #             },
    #         },
    #     },
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "event_type",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "event_type": SemanticDomainTypes.NUMERIC.value,
    #             },
    #         },
    #     },
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "user_id",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "user_id": SemanticDomainTypes.NUMERIC.value,
    #             },
    #         },
    #     },
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "event_ts",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "event_ts": SemanticDomainTypes.TEXT.value,
    #             },
    #         },
    #     },
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "server_ts",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "server_ts": SemanticDomainTypes.TEXT.value,
    #             },
    #         },
    #     },
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "device_ts",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "device_ts": SemanticDomainTypes.TEXT.value,
    #             },
    #         },
    #     },
    #     {
    #         "rule_name": "my_rule",
    #         "domain_type": MetricDomainTypes.COLUMN.value,
    #         "domain_kwargs": {
    #             "column": "user_agent",
    #         },
    #         "details": {
    #             INFERRED_SEMANTIC_TYPE_KEY: {
    #                 "user_agent": SemanticDomainTypes.TEXT.value,
    #             },
    #         },
    #     },
    # ]
    # TODO: <Alex>ALEX</Alex>
