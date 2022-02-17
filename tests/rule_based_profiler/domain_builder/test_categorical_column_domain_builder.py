from typing import List

from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.categorical_column_domain_builder import (
    CategoricalColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.types import Domain


def test_single_batch_very_few_cardinality(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        data_context=data_context,
        batch_request=None,
    )
    domains: List[Domain] = domain_builder.get_domains()

    assert len(domains) == 7

    alice_column_names = [
        "id",
        "event_type",
        "user_id",
        "event_ts",
        "server_ts",
        "device_ts",
        "user_agent",
    ]
    alice_all_column_domains = [
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": column_name,
            },
            "details": {},
        }
        for column_name in alice_column_names
    ]
    assert domains == alice_all_column_domains
