from typing import List

import os

import pytest
from ruamel.yaml import YAML

from contrib.capitalone_dataprofiler_expectations.capitalone_dataprofiler_expectations.rule_based_profiler.domain_builder.data_profiler_column_domain_builder import DataProfilerColumnDomainBuilder

from great_expectations.rule_based_profiler.domain_builder import DomainBuilder

yaml = YAML(typ="safe")


@pytest.mark.integration
@pytest.mark.slow  # 1.21s
def test_data_profiler_column_domain_builder(
):

    test_root_path = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    )

    profile_path = os.path.join(
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    variables, a, b, c = dict(), dict(), dict(), dict()

    b["variables"] = c

    a["variables"] = b

    variables["parameter_nodes"] = a

    variables["parameter_nodes"]["variables"]["variables"]["profile_path"] = profile_path

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder()
    text_column_names = domain_builder.get_effective_column_names(
        rule_name="text_rule", variables=variables
    )
    numeric_column_names = domain_builder.get_effective_column_names(
        rule_name="numeric_rule", variables=variables
    )
    assert (text_column_names == ['store_and_fwd_flag'])
    assert (numeric_column_names == ['VendorID', 'passenger_count', 'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']
)

    domain_builder: DomainBuilder = DataProfilerColumnDomainBuilder()
    text_domains = domain_builder._get_domains(
        rule_name="text_rule", variables=variables
    )
    numeric_domains = domain_builder._get_domains(
        rule_name="numeric_rule", variables=variables
    )
    assert (text_domains == [{
        "domain_type": "column",
        "domain_kwargs": {
            "column": "store_and_fwd_flag"
        },
        "rule_name": "text_rule"
    }])
    assert (numeric_domains == [{
        "domain_type": "column",
        "domain_kwargs": {
            "column": "VendorID"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "passenger_count"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "trip_distance"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "RatecodeID"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "PULocationID"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "DOLocationID"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "payment_type"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "fare_amount"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "extra"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "mta_tax"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "tip_amount"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "tolls_amount"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "improvement_surcharge"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "total_amount"
        },
        "rule_name": "numeric_rule"
    }, {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "congestion_surcharge"
        },
        "rule_name": "numeric_rule"
    }])