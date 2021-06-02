from typing import List

import pandas as pd

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
    SingleTableDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.self_check.util import build_pandas_validator_with_data
from great_expectations.validator.validator import Validator


# noinspection PyPep8Naming
def test_build_single_table_domain(
    table_Users_domain,
):
    batch_definition: BatchDefinition = BatchDefinition(
        datasource_name="my_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="my_data_asset",
        batch_identifiers=IDDict({}),
    )

    df: pd.DataFrame = pd.DataFrame(
        {
            "a": [0, 1, 2, 3, None],
            "b": [0, 1, 2, 3, None],
        }
    )
    validator: Validator = build_pandas_validator_with_data(
        df=df,
        batch_definition=batch_definition,
    )

    domain_builder: DomainBuilder = SingleTableDomainBuilder()
    domains: List[Domain] = domain_builder.get_domains(
        validator=validator,
        batch_ids=None,
    )

    assert len(domains) == 1
    assert domains[0].domain_kwargs.batch_id == "f576df3a81c34925978336d530453bc4"
