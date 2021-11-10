from datetime import datetime

import pandas as pd
import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext


def test_expect_column_max_to_be_between_warn_parse_strings_as_datetimes(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame(
        {
            "a": [
                "2021-01-01",
                "2021-01-31",
                "2021-02-28",
                "2021-03-20",
                "2021-02-21",
                "2021-05-01",
                "2021-06-18",
            ]
        }
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    with pytest.warns(DeprecationWarning) as record:
        validator.expect_column_max_to_be_between(
            column="a",
            min_value=datetime.strptime("2021-02-20", "%Y-%m-%d"),
            max_value=datetime.strptime("2021-07-20", "%Y-%m-%d"),
            parse_strings_as_datetimes=True,
        )
    assert (
        'The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated'
        in str(record.list[0].message)
    )
