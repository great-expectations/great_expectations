from great_expectations.validator.validator import Validator
import pytest

import pandas as pd

from great_expectations.datasource.misc_types import (
    BatchIdentifiers,
    NewRuntimeBatchRequest,
)
from great_expectations.datasource.runtime_pandas_datasource import (
    RuntimePandasDatasource,
)
from tests.test_utils import _get_batch_request_from_validator, _get_data_from_validator
from tests.datasource.new_fixtures import test_dir_oscar

@pytest.mark.skip(reason="not messing around with validator-like batches yet")
def test_RuntimePandasDatasource_get_batch():
    my_datasource = RuntimePandasDatasource(name="my_datasource")

    raw_df = pd.DataFrame({
        "x": range(10)
    })

    df = my_datasource.get_batch(raw_df)

    batch_request = _get_batch_request_from_validator(df)
    assert batch_request == NewRuntimeBatchRequest(
        datasource_name="my_datasource"
    )

def test_RuntimePandasDatasource_get_validator_from_data_argument():
    my_datasource = RuntimePandasDatasource(name="my_datasource")

    raw_df = pd.DataFrame({
        "x": range(10)
    })

    df = my_datasource.get_validator(raw_df, timestamp=0)
    assert isinstance(df, Validator)

    batch_request = _get_batch_request_from_validator(df)
    assert batch_request == NewRuntimeBatchRequest(
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        batch_identifiers=BatchIdentifiers(
            id_= None,
            timestamp= 0,
        ),
        data=raw_df,
    )

def test_RuntimePandasDatasource_get_validator_from_batch_request_argument():
    my_datasource = RuntimePandasDatasource(name="my_datasource")

    raw_df = pd.DataFrame({
        "x": range(10)
    })

    batch_request = NewRuntimeBatchRequest(
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        batch_identifiers=BatchIdentifiers(
            id_= "some_id",
            timestamp= 100,
        ),
        data=raw_df,
    )

    df = my_datasource.get_validator(
        batch_request=batch_request
    )
    assert isinstance(df, Validator)

    batch_request = _get_batch_request_from_validator(df)
    assert batch_request == NewRuntimeBatchRequest(
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        batch_identifiers=BatchIdentifiers(
            id_= "some_id",
            timestamp= 100,
        ),
        data=raw_df,
    )

#!!!
@pytest.mark.skip(reason="Doesn't work yet")
def test_RuntimePandasDatasource_get_validator_from_method_argument(test_dir_oscar):
    my_datasource = RuntimePandasDatasource(name="my_datasource")

    df = my_datasource.get_validator(
        method="read_csv",
        filepath_or_buffer=test_dir_oscar+"/A/data-202201.csv",
    )
    assert isinstance(df, Validator)

    batch_request = _get_batch_request_from_validator(df)
    assert isinstance(batch_request, NewRuntimeBatchRequest)

    print(batch_request.data)
    target_batch_request = NewRuntimeBatchRequest(
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        batch_identifiers=BatchIdentifiers(
            id_= "some_id",
            timestamp= 100,
        ),
        data=pd.DataFrame({
            "x": [1,2],
            "y": [2,3],
        }),
    )
    assert batch_request == target_batch_request