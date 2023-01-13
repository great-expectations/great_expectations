from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pydantic import ValidationError

from great_expectations.data_context.util import file_relative_path
from great_expectations.experimental.datasources import PandasDatasource
from great_expectations.experimental.datasources.interfaces import BatchRequest

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import Batch
    from great_expectations.experimental.datasources.pandas_datasource import CSVAsset


@pytest.fixture
def pandas_batch() -> Batch:
    datasource = PandasDatasource(name="pandas_datasource")
    data_asset_name = "csv_asset"
    data_path = Path(
        file_relative_path(__file__, "../../test_sets/taxi_yellow_tripdata_samples/")
    )
    regex = r"yellow_tripdata_sample_2018-01.csv"
    data_asset: CSVAsset = datasource.add_csv_asset(
        name=data_asset_name,
        data_path=data_path,
        regex=regex,
    )
    batch_request = BatchRequest(
        datasource_name=datasource.name, data_asset_name=data_asset.name, options={}
    )
    batch_list: list[Batch] = datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list) == 1
    return batch_list[0]


@pytest.mark.unit
@pytest.mark.parametrize(
    ["n_rows", "success"],
    [
        (
            None,
            True,
        ),
        (
            3,
            True,
        ),
        (
            7,
            True,
        ),
        (
            -9996,
            True,
        ),
        (
            "invalid_value",
            False,
        ),
    ],
)
def test_batch_head(
    pandas_batch: Batch, n_rows: int | str | None, success: bool
) -> None:
    if success:
        head_df: pd.DataFrame
        if n_rows:
            head_df = pandas_batch.head(n_rows=n_rows)
            assert isinstance(head_df, pd.DataFrame)
            if n_rows > 0:
                assert len(head_df.index) == n_rows
            else:
                assert len(head_df.index) == n_rows + 10000
        else:
            head_df = pandas_batch.head()
            assert isinstance(head_df, pd.DataFrame)
            assert len(head_df.index) == 5

    else:
        with pytest.raises(ValidationError) as e:
            pandas_batch.head(n_rows=n_rows)
        assert str(e.value) == (
            "1 validation error for Head\n"
            "n_rows\n"
            "  value is not a valid integer (type=type_error.integer)"
        )
