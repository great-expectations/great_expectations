from __future__ import annotations

from pathlib import Path
from pprint import pformat as pf
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from great_expectations.data_context.util import file_relative_path
from great_expectations.experimental.datasources import (
    PandasDatasource,
    SqliteDatasource,
)
from great_expectations.experimental.datasources.interfaces import Batch, BatchRequest

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.pandas_datasource import CSVAsset
    from great_expectations.experimental.datasources.sqlite_datasource import (
        SqliteTableAsset,
    )


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


@pytest.fixture
def sqlite_batch() -> Batch:
    db_file = file_relative_path(
        __file__,
        Path("../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"),
    )
    connection_string = f"sqlite:///{db_file}"
    datasource = SqliteDatasource(
        name="sqlite_datasource",
        connection_string=connection_string,
    )
    data_asset_name = "table_asset"
    table_name = ""
    data_asset: SqliteTableAsset = datasource.add_table_asset(
        name=data_asset_name,
        table_name=table_name,
    )
    batch_request = BatchRequest(
        datasource_name=datasource.name, data_asset_name=data_asset.name, options={}
    )
    batch_list: list[Batch] = datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list) == 1
    return batch_list[0]


BATCH_HEAD_TEST_CASES = [
    (
        None,
        True,
        (
            "   vendor_id      pickup_datetime  ... total_amount  congestion_surcharge\n"
            "0          2  2018-01-11 18:24:44  ...        12.36                   NaN\n"
            "1          2  2018-01-05 15:31:57  ...         7.88                   NaN\n"
            "2          2  2018-01-01 05:07:32  ...        17.76                   NaN\n"
            "3          2  2018-01-11 13:35:39  ...        13.56                   NaN\n"
            "4          1  2018-01-01 12:49:52  ...        11.80                   NaN\n"
            "\n"
            "[5 rows x 18 columns]"
        ),
    ),
    (
        3,
        True,
        (
            "   vendor_id      pickup_datetime  ... total_amount  congestion_surcharge\n"
            "0          2  2018-01-11 18:24:44  ...        12.36                   NaN\n"
            "1          2  2018-01-05 15:31:57  ...         7.88                   NaN\n"
            "2          2  2018-01-01 05:07:32  ...        17.76                   NaN\n"
            "\n"
            "[3 rows x 18 columns]"
        ),
    ),
    (
        7,
        True,
        (
            "   vendor_id      pickup_datetime  ... total_amount  congestion_surcharge\n"
            "0          2  2018-01-11 18:24:44  ...        12.36                   NaN\n"
            "1          2  2018-01-05 15:31:57  ...         7.88                   NaN\n"
            "2          2  2018-01-01 05:07:32  ...        17.76                   NaN\n"
            "3          2  2018-01-11 13:35:39  ...        13.56                   NaN\n"
            "4          1  2018-01-01 12:49:52  ...        11.80                   NaN\n"
            "5          2  2018-01-07 10:17:50  ...        26.62                   NaN\n"
            "6          2  2018-01-18 10:00:48  ...         7.80                   NaN\n"
            "\n"
            "[7 rows x 18 columns]"
        ),
    ),
    (
        -9996,
        True,
        (
            "   vendor_id      pickup_datetime  ... total_amount  congestion_surcharge\n"
            "0          2  2018-01-11 18:24:44  ...        12.36                   NaN\n"
            "1          2  2018-01-05 15:31:57  ...         7.88                   NaN\n"
            "2          2  2018-01-01 05:07:32  ...        17.76                   NaN\n"
            "3          2  2018-01-11 13:35:39  ...        13.56                   NaN\n"
            "\n"
            "[4 rows x 18 columns]"
        ),
    ),
    (
        "invalid_value",
        False,
        (
            "1 validation error for Head\n"
            "n_rows\n"
            "  value is not a valid integer (type=type_error.integer)"
        ),
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize(
    ["n_rows", "success", "stdout"],
    BATCH_HEAD_TEST_CASES,
)
def test_pandas_batch_head(
    pandas_batch: Batch, n_rows: int | str | None, success: bool, stdout: str
) -> None:
    if success:
        if n_rows:
            assert pf(pandas_batch.head(n_rows=n_rows)) == stdout
        else:
            assert pf(pandas_batch.head()) == stdout
    else:
        with pytest.raises(ValidationError) as e:
            pandas_batch.head(n_rows=n_rows)
        assert str(e.value) == stdout


@pytest.mark.unit
@pytest.mark.parametrize(
    ["n_rows", "success", "stdout"],
    BATCH_HEAD_TEST_CASES,
)
def test_sqlite_batch_head(
    sqlite_batch: Batch, n_rows: int | str | None, success: bool, stdout: str
) -> None:
    if success:
        if n_rows:
            assert pf(pandas_batch.head(n_rows=n_rows)) == stdout
        else:
            assert pf(pandas_batch.head()) == stdout
    else:
        with pytest.raises(ValidationError) as e:
            pandas_batch.head(n_rows=n_rows)
        assert str(e.value) == stdout
