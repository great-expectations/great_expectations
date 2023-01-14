from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pydantic import ValidationError

from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.experimental.datasources import (
    PandasDatasource,
    SqliteDatasource,
)
from great_expectations.experimental.datasources.interfaces import BatchRequest
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import Batch
    from great_expectations.experimental.datasources.pandas_datasource import CSVAsset
    from great_expectations.experimental.datasources.sqlite_datasource import (
        SqliteTableAsset,
    )
    from great_expectations.validator.computed_metric import MetricValue


def pandas_batch() -> Batch:
    datasource = PandasDatasource(name="pandas_datasource")
    data_asset_name = "csv_asset"
    data_path = Path(
        file_relative_path(__file__, "../../test_sets/taxi_yellow_tripdata_samples/")
    )
    regex = r"yellow_tripdata_sample_2019-01.csv"
    asset: CSVAsset = datasource.add_csv_asset(
        name=data_asset_name,
        data_path=data_path,
        regex=regex,
    )
    batch_request = BatchRequest(
        datasource_name=datasource.name, data_asset_name=asset.name, options={}
    )
    batch_list: list[Batch] = datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list) == 1
    return batch_list[0]


def sqlite_batch() -> Batch:
    db_file = (
        Path(__file__)
        / "../../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    ).resolve()
    datasource = SqliteDatasource(
        name="sqlite_datasource",
        connection_string=f"sqlite:///{db_file}",
    )
    asset: SqliteTableAsset = datasource.add_table_asset(
        name="table_asset",
        table_name="yellow_tripdata_sample_2019_01",
    )
    batch_request = BatchRequest(
        datasource_name=datasource.name,
        data_asset_name=asset.name,
        options={},
    )
    batch_list: list[Batch] = datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list) == 1
    return batch_list[0]


@pytest.fixture(params=[pandas_batch, sqlite_batch])
def batch(request: pytest.FixtureRequest) -> Batch:
    return request.param()


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
            -100,
            True,
        ),
        (
            "invalid_value",
            False,
        ),
        (
            1.5,
            False,
        ),
    ],
)
def test_batch_head(batch: Batch, n_rows: int | str | None, success: bool) -> None:
    if success:
        head_df: pd.DataFrame
        if n_rows is not None:
            # if n_rows is not None we pass it to Batch.head()
            head_df = batch.head(n_rows=n_rows)
            assert isinstance(head_df, pd.DataFrame)
            if n_rows > 0:
                assert len(head_df.index) == n_rows
            else:
                resolved_metrics: dict[tuple[str, str, str], MetricValue] = {}
                row_count_metric = MetricConfiguration(
                    metric_name="table.row_count",
                    metric_domain_kwargs={"batch_id": batch.id},
                    metric_value_kwargs=None,
                )
                if isinstance(batch.data.execution_engine, SqlAlchemyExecutionEngine):
                    row_count_partial_fn_metric = MetricConfiguration(
                        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                        metric_domain_kwargs={"batch_id": batch.id},
                        metric_value_kwargs=None,
                    )
                    resolved_metrics.update(
                        batch.data.execution_engine.resolve_metrics(
                            metrics_to_resolve=(row_count_partial_fn_metric,)
                        )
                    )
                    row_count_metric.metric_dependencies = {
                        "metric_partial_fn": row_count_partial_fn_metric
                    }

                resolved_metrics = batch.data.execution_engine.resolve_metrics(
                    metrics_to_resolve=(row_count_metric,), metrics=resolved_metrics
                )
                row_count: int = resolved_metrics[row_count_metric.id]
                assert len(head_df.index) == n_rows + row_count
        else:
            # default to 5 rows
            head_df = batch.head()
            assert isinstance(head_df, pd.DataFrame)
            assert len(head_df.index) == 5
    else:
        with pytest.raises(ValidationError) as e:
            batch.head(n_rows=n_rows)
        assert str(e.value) == (
            "1 validation error for Head\n"
            "n_rows\n"
            "  value is not a valid integer (type=type_error.integer)"
        )
