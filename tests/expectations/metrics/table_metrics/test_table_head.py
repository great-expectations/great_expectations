from unittest import mock

import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.table_metrics.table_head import TableHead
from tests.test_utils import get_sqlite_temp_table_names_from_engine


@pytest.fixture
def sqlite_engine():
    sqlite_engine = sa.create_engine("sqlite://")
    dataframe = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]})
    add_dataframe_to_db(
        df=dataframe,
        name="test_table",
        con=sqlite_engine,
        index=False,
    )
    return sqlite_engine


@pytest.fixture
def sqlite_batch_with_table_name(sqlite_engine) -> SqlAlchemyExecutionEngine:
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_engine
    )
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        table_name="test_table",
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.fixture
def sqlite_batch_with_selectable_with_temp_table(
    sqlite_engine,
) -> SqlAlchemyExecutionEngine:
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_engine
    )
    selectable = sa.select("*").select_from(sa.text("main.test_table"))
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine, selectable=selectable, create_temp_table=True
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.fixture
def sqlite_batch_with_selectable_without_temp_table(
    sqlite_engine,
) -> SqlAlchemyExecutionEngine:
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_engine
    )
    selectable = sa.select("*").select_from(sa.text("main.test_table"))
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=selectable,
        create_temp_table=False,
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "execution_engine, n_rows, fetch_all, expected_shape, expected_columns, expected_values, expected_temp_tables",
    [
        (
            "sqlite_batch_with_table_name",
            2,
            False,
            (2, 2),
            ["a", "b"],
            [[1, 2], [2, 3]],
            0,
        ),
        (
            "sqlite_batch_with_table_name",
            None,
            True,
            (4, 2),
            ["a", "b"],
            [[1, 2], [2, 3], [3, 4], [4, 5]],
            0,
        ),
        (
            "sqlite_batch_with_selectable_with_temp_table",
            2,
            False,
            (2, 2),
            ["a", "b"],
            [[1, 2], [2, 3]],
            1,
        ),
        (
            "sqlite_batch_with_selectable_with_temp_table",
            None,
            True,
            (4, 2),
            ["a", "b"],
            [[1, 2], [2, 3], [3, 4], [4, 5]],
            1,
        ),
        (
            "sqlite_batch_with_selectable_without_temp_table",
            2,
            False,
            (2, 2),
            ["a", "b"],
            [[1, 2], [2, 3]],
            0,
        ),
        (
            "sqlite_batch_with_selectable_without_temp_table",
            None,
            True,
            (4, 2),
            ["a", "b"],
            [[1, 2], [2, 3], [3, 4], [4, 5]],
            0,
        ),
    ],
)
def test_table_head_sqlite(
    execution_engine,
    n_rows,
    fetch_all,
    expected_shape,
    expected_columns,
    expected_values,
    request,
    expected_temp_tables,
):
    engine = request.getfixturevalue(execution_engine)
    table_head = TableHead()
    res = table_head._sqlalchemy(
        execution_engine=engine,
        metric_domain_kwargs={},
        metric_value_kwargs={"n_rows": n_rows, "fetch_all": fetch_all},
        metrics={},
        runtime_configuration={},
    )

    assert res.shape == expected_shape
    assert res.columns.tolist() == expected_columns
    assert res.values.tolist() == expected_values
    assert (
        len(get_sqlite_temp_table_names_from_engine(engine.engine))
        == expected_temp_tables
    )


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "execution_engine",
    [
        "sqlite_batch_with_table_name",
        "sqlite_batch_with_selectable_with_temp_table",
        "sqlite_batch_with_selectable_without_temp_table",
    ],
)
@pytest.mark.parametrize(
    "n_rows",
    [None, 0, 1, 2],
)
@pytest.mark.parametrize(
    "fetch_all",
    [None, True, False],
)
def test_limit_included_in_head_query(
    execution_engine,
    n_rows,
    fetch_all,
    request,
):
    engine = request.getfixturevalue(execution_engine)
    table_head = TableHead()

    with mock.patch(
        "great_expectations.compatibility.sqlalchemy_and_pandas.pd.read_sql"
    ) as mock_node:
        table_head._sqlalchemy(
            execution_engine=engine,
            metric_domain_kwargs={},
            metric_value_kwargs={"n_rows": n_rows, "fetch_all": fetch_all},
            metrics={},
            runtime_configuration={},
        )

        args, kwargs = mock_node.call_args
        mock_node.assert_called_once()

        assert ("limit" in str(kwargs["sql"]).lower()) == (fetch_all is not True)
