import great_expectations as gx
import os
from great_expectations.self_check.util import build_sa_execution_engine
import pandas as pd
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.expectations.metrics.table_metrics.table_head import TableHead
import pytest
from great_expectations.compatibility import sqlalchemy


@pytest.fixture
def sqlalchemy_test_engine(sa):
    return build_sa_execution_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]}), sa
    )


def test_head_table_name_not_fetch_all_2_rows(sqlalchemy_test_engine):
    execution_engine: SqlAlchemyExecutionEngine = sqlalchemy_test_engine
    table_head = TableHead()

    table_name = "test_table"
    n_rows = 2
    fetch_all = False

    res = table_head._sqlalchemy(
        execution_engine=execution_engine,
        metric_domain_kwargs={"table_name": table_name},
        metric_value_kwargs={"n_rows": n_rows, "fetch_all": fetch_all},
        metrics={},
        runtime_configuration={},
    )

    assert res.shape == (n_rows, 2)
    assert res.columns.tolist() == ["a", "b"]
    assert res.values.tolist() == [[1, 2], [2, 3]]


def test_head_no_table_name_not_fetch_all_2_rows(sqlalchemy_test_engine):
    execution_engine: SqlAlchemyExecutionEngine = sqlalchemy_test_engine
    table_head = TableHead()

    table_name = None
    n_rows = 2
    fetch_all = False

    res = table_head._sqlalchemy(
        execution_engine=execution_engine,
        metric_domain_kwargs={"table_name": table_name},
        metric_value_kwargs={"n_rows": n_rows, "fetch_all": fetch_all},
        metrics={},
        runtime_configuration={},
    )

    assert res.shape == (n_rows, 2)
    assert res.columns.tolist() == ["a", "b"]
    assert res.values.tolist() == [[1, 2], [2, 3]]


def test_head_table_name_is_anonymous_not_fetch_all(sqlalchemy_test_engine):
    execution_engine: SqlAlchemyExecutionEngine = sqlalchemy_test_engine
    table_head = TableHead()

    table_name = sqlalchemy._anonymous_label("test_table")
    n_rows = 2
    fetch_all = False

    res = table_head._sqlalchemy(
        execution_engine=execution_engine,
        metric_domain_kwargs={"table_name": table_name},
        metric_value_kwargs={"n_rows": n_rows, "fetch_all": fetch_all},
        metrics={},
        runtime_configuration={},
    )

    assert res.shape == (n_rows, 2)
    assert res.columns.tolist() == ["a", "b"]
    assert res.values.tolist() == [[1, 2], [2, 3]]


def test_head_table_name_fetch_all(sqlalchemy_test_engine):
    execution_engine: SqlAlchemyExecutionEngine = sqlalchemy_test_engine
    table_head = TableHead()

    table_name = "test_table"
    fetch_all = True

    with pytest.warns(
        UserWarning,
        match="fetch_all loads all of the rows into memory. This may cause performance issues.",
    ):
        res = table_head._sqlalchemy(
            execution_engine=execution_engine,
            metric_domain_kwargs={"table_name": table_name},
            metric_value_kwargs={"fetch_all": fetch_all},
            metrics={},
            runtime_configuration={},
        )
        assert res.shape == (4, 2)
        assert res.columns.tolist() == ["a", "b"]
        assert res.values.tolist() == [[1, 2], [2, 3], [3, 4], [4, 5]]
