import pytest

from great_expectations.exceptions import BatchKwargsError
from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.datasource.generator import QueryGenerator


def test_basic_operation():
    # We should be able to include defined queries as part of configuration
    generator = QueryGenerator(
        queries={
            "my_asset": "SELECT * FROM my_table WHERE value = $condition",
            "my_simple_asset": "SELECT c1, c2 FROM my_table"
        }
    )

    # Returned assets should be typed and processed by template language
    batch_kwargs = generator.yield_batch_kwargs("my_asset", condition="foo")
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "SELECT * FROM my_table WHERE value = foo"

    # Without a template, everything should still work
    batch_kwargs = generator.yield_batch_kwargs("my_simple_asset")
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "SELECT c1, c2 FROM my_table"

    # When a data asset is configured to require a template but it is not available, we should
    # fail with an informative message
    with pytest.raises(BatchKwargsError) as exc:
        generator.yield_batch_kwargs("my_asset")
        assert "missing template key" in exc.message


def test_add_query():
    generator = QueryGenerator()
    generator.add_query("my_asset", "select * from my_table where val > $condition")

    batch_kwargs = generator.yield_batch_kwargs("my_asset", condition=5)
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "select * from my_table where val > 5"


def test_partition_id():
    generator = QueryGenerator(
        queries={
            "my_asset": "SELECT * FROM my_table WHERE value = $partition_id",
        }
    )

    batch_kwargs = generator.build_batch_kwargs_from_partition_id("my_asset", "foo")
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "SELECT * FROM my_table WHERE value = foo"
