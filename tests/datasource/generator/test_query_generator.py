from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.datasource.generator import QueryGenerator


def test_basic_operation(basic_sqlalchemy_datasource):
    # We should be able to include defined queries as part of configuration
    generator = QueryGenerator(
        datasource=basic_sqlalchemy_datasource,
        queries={
            "my_asset": "SELECT * FROM my_table WHERE value = $condition",
            "my_simple_asset": "SELECT c1, c2 FROM my_table"
        }
    )

    # Returned assets should be typed and processed by template language
    batch_kwargs = generator.yield_batch_kwargs("my_asset", query_parameters={'condition': "foo"})
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "SELECT * FROM my_table WHERE value = $condition"
    assert batch_kwargs.query_parameters == {'condition': "foo"}

    # Without a template, everything should still work
    batch_kwargs = generator.yield_batch_kwargs("my_simple_asset")
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "SELECT c1, c2 FROM my_table"


def test_partition_id(basic_sqlalchemy_datasource):
    generator = QueryGenerator(
        datasource=basic_sqlalchemy_datasource,
        queries={
            "my_asset": "SELECT * FROM my_table WHERE value = $partition_id",
        }
    )

    batch_kwargs = generator.build_batch_kwargs("my_asset", partition_id="foo")
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceQueryBatchKwargs)
    assert batch_kwargs.query == "SELECT * FROM my_table WHERE value = $partition_id"
    assert batch_kwargs.query_parameters == {"partition_id": "foo"}
