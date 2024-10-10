from tests.integration.conftest import DataSourceType, parameterize_batch_for_data_sources


@parameterize_batch_for_data_sources(
    types=[DataSourceType.FOO, DataSourceType.BAR],
    data=[1, 2],
    # description="test_stuff",
)
def test_stuff(batch_for_datasource) -> None:
    assert batch_for_datasource == [1, 2, 3]  # this would actually be a Batch object
