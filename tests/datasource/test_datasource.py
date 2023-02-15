from great_expectations.datasource import LegacyDatasource


def test_list_generators_returns_empty_list_if_no_generators_exist():
    datasource = LegacyDatasource(name="foo")
    assert isinstance(datasource, LegacyDatasource)
    obs = datasource.list_batch_kwargs_generators()
    assert obs == []
