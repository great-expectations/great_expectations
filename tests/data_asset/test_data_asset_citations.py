def test_data_asset_citations(pandas_dataset):
    citation_date = "2020-02-27T12:34:56.1234Z"
    pandas_dataset.add_citation("test citation", citation_date=citation_date)
    suite = pandas_dataset.get_expectation_suite()
    assert suite.meta["citations"][0] == {
        "comment": "test citation",
        "batch_kwargs": pandas_dataset.batch_kwargs,
        "batch_parameters": pandas_dataset.batch_parameters,
        "batch_markers": pandas_dataset.batch_markers,
        "citation_date": citation_date,
    }
