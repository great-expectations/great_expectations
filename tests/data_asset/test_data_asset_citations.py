

def test_data_asset_citations(dataset):
    # TODO: We don't need to run this test against all backends...
    citation_date = "2020-02-27T12:34:56.1234Z"
    dataset.add_citation("test citation", citation_date=citation_date)
    suite = dataset.get_expectation_suite()
    assert suite.meta["citations"][0] == {
        "comment": "test citation",
        "batch_kwargs": dataset.batch_kwargs,
        "batch_parameters": dataset.batch_parameters,
        "batch_markers": dataset.batch_markers,
        "citation_date": citation_date
    }