def test_repeated_append_expectations(dataset):
    # Repeatedly evaluating the "same" expectation should add it only one time.
    assert 3 == len(dataset.get_expectation_suite().expectations)
    dataset.expect_column_to_exist('nulls')
    assert 3 == len(dataset.get_expectation_suite().expectations)
