def test_repeated_append_expectations(dataset, test_backend):
    if test_backend == "mysql":
        expected_expectation_count = 2
    else:
        expected_expectation_count = 3
    # Repeatedly evaluating the "same" expectation should add it only one time.
    assert expected_expectation_count == len(
        dataset.get_expectation_suite().expectations
    )
    dataset.expect_column_to_exist("nulls")
    assert expected_expectation_count == len(
        dataset.get_expectation_suite().expectations
    )
