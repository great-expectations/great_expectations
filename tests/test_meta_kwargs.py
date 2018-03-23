import great_expectations as ge


def test_expect_column_values_to_not_be_in_set():
    """
    Cases Tested:
    -Repeat values being returned
    -Running expectations only on nonmissing values
    """

    D = ge.dataset.PandasDataset({
        'x' : [1,2,4],
        'z' : ['hello', 'jello', 'mello'],
        'a' : [1,1,2],
        'n' : [None,None,2],
    })
    D.set_default_expectation_argument("result_format", "COMPLETE")

    out = D.expect_column_values_to_not_be_in_set('x', meta={'values_set': [1,2]})
    assert out['success'] == False
    assert out['result']['unexpected_index_list'] == [0, 1]
    assert out['result']['unexpected_list'] == [1,2]

    out = D.expect_column_values_to_not_be_in_set('x', meta={'values_set': [5,6]})
    assert out['success'] == True
    assert out['result']['unexpected_index_list'] == []
    assert out['result']['unexpected_list'] == []

    print(D.get_expectations_config())

    D._expectations_config['meta']['expect_column_values_to_not_be_in_set'] = {}
    D._expectations_config['meta']['expect_column_values_to_not_be_in_set']['values_set'] = [5,6]

    out = D.expect_column_values_to_not_be_in_set('x', meta={'values_set': [5,6]})
    assert out['success'] == True
    assert out['result']['unexpected_index_list'] == []
    assert out['result']['unexpected_list'] == []

    print(D.get_expectations_config())

