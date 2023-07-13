from great_expectations.datasource.data_connector.batch_filter import parse_batch_slice


def test_parse_batch_slice():
    assert parse_batch_slice("[0:5]") == slice(0, 5, None)
    assert parse_batch_slice("[-5:]") == slice(-5, None, None)
    assert parse_batch_slice("[:5]") == slice(None, 5, None)
    assert parse_batch_slice("[:]") == slice(None, None, None)
    assert parse_batch_slice("[0:5:2]") == slice(0, 5, 2)
    assert parse_batch_slice("-5:") == slice(-5, None, None)
