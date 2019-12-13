from great_expectations.datasource.generator.passthrough_generator import PassthroughGenerator


def test_passthrough_generator(caplog):
    generator = PassthroughGenerator()

    # We should not be able to provide generator_asset names, and we should warn if someone tries
    assert generator.get_available_data_asset_names() == {"names": []}
    # assert caplog.messages[0] == "PassthroughGenerator cannot identify data_asset_names, but can accept any object as a valid data_asset."

    # We should not be able to get partition ids, and we should warn if someone tries
    caplog.clear()
    assert generator.get_available_partition_ids("dummy_asset") == []
    assert caplog.messages[0] == "PassthroughGenerator cannot identify partition_ids, but can accept partition_id " \
                                 "together with a valid GE object."

    # We should be able to iterate over manually-specified kwargs
    kwargs = generator.yield_batch_kwargs("dummy_asset", limit=5)
    assert len(kwargs) == 1
    assert kwargs['limit'] == 5

    kwargs = generator.build_batch_kwargs_from_partition_id("another_dummy_asset", partition_id='test', batch_kwargs={
        'limit': 5
    }, reader_options={})
    assert len(kwargs) == 3
    assert kwargs['limit'] == 5
    assert kwargs['reader_options'] == {}
    assert kwargs['partition_id'] == 'test'
