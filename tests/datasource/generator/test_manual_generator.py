from great_expectations.datasource.generator.manual_generator import ManualGenerator


def test_manual_generator():
    generator = ManualGenerator(assets={
        "asset1": [
            {
                "partition_id": 1,
                "path": "/data/file_1.csv",
                "reader_options": {
                    "sep": ";"
                }
            },
            {
                "partition_id": 2,
                "path": "/data/file_2.csv",
                "reader_options": {
                    "header": 0
                }
            }
        ],
        "logs": {
            "s3": "s3a://my_bucket/my_prefix/data/file.csv.gz"
        }
    })

    # We should be able to provide generator_asset names
    assert generator.get_available_data_asset_names() == {"names": ["asset1", "logs"]}

    # We should not be able to provide partition ids
    assert generator.get_available_partition_ids("asset1") == [1, 2]
    assert generator.get_available_partition_ids("logs") == []

    # We should be able to iterate over manually-specified kwargs
    kwargs = generator.yield_batch_kwargs("logs", limit=5)
    assert len(kwargs) == 2
    assert kwargs['limit'] == 5
    assert kwargs['s3'] == "s3a://my_bucket/my_prefix/data/file.csv.gz"

    kwargs = generator.build_batch_kwargs_from_partition_id("asset1", partition_id=1)
    assert len(kwargs) == 3
    assert kwargs['path'] == '/data/file_1.csv'
    assert kwargs['reader_options'] == {'sep': ';'}
    assert kwargs['partition_id'] == 1
