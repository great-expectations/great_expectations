from great_expectations.datasource.batch_kwargs_generator.manual_batch_kwargs_generator import (
    ManualBatchKwargsGenerator,
)


def test_manual_generator(basic_pandas_datasource):
    generator = ManualBatchKwargsGenerator(
        datasource=basic_pandas_datasource,
        assets={
            "asset1": [
                {
                    "partition_id": 1,
                    "path": "/data/file_1.csv",
                    "reader_options": {"sep": ";"},
                },
                {
                    "partition_id": 2,
                    "path": "/data/file_2.csv",
                    "reader_options": {"header": 0},
                },
            ],
            "logs": {"s3": "s3a://my_bucket/my_prefix/data/file.csv.gz"},
        },
    )

    # We should be able to provide generator_asset names
    assert generator.get_available_data_asset_names() == {
        "names": [("asset1", "manual"), ("logs", "manual")]
    }

    # We should be able to get partition ids
    assert generator.get_available_partition_ids(data_asset_name="asset1") == [1, 2]
    assert generator.get_available_partition_ids(data_asset_name="logs") == []

    # We should be able to iterate over manually-specified kwargs
    kwargs = generator.yield_batch_kwargs("logs", limit=5)
    assert len(kwargs) == 3
    assert kwargs["reader_options"] == {
        "nrows": 5
    }  # IMPORTANT: Note that *limit* was a batch parameter,
    # and *because we used a PandasDatasource was translated into reader_options
    assert kwargs["s3"] == "s3a://my_bucket/my_prefix/data/file.csv.gz"

    kwargs = generator.build_batch_kwargs(data_asset_name="asset1", partition_id=2)
    assert len(kwargs) == 4
    assert kwargs["path"] == "/data/file_2.csv"
    assert kwargs["reader_options"] == {"header": 0}
    assert kwargs["datasource"] == "basic_pandas_datasource"
