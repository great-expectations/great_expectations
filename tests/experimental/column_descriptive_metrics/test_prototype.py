# ## Connect to Data


def test_prototype():
    import great_expectations as gx

    context = gx.get_context()

    validator = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )

    context.list_datasources()

    datasource = context.get_datasource("default_pandas_datasource")

    data_asset = datasource.assets[0]

    batch_request = data_asset.build_batch_request()

    #
    #
    # (invoked via the Agent via an AgentAction)

    from great_expectations.experimental.column_descriptive_metrics.prototype import (
        CloudStoreBackend,
        ColumnDescriptiveMetricsStore,
        inspect_asset,
    )

    metrics = inspect_asset(data_asset, batch_request)

    # Another option, invoked via agent:
    # asset_inspector = AssetInspector(asset=data_asset)
    # metrics = asset_inspector.inspect_batch(batch_request)

    cloud_store_backend = CloudStoreBackend()

    column_descriptive_metrics_store = ColumnDescriptiveMetricsStore(
        backend=cloud_store_backend
    )

    column_descriptive_metrics_store.create(metrics)
