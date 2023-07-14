"""
This file contains demo code for the column_descriptive_metrics module.
Unit, integration and end-to-end tests should be written to replace this code.
"""


from great_expectations.experimental.column_descriptive_metrics.asset_inspector import (
    AssetInspector,
)

import pandas as pd
import great_expectations as gx


def test_demo_asset_inspector():
    context = gx.get_context()
    datasource = context.sources.add_pandas(name="my_pandas_datasource")

    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)

    name = "dataframe"
    data_asset = datasource.add_dataframe_asset(name=name)
    my_batch_request = data_asset.build_batch_request(dataframe=df)
    # my_batch = data_asset.get_batch_list_from_batch_request(
    #     batch_request=my_batch_request
    # )[0]

    # From here down assume we just have the batch request from the agent action:
    datasource_from_action = context.get_datasource(my_batch_request.datasource_name)
    data_asset_from_action = datasource_from_action.get_data_asset(
        my_batch_request.data_asset_name
    )
    batch_from_action = data_asset_from_action.get_batch_list_from_batch_request(
        my_batch_request
    )[0]

    asset_inspector = AssetInspector()
    # metrics = asset_inspector.get_column_descriptive_metrics(
    #     batch_request=my_batch_request
    # )
    metrics = asset_inspector.get_column_descriptive_metrics(batch=batch_from_action)
    print(metrics)
