.. _datasource_reference:

#############################
Datasource Reference
#############################

To have a Datasource produce Data Assets of a custom type, such as when adding custom expectations by subclassing an
existing DataAsset type, use the `data_asset_type` parameter to configure the datasource to load and return DataAssets
of the custom type.

For example:

.. code-block:: yaml

  datasources:
    pandas:
      class_name: PandasDatasource
      data_asset_type:
        class_name: MyCustomPandasAsset
        module_name: internal_pandas_assets

Given the above configuration, we can observe the following:

>>> batch_kwargs = {
...   "datasource": "pandas",
...   "dataset": {"a": [1, 2, 3]}
... }
>>> batch = context.get_batch(batch_kwargs, my_suite)
>>> isinstance(batch, MyCustomPandasAsset)
True
