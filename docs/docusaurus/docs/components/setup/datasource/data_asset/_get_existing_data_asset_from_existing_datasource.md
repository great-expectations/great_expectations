
If you already have an instance of your Data Asset stored in a Python variable, you do not need to retrieve it again.  If you do not, you can instantiate a previously defined Datasource with your Data Context's `get_datasource(...)` method.  Likewise, a Datasource's `get_asset(...)` method will instantiate a previously defined Data Asset.

In this example we will use a previously defined Datasource named `my_datasource` and a previously defined Data Asset named `my_asset`.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_asset"
```

