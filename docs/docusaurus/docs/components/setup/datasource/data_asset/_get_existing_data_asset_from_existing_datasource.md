
If you already have an instance of your Data Asset stored in a Python variable, you do not need to retrieve it again.  If you do not, you can instantiate a previously defined Data Source with your Data Context's `data_sources.get(...)` method.  Likewise, a Data Source's `get_asset(...)` method will instantiate a previously defined Data Asset.

In this example we will use a previously defined Data Source named `my_datasource` and a previously defined Data Asset named `my_asset`.

```python title="Python" name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_asset"
```

