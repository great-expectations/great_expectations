A Query Data Asset consists of the records returned by a SQL query.  It takes two required parameters:
- **query:** The SQL query that the Data Asset will retrieve records from.
- **name:** The name used to reference the Query Data Asset within GX.  You may assign this arbitrarily, but all Data Assets within the same Data Source must have unique names.

Replace the values of `asset_name` and `asset_query` in the following code, then execute it to add a Query Data Asset to your Data Source:

```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py query asset"
```