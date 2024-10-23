A Table Data Asset consists of the records in a single table.  It takes two required parameters:
- **table_name:** The name of the SQL table that the Table Data Asset will retrieve records from.
- **name:** The name used to reference the Table Data Asset within GX.  You may assign this arbitrarily, but all Data Assets  within the same Data Source must have unique names.

Replace the values of `asset_name` and `database_table_name` in the following code, then execute it to add a Table Data Asset to your Data Source:

```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py table asset"
```