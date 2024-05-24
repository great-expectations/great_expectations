A Table Data Asset consists of the records in a single table.  It takes two required parameters:
- **table_name:** The name of the SQL table that the Table Data Asset will retrieve records from.
- **name:** The name used to reference the Table Data Asset within GX.  You may assign this arbitrarily, but all Data Assets  within the same Data Source must have unique names.

Replace the values of `asset_name` and `asset_table` in the following code, then execute it to add a Table Data Asset to your Data Source:

```python title="Python"
asset_name = "MY TABLE ASSET"
asset_table = "postgres_taxi_data"
data_source.add_table_asset(table_name=asset_table, name=asset_name)
```