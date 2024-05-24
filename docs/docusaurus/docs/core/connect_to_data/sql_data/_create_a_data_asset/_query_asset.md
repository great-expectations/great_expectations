A Query Data Asset consists of the records returned by a SQL query.  It takes two required parameters:
- **query:** The SQL query that the Data Asset will retrieve records from.
- **name:** The name used to reference the Query Data Asset within GX.  You may assign this arbitrarily, but all Data Assets within the same Data Source must have unique names.

Replace the values of `asset_name` and `asset_query` in the following code, then execute it to add a Query Data Asset to your Data Source:

```python title="Python"
asset_name = "MY QUERY ASSET"
asset_query = "SELECT * from postgres_taxi_data"
data_source.add_query_asset(query=asset_query, name=asset_name)
```