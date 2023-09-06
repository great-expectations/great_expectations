Connecting to your data consists of two parts.  First, you will tell GX where your data is located and how to access it:

```python
datasource_name = "my_new_datasource"
path_to_folder_containing_csv_files = "<insert_path_to_files_here>"

datasource = context.sources.add_pandas_filesystem(
    name=datasource_name, base_directory=path_to_folder_containing_csv_files
)
```

Second, you will specify the file or files at that location that GX should connect to:

```python
asset_name = "my_taxi_data_asset"
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

data_asset = datasource.add_csv_asset(name=asset_name, batching_regex=batching_regex)
```