import InfoUsingPandasToConnectToDifferentFileTypes from '../_components/_using_pandas_to_connect_to_different_file_types.md'
import InfoHowBatchingRegexGroupsRecords from '../_components/_how_batching_regex_groups_records.md'

1. Define the parameters needed to create a pandas CSV Data Asset for an Azure Blob Storage Data Source:

    - `name`: The Data Asset's name.  You may set this to any value you prefer.  In the following example, this is `"my_taxi_data_asset"`.
    - `batching_regex`: A regular expression.  Your Data Asset will make available the data in all files that match the regex your provide.
    - abs_container: The name of your Azure Blob Storage container
    - `abs_name_starts_with`: A string indicating what part of the `batching_regex` to truncate from the final Batch names.
    - `abs_recursive_file_discovery`: (Optional) A boolean indicating if files should be searched recursively from subfolders.
    
    ```python title="Python"
    asset_name = "my_taxi_data_asset"
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    abs_container = "my_container"
    abs_name_starts_with = "data/taxi_yellow_tripdata_samples/"
    ```
   
    <InfoHowBatchingRegexGroupsRecords/>

2. Run the following code to create your Data Asset:

    ```python title="Python"
    data_asset = datasource.add_csv_asset(
        name=asset_name,
        batching_regex=batching_regex,
        abs_container=abs_container,
        abs_name_starts_with=abs_name_starts_with
    )
    ```

  <InfoUsingPandasToConnectToDifferentFileTypes/>