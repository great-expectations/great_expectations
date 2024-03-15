import InfoHowBatchingRegexGroupsRecords from '../_components/_how_batching_regex_groups_records.md'

1. Define the parameters needed to create a Spark CSV Data Asset for an filesystem Data Source:

    - `name`: The Data Asset's name.  You may set this to any value you prefer.  In the following example, this is `"my_taxi_data_asset"`.
    - `batching_regex`: A regular expression.  Your Data Asset will make available the data in all files that match the regex your provide.  You can include a folder path (relative to your Datasource's `base_directory`) in the regular expression to match files that are nested in folders under the `base_directory`.
    
    ```python title="Python"
    asset_name = "my_taxi_data_asset"
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    ```
   
    <InfoHowBatchingRegexGroupsRecords/>

3. Run the following code to create your Data Asset:

    ```python title="Python"
    data_asset = datasource.add_csv_asset(
        name=asset_name,
        batching_regex=batching_regex,
    )
    ```