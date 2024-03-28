import InfoHowBatchingRegexGroupsRecords from '../_components/_how_batching_regex_groups_records.md'

1. Define the parameters needed to create a Spark CSV Data Asset for a GCS Data Source:

    - `asset_name`: The Data Asset's name.  You may set this to any value you prefer.  In the following example, this is `"my_taxi_data_asset"`.
    - `gcs_prefix`: The folder containing your CSV files.
    - `batching_regex`: A regular expression.  Your Data Asset will make available the data in all files that match the regex your provide.
    - `header`: A boolean that indicates if the first row of your CSV files is a header row containing column names.
    - `infer_schema`: A boolean that indicates if Spark should infer the column types (String, Double, Long, etc) of your CSV files.
    
    ```python title="Python"
    asset_name = "my_taxi_data_asset"
    gcs_prefix = "data/taxi_yellow_tripdata_samples/"
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    header=True
    infer_schema=True
    ```
   
    <InfoHowBatchingRegexGroupsRecords/>

3. Run the following code to create your Data Asset:

    ```python title="Python"
    data_asset = datasource.add_csv_asset(
        name=asset_name,
        batching_regex=batching_regex,
        gcs_prefix=gcs_prefix,
        header=header,
        infer_schema=infer_schema
    )
    ```