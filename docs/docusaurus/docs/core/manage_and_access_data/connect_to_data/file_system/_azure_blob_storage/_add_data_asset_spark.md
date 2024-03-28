1. Define the parameters needed to create a Spark CSV Data Asset for an Azure Blob Storage Data Source:

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
   
    :::info How `batching_regex` groups records

    The records in each file matched by your `batching_regex` will be grouped as a Batch inside your Data Asset.

    For example, let's say that your S3 bucket has the following files:

      - yellow_tripdata_sample_2021-11.csv
      - yellow_tripdata_sample_2021-12.csv
      - yellow_tripdata_sample_2023-01.csv

    If you define a `batching_regex` using the full name of a file with no regex groups, such as `batching_regex = "yellow_tripdata_sample_2021-11.csv"` your Data Asset will contain only one batch, which will correspond to that file.

    However, if you define a `batching_regex` using a partial file name with a regex group, such as `batching_regex = "yellow_tripdata_sample(?P<year>\d{4})-(?P<month>\d{2})\.csv"` your Data Asset will contain 3 Batches, one corresponding to each file.  When you request data from the Data Asset in the future, you can use the keys `year` and `month` (corresponding to the regex groups `(?P<year>\d{4})` and `(?P<month\d{2})`) to indicate exactly which set of data you want to request from the available Batches. 

    :::

2. Run the following code to create your Data Asset:

    ```python title="Python"
    data_asset = datasource.add_csv_asset(
        name=asset_name,
        batching_regex=batching_regex,
        abs_container=abs_container,
        abs_name_starts_with=abs_name_starts_with
    )
    ```