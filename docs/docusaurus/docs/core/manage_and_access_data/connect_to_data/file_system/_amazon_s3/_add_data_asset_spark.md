1. Define the parameters needed to create a Spark CSV Data Asset for an Amazon S3 Data Source:

    - `asset_name`: The Data Asset's name.  You may set this to any value you prefer.  In the following example, this is `"my_taxi_data_asset"`.
    - `s3_prefix`: The folder containing your CSV files.
    - `batching_regex`: A regular expression.  Your Data Asset will make available the data in all files that match the regex your provide.
    - `header`: A boolean that indicates if the first row of the CSV is a header row containing column names.
    - `infer_schema`: A boolean that indicates if Spark should infer the column types (String, Double, Long, etc) of the CSV file.
    
    ```python title="Python"
    asset_name = "my_taxi_data_asset"
    s3_prefix = "data/taxi_yellow_tripdata_samples/"
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    header=True
    infer_schema=True
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
        s3_prefix=s3_prefix,
        header=header,
        infer_schema=infer_schema
    )
    ```