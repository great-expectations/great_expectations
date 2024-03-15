:::info How `batching_regex` groups records

The records in each file matched by your `batching_regex` will be grouped as a Batch inside your Data Asset.

For example, let's say that your S3 bucket has the following files:

  - yellow_tripdata_sample_2021-11.csv
  - yellow_tripdata_sample_2021-12.csv
  - yellow_tripdata_sample_2023-01.csv

If you define a `batching_regex` using the full name of a file with no regex groups, such as `batching_regex = "yellow_tripdata_sample_2021-11.csv"` your Data Asset will contain only one batch, which will correspond to that file.

However, if you define a `batching_regex` using a partial file name with a regex group, such as `batching_regex = "yellow_tripdata_sample(?P<year>\d{4})-(?P<month>\d{2})\.csv"` your Data Asset will contain 3 Batches, one corresponding to each file.  When you request data from the Data Asset in the future, you can use the keys `year` and `month` (corresponding to the regex groups `(?P<year>\d{4})` and `(?P<month\d{2})`) to indicate exactly which set of data you want to request from the available Batches. 

:::