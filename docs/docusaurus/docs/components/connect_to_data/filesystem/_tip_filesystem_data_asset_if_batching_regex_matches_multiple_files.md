:::tip What if the `batching_regex` matches multiple files?
Your Data Asset will connect to all files that match the regex that you provide.  Each matched file will become an individual Batch inside your Data Asset.

For example:

Let's say that you have a filesystem Datasource pointing to a base folder that contains the following files:
- "taxi_data_2019.csv"
- "taxi_data_2020.csv"
- "taxi_data_2021.csv"


If you define a Data Asset using the full file name with no regex groups, such as "taxi_data_2019\.csv" your Data Asset will contain only one Batch, which will correspond to that file.

However, if you define a partial file name with a regex group, such as `"taxi_data_(?P<year>\d{4})\.csv"` your Data Asset will contain 3 Batches, one corresponding to each matched file.  When you send a Batch Request featuring this Data Asset in the future, all three Batches will be returned by default.  However, you can also use the group name from your `batching_regex`, `year`, to indicate a specific Batch to return.
:::