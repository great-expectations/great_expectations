:::tip What if the `batching_regex` matches multiple files?
Your Data Asset will connect to all files that match the regex that you provide.  Each matched file will become an individual Batch inside your Data Asset.

For example:

Let's say that you have a filesystem Data Source pointing to a base folder that contains the following files:
- "yellow_tripdata_sample_2019-03.csv"
- "yellow_tripdata_sample_2020-07.csv"
- "yellow_tripdata_sample_2021-02.csv"


If you define a Data Asset using the full file name with no regex groups, such as "yellow_tripdata_sample_2019-03\.csv" your Data Asset will contain only one Batch, which will correspond to that file.

However, if you define a partial file name with a regex group, such as `r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv"` your Data Asset can be organized ("partitioned") into Batches according to the two dimensions, defined by the group names, `"year"` and `"month"`.  When you send a Batch Request query featuring this Data Asset in the future, you can use these group names with their respective values as options to control which Batches will be returned.
For example, you could return all Batches in the year of 2021, or the one Batch for July of 2020.
:::