Runtime Data Connectors put a wrapper around a single Batch of data, and therefore do not support Data Asset configurations that permit the return of more than one Batch of data.  In fact, since you will use a Batch Request to pass in or specify the data that a Runtime Data Connector uses, there is no need to specify a Data Asset configuration at all.

Instead, you will provide a `batch_identifiers` list which will be used to attach identifying information to a returned Batch so that you can reference the same data again in the future.

For this example, lets assume we have the following files in our `data` directory:
- `yellow_tripdata_sample_2020-01.csv`
- `yellow_tripdata_sample_2020-02.csv`
- `yellow_tripdata_sample_2020-03.csv`

With a Runtime Data Connector you won't actually refer to them in your configuration!  As mentioned above, you will provide the path or dataframe for one of those files to the Data Connector as part of a Batch Request.

Therefore, the file names are inconsequential to your Runtime Data Connector's configuration.  In fact, the `batch_identifiers` that you define in your Runtime Data Connector's configuration can be completely arbitrary. However, it is advised you name them after something meaningful regarding your data or the circumstances under which you will be accessing your data.

For instance, let's assume you are getting a daily update to your data, and so you are running daily validations.  You could then choose to identify your Runtime Data Connector's Batches by the timestamp at which they are requested.

To do this, you would simply add a `batch_timestamp` entry in your `batch_identifiers` list.  This would look like:

```python
            "batch_identifiers": ["batch_timestamp"]
```

Then, when you create your Batch Request you would populate the `batch_timestamp` value in its `batch_identifiers` dictionary with the value of the current date and time.  This will attach the current date and time to the returned Batch, allowing you to reference the Batch again in the future even if the current data (the data that would be provided by the Runtime Data Connector if you requested a new Batch) had changed.

The full configuration for your Datasource should now look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_runtime_data_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_timestamp"]
        }
    }
}
```

:::caution

We stated above that the names that you use for your `batch_identifiers` in a Runtime Data Connector's configuration can be completely arbitrary, and will be used as keys for the `batch_identifiers` dictionary in future Batch Requests.  

However, the same holds true for the _values_ you pass in for each key in your Batch Request's `batch_identifiers`!

Always make sure that your Batch Requests utilizing Runtime Data Connectors are providing meaningful identifying information, consistent with the keys that are derived from the `batch_identifiers` you have defined in your Runtime Data Connector's configuration.

:::