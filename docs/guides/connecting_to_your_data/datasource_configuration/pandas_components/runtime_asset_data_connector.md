Runtime Data Connectors put a wrapper around a single Batch of data, and therefore does not support Data Asset configurations that permit the return of more than one Batch of data.  Therefore, with a Runtime Data Connector your configuration will not be used to determine which Batch is returned in a Batch Request.  Rather, the Data Asset configuration you provide will be used to attach identifiers to a returned Data Asset so that you can reference the same data again in the future.

You can define multiple Data Assets for a Runtime Data Connector, and each definition you provide will constitute an alternative way for you to refer back to the data that is provided by the Data Connector.

First, add an entry

For this example, lets assume we have the following files in our `data` directory:
- `yellow_tripdata_sample_2020-01.csv`
- `yellow_tripdata_sample_2020-02.csv`
- `yellow_tripdata_sample_2020-03.csv`

With a Runtime Data Connector, the file names are inconsequential.  You will always only get one Batch back from a Runtime Data Connector: a Batch that reflects the current state of your data.  Therefore, the Data Assets you define can be completely arbitrary, but it is advised you name them after something meaningful regarding your data or the circumstances under which you will be accessing your data.

For instance, let's assume you are getting a daily update to your data, and so you are running daily validations.  You could then

In this case, we want to define a single Data Asset for each month.  To do so, we will need an entry in the `assets` dictionary for each month, as well: one for each Data Asset we want to create.

Let's walk through the creation of the Data Asset for January's data.

First, you need to add an empty dictionary entry into the `assets` dictionary.  Since the key you associate with this entry will be treated as the Data Asset's name, go ahead and name it `yellow_trip_data_jan`.

At this point, your entry in the `assets dictionary will look like:

```python
  "yellow_trip_data_jan": {}
```

Next, you will need to define the `pattern` value and `group_names` value for this Data Asset.

Since you want this Data Asset to only match the file `yellow_tripdata_sample_2020-01.csv` value for the `pattern` key should be one that does not contain any regex special characters that can match on more than one value.  An example follows:

```python
"pattern": "yellow_tripdata_sample_2020-(01)\\.csv"
```

:::note
The pattern we defined contains a regex group, even though we logically don't need a group to identify the desired Batch in a Data Asset that can only return one Batch.  This is because Great Expectations currently does not permit `pattern` to be defined without also having `group_names` defined.  Thus, in the example above you are creating a group that corresponds to `01` so that there is a valid group to associate a `group_names` entry with.
:::

Since none of the characters in this regex can possibly match more than one value, the only file that can possibly be matched is the one you want it to match: `yellow_tripdata_sample_2020-01.csv`.  This batch will also be associated with the Batch Identifier `01`, but you won't need to use that to specify the Batch in a Batch Request as it is the only Batch that this Data Asset is capable of returning.

To correspond to the single group that was defined in your regex, you will define a single entry in the list for the `group_names` key.  Since the `assets` dictionary key is used for this Data Asset's name, you can give this group a name relevant to what it is matching on:

```python
    "group_names": ["month"],
```

Put entirely together, your `assets` entry will look like:

```python
  "yellow_tripdata_jan": {
    "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
    "group_names": ["month"],
  }
```

Looking back at our sample files, this entry will result in the `ConfiguredAssetFilesystemDataConnector` providing one Data Asset, which can be accessed by the name `yellow_tripdata_jan`.  In future workflows you will be able to refer to this Data Asset and its single corresponding Batch by providing that name.

With all of these values put together into a single dictionary, your Data Connector configuration will look like this:

```python
        "name_of_my_configured_data_connector": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "../data",
                "yellow_tripdata_jan": {
                  "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                  "group_names": ["month"],
                }
            }
        }
```

And the full configuration for your Datasource should look like:

```python
datasource_config = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_configured_data_connector": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "../data",
                "yellow_tripdata_jan": {
                  "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                  "group_names": ["month"],
                }
            }
        }
    }
}
```

:::note

Because Configured Data Assets require that you explicitly define each Data Asset they provide access to, you will have to add `assets` entries for February and March if you also want to access `yellow_tripdata_sample_2020-02.csv` and `yellow_tripdata_sample_2020-03.csv` in the same way.

:::
