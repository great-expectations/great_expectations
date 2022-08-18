import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import TipMoreInfoOnRegex from '../components/tip_more_info_on_regex.mdx'

<Tabs
  groupId="batch-count"
  defaultValue='single'
  values={[
  {label: 'Single Batch Configuration', value:'single'},
  {label: 'Multi-Batch Configuration', value:'multi'},
  ]}>
    
  <TabItem value="single">

Because you are explicitly defining each Data Asset in a `ConfiguredAssetDataConnector`, it is very easy to define one that can will only have one Batch.

The simplest way to do this is to define a Data Asset with a `pattern` value that does not contain any regex special characters which would match on more than one value.

For this example, lets assume we have the following files in our `data` directory:
- `yellow_tripdata_sample_2020-01.csv`
- `yellow_tripdata_sample_2020-02.csv`
- `yellow_tripdata_sample_2020-03.csv`

In this case, we want to define a single Data Asset for each month.  To do so, we will need an entry in the `assets` dictionary for each month, as well: one for each Data Asset we want to create.

Let's walk through the creation of the Data Asset for January's data.

First, you need to add an empty dictionary entry into the `assets` dictionary.  Since the key you associate with this entry will be treated as the Data Asset's name, go ahead and name it `yellow_trip_data_jan`.

At this point, your entry in the `assets dictionary will look like:

```python
  "yellow_trip_data_jan": {}
```

Next, you will need to define the `pattern` value and `group_names` value for this Data Asset.

Since you want this Data Asset to only match the file `yellow_tripdata_sample_2020-01.csv` value for the `pattern` key should be one that does not contain any regex special characters that can match on more than one value.  An example follows:

```python"
"pattern": "yellow_tripdata_sample_2020-(01)\\.csv"
```

:::note
The pattern we defined contains a regex group, even though we logically don't need a group to identify the desired Batch in a Data Asset that can only return one Batch.  This is because Great Expectations currently does not permit `pattern` to be defined without also having `group_names` defined.  Thus, in the example above you are creating a group that corresponds to `01` so that there is a valid group to associate a `group_names` entry with.
:::

Since none of the characters in this regex can possibly match more than one value, the only file that can possibly be matched is the one you want it to match: `yellow_tripdata_sample_2020-01.csv`.  This batch will also be associated with the Batch Identifier `01`, but you won't need to use that to specify the Batch in a Batch Request as it is the only Batch that this Data Asset is capable of returning.

To correspond to the single group that was defined in your regex, you will define a single entry in the list for the `group_names` key.  Since the `assets` dictionary key is used for this Data Asset's name, you can give this group a name relevant to what it is matching on:

```python"
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

```python"
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

```python"
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

  </TabItem>
  <TabItem value="multi">


Configuring a `ConfiguredAssetFilesystemDataConnector` so that its Data Assets are capable of returning more than one Batch is just a matter of defining an appropriate regular expression.  For this kind of configuration, the regular expression you define should include at least one group that contains regular expression special characters capable of matching more than one value.

For this example, lets assume we have the following files in our `data` directory:
- `yellow_tripdata_sample_2020-01.csv`
- `yellow_tripdata_sample_2020-02.csv`
- `yellow_tripdata_sample_2020-03.csv`

In this case, we want to define a Data Asset that contains all of our data for the year 2020.

First, you need to add an empty dictionary entry into the `assets` dictionary.  Since the key you associate with this entry will be treated as the Data Asset's name, go ahead and name it `yellow_trip_data_2020`.

At this point, your entry in the `assets dictionary will look like:

```python
  "yellow_trip_data_2020": {}
```

Next, you will need to define the `pattern` value and `group_names` value for this Data Asset.

Since you want this Data Asset to all of the 2020 files, the value for `pattern` needs to be a regular expression that is capable of matching all of the files.  To do this, we will need to use regular expression special characters that are capable of matching one more than one value.

Looking back at the files in our `data` directory, you can see that each file differs from the others only in the digits indicating the month of the file.  Therefore, the regular expression we create will separate those specific characters into a group, and will define the content of that group using special characters capable of matching on any values, like so:

```python"
"pattern": "yellow_tripdata_sample_2020-(.*)\\.csv"
```

To correspond to the single group that was defined in your `pattern`, you will define a single entry in the list for the `group_names` key.  Since the `assets` dictionary key is used for this Data Asset's name, you can give this group a name relevant to what it is matching on:

```python"
    "group_names": ["month"],
```

Since the group in the above regular expression will match on any characters, this regex will successfully match on each of the file names in our `data` directory, and will associate each file with the identifier `month` that corresponds to the file's grouped characters:
- `yellow_tripdata_sample_2020_01.csv` will be Batch identified by a `month` value of `01`
- `yellow_tripdata_sample_2020_02.csv` will be Batch identified by a `month` value of `02`
- `yellow_tripdata_sample_2020_03.csv` will be Batch identified by a `month` value of `03`

Put entirely together, your `assets` entry will look like:

```python
  "yellow_tripdata_2020": {
    "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
    "group_names": ["month"],
  }
```

Looking back at our sample files, this entry will result in the `ConfiguredAssetFilesystemDataConnector` providing one Data Asset, which can be accessed by the name `yellow_tripdata_2020`.  In future workflows you will be able to refer to this Data Asset and by providing that name, and refer to a specific Batch in this Data Asset by providing your Batch Request with a `batch_identifier` entry using the key `month` and the value corresponding to the month portion of the filename of the file that corresponds to the Batch in question.

<TipMoreInfoOnRegex />

With all of these values put together into a single dictionary, your Data Connector configuration will look like this:

```python"
        "name_of_my_configured_data_connector": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "../data",
                "yellow_tripdata_2020": {
                  "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                  "group_names": ["month"],
                }
            }
        }
```

And the full configuration for your Datasource should look like:

```python"
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
                  "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                  "group_names": ["month"],
                }
            }
        }
    }
}
```

:::tip

Remember that when you are working with a Configured Asset Data Connector you need to explicitly define each of your Data Assets.  So, if you want to add additional Data Assets, go ahead and repeat the process of defining an entry in your configuration's `assets` dictionary to do so.

:::

  </TabItem>
  </Tabs>