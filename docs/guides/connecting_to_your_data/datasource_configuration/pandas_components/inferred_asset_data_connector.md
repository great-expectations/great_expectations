import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


<Tabs
  groupId="batch-count"
  defaultValue='single'
  values={[
  {label: 'Single Batch Configuration', value:'single'},
  {label: 'Multi-Batch Configuration', value:'multi'},
  ]}>
    
  <TabItem value="single">

Because of the simple regex matching that groups files into Batches for a given Data Asset, it is actually quite straight forward to create a Data Connector which has Data Assets that are only capable of providing a single Batch.  All you need to do is define a regular expression that consists of a single group which corresponds to a unique portion of your data files' names that is unique for each file.

The simplest way to do this is to define a group that consists of the entire file name.

For this example, lets assume we have the following files in our `data` directory:
- `yellow_tripdata_sample_2020-01.csv`
- `yellow_tripdata_sample_2020-02.csv`
- `yellow_tripdata_sample_2020-03.csv`

In this case you could define the `pattern` key as follows:

```python
"pattern": "(.*)\\.csv"
```

This regex will match the full name of any file that has the `.csv` extension, and will put everything prior to `.csv` extension into a group.

Since each `.csv` file will necessarily have a unique name preceeding its extension, the content that matches this pattern will be unique for each file.  This will ensure that only one file is included as a Batch for each Data Asset.

To correspond to the single group that was defined in your regex, you will define a single entry in the list for the `group_names` key.  Since the first group in an Inferred Asset Data Connector is used to generate names for the inferred Data Assets, you should name that group as follows:

```python
    "group_names": ["data_asset_name"],
```

Looking back at our sample files, this regex will result in the `InferredAssetFilesystemDataConnector` providing three Data Assets, which can be accessed by the portion of the file that matches the first group in our regex.  In future workflows you will be able to refer to one of these Data Assets in a Batch Request py providing one of the following `data_asset_name`s:
- `yellow_tripdata_sample_2020-01`
- `yellow_tripdata_sample_2020-02`
- `yellow_tripdata_sample_2020-03`

:::note 

Since we did not include `.csv` in the first group of the regex we defined, the `.csv` portion of the filename will be dropped from the value that is recognized as a valid `data_asset_name`.

:::

With all of these values put together into a single dictionary, your Data Connector configuration will look like this:

```python
        "name_of_my_inferred_data_connector": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "default_regex": {
                "pattern": "(.*)\\.csv",
                "group_names": ["data_asset_name"],
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
        "name_of_my_inferred_data_connector": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "default_regex": {
                "pattern": "(.*)\\.csv",
                "group_names": ["data_asset_name"],
            }
        }
    }
}
```


  </TabItem>
  <TabItem value="multi">


Configuring an `InferredAssetFilesystemDataConnector` so that its Data Assets are capable of returning more than one Batch is just a matter of defining an appropriate regular expression.  For this kind of configuration, the regular expression you define should have two or more groups.

The first group will be treated as the Data Asset's name.  It should be a portion of your file names that occurs in more than one file.  The files that match this portion of the regular expression will be grouped together as a single Data Asset.

Any additional groups that you include in your regular expression will be used to identify specific Batches among those that are grouped together in each Data Asset.

For this example, lets assume you have the following files in our `data` directory:
- `yellow_tripdata_sample_2020-01.csv`
- `yellow_tripdata_sample_2020-02.csv`
- `yellow_tripdata_sample_2020-03.csv`

You can configure a Data Asset that groups these files together and differentiates each batch by month with the regex:

```python
    "default_regex": "(yellow_tripdata_sample_2020)-(\\d.*)\\.csv"
```

    


  </TabItem>
  </Tabs>