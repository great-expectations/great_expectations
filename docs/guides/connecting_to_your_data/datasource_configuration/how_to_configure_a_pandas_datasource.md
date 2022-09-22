---
title: "How to configure a Pandas Datasource"
---
# [![Connect to data icon](../../../images/universal_map/Outlet-active.png)](../connect_to_data_overview.md) How to configure a Pandas Datasource

import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

import InferredAssetDataConnector from './pandas_components/inferred_asset_data_connector.md'
import ConfiguredAssetDataConnector from './pandas_components/configured_asset_data_connector.md'
<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>
import TipCustomDataConnectorModuleName from './components/_tip_custom_data_connector_module_name.mdx'
import PartFilesystemBaseDirectory from './components/_part_base_directory_for_filesystem.mdx'
import PartIntroForSingleOrMultiBatchConfiguration from './components/_part_intro_for_single_or_multi_batch_configuration.mdx'
import RuntimeAssetDataConnector from './pandas_components/runtime_asset_data_connector.md'

This guide will walk you through the process of configuring a Pandas Datasource from scratch, verifying that your configuration is valid, and adding it to your Data Context.  By the end of this guide you will have a Pandas Datasource which you can use in future workflows for creating Expectations and validating data.

<Prerequisites>

- Initialized a Data Context in your current working directory.
- Filesystem data available in a sub-folder of your Data Context's root directory.
- Familiarity with [how to choose which DataConnector to use](../how_to_choose_which_dataconnector_to_use.md).
- Familiarity with [how to choose between working with a single or multiple Batches of data](../how_to_choose_between_working_with_a_single_or_multiple_batches_of_data.md).

</Prerequisites>

## Steps

### 1. Import necessary modules and initialize your Data Context

```python
import great_expectations as gx
from ruamel import yaml

data_context: gx.DataContext = gx.get_context()

```
The `great_expectations` module will give you access to your Data Context, which is the entry point for working with a Great Expectations project.

The `yaml` module from `ruamel` will be used in validating your Datasource's configuration.  Great Expectations will use a Python dictionary representation of your Datasource configuration when you add your Datasource to your Data Context.  However, Great Expectations saves configurations as `yaml` files, so when you validate your configuration you will need to convert it from a Python dictionary to a `yaml` string, first.

Your Data Context that is initialized by `get_data_context()` will be the Data Context defined in your current working directory.  It will provide you with convenience methods that we will use to validate your Datasource configuration and add your Datasource to your Great Expectations project once you have configured it.

### 2. Create a new Datasource configuration.

A new Datasource can be configured in Python as a dictionary with a specific set of keys.  We will build our Datasource configuration from scratch in this guide, although you can just as easily modify an existing one.

To start, create an empty dictionary.  You will be populating it with keys as you go forward.

At this point, the configuration for your Datasource is merely:

```python
datasource_config: dict = {}
```

However, from this humble beginning you will be able to build a full Datasource configuration.

#### The keys needed for your Datasource configuration

At the top level, your Datasource's configuration will need the following keys: 
- `name`: The name of the Datasource, which will be used to reference the datasource in Batch Requests.
- `class_name`: The name of the Python class instantiated by the Datasource.  Typically, this will be the `Datasource` class.
- `module_name`: the name of the module that contains the Class definition indicated by `class_name`.
- `execution_engine`: a dictionary containing the `class_name` and `module_name` of the Execution Engine instantiated by the Datasource.
- `data_connectors`: the configurations for any Data Connectors and their associated Data Assets that you want to have available when utilizing the Datasource.

In the following step we will add those keys and their corresponding values to your currently empty Datasource configuration dictionary.

### 3. Name your Datasource

The first key that you will need to define for your new Datasource is its `name`.  You will use this to reference the Datasource in future workflows.  It can be anything you want it to be, but ideally you will name it something relevant to the data that it interacts with.

For the purposes of this example, we will name this Datasource:

```python
    "name": "my_datasource_name",
```

You should, however, name your Datsource something more relevant to your data.

At this point, your configuration should now look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",  # Preferably name it something relevant
}
```

### 4. Specify the Datasource class and module

The `class_name` and `module_name` for your Datasource will almost always indicate the `Datasource` class found at `great_expectations.datasource`.  You may replace this with a specialized subclass, or a custom class, but for almost all regular purposes these two default values will suffice.  For the purposes of this guide, add those two values to their corresponding keys.

```python
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
```

Your full configuration should now look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",  # Preferably name it something relevant
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
}
```
    

### 5. Add the Pandas Execution Engine to your Datasource configuration

Your Execution Engine is where you will specify that you want this Datasource to use Pandas in the backend.  As with the Datasource top level configuration, you will need to provide the `class_name` and `module_name` that indicate the class definition and containing module for the Execution Engine that you will use.  For the purposes of this guide, these will consist of the `PandasExecutionEngine` found at `great_expectations.execution_engine`.

The `execution_engine` key and its corresponding value will therefore look like this:

```python
    "execution_engine": {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine",
    },   
```

After adding the above snippet to your Datasource configuration, your full configuration dictionary should now look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
}
```

### 6. Add a dictionary as the value of the `data_connectors` key

The `data_connectors` key should have a dictionary as its value.  Each key/value pair in this dictionary will correspond to a Data Connector's name and configuration, respectively.

The keys in the `data_connectors` dictionary will be the names of the Data Connectors, which you will use to indicate which Data Connector to use in future workflows.  As with value of your Datasource's `name` key, you can use any value you want for a Data Connector's name.  Ideally, you will use something relevant to the data that each particular Data Connector will provide; the only significant difference is that for Data Connectors the name of the Data Connector _is_ its key in the `data_connectors` dictionary.

The values for each of your `data_connectors` keys will be the Data Connector configurations that correspond to each Data Connector's name.  You may define multiple Data Connectors in the `data_connectors` dictionary by including multiple key/value pairs.

For now, start by adding an empty dictionary as the value of the `data_connectors` key.  We will begin populating it with Data Connector configurations in the next step.

Your current configuration should look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {}
}
```


### 7. Configure your individual Data Connectors

For each Data Connector configuration, you will need to specify which type of Data Connector you will be using.  For Pandas, the most likely ones will be the `InferredAssetFilesystemDataConnector`, the `ConfiguredAssetFilesystemDataConnector`, and the `RuntimeDataConnector`.  If you are uncertain as to which one best suits your needs, please refer to our guide on [how to choose which Data Connector to use](../how_to_choose_which_dataconnector_to_use.md).

#### Data Connector example configurations:

<Tabs
  groupId="data-asset-type"
  defaultValue='inferred'
  values={[
  {label: 'InferredAssetFilesystemDataConnector', value:'inferred'},
  {label: 'ConfiguredAssetDataConnector', value:'configured'},
  {label: 'RuntimeDataConnector', value:'runtime'},
  ]}>

  <TabItem value="inferred">

:::tip 

The `InferredDataConnector` is ideal for:
- quickly setting up a Datasource and getting access to data
- diving straight in to working with Great Expectations
- initial data discovery and introspection
 
However, the `InferredDataConnector` allows less control over the definitions of your Data Assets than the `ConfiguredAssetDataConnector` provides.  

If you are at the point of building a repeatable workflow, we encourage using the `ConfiguredAssetDataConnector` instead.

:::

Remember, the key that you provide for each Data Connector configuration dictionary will be used as the name of the Data Connector.  For this example, we will use the name `name_of_my_inferred_data_connector` but you may have it be anything you like.

At this point, your configuration should look like:

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
        "name_of_my_inferred_data_connector": {}
        }
    }
}
```

When defining an `InferredAssetFilesystemDataConnector` you will need to provide values for three keys in Data Connector's configuration dictionary (the currently empty dictionary that corresponds to `"name_of_my_inferred_data_connector"` in the example above).  These key/value pairs consist of:
- `class_name`: The name of the Class that will be instantiated for this `DataConnector`.
- `base_directory`: The string representation of the directory that contains your filesystem data.
- `default_regex`: A dictionary that describes how the data should be grouped into Batches.

Additionally, you may optionally choose to define:
- `glob_directive`: A regular expression that can be used to access source data files contained in subfolders of your `base_directory`.  If this is not defined, the default value of `*` will cause your Data Connector to only look at files in the `base_directory` itself.

For this example, you will be using the `InferredAssetFilesystemDataConnector` as your `class_name`.  This is a subclass of the `InferredAssetDataConnector` that is specialized to support filesystem Execution Engines, such as the `PandasExecutionEngine`.  This key/value entry will therefore look like:

```python
        "class_name": "InferredAssetFilesystemDataConnector",
```

<TipCustomDataConnectorModuleName />

<PartFilesystemBaseDirectory />

With these values added, along with a blank dictionary for `default_regex` (we will define it in the next step), your full configuration should now look like:

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
        "name_of_my_inferred_data_connector": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "default_regex": {}
        }
    }
}
```
:::info Optional parameter: `glob_directive`

The `glob_directive` parameter is provided to give the `DataConnector` information about the directory structure to expect when identifying source data files to check against each Data Asset's `default_regex`.  If you do not specify a value for `glob_directive` a default value of `"*"` will be used. This will cause your Data Asset to check all files in the folder specified by `base_directory` to determine which should be returned as Batches for the Data Asset, but will ignore any files in subdirectories.

Overriding the `glob_directive` by providing your own value will allow your Data Connector to traverse subdirectories or otherwise alter which source data files are compared against your Data Connector's `default_regex`.

For example, assume your source data is in files contained by subdirectories of your `base_folder`, like so:
- 2019/yellow_taxidata_2019_01.csv
- 2020/yellow_taxidata_2020_01.csv
- 2021/yellow_taxidata_2021_01.csv
- 2022/yellow_taxidata_2022_01.csv

To include all of these files, you would need to tell the Data connector to look for files that are nested one level deeper than the `base_directory` itself. 

 You would do this by setting the `glob_directive` key in your Data Connector config to a value of `"*/*"`.  This value will cause the Data Connector to look for regex matches against the file names for all files found in any subfolder of your `base_directory`.  Such an entry would look like:

```python
    "glob_directive": "*/*",
```

The `glob_directive` parameter works off of regex.  You can also use it to limit the files that will be compared against the Data Connector's `default_regex` for a match.  For example, to only permit `.csv` files to be checked for a match, you could specify the `glob_directive` as `"*.csv"`.  To only check for matches against the `.csv` files in subdirectories, you would use the value `*/*.csv`, and so forth.

In this guide's examples, all of our data is assumed to be in the `base_directory` folder.  Therefore, you will not need to add an entry for `glob_directive` to your configuration.  However, if you were to include the example `glob_directive` from above, your full configuration would currently look like:

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
        "name_of_my_inferred_data_connector": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "glob_directive": "*/*",
            "default_regex": {}
        }
    }
}
```

:::

  </TabItem>
  <TabItem value="configured">

:::tip 

A `ConfiguredAssetDataConnector` allows you to have the most fine-tuning, and requires an explicit listing of each Data Asset you want to connect to.

However, a `ConfiguredAssetDataConnector` can be used to functionally duplicate the ability of an `InferredAssetDataConnector` to grant access to all files in a folder based on a regex match by defining a Data Asset that uses the same `pattern` as you would use in an `InferredAssetDataConnector`'s `default_regex`.  This will cause the Data Asset in question to have one Batch for each matched file, just as the `InferredAssetDataConnector` would have one Data Asset for each file.  Therefore, the only difference would be in how you accessed the data in question.  

With an `InferredAssetDataConnector`, you would need to provide your Batch Request the name of the Data Asset in question in order to retrieve it as a Batch from a Batch Request.  

With a `ConfiguredAssetDataConnector`, you would provide your Batch Request with the name of the Data Asset that was configured with an `InferredAssetDataConnector` style pattern, and then provide the same name as you would have used for an `InferredAssetDataConnector`'s Data Asset as a `batch_identifier` in your Batch Request.  This will result in the `ConfiguredAssetDataConnector` providing the same data in response to the Batch Request as an `InferredAssetDataConnector` requesting the data as a Data Asset would.

:::

Remember, the key that you provide for each Data Connector configuration dictionary will be used as the name of the Data Connector.  For this example, we will use the name `name_of_my_configured_data_connector` but you may have it be anything you like.

At this point, your configuration should look like:

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
        "name_of_my_configured_data_connector": {}
        }
    }
}
```

When defining a `ConfiguredAssetFilesystemDataConnector` you will need to provide values for three keys in Data Connector's configuration dictionary (the currently empty dictionary that corresponds to `"name_of_my_configured_data_connector"` in the example above).  These key/value pairs consist of:
- `class_name`: The name of the Class that will be instantiated for this `DataConnector`.
- `base_directory`: The string representation of the directory that contains your filesystem data.
- `assets`: A dictionary containing one entry for each Data Asset made available by the Data Connector.  You will need to explicitly define an entry for each Data Asset you want to make available through a configured Data Connector.

For this example, you will be using the `ConfiguredAssetFilesystemDataConnector` as your `class_name`.  This is a subclass of the `ConfiguredAssetDataConnector` that is specialized to support filesystem Execution Engines, such as the `PandasExecutionEngine`.  This key/value entry will therefore look like:

```python
        "class_name": "ConfiguredAssetFilesystemDataConnector",
```

<TipCustomDataConnectorModuleName />

<PartFilesystemBaseDirectory />

With these values added, along with a blank dictionary for `assets` (we will define entries for it in the next step), your full configuration should now look like:

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
        "name_of_my_configured_data_connector": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "assets": {}
        }
    }
}
```

  </TabItem>
  <TabItem value="runtime">

:::tip

A `RuntimeDataConnector` is used to connect to an in-memory dataframe or path.  The dataframe or path used for a `RuntimeDataConnector` is therefore passed to the `RuntimeDataConnector` as part of a Batch Request, rather than being a static part of the `RuntimeDataConnector`'s configuration.

A Runtime Data Connector will always only return one Batch of data: the _current_ data that was passed in or specified as part of a Batch Request.  This means that a `RuntimeDataConnector` does not define Data Assets like an `InferredDataConnector` or a `ConfiguredDataConnector` would.  

Instead, a Runtime Data Connector's configuration will provides a way for you to attach identifying values to a returned Batch of data so that the data _as it was at the time it was returned_ can be referred to again in the future.

For more information on configuring a Batch Request for a Pandas Runtime Data Connector, please see our guide on [how to create a Batch of data from an in-memory Spark or Pandas dataframe or path](../how_to_create_a_batch_of_data_from_an_in_memory_spark_or_pandas_dataframe.md).

:::

Remember, the key that you provide for each Data Connector configuration dictionary will be used as the name of the Data Connector.  For this example, we will use the name `name_of_my_runtime_data_connector` but you may have it be anything you like.

At this point, your configuration should look like:

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
        "name_of_my_runtime_data_connector": {}
        }
    }
}
```

All you will need to define in your Runtime Data Connector's configuration is the `class_name` for the Data Connector you will use and a list of `batch_identifiers` that you will provide to any Batch Request that utilizes the Data Connector so that the returned Batch can be referred back to in the future.

For this example, you will be using the `RuntimeDataConnector` as your `class_name`.  This key/value entry will therefore look like:

```python
    "class_name": "RuntimeDataConnector",
```

After including a blank list for your `batch_identifiers`, your full configuration should now look like:

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
            "batch_identifiers": []
        }
    }
}
```

<TipCustomDataConnectorModuleName />

  </TabItem>
  </Tabs>


### 8. Configure your Data Connector's Data Assets

<Tabs
  groupId="data-asset-type"
  defaultValue='inferred'
  values={[
  {label: 'InferredAssetFilesystemDataConnector', value:'inferred'},
  {label: 'ConfiguredAssetDataConnector', value:'configured'},
  {label: 'RuntimeDataConnector', value:'runtime'},
  ]}>

  <TabItem value="inferred">

In an Inferred Asset Data Connector for filesystem data, a regular expression is used to group the files into Batches for a Data Asset.  This is done with the value we will define for the Data Connector's `default_regex` key.  The value for this key will consist of a dictionary that contains two values:
- `pattern`: This is the regex pattern that will define your Data Asset's potential Batch or Batches.
- `group_names`: This is a list of names that correspond to the groups you defined in `pattern`'s regular expression.

The `pattern` in `default_regex` will be matched against the files in your `base_directory`, and everything that matches against the first group in your regex will become a Batch in a Data Asset that possesses the name of the matching text.  Any files that have a matching string for the first group will become Batches in the same Data Asset.

<PartIntroForSingleOrMultiBatchConfiguration />

<InferredAssetDataConnector/>

  </TabItem>
  <TabItem value="configured">

In a Configured Asset Data Connector for filesystem data, each entry in the `assets` dictionary will correspond to an explicitly defined Data Asset.  The key provided will be used as the name of the Data Asset, while the value will be a dictionary that contains two additional keys:
- `pattern`: This is the regex pattern that will define your Data Asset's potential Batch or Batches.
- `group_names`: This is a list of names that correspond to the groups you defined in `pattern`'s regular expression.

The `pattern` in each `assets` entry will be matched against the files in your `base_directory`, and everything that matches against the `pattern`'s value will become a Batch in a Data Asset with a name matching the key for this entry in the `assets` dictionary.

<PartIntroForSingleOrMultiBatchConfiguration />

<ConfiguredAssetDataConnector/>

  </TabItem>
  <TabItem value="runtime">

<RuntimeAssetDataConnector />

  </TabItem>
  </Tabs>


### 9. Test your configuration with `.test_yaml_config(...)`

Now that you have a full Datasource configuration, you can confirm that it is valid by testing it with the `.test_yaml_config(...)` method.  To do this, execute the Python code:

```python
data_context.test_yaml_config(yaml.dump(datasource_config))
```

When executed, `test_yaml_config` will instantiate the component described by the yaml configuration that is passed in and then run a self check procedure to verify that the component works as expected.

For a Datasource, this includes:
- confirming that the connection works
- gathering a list of available Data Assets
- verifying that at least one Batch can be fetched from the Datasource

For more information on the `.test_yaml_config(...)` method, please see our guide on [how to configure `DataContext` components using `test_yaml_config`](../../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md).

### 10. (Optional) Add more Data Connectors your configuration

The `data_connectors` dictionary in your `datasource_config` can contain multiple entries.  If you want to add additional Data Connectors, just go through the process starting at step 7 again.

### 11. Add your new Datasource to your Data Context

Now that you have verified that you have a valid configuration you can add your new Datasource to your Data Context with the command:
 
```python
data_context.add_datasource(**datasource_config)
```

:::caution 

If the value of `datasource_config["name"]` corresponds to a Datasource that is already defined in your Data Context, then using the above command will overwrite the existing Datasource.

:::

:::tip 

If you want to ensure that you only add a Datasource when it won't overwrite an existing one, you can use the following code instead:

```python
# add_datasource only if it doesn't already exist in your Data Context
try:
    data_context.get_datasource(datasource_config["name"])
except ValueError:
    data_context.add_datasource(**datasource_config)
else:
    print(f"The datasource {datasource_config["name"]} already exists in your Data Context!")
```

:::

## Next Steps

Congratulations!  You have fully configured a Datasource and verified that it can be used in future workflows to provide a Batch or Batches of data.

:::tip

For more information on using Batch Requests to retrieve data, please see our guide on [how to get one or more Batches of data from a configured Datasource](../how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.md).

:::

You can now move forward and [create Expectations for your Datasource](../../expectations/create_expectations_overview.md).
