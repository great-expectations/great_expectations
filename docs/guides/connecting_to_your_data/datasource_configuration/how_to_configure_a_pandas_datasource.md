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

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>

This guide will walk you through the process of configuring a Pandas Datasource from scratch, verifying that your configuration is valid, and adding it to your Data Context.  By the end of this guide you will have a Pandas Datasource which you can use in future workflows for creating Expectations and validating data.

<Prerequisites>

- Initialized a Data Context in your current working directory.
- Filesystem data available in a sub-folder of your Data Context's root directory.
- Familiarity with [how to choose which DataConnector to use](../how_to_choose_which_dataconnector_to_use.md).
- Familiarity with [how to choose between working with a single or multiple Batches of data](../how_to_choose_between_working_with_a_single_or_multiple_batches_of_data.md).

</Prerequisites>

## Steps

### 1. Import necessary modules and initialize your Data Context

```python"
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

```python"
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

```python"
    "name": "my_datasource_name",
```

You should, however, name your Datsource something more relevant to your data.

At this point, your configuration should now look like:

```python"
datasource_config: dict = {
    "name": "my_datasource_name",  # Preferably name it something relevant
}
```

### 4. Specify the Datasource class and module

The `class_name` and `module_name` for your Datasource will almost always indicate the `Datasource` class found at `great_expectations.datasource`.  You may replace this with a specialized subclass, or a custom class, but for almost all regular purposes these two default values will suffice.  For the purposes of this guide, add those two values to their corresponding keys.

```python"
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
```

Your full configuration should now look like:

```python"
datasource_config: dict = {
    "name": "my_datasource_name",  # Preferably name it something relevant
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
}
```
    

### 5. Add the Pandas Execution Engine to your Datasource configuration

Your Execution Engine is where you will specify that you want this Datasource to use Pandas in the backend.  As with the Datasource top level configuration, you will need to provide the `class_name` and `module_name` that indicate the class definition and containing module for the Execution Engine that you will use.  For the purposes of this guide, these will consist of the `PandasExecutionEngine` found at `great_expectations.execution_engine`.

The `execution_engine` key and its corresponding value will therefore look like this:

```python"
    "execution_engine": {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine",
    },   
```

After adding the above snippet to your Datasource configuration, your full configuration dictionary should now look like:

```python"
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

### 6. Add a dictionary as the value of the `data_connector` key

The `data_connector` key should have a dictionary as its value.  Each key/value pair in this dictionary will correspond to a Data Connector's name and configuration, respectively.

The keys in the `data_connector` dictionary will be the names of the Data Connectors, which you will use to indicate which Data Connector to use in future workflows.  As with value of your Datasource's `name` key, you can use any value you want for a Data Connector's name.  Ideally, you will use something relevant to the data that each particular Data Connector will provide; the only significant difference is that for Data Connectors the name of the Data Connector _is_ its key in the `data_connector` dictionary.

The values for each of your `data_connector` keys will be the Data Connector configurations that correspond to each Data Connector's name.  You may define multiple Data Connectors in the `data_connector` dictionary by including multiple key/value pairs.

For now, start by adding an empty dictionary as the value of the `data_connector` key.  We will begin populating it with Data Connector configurations in the next step.

Your current configuration should look like:

```python"
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

For each Data Connector configuration, you will need to specify which type of Data Connector you will be using.  For Pandas, the most likely ones will be the `InferredAssetFilesystemDataConnector`, the `ConfiguredAssetDataConnector`, and the `RuntimeDataConnector`.  If you are uncertain as to which one best suits your needs, please refer to our guide on [how to choose which Data Connector to use](../how_to_choose_which_dataconnector_to_use.md).

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

Remember, the key that you provide for each Data Connector configuration dictionary will be used as the name of the Data Connector.  for this example, we will use the name `name_of_my_inferred_data_connector` but you may have it be anything you like.

At this point, your configuration should look like:

```python"
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

For this example, you will be using the `InferredAssetFilesystemDataConnector` as your `class_name`.  This is a subclass of the `InferredAssetDataConnector` that is specialized to support filesystem Execution Engines, such as the `PandasExecutionEngine`.  This key/value entry will therefore look like:

```python"
        "class_name": "InferredAssetFilesystemDataConnector",
```

For the base directory, you will want to put the relative path of your data from the folder that contains your Data Context.  In this example we will use the same path that was used in the [Getting Started Tutorial, Step 2: Connect to Data](../../../tutorials/getting_started/tutorial_connect_to_data.md).  Since we are manually entering this value rather than letting the CLI generate it, the key/value pair will look like:

```python"
        "base_directory": "./data",
```

With these values added, along with a blank dictionary for `default_regex` (we will define it in the next step), your full configuration should now look like:

```python"
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

  </TabItem>
  <TabItem value="configured">

  </TabItem>
  <TabItem value="runtime">

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

This means that when configuring your Data Connector's regular expression, you have the option to design it so that the Data Connector is only capable of returning a single Batch per Data Asset, or so that it is capable of returning multiple Batches grouped into individual Data Assets.  Each type of configuration is useful in certain cases, so we will provide examples of both.

:::tip 

If you are uncertain as to which type of configuration is best for your use case, please refer to our guide on [how to choose between working with a single or multiple Batches of data](../how_to_choose_between_working_with_a_single_or_multiple_batches_of_data.md).

:::

<InferredAssetDataConnector/>

  </TabItem>
  <TabItem value="configured">

  </TabItem>
  <TabItem value="runtime">

  </TabItem>
  </Tabs>


### 9. Test your configuration with `.test_yaml_config(...)`

Now that you have a full Datasource configuration, you can confirm that it is valid by testing it with the `.test_yaml_config(...)` method.  To do this, execute the Python code:

```python"
data_context.test_yaml_config(yaml.dump(datasource_config))
```

When executed, `test_yaml_config` will instantiate the component described by the yaml configuration that is passed in and then run a self check procedure to verify that the component works as expected.

For a Datasource, this includes:
- confirming that the connection works
- gathering a list of available Data Assets
- verifying that at least one Batch can be fetched from the Datasource

For more information on `test_yaml_config`, please see our guide on [how to configure `DataContext` components using `test_yaml_config`](../../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md).

### 10. Add your new Datasource to your Data Context

Now that you have verified that you have a valid configuration you can add your new Datasource to your Data Context with the command:
 
```python"
data_context.add_datasource(**datasource_config)
```

:::caution 

If the value of `datasource_config["name"]` corresponds to a Datasource that is already defined in your Data Context, then using the above command will overwrite the existing Datasource.

:::

:::tip 

If you want to ensure that you only add a Datasource when it won't overwrite an existing one, you can use the following code instead:

```python"
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

For more information on using Batch Requests to retrieve data, please see our guide on [how to get a Batch of data from a configured Datasource](../how_to_get_a_batch_of_data_from_a_configured_datasource.md).

:::

You can now move forward and [create Expectations for your Datasource](../../expectations/create_expectations_overview.md).
