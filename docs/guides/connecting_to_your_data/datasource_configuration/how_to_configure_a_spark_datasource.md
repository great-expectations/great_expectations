---
title: How to configure a Spark datasource
---
# [![Connect to data icon](../../../images/universal_map/Outlet-active.png)](../connect_to_data_overview.md) How to configure a Spark Datasource

import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>

import TipCustomDataConnectorModuleName from './components/tip_custom_data_connector_module_name.mdx'
import FilesystemBaseDirectory from './components/base_directory_for_filesystem.mdx'
import IntroForSingleOrMultiBatchConfiguration from './components/intro_for_single_or_multi_batch_configuration.mdx'
import RuntimeAssetDataConnector from './pandas_components/runtime_asset_data_connector.md'

import Intro from './components/_intro.mdx';
import ImportNecessaryModulesAndInitializeYourDataContext from './filesystem_components/_import_necessary_modules_and_initialize_your_data_context.mdx'
import CreateANewDatasourceConfiguration from './components/_create_a_new_datasource_configuration.mdx'
import TheKeysYouNeedForYourDatasource from './components/_the_keys_you_need_for_your_datasource.mdx'
import NameYourDatasource from './components/_name_your_datasource.mdx'
import SpecifyTheDatasourceClassAndModule from './components/_specify_the_datasource_class_and_module.mdx'
import AddTheBackendExecutionEngineToYourDatasourceConfiguration from './components/_add_the_backend_execution_engine_to_your_datasource_configuration.mdx'
import AddADictionaryAsTheValueOfTheDataConnectorsKey from './components/_add_a_dictionary_as_the_value_of_the_data_connectors_key.mdx'
import ConfigureYourIndividualDataConnectors from './filesystem_components/_configure_your_individual_data_connectors.mdx'
import TipWhichDataConnectorToUse from './components/_tip_which_data_connector_to_use.mdx'
import TipInferredDataConnectorOverview from './components/_tip_inferred_data_connector_overview.mdx'
import NameTheDataConnector from './components/_name_the_data_connector.mdx'
import DataConnectorKeysOverview from './components/_data_connector_keys_overview.mdx'
import GlobDirectiveDetails from './spark_components/_glob_directive_details.mdx'
import TipConfiguredDataConnectorOverview from './components/_tip_configured_data_connector_overview.mdx'
import TipRuntimeDataConnectorOverview from './components/_tip_runtime_data_connector_overview.mdx'
import AssetKeysOverviewInferred from './components/_asset_keys_overview_inferred.mdx'
import AssetKeysOverviewConfigured from './components/_asset_keys_overview_configured.mdx'

import ConfiguredAssets from './spark_components/_configured_assets.mdx'
import InferredAssets from './spark_components/_inferred_assets.mdx'
import RuntimeAssets from './spark_components/_runtime_assets.mdx'

import TestYourConfigurationWithTestYamlConfig from './components/_test_your_configuration_with_test_yaml_config.mdx'
import StepAddMoreDataConnectorsToYourConfig from './components/_step_add_more_data_connectors_to_your_config.mdx'
import StepAddYourNewDatasourceToYourDataContext from './components/_step_add_your_new_datasource_to_your_data_context.mdx'
import StepNext from './components/_step_next.mdx'

import BatchSpecInferredOrConfigured from './spark_components/_batch_spec_inferred_or_configured.mdx'

<Intro backend="Spark" />

<Prerequisites>

- Initialized a Data Context in your current working directory.
- Access to a working Spark installation.
- Filesystem data available in a sub-folder of your Data Context's root directory.
- Familiarity with [how to choose which DataConnector to use](../how_to_choose_which_dataconnector_to_use.md).
- Familiarity with [how to choose between working with a single or multiple Batches of data](../how_to_choose_between_working_with_a_single_or_multiple_batches_of_data.md).

</Prerequisites>

## Steps

### 1. Import necessary modules and initialize your Data Context

<ImportNecessaryModulesAndInitializeYourDataContext />

### 2. Create a new Datasource configuration.

<CreateANewDatasourceConfiguration />

<TheKeysYouNeedForYourDatasource />

### 3. Name your Datasource

<NameYourDatasource />

### 4. Specify the Datasource class and module

<SpecifyTheDatasourceClassAndModule />
    

### 5. Add the Spark Execution Engine to your Datasource configuration

<AddTheBackendExecutionEngineToYourDatasourceConfiguration backend="Spark" />

For the purposes of this guide, these will consist of the `SparkDFExecutionEngine` found at `great_expectations.execution_engine`.  The `execution_engine` key and its corresponding value will therefore look like this:

```python
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",
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
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
}
```

### 6. Add a dictionary as the value of the `data_connectors` key

<AddADictionaryAsTheValueOfTheDataConnectorsKey />

Your current configuration should look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {}
}
```


### 7. Configure your individual Data Connectors

<ConfigureYourIndividualDataConnectors backend="Spark" />


<TipWhichDataConnectorToUse />


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

<TipInferredDataConnectorOverview />

<NameTheDataConnector data_connector_name="name_of_my_inferred_data_connector" />

At this point, your configuration should look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_inferred_data_connector": {}
        }
    }
}
```

<DataConnectorKeysOverview data_connector_type="InferredAssetFilesystemDataConnector" data_connector_name="name_of_my_inferred_data_connector" runtime={false} batch_spec_passthrough={true} glob_directive={true} />

For this example, you will be using the `InferredAssetFilesystemDataConnector` as your `class_name`.  This is a subclass of the `InferredAssetDataConnector` that is specialized to support filesystem Execution Engines, such as the `SparkDFExecutionEngine`.  This key/value entry will therefore look like:

```python
        "class_name": "InferredAssetFilesystemDataConnector",
```

<TipCustomDataConnectorModuleName />

<FilesystemBaseDirectory />

With these values added, along with blank dictionary for `default_regex` (we will define it in the next step) and `batch_spec_passthrough`, your full configuration should now look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_inferred_data_connector": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "default_regex": {}
            "bass_spec_passthrough": {}
        }
    }
}
```
:::info Optional parameter: `glob_directive`

<GlobDirectiveDetails />

:::

  </TabItem>
  <TabItem value="configured">

<TipConfiguredDataConnectorOverview />

<NameTheDataConnector data_connector_name="name_of_my_configured_data_connector" />

At this point, your configuration should look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_configured_data_connector": {}
        }
    }
}
```

<DataConnectorKeysOverview data_connector_type="ConfiguredAssetFilesystemDataConnector" data_connector_name="name_of_my_configured_data_connector" runtime={false} batch_spec_passthrough={true} glob_directive={false} />

For this example, you will be using the `ConfiguredAssetFilesystemDataConnector` as your `class_name`.  This is a subclass of the `ConfiguredAssetDataConnector` that is specialized to support filesystem Execution Engines, such as the `SparkDFExecutionEngine`.  This key/value entry will therefore look like:

```python
        "class_name": "ConfiguredAssetFilesystemDataConnector",
```

<TipCustomDataConnectorModuleName />

<FilesystemBaseDirectory />

With these values added, along with blank dictionaries for `assets` and `batch_spec_passthrough`, your full configuration should now look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_configured_data_connector": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "assets": {}
            "batch_spec_passthrough": {}
        }
    }
}
```

  </TabItem>
  <TabItem value="runtime">

<TipRuntimeDataConnectorOverview />

<NameTheDataConnector data_connector_name="name_of_my_runtime_data_connector" />

At this point, your configuration should look like:

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_runtime_data_connector": {}
        }
    }
}
```

<DataConnectorKeysOverview data_connector_type="RuntimeDataConnector" data_connector_name="name_of_my_runtime_data_connector" runtime={true} batch_spec_passthrough={false} glob_directive={false} />


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
        "class_name": "SparkDFExecutionEngine",  
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


### 8. Configure the values for `batch_spec_passthrough`

<Tabs
  groupId="data-asset-type"
  defaultValue='inferred'
  values={[
  {label: 'InferredAssetFilesystemDataConnector', value:'inferred'},
  {label: 'ConfiguredAssetDataConnector', value:'configured'},
  {label: 'RuntimeDataConnector', value:'runtime'},
  ]}>

  <TabItem value="inferred">

<BatchSpecInferredOrConfigured />

And your full configuration will look like:      

```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_inferred_data_connector": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "default_regex": {}
            "bass_spec_passthrough": {
                "reader_method": "csv",
                "reader_options": {
                    "header": True,
                    "inferSchema": True,
                }
            }
        }
    }
}
```

  </TabItem>
  <TabItem value="configured">

<BatchSpecInferredOrConfigured />

And your full configuration will look like:


```python
datasource_config: dict = {
    "name": "my_datasource_name",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",  
        "module_name": "great_expectations.execution_engine",
    },
    "data_connectors": {
        "name_of_my_configured_data_connector": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "./data",
            "assets": {}
            "batch_spec_passthrough": {
                "reader_method": "csv",
                "reader_options": {
                    "header": True,
                    "inferSchema": True,
                }
            }
        }
    }
}
```

      
  </TabItem>
  <TabItem value="runtime">

Runtime Data Connectors always work with a single Batch of data which is specified by a dataframe or path in a Batch Request.  Because of this, the `batch_spec_passthrough` parameters will also be passed in through your Batch Request as well, and you will not need to enter them here.

  </TabItem>
  </Tabs>

### 9. Configure your Data Connector's Data Assets

<Tabs
  groupId="data-asset-type"
  defaultValue='inferred'
  values={[
  {label: 'InferredAssetFilesystemDataConnector', value:'inferred'},
  {label: 'ConfiguredAssetDataConnector', value:'configured'},
  {label: 'RuntimeDataConnector', value:'runtime'},
  ]}>

  <TabItem value="inferred">

<AssetKeysOverviewInferred />

<IntroForSingleOrMultiBatchConfiguration />

<InferredAssets/>

  </TabItem>
  <TabItem value="configured">

<AssetKeysOverviewConfigured />

<IntroForSingleOrMultiBatchConfiguration />

<ConfiguredAssets />

  </TabItem>
  <TabItem value="runtime">

<RuntimeAssets />

  </TabItem>
  </Tabs>

### 10. Test your configuration with `.test_yaml_config(...)`

<TestYourConfigurationWithTestYamlConfig />

### 11. (Optional) Add more Data Connectors to your configuration

<StepAddMoreDataConnectorsToYourConfig />

### 12. Add your new Datasource to your Data Context

<StepAddYourNewDatasourceToYourDataContext />

## Next Steps

<StepNext />
