---
title: How to configure a SQL Datasource
---
# [![Connect to data icon](../../../images/universal_map/Outlet-active.png)](../connect_to_data_overview.md) How to configure a SQL Datasource

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

import SectionIntro from './components/_section_intro.mdx';
import SectionPrerequisites from './sql_components/_section_prerequisites.mdx'
import SectionImportNecessaryModulesAndInitializeYourDataContext from './filesystem_components/_section_import_necessary_modules_and_initialize_your_data_context.mdx'
import SectionCreateANewDatasourceConfiguration from './components/_section_create_a_new_datasource_configuration.mdx'
import SectionSpecifyTheDatasourceClassAndModule from './components/_section_specify_the_datasource_class_and_module.mdx'
import SectionNameYourDatasource from './components/_section_name_your_datasource.mdx'
import SectionAddTheExecutionEngineToYourDatasourceConfiguration from './sql_components/_section_add_the_execution_engine_to_your_datasource_configuration.mdx'
import SectionAddADictionaryAsTheValueOfTheDataConnectorsKey from './sql_components/_section_add_a_dictionary_as_the_value_of_the_data_connectors_key.mdx'
import SectionConfigureYourIndividualDataConnectors from './sql_components/_section_configure_your_individual_data_connectors.mdx'
import SectionDataConnectorExampleConfigurations from './sql_components/_section_data_connector_example_configurations.mdx'
import SectionConfigureYourDataAssets from './sql_components/_section_configure_your_data_assets.mdx'
import SectionTestYourConfigurationWithTestYamlConfig from './components/_section_test_your_configuration_with_test_yaml_config.mdx'
import SectionAddMoreDataConnectorsToYourConfig from './components/_section_add_more_data_connectors_to_your_config.mdx'
import SectionAddYourNewDatasourceToYourDataContext from './components/_section_add_your_new_datasource_to_your_data_context.mdx'
import SectionNextSteps from './components/_section_next_steps.mdx'

import AdditionalInfoSplittingMethods from './sql_components/_table_splitting_methods.mdx'
import AdditionalInfoSamplingMethods from './sql_components/_table_sampling_methods.mdx'
import AdditionalInfoIntrospectionDirectives from './sql_components/_part_introspection_directives.mdx'

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>

<SectionIntro backend="SQL" />

## Steps

### 1. Import necessary modules and initialize your Data Context

<SectionImportNecessaryModulesAndInitializeYourDataContext />

### 2. Create a new Datasource configuration.

<SectionCreateANewDatasourceConfiguration />

### 3. Name your Datasource

<SectionNameYourDatasource />

### 4. Specify the Datasource class and module

<SectionSpecifyTheDatasourceClassAndModule />

### 5. Add the SqlAlchemy Execution Engine to your Datasource configuration

<SectionAddTheExecutionEngineToYourDatasourceConfiguration />

### 6. Add a dictionary as the value of the `data_connectors` key

<SectionAddADictionaryAsTheValueOfTheDataConnectorsKey />

### 7. Configure your individual Data Connectors (Splitting, sampling, etc.)

<SectionConfigureYourIndividualDataConnectors backend="SQL" />

#### Data Connector example configurations:

<SectionDataConnectorExampleConfigurations />

### 8. Configure your Data Connector's Data Assets (Splitting, sampling, etc.)

<SectionConfigureYourDataAssets />

### 9. Test your configuration with `.test_yaml_config(...)`

<SectionTestYourConfigurationWithTestYamlConfig />

### 10. (Optional) Add more Data Connectors to your configuration

<SectionAddMoreDataConnectorsToYourConfig />

### 11. Add your new Datasource to your Data Context

<SectionAddYourNewDatasourceToYourDataContext />

## Next steps

<SectionNextSteps />

## Additional notes

### Splitting methods

<AdditionalInfoSplittingMethods />

### Sampling methods

<AdditionalInfoSamplingMethods />

### Introspection directives

<AdditionalInfoIntrospectionDirectives />
