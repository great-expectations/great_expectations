import GxData from '../../_core_components/_data.jsx';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import RecommendedVirtualEnvironment from '../../_core_components/prerequisites/_recommended_virtual_environment.md';
import InfoUsingAVirtualEnvironment from '../../_core_components/admonitions/_if_you_are_using_a_virtual_environment.md';

Azure Blob Storage stores unstructured data on the Microsoft cloud data storage platform. To validate Azure Blob Storage data with GX Core you install additional Python libraries and define a connection string.

## Prerequisites

- An [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage). 
- [Azure storage account access keys](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).
- <PrereqPythonInstalled/>
- <RecommendedVirtualEnvironment/>

## Installation
  
1. Install the Python dependencies for Azure Blob Storage support.

   Run the following code to install GX Core with the additional Python libraries needed to work with Azure Blob Storage:

   :::info
   <InfoUsingAVirtualEnvironment/>
   :::

   ```bash title="Terminal input"
   python -m pip install 'great_expectations[azure]'
   ```

3. Configure your Azure Blob Storage credentials.

   To store your Azure Blob Storage credentials as an environment variable, replace `<YOUR-STORAGE-ACCOUNT-NAME>` and `<YOUR-STORAGE-ACCOUNT-KEY>` in the following terminal command with your Azure Blob Storage account values:

   ```bash title="Terminal input"
   export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY>"
   ```

   :::info

   You can manage your credentials for all environments and Data Sources by storing them as environment variables.  To do this, enter `export ENV_VARIABLE_NAME=env_var_value` in the terminal or add the equivalent command to your `~/.bashrc` file.
  
   As an alternative to environment variables, you can also [store credentials in the file `config_variables.yml`](/core/configure_project_settings/configure_credentials/configure_credentials.md?storage_type=config_yml) after you have [created a File Data Context](/core/set_up_a_gx_environment/create_a_data_context.md?context_type=file).

   :::
