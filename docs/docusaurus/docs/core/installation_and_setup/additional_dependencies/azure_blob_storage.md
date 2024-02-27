---
title: Install and set up Azure Blob Storage support
---
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';

Azure Blob Storage allows users to store unstructured data on Microsoft's cloud data storage platform.  With the installation of some additional Python libraries and the provision of a connection string, you will be able to use Great Expectations (GX) alongside data kept in Azure Blob Storge.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](./set_up_a_python_environment#optional-create-a-virtual-environment).
- An [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage). A [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal) is required to complete the setup.

## Installation
  
1. Install the Python dependencies for AWS S3 support.

  :::info 
  
  If you [installed GX in a virtual environment](/core/installation_and_setup/set_up_a_python_environment#optional-create-a-virtual-environment) then that environment should be active when installing these dependencies.
  
  :::

  To install Great Expectations with the additional Python libraries needed to work with Azure Blob Storage run the following command:

  ```bash title="Terminal input"
  python -m pip install 'great_expectations[azure]'
  ```

2. Configure your Azure Blob Storage credentials.

  You can manage your credentials by storing them as environment variables.  To do this, you will enter `export ENV_VARIABLE_NAME=env_var_value` in the terminal or adding the equivalent command to your `~/.bashrc` file.  For Azure Blob Storage credentials, that should look like:

  ```title="Terminal input"
  export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY>"
  ```

  When entering this command, replace `<YOUR-STORAGE-ACCOUNT-NAME>` and `<YOUR-STORAGE-ACCOUNT-KEY>` with the appropriate values for your Azure Blob Storage account.

  :::info 
  
  If you do not want to store your credentials as environment variables, you can alternatively [store them in the file `config_variables.yml`](/core/installation_and_setup/manage_credentials?credential-style=yaml) after you have [created a File Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=file#initialize-a-new-data-context).
  
  :::
  
## Next steps

- [Install additional dependencies (if more are required)](/core/installation_and_setup/additional_dependencies/additional_dependencies.md)
- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)
