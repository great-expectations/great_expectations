---
title: Install additional dependencies
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';

Some environments and Data Sources require additional Python libraries or third party utilities that are not included in the base installation of GX Core. Use the information provided here to install the necessary dependencies for Amazon S3, Azure Blob Storage, Google Cloud Storage, and SQL Data Sources.

<Tabs
  queryString="dependencies"
  groupId="install-dependencies"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'SQL Databases', value:'sql'},
  ]}>
<TabItem value="amazon">

Amazon S3 is a service offered by [Amazon Web Services (AWS)](https://aws.amazon.com).  With it, you can access objects stored in S3 buckets through a web interface.  Great Expectations (GX) uses the Python library `boto3` to access S3, but you will need to configure your Amazon S3 account and credentials through AWS and the AWS command line interface (CLI).

## Prerequisites

- The AWS CLI. See [Installing or updating the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

- AWS credentials. See [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

## Installation

1. Verify the installation of the AWS CLI by running the command:

  ```bash title="Terminal command"
  aws --version
  ```

  If this command does not return AWS CLI version information, reinstall or update the AWS CLI.  See [Install or update to the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

2. Verify that your AWS credentials are properly configured by running the command:

  ```bash title="Terminal command"
  aws sts get-caller-identity
  ```

  If your credentials are properly configured, this will output your `UserId`, `Account` and `Arn`.  If your credentials are not configured correctly, this will throw an error.

  If an error is thrown, or if you were unable to use the AWS CLI to verify your credentials' configuration, you can find additional guidance on configuring your AWS credentials by referencing [Amazon's documentation on configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
  
3. Install the Python dependencies for AWS S3 support.

  :::info 
  
  If you [installed GX in a virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment) then that environment should be active when installing these dependencies.
  
  :::

  To install the optional dependencies required by GX to work with AWS S3, run the command:

  ```bash title="Terminal input"
  python -m pip install 'great_expectations[s3]'
  ```

  This will install Great Expectations along with the requirements for the `boto3` Python library.


</TabItem>
<TabItem value="azure">

Azure Blob Storage stores unstructured data on the Microsoft cloud data storage platform. To validate Azure Blob Storage data with Great Expectations (GX) you install additional Python libraries and define a connection string.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment).
- An [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage). 
- [Azure storage account access keys](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).

## Installation
  
1. Install the Python dependencies for AWS S3 support.

  :::info 
  
  If you [installed GX in a virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment) then that environment should be active when installing these dependencies.
  
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
  
  If you do not want to store your credentials as environment variables, you can alternatively [store them in the file `config_variables.yml`](/core/installation_and_setup/manage_credentials.md?credential-style=yaml) after you have [created a File Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=file#initialize-a-new-data-context).
  
  :::

</TabItem>
<TabItem value="gcs">

To validate Google Cloud Platform (GCP) data with GX Core, you create your GX Python environment, install GX Core locally, and then configure the necessary dependencies.

## Prerequisites

- <PrereqPythonInstalled/>
- pip. See [Installation and downloads](https://pypi.org/project/pip/).
- A [GCP service account](https://cloud.google.com/iam/docs/service-account-overview) with permissions to access GCP resources and storage Objects.
- The `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set. See [Set up Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc).  
- Google Cloud API authentication is set up. See [Set up authentication](https://cloud.google.com/storage/docs/reference/libraries#authentication).

## Installation

1. Run the following code to confirm your Python version:

    ```bash title="Terminal input"
    python --version
    ```
    If your existing Python version is not 3.8 to 3.11, see [Active Python Releases](https://www.python.org/downloads/).

2. Run the following code to create a virtual environment and a directory named `my_venv`:

    ```bash title="Terminal input"
    python -m venv my_venv
    ```
    Optional. Replace `my_venv` with another directory name. 

    If you prefer, you can use virtualenv, pyenv, and similar tools to install GX in virtual environments.

3. Run the following code to activate the virtual environment: 

    ```bash title="Terminal input"
    source my_venv/bin/activate
    ```

4. Run the following code to install optional dependencies:

    ```bash title="Terminal input"
    python -m pip install 'great_expectations[gcp]'
    ```

5. Run the following code to confirm GX was installed successfully:

    ```bash title="Terminal input"
    great_expectations --version
    ```
    The output should be `great_expectations, version <version_number>`.

</TabItem>
<TabItem value="sql">

To validate data stored on SQL databases with GX Core, you create your GX Python environment, install GX Core locally, and then configure the necessary dependencies.

## Prerequisites

- <PrereqPythonInstalled/>
- pip. See [Installation and downloads](https://pypi.org/project/pip/).
- The necessary environment variables are set to allow access to the SQL database. See [Manage credentials](../manage_credentials.md).

## Installation

1. Run the following code to confirm your Python version:

    ```bash title="Terminal input"
    python --version
    ```
    If your existing Python version is not 3.8 to 3.11, see [Active Python Releases](https://www.python.org/downloads/).

2. Run the following code to create a virtual environment and a directory named `my_venv`:

    ```bash title="Terminal input"
    python -m venv my_venv
    ```
    Optional. Replace `my_venv` with another directory name. 

    If you prefer, you can use virtualenv, pyenv, and similar tools to install GX in virtual environments.

3. Run the following code to activate the virtual environment: 

    ```bash title="Terminal input"
    source my_venv/bin/activate
    ```

4. Run the following code to install optional dependencies for SQLAlchemy:

    ```bash title="Terminal input"
    python -m pip install 'great_expectations[sqlalchemy]'
    ```
    To install optional dependencies for a different SQL database, see [SQL database dependency commands](#sql-database-dependency-commands).

5. Run the following code to confirm GX was installed successfully:

    ```bash title="Terminal input"
    great_expectations --version
    ```
    The output should be `great_expectations, version <version_number>`.

## SQL database dependency commands

The following table lists the installation commands used to install GX Core dependencies for specific SQL databases. These dependencies are required for the successful operation of GX Core.

| SQL Database | Command |
| :-- | :-- | 
| AWS Athena | `pip install 'great_expectations[athena]'` |
| BigQuery | `pip install 'great_expectations[bigquery]'` |
| MSSQL | `pip install 'great_expectations[mssql]'` |
| PostgreSQL | `pip install 'great_expectations[postgresql]'` |
| Redshift | `pip install 'great_expectations[redshift]'` |
| Snowflake | `pip install 'great_expectations[snowflake]'` |
| Trino | `pip install 'great_expectations[trino]'` |

</TabItem>
</Tabs>


## Next steps

- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)
