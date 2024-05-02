---
title: Install additional dependencies
id: additional_dependencies
toc_min_heading_level: 3
toc_max_heading_level: 3
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';

Some environments and Data Sources require additional Python libraries or third party utilities that are not included in the base installation of GX Core. Use the information provided here to install the necessary dependencies for Amazon S3, Azure Blob Storage, Google Cloud Storage, and SQL databases.

<Tabs
  queryString="dependencies"
  groupId="additional-dependencies"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'SQL databases', value:'sql'},
  ]}>
<TabItem value="amazon">

GX Core uses the Python library `boto3` to access objects stored in Amazon S3 buckets, but you must configure your Amazon S3 account and credentials through AWS and the AWS command line interface (CLI).

## Prerequisites

- The AWS CLI. See [Installing or updating the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

- AWS credentials. See [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

## Installation

Python interacts with AWS through the boto3 library. GX Core makes use of this library in the background when working with AWS. Although you won't use boto3 directly, you'll need to install it in your virtual environment.

To set up boto3 with AWS, and use boto3 within Python, see the [Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

1. Run the following code to verify the AWS CLI version:

  ```bash title="Terminal input"
  aws --version
  ```

  If this command does not return AWS CLI version information, reinstall or update the AWS CLI.  See [Install or update to the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

2. Run one of the following pip commands to install boto3 in your virtual environment:

  ```bash title="Terminal input"
  python -m pip install boto3
  ```

  or

  ```bash title="Terminal input"
  python3 -m pip install boto3
  ```

3. Run the following code to verify your AWS credentials are properly configured:

  ```bash title="Terminal input"
  aws sts get-caller-identity
  ```

  When your credentials are properly configured, your `UserId`, `Account`, and `Arn` are returned. If your credentials are not configured correctly, an error message appears. If you received an error message, or you couldn't verify your credentials, see [Configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
  
4. Install the Python dependencies for AWS S3 support.

  :::info 
  
  If you [installed GX in a virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment), your environment should be active when you install these dependencies.
  
  :::

  Python interacts with AWS through the `boto3` library. GX makes use of this library in the background when working with AWS. Although you won't use `boto3` directly, you'll need to install it for GX to work with AWS.

  Run the following code to install the dependencies required by GX to work with AWS S3:

  ```bash title="Terminal input"
  python -m pip install 'great_expectations[s3]'
  ```

  For more information on the `boto3` Python library see the [official Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).


</TabItem>
<TabItem value="azure">

Azure Blob Storage stores unstructured data on the Microsoft cloud data storage platform. To validate Azure Blob Storage data with GX Core you install additional Python libraries and define a connection string.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment).
- An [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage). 
- [Azure storage account access keys](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).

## Installation
  
1. Install the Python dependencies for AWS S3 support.

  :::info 
  
  If you [installed GX in a virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment), your environment should be active when you install these dependencies.
  
  :::

  Run the following code to install GX Core with the additional Python libraries needed to work with Azure Blob Storage:

  ```bash title="Terminal input"
  python -m pip install 'great_expectations[azure]'
  ```

2. Configure your Azure Blob Storage credentials.

  You can manage your credentials by storing them as environment variables.  To do this, enter `export ENV_VARIABLE_NAME=env_var_value` in the terminal or add the equivalent command to your `~/.bashrc` file. For example:

  ```title="Terminal input"
  export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY>"
  ```

  When entering this command, replace `<YOUR-STORAGE-ACCOUNT-NAME>` and `<YOUR-STORAGE-ACCOUNT-KEY>` with your Azure Blob Storage account values.

  :::info 
  
  If you do not want to store your credentials as environment variables, you can [store them in the file `config_variables.yml`](/core/installation_and_setup/manage_credentials.md#yaml-file) after you have [created a File Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=file#initialize-a-new-data-context).
  
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
