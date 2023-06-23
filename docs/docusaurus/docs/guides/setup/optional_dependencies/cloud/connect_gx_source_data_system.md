---
sidebar_label: "Connect to a Source Data System"
title: "Connect to a Source Data System"
id: connect_gx_source_data_system
description: Install and configure Great Expectations to access data stored on Amazon S3, Google Cloud Storage, Microsoft Azure Blob Storage, and SQL databases.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrereqInstalledAwsCli from '/docs/components/prerequisites/_aws_installed_the_aws_cli.mdx'
import PrereqAwsConfiguredCredentials from '/docs/components/prerequisites/_aws_configured_your_credentials.mdx'
import AwsVerifyInstallation from '/docs/components/setup/dependencies/_aws_verify_installation.md'
import AwsVerifyCredentialsConfiguration from '/docs/components/setup/dependencies/_aws_verify_installation.md'
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'
import S3InstallDependencies from '/docs/components/setup/dependencies/_s3_install_dependencies.md'
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'
import LinksAfterInstallingGx from '/docs/components/setup/next_steps/_links_after_installing_gx.md'
import PrereqGcpServiceAccount from '/docs/components/prerequisites/_gcp_service_account.md'
import GcpVerifyCredentials from '/docs/components/setup/dependencies/_gcp_verify_credentials_configuration.md'
import GcpInstallDependencies from '/docs/components/setup/dependencies/_gcp_install_dependencies.md'
import PrereqAbsConfiguredAnAbsAccount from '/docs/components/prerequisites/_abs_configured_an_azure_storage_account_and_kept_connection_string.md'
import AbsInstallDependencies from '/docs/components/setup/dependencies/_abs_install_dependencies.md'
import AbsConfigureCredentialsInDataContext from '/docs/components/setup/dependencies/_abs_configure_credentials_in_data_context.md'
import AbsFurtherConfiguration from '/docs/components/setup/next_steps/_links_for_adding_azure_blob_storage_configurations_to_data_context.md'
import InstallDependencies from '/docs/components/setup/dependencies/_sql_install_dependencies.mdx'
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';


This is where you'll find information about creating your Great Expectations (GX) Python environment, installing GX locally, and how to configure the dependencies necessary to access Source Data stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or SQL databases. GX uses the term Source Data when referring to data in its original format, and the term Source Data System when referring to the storage location for Source Data.

<Tabs
  groupId="install-gx-cloud-storage"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'SQL databases', value:'sql'},
  ]}>
<TabItem value="amazon">

## Amazon S3

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on Amazon S3.

### Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqInstalledAwsCli />
- <PrereqAwsConfiguredCredentials />

</Prerequisites>

### Ensure your AWS CLI version is the most recent

<AwsVerifyInstallation />

### Ensure your AWS credentials are correctly configured

<AwsVerifyCredentialsConfiguration />

### Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### Create a Python virtual environment

<PythonCreateVenv />

### Install GX with optional dependencies for S3

<S3InstallDependencies />

### Verify the GX has been installed correctly

<GxVerifyInstallation />

### Next steps

Now that you have installed GX with the necessary dependencies for working with S3, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />

</TabItem>
<TabItem value="azure">

## Microsoft Azure Blob Storage

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on Microsoft Azure Blob Storage.

### Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqAbsConfiguredAnAbsAccount />

</Prerequisites>

### Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### Create a Python virtual environment

<PythonCreateVenv />

### Install GX with optional dependencies for Azure Blob Storage

<AbsInstallDependencies />

### Verify that GX has been installed correctly

<GxVerifyInstallation />

### Configure the `config_variables.yml` file with your Azure Storage credentials

<AbsConfigureCredentialsInDataContext />

### Next steps

<AbsFurtherConfiguration />

</TabItem>
<TabItem value="gcs">

## GCS

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on GCS.

### Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqGcpServiceAccount />

</Prerequisites>

### Ensure your GCP credentials are correctly configured

<GcpVerifyCredentials />

### Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### Create a Python virtual environment

<PythonCreateVenv />

### Install optional dependencies

<GcpInstallDependencies />

### Verify that GX has been installed correctly

<GxVerifyInstallation />

### Next steps

Now that you have installed GX with the necessary dependencies for working with GCS, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />

</TabItem>
<TabItem value="sql">

## SQL databases

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on SQL databases.

### Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip

</Prerequisites>

### Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### Create a Python virtual environment

<PythonCreateVenv />

### Install GX with optional dependencies for SQL databases

<InstallDependencies install_key="sqlalchemy" database_name="SQL databases"/>

:::caution Additional dependencies for some SQL dialects

The above pip instruction will install GX with basic SQL support through SqlAlchemy.  However, certain SQL dialects require additional dependencies.  Depending on the SQL database type you will be working with, you may wish to use one of the following installation commands, instead:

- AWS Athena: `pip install 'great_expectations[athena]'`
- BigQuery: `pip install 'great_expectations[bigquery]'`
- MSSQL: `pip install 'great_expectations[mssql]'`
- PostgreSQL: `pip install 'great_expectations[postgresql]'`
- Redshift: `pip install 'great_expectations[redshift]'`
- Snowflake: `pip install 'great_expectations[snowflake]'`
- Trino: `pip install 'great_expectations[trino]'`

:::

:::caution SqlAlchemy version

Great Expectations does not currently support SqlAlchemy 2.0.

If you install SqlAlchemy independently of the above pip commands, be certain to install the most recent SqlAlchemy version prior to 2.0.

:::

### Verify that GX has been installed correctly

<GxVerifyInstallation />

### Set up credentials

Different SQL dialects have different requirements for connection strings and methods of configuring credentials.  By default, GX allows you to define credentials as environment variables or as values in your Data Context ([once you have initialized one](/docs/guides/setup/configuring_data_contexts/initializing_data_contexts/how_to_initialize_a_filesystem_data_context_in_python)).

There may also be third party utilities for setting up credentials of a given SQL database type.  For more information on setting up credentials for a given source database, please reference the official documentation for that SQL dialect as well as our guide on [how to set up credentials(/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials).

### Next steps

Now that you have installed GX with the necessary dependencies for working with SQL databases, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />

</TabItem>
</Tabs>