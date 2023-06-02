---
sidebar_label: "Connect Great Expectations to Datasources "
title: "Connect Great Expectations to Datasources"
id: install_gx_cloud_storage
description: Install and configure Great Expectations to access data stored on Amazon S3, Google Cloud Storage, and Microsoft Azure Blob Storage.
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
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'
import PrereqAbsConfiguredAnAbsAccount from '/docs/components/prerequisites/_abs_configured_an_azure_storage_account_and_kept_connection_string.md'
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import AbsInstallDependencies from '/docs/components/setup/dependencies/_abs_install_dependencies.md'
import AbsConfigureCredentialsInDataContext from '/docs/components/setup/dependencies/_abs_configure_credentials_in_data_context.md'
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

This is where you'll find information about creating your Great Expectations (GX) Python environment, installing GX locally, and how to configure the dependencies necessary to access data stored Amazon S3, Google Cloud Storage (GCS), and  Microsoft Azure Blob Storage.

<Tabs
  groupId="install-gx-cloud-storage"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  ]}>
  <TabItem value="amazon">

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on Amazon S3.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqInstalledAwsCli />
- <PrereqAwsConfiguredCredentials />

</Prerequisites>

## Ensure your AWS CLI version is the most recent

<AwsVerifyInstallation />

## Ensure your AWS credentials are correctly configured

<AwsVerifyCredentialsConfiguration />

## Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

## Create a Python virtual environment

<PythonCreateVenv />

## Install GX with optional dependencies for S3

<S3InstallDependencies />

## Verify that GX has been installed correctly

<GxVerifyInstallation />

## Next steps

Now that you have installed GX with the necessary dependencies for working with S3, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />

  </TabItem>
<TabItem value="gcs">

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on GCS.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqGcpServiceAccount />

</Prerequisites>

## Ensure your GCP credentials are correctly configured

<GcpVerifyCredentials />

## Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

## Create a Python virtual environment

<PythonCreateVenv />

## Install GX with optional dependencies for GCS

<GcpInstallDependencies />

## Verify that GX has been installed correctly

<GxVerifyInstallation />

## Next steps

Now that you have installed GX with the necessary dependencies for working with GCS, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />

</TabItem>
<TabItem value="azure">

Create your GX Python environment, install Great Expectations locally, and then configure the necessary dependencies to access data stored on Microsoft Azure Blob Storage.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqAbsConfiguredAnAbsAccount />

</Prerequisites>

## Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

## Create a Python virtual environment

<PythonCreateVenv />

## Install GX with optional dependencies for Azure Blob Storage

<AbsInstallDependencies />

## Verify that GX has been installed correctly

<GxVerifyInstallation />

## Configure the `config_variables.yml` file with your Azure Storage credentials

<AbsConfigureCredentialsInDataContext />

## Next steps

<AbsFurtherConfiguration />

</TabItem>
</Tabs>