---
title: How to set up GX to work with data in Azure Blob Storage
tag: [how-to, setup]
keywords: [Great Expectations, Data Context, Filesystem, ABS, Azure Blob Storage]

---

# How to set up Great Expectations to work with data in Azure Blob Storage

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ## Prerequisites -->
import PrereqAbsConfiguredAnAbsAccount from '/docs/components/prerequisites/_abs_configured_an_azure_storage_account_and_kept_connection_string.md'

<!-- ### 1. Check your Python version -->
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'

<!-- ### 2. Create a Python virtual environment -->
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 3. Install GX with optional dependencies for ABS -->
import AbsInstallDependencies from '/docs/components/setup/dependencies/_abs_install_dependencies.md'

<!-- ### 4. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'

<!-- ### 5. Configure the `config_variables.yml` file with your Azure Storage credentials -->
import AbsConfigureCredentialsInDataContext from '/docs/components/setup/dependencies/_abs_configure_credentials_in_data_context.md'

<!-- ## Next steps -->
import AbsFurtherConfiguration from '/docs/components/setup/next_steps/_links_for_adding_azure_blob_storage_configurations_to_data_context.md'

This guide will walk you through best practices for creating your GX Python environment and demonstrate how to locally install Great Expectations along with the necessary dependencies for working with data stored in Azure Blob Storage.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqAbsConfiguredAnAbsAccount />

</Prerequisites>

## Steps

### 1. Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### 2. Create a Python virtual environment

<PythonCreateVenv />

### 3. Install GX with optional dependencies for Azure Blob Storage

<AbsInstallDependencies />

### 4. Verify that GX has been installed correctly

<GxVerifyInstallation />

### 5. Configure the `config_variables.yml` file with your Azure Storage credentials

<AbsConfigureCredentialsInDataContext />

## Next steps

<AbsFurtherConfiguration />


