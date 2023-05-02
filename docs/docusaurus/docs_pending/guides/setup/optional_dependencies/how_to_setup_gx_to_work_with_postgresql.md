---
title: How to set up GX to work with PostgreSQL
tag: [how-to, setup]
keywords: [Great Expectations, SQL, PostgreSQL]
---

# How to set up Great Expectations to work with PostgreSQL

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ## Prerequisites -->

<!-- ### 1. Check your Python version -->
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'

<!-- ### 2. Create a Python virtual environment -->
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 3. Install GX with optional dependencies for ??? -->
import InstallDependencies from '/docs/components/setup/dependencies/_sql_install_dependencies.mdx'

<!-- ### 4. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'

<!-- ### 5. Initialize a Data Context to store your credentials -->
import InitializeDataContextFromCli from '/docs/components/setup/data_context/_filesystem_data_context_initialize_with_cli.md'
import VerifyDataContextInitializedFromCli from '/docs/components/setup/data_context/_filesystem_data_context_verify_initialization_from_cli.md'

<!-- ### 6. Configure the `config_variables.yml` file with your Azure Storage credentials -->
import ConfigureCredentialsInDataContext from '/docs/components/setup/dependencies/_postgresql_configure_credentials_in_config_variables_yml.md'

<!-- ## Next steps -->
import PostgreSqlFurtherConfiguration from '/docs/components/setup/next_steps/_links_for_adding_postgresql_configurations_to_data_context.md'


This guide will walk you through best practices for creating your GX Python environment and demonstrate how to locally install Great Expectations along with the necessary dependencies for working with PostgreSQL.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip

</Prerequisites>

## Steps

### 1. Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### 2. Create a Python virtual environment

<PythonCreateVenv />

### 3. Install GX with optional dependencies for PostgreSQL

<InstallDependencies install_key="postgresql" database_name="PostgreSQL"/>

### 4. Verify that GX has been installed correctly

<GxVerifyInstallation />

### 5. Initialize a Data Context to store your PostgreSQL credentials

<InitializeDataContextFromCli />

:::info Verifying the Data Context initialized successfully

<VerifyDataContextInitializedFromCli />

:::

### 6. Configure the `config_variables.yml` file with your PostgreSQL credentials

<ConfigureCredentialsInDataContext />

## Next steps

<PostgreSqlFurtherConfiguration />
