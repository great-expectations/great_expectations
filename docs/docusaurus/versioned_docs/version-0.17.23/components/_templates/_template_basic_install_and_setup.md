---
[//]: # (TODO: title: How to set up GX to work with general $TODO$)
tag: [how-to, setup]
[//]: # (TODO: keywords: [Great Expectations, SQL, $TODO$])
---

[//]: # (TODO: # How to set up Great Expectations to work with general $TODO$)

import TechnicalTag from '../../term_tags/_tag.mdx';
import Prerequisites from '../_prerequisites.jsx'

<!-- ## Introduction -->
import IntroInstallPythonGxAndDependencies from '../setup/installation/_intro_python_environment_with_dependencies.mdx'

<!-- ## Prerequisites -->

<!-- ### 1. Check your Python version -->
import PythonCheckVersion from '../setup/python_environment/_python_check_version.mdx'

<!-- ### 2. Create a Python virtual environment -->
import PythonCreateVenv from '../setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '../setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 3. Install GX with optional dependencies for ??? -->
import InstallDependencies from '../setup/dependencies/_sql_install_dependencies.mdx'

<!-- ### 4. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '../setup/_gx_verify_installation.md'

<!-- ### 5. Initialize a Data Context to store your credentials -->
import InitializeDataContextFromCli from '../setup/data_context/_filesystem_data_context_initialize_with_cli.md'
import VerifyDataContextInitializedFromCli from '../setup/data_context/_filesystem_data_context_verify_initialization_from_cli.md'

<!-- ### 6. Configure the `config_variables.yml` file with your credentials -->
[//]: # (TODO: import ConfigureCredentialsInDataContext from '../setup/dependencies/_postgresql_configure_credentials_in_config_variables_yml.md')


<!-- ## Next steps -->
[//]: # (TODO: import FurtherConfiguration from '../setup/next_steps/_links_after_installing_gx.md')


[//]: # (TODO:<IntroInstallPythonGxAndDependencies dependencies="$TODO$" />)

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

[//]: # (TODO: ### 3. Install GX with optional dependencies for $TODO$)

[//]: # (TODO: <InstallDependencies install_key="sqlalchemy" database_name="SQL"/>)

### 4. Verify that GX has been installed correctly

<GxVerifyInstallation />

[//]: # (TODO: ### 5. Initialize a Data Context to store your PostgreSQL credentials)

<InitializeDataContextFromCli />

:::info Verifying the Data Context initialized successfully

<VerifyDataContextInitializedFromCli />

:::

[//]: # (TODO: ### 6. Configure the `config_variables.yml` file with your PostgreSQL credentials)

<ConfigureCredentialsInDataContext />

## Next steps

<FurtherConfiguration />


