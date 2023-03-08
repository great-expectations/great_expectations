---
[//]: # (TODO: title: How to set up GX to work with general $TODO$)
tag: [how-to, setup]
[//]: # (TODO: keywords: [Great Expectations, SQL, $TODO$])
---

[//]: # (TODO: # How to set up Great Expectations to work with general $TODO$)

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

<!-- ## Next steps -->
[//]: # (TODO: import )


## Introduction

[//]: # (TODO: This guide will walk you through best practices for creating your GX Python environment and demonstrate how to locally install Great Expectations along with the necessary dependencies for working with $TODO$.)

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- 
- A passion for data quality

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


[//]: # (TODO: ## Next steps)




