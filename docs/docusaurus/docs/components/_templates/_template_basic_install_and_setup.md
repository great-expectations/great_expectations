---
[//]: # (TODO: title: How to set up GX to work with)
tag: [how-to, setup]
keywords: [Great Expectations]

---

[//]: # (TODO: # How to set up Great Expectations to work with)

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ## Prerequisites -->

<!-- ### 1. Check your Python version -->
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'

<!-- ### 2. Create a Python virtual environment -->
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 3. Install GX with optional dependencies for ABS -->
[//]: # (TODO: import InstallDependencies from 'README.md')

<!-- ### 4. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'




<!-- ## Next steps -->
import AbsFurtherConfiguration from '/docs/components/setup/next_steps/_links_for_adding_azure_blob_storage_configurations_to_data_context.md'

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

### 3. Install GX with optional dependencies for Azure Blob Storage

<AbsInstallDependencies />

### 4. Verify that GX has been installed correctly

<GxVerifyInstallation />



[//]: # (TODO: ## Next steps)




