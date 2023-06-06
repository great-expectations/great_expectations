---
title: How to set up GX to work with data on GCS
tag: [how-to, setup]
keywords: [Great Expectations, Data Context, Filesystem, GCS, Google Cloud Storage]

---

# How to set up Great Expectations to work with data on Google Cloud Storage

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ## Prerequisites -->
import PrereqGcpServiceAccount from '/docs/components/prerequisites/_gcp_service_account.md'

<!-- ### 1. Ensure your AWS CLI version is the most recent -->
import GcpVerifyCredentials from '/docs/components/setup/dependencies/_gcp_verify_credentials_configuration.md'

<!-- ### 2. Ensure your AWS credentials are correctly configured -->
import AwsVerifyCredentialsConfiguration from '/docs/components/setup/dependencies/_aws_verify_installation.md'

<!-- ### 3. Check your Python version -->
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'

<!-- ### 4. Create a Python virtual environment -->
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 5. Install GX with optional dependencies for GCS -->
import GcpInstallDependencies from '/docs/components/setup/dependencies/_gcp_install_dependencies.md'

<!-- ### 6. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'

<!-- ## Next steps -->
import LinksAfterInstallingGx from '/docs/components/setup/next_steps/_links_after_installing_gx.md'

This guide will walk you through best practices for creating your GX Python environment and demonstrate how to locally install Great Expectations along with the necessary dependencies for working with data stored on Google Cloud Storage.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqGcpServiceAccount />

</Prerequisites>

## Steps

### 1. Ensure your GCP credentials are correctly configured

<GcpVerifyCredentials />

### 2. Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### 3. Create a Python virtual environment

<PythonCreateVenv />

### 4. Install GX with optional dependencies for GCS

<GcpInstallDependencies />

### 5. Verify that GX has been installed correctly

<GxVerifyInstallation />

## Next steps

Now that you have installed GX with the necessary dependencies for working with GCS, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />

