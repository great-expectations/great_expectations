---
title: How to set up GX to work with data on AWS S3
tag: [how-to, setup]
keywords: [Great Expectations, Data Context, Filesystem, Amazon Web Services S3]

---

# How to set up Great Expectations to work with data on Amazon Web Services S3

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ## Prerequisites -->
import PrereqInstalledAwsCli from '/docs/components/prerequisites/_aws_installed_the_aws_cli.mdx'
import PrereqAwsConfiguredCredentials from '/docs/components/prerequisites/_aws_configured_your_credentials.mdx'

<!-- ### 1. Ensure your AWS CLI version is the most recent -->
import AwsVerifyInstallation from '/docs/components/setup/dependencies/_aws_verify_installation.md'

<!-- ### 2. Ensure your AWS credentials are correctly configured -->
import AwsVerifyCredentialsConfiguration from '/docs/components/setup/dependencies/_aws_verify_installation.md'

<!-- ### 3. Check your Python version -->
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'

<!-- ### 4. Create a Python virtual environment -->
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 4. Install GX with optional dependencies for S3 -->
import S3InstallDependencies from '/docs/components/setup/dependencies/_s3_install_dependencies.md'

<!-- ### 5. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'

<!-- ## Next steps -->
import LinksAfterInstallingGx from '/docs/components/setup/next_steps/_links_after_installing_gx.md'

This guide will walk you through best practices for creating your GX Python environment and demonstrate how to locally install Great Expectations along with the necessary dependencies for working with data stored in Amazon Web Services S3 storage.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip
- <PrereqInstalledAwsCli />
- <PrereqAwsConfiguredCredentials />

</Prerequisites>

## Steps

### 1. Ensure your AWS CLI version is the most recent

<AwsVerifyInstallation />

### 2. Ensure your AWS credentials are correctly configured

<AwsVerifyCredentialsConfiguration />

### 3. Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### 4. Create a Python virtual environment

<PythonCreateVenv />

### 4. Install GX with optional dependencies for S3

<S3InstallDependencies />

### 5. Verify that GX has been installed correctly

<GxVerifyInstallation />

## Next steps

Now that you have installed GX with the necessary dependencies for working with S3, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />
