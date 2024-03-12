---
title: Install and set up Google Cloud Storage support
---

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';

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


## Next steps

- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)
