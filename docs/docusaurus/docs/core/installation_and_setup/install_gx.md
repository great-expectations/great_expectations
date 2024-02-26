---
title: Install Great Expectations 1.0
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import InProgress from '../_core_components/_in_progress.md';

Greate Expectations (GX) is the leading tool for validating and documenting your data. GX 1.0 is the open source Python library that supports this tool.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](./set_up_a_python_environment#optional-create-a-virtual-environment).

## Install the GX 1.0 Python library

<Tabs queryString="install-location" groupId="install-location" defaultValue='local' values={[{label: 'Local', value:'local'}, {label: 'EMR Spark notebook', value:'spark-notebook'}]}>

  <TabItem value="local" label="Local">

<InProgress/>

1. (Optional) Activate your virtual environment.

  If you created a virtual environment for your GX Python installation, navigate to the folder that contains your virtual environment and activate it by running the following command:

  ```shell title="Terminal input"
  source my_venv/bin/activate
  ```

2. Ensure you have the latest version of `pip`:

  ```shell title="Terminal input"
  python -m ensurepip --upgrade
  ```

3. Install the GX 1.0 library:

  ```shell terminal="Terminal input"
  pip install great_expectations
  ```

4. Verify your GX installation:


  </TabItem>

  <TabItem value="spark-notebook" label="EMR Spark notebook">

<InProgress/>

  </TabItem>

</Tabs>

## Next steps
- [Install additional dependencies](/core/installation_and_setup/additional_dependencies/additional_dependencies.md)
- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)
