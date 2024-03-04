---
title: Install Great Expectations 1.0
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import ReleaseVersionBox from '../../components/versions/_gx_version_code_box.mdx'

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';

import AwsS3Support from './additional_dependencies/amazon_s3.md'

import InProgress from '../_core_components/_in_progress.md';

Greate Expectations (GX) is the leading tool for validating and documenting your data. GX 1.0 is the open source Python library that supports this tool.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](./set_up_a_python_environment#optional-create-a-virtual-environment).

## Install the GX 1.0 Python library

<Tabs queryString="install-location" groupId="install-location" defaultValue='local' values={[{label: 'Local', value:'local'}, {label: 'EMR Spark notebook', value:'spark-notebook'}, {label: 'GX Cloud', value:'gx-cloud'}]}>

  <TabItem value="local" label="Local">

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

4. Verify that GX installed successfully with the CLI command:

  ```bash title="Terminal input"
  great_expectations --version
  ```

  The output you receive if GX was successfully installed will be:

  <ReleaseVersionBox/>


  </TabItem>

  <TabItem value="spark-notebook" label="EMR Spark notebook">

<InProgress/>

  </TabItem>

  <TabItem value="gx-cloud" label="GX Cloud">

GX Cloud provides a web interface for using GX to validate your data without creating and running complex Python code.  However, GX Core is also capable of connecting to a GX Cloud account should you wish to engage in further customization or automation with Python scripts.

To deploy a GX Agent, which will run the jobs sent from the GX Cloud interface, see [Connect GX Cloud](/cloud/connect/connect_lp.md).

To connect to GX Cloud from a Python script utilizing a [local installation of GX 1.0](/core/installation_and_setup/install_gx.md?install-location=local), see [Connect to an existing Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=gx_cloud#connect-to-an-existing-data-context).



  </TabItem>

</Tabs>


## Next steps
- [Install additional dependencies](/core/installation_and_setup/additional_dependencies/additional_dependencies.md)
- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)
