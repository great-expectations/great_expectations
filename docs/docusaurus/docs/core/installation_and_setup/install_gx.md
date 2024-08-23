---
title: Install GX Core
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import InstallLocal from './_install_gx/_local_installation.md';
import InstallEmrSpark from './_install_gx/_emr_spark_installation.md';
import InstallDatabricks from './_install_gx/_databricks_installation.md';
import InstallGxCloud from './_install_gx/_gx_cloud_installation.md';

import GxData from '../../components/_data.jsx';
import PythonVersion from '../_core_components/_python_version.md';

To use GX Core, you need to install Python and the GX Core Python library. GX also recommends you set up a virtual environment for your GX Python projects.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](/core/installation_and_setup/set_up_a_python_environment.mdx#optional-create-a-virtual-environment)
- Internet access
- Permissions to download and install packages in your environment

## Install Python

1. Reference the [official Python documentation](https://www.python.org/) to install an appropriate version of Python.

  GX Requires <PythonVersion/>, which can be found on the [official Python downloads page](https://www.python.org/downloads/).


2. Verify your Python installation.

  Run the following command to display your Python version:

  ```shell title="Terminal input"
  python --version
  ```

  You should receive a response similar to:

  ```shell title="Terminal output"
  Python 3.8.6
  ```

## (Optional) Create a virtual environment

  Although it is an optional step the best practice when working with a Python project is to do so in a virtual environment.  A virtual environment ensures that any libraries and dependencies that you install as part of your project do not encounter or cause conflicts with libraries and dependencies installed for other Python projects.

  There are various tools such as virtualenv and pyenv which can be used to create a virtual environment.  This example uses `venv` because it is included with Python 3.

1. Create the virtual environment with `venv`.

  To create your virtual environment, run the following code from the folder the environment should reside in:

  ```shell title="Terminal input"
  python -m venv my_venv
  ```

  This command creates a new directory named `my_venv` which will contain your virtual environment.

  :::tip Virtual environment names

  If you wish to use a different name for your virtual environment, replace `my_venv` in the example with the name you would prefer.  You will also have to replace `my_venv` with your virtual environment's actual name in any other example code that includes `my_venv`.

  :::

2. (Optional) Test your virtual environment by activating it.

  Activate your virtual environment by running the following command from the folder it was installed in:

  ```shell title="Terminal input"
  source my_venv/bin/activate
  ```

## Install the GX Core Python library

<Tabs queryString="install-location" groupId="install-location" defaultValue='local' values={[{label: 'Local', value:'local'}, {label: 'Hosted environment', value:'hosted'}, {label: 'GX Cloud', value:'gx-cloud'}]}>

  <TabItem value="local" label="Local">
<InstallLocal/>
  </TabItem>


  <TabItem value="hosted" label="Hosted">

Hosted environments such as EMR Spark clusters or Databricks clusters do not provide for a filesystem where you can install your GX instance.  Instead, you must install GX in-memory using the Python-style notebooks available on those platforms.

<Tabs queryString="hosted-type" groupId="hosted-type" defaultValue='spark-notebook' values={[{label: 'EMR Spark notebook', value:'spark-notebook'}, {label: 'Databricks notebook', value:'databricks-notebook'}]}>

  <TabItem value="spark-notebook">
<InstallEmrSpark/>
  </TabItem>

  <TabItem value="databricks-notebook">
<InstallDatabricks/>
  </TabItem>

</Tabs>

  </TabItem>

  <TabItem value="gx-cloud" label="GX Cloud">
<InstallGxCloud/>
  </TabItem>

</Tabs>


## Next steps

- [Install additional dependencies](/core/installation_and_setup/additional_dependencies/additional_dependencies.md)
- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)
