---
title: Install GX
toc_max_heading_level: 2
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import GxData from '../_core_components/_data.jsx'
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import InstallLocal from './_install_gx/_local_installation.md';
import InstallEmrSpark from './_install_gx/_emr_spark_installation.md';
import InstallDatabricks from './_install_gx/_databricks_installation.md';
import InstallGxCloud from './_install_gx/_gx_cloud_installation.md';

import PythonVersion from '../_core_components/_python_version.md';

GX Core is a Python library.  Follow the instructions in this guide to install GX in your local Python environment, or as a notebook-scoped library in hosted environments such as Databricks or EMR Spark clusters.

## Prerequisites

- <PrereqPythonInstalled/>
- Recommended. [A Python virtual environment](/core/set_up_a_gx_environment/install_python.md#optional-create-a-virtual-environment)
- Internet access
- Permissions to download and install packages in your environment

## Install the GX Python library

<Tabs queryString="install-location" groupId="install-location" defaultValue='local' values={[{label: 'Local', value:'local'}, {label: 'Hosted environment', value:'hosted'}, {label: 'GX Cloud', value:'gx-cloud'}]}>

  <TabItem value="local" label="Local">
<InstallLocal/>
  </TabItem>

  <TabItem value="hosted" label="Hosted">

Hosted environments such as EMR Spark or Databricks clusters do not provide a filesystem to install your GX instance.  Instead, you must install GX in memory using the Python-style notebooks available on those platforms.

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

