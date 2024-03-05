---
title: Install Great Expectations 1.0
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import InstallLocal from './_install_gx/_local_installation.md';
import InstallEmrSpark from './_install_gx/_emr_spark_installation.md';
import InstallDatabricks from './_install_gx/_databricks_installation.md';
import InstallGxCloud from './_install_gx/_gx_cloud_installation.md';

Greate Expectations (GX) is the leading tool for validating and documenting your data. GX 1.0 is the open source Python library that supports this tool.

## Prerequisites

- <PrereqPythonInstalled/>
- (Recommended) [A Python virtual environment](/docs/core/installation_and_setup/set_up_a_python_environment.md#optional-create-a-virtual-environment).

## Install the GX 1.0 Python library

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
