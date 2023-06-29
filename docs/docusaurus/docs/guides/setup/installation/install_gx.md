---
sidebar_label: "Install GX"
title: "Install Great Expectations"
id: install_gx
description: Install Great Expectations locally, or in a hosted environment.
---

import Preface from './components_local/_preface.mdx'
import CheckPythonVersion from './components_local/_check_python_version.mdx'
import ChooseInstallationMethod from './components_local/_choose_installation_method.mdx'
import InstallGreatExpectations from './components_local/_install_great_expectations.mdx'
import VerifyGeInstallSucceeded from './components_local/_verify_ge_install_succeeded.mdx'
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

You can install Great Expectations (GX) locally, or in hosted environments such as Databricks, Amazon EMR, or Google Cloud Composer. Installing GX locally lets you test features and functionality to determine if it's suitable for your use case. 

:::info Windows Support

Windows support for the open source Python version of GX is currently unavailable. If you’re using GX in a Windows environment, you might experience errors or performance issues.

:::

<Tabs
  groupId="install-gx"
  defaultValue='local'
  values={[
  {label: 'Install GX locally', value:'local'},
  {label: 'Install GX in a hosted environment', value:'hosted'},
  ]}>
  <TabItem value="local">

## Local

Install Great Expectations (GX) locally.

<Preface />

### Check Python version
<CheckPythonVersion />

### Choose installation method
<ChooseInstallationMethod />

### Install GX
<InstallGreatExpectations />

### Confirm GX installation
<VerifyGeInstallSucceeded />

</TabItem>
<TabItem value="hosted">

## Hosted

Great Expectations can be deployed in environments such as Databricks, Amazon EMR, or Google Cloud Composer. These environments do not always have a file system that allows a Great Expectations installation. To install Great Expectations in a hosted environment, see one of the following guides:

- [How to Use Great Expectations in Databricks](https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_databricks)
- [How to instantiate a Data Context on an EMR Spark cluster](https://docs.greatexpectations.io/docs/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)

</TabItem>
</Tabs>
