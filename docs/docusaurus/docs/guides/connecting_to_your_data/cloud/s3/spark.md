---
title: How to connect to data on S3 using Spark
---

import NextSteps from '../../components/next_steps.md'
import Congratulations from '../../components/congratulations.md'
import Prerequisites from '@site/docs/components/_prerequisites.jsx'
import WhereToRunCode from '../../components/where_to_run_code.md'
import InstantiateYourProjectSDatacontext from './components_spark/_instantiate_your_projects_datacontext.md'
import ConfigureYourDatasource from './components_spark/_configure_your_datasource.md'
import SaveTheDatasourceConfigurationToYourDatacontext from './components_spark/_save_the_datasource_configuration_to_your_datacontext.md'
import TestYourNewDatasource from './components_spark/_test_your_new_datasource.md'

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on AWS S3 using Spark.
This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

## Prerequisites

<Prerequisites>

- Access to data on an AWS S3 bucket
- Access to a working Spark installation

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

<InstantiateYourProjectSDatacontext />

### 3. Configure your Datasource

<ConfigureYourDatasource />

### 4. Save the Datasource configuration to your DataContext

<SaveTheDatasourceConfigurationToYourDatacontext />

### 5. Test your new Datasource

<TestYourNewDatasource />

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [spark_s3_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py)
- [spark_s3_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py)

## Next Steps

<NextSteps />
