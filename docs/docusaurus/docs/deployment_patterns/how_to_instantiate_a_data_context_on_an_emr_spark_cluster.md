---
title: Instantiate a Data Context on an EMR Spark cluster
description: "Instantiate a Data Context on an EMR Spark cluster"
sidebar_label: "Amazon EMR Spark cluster"
sidebar_custom_props: { icon: 'img/integrations/spark_icon.png' }
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'


Use the information provided here to instantiate a Data Context on an EMR Spark cluster without a full configuration directory.

## Prerequisites

<Prerequisites>

</Prerequisites>

## Install Great Expectations on your EMR Spark cluster

- Copy this code snippet into a cell in your EMR Spark notebook and then run it:

  ```python
  sc.install_pypi_package("great_expectations")
  ```

## Configure a Data Context in code

1. Create an in-code Data Context. See [Instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context).

2. Copy the Python code at the end of **How to instantiate an Ephemeral Data Context** into a cell in your EMR Spark notebook, or use the other examples to customize your configuration. The code instantiates and configures a Data Context for an EMR Spark cluster.

## Test your configuration

1. Execute the cell with the snippet you copied in the previous step.

2. Copy the code snippet into a cell in your EMR Spark notebook.

3. Run the following command to verify that an error isn't returned:

  ```python
      context.list_datasources()
   ```

