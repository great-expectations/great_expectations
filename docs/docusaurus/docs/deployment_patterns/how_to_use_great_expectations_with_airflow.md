---
title: How to Use Great Expectations with Airflow
sidebar_label: "Airflow"
description: "Run a Great Expectations checkpoint in Apache Airflow"
id: how_to_use_great_expectations_with_airflow
sidebar_custom_props: { icon: 'img/integrations/airflow_icon.png' }
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'

Learn how to run a Great Expectations checkpoint in Apache Airflow, and how to use an Expectation Suite within an Airflow directed acyclic graphs (DAG) to trigger a data asset validation.

Airflow is a data orchestration tool for creating and maintaining data pipelines through DAGs written in Python. DAGs complete work through operators, which are templates that encapsulate a specific type of work. This document explains how to use the `GreatExpectationsOperator` to perform data quality work in an Airflow DAG.

Before you create your DAG, make sure you have a Data Context and Checkpoint configured. A [Data Context](https://docs.greatexpectations.io/docs/terms/data_context) represents a Great Expectations project. It organizes storage and access for Expectation Suites, Datasources, notification settings, and data fixtures.  [Checkpoints](https://docs.greatexpectations.io/docs/terms/checkpoint) provide a convenient abstraction for bundling the validation of a Batch (or Batches) of data against an Expectation Suite (or several), as well as the actions that should be taken after the validation.

This guide focuses on using Great Expectations with Airflow in a self-hosted environment. To use Great Expectations with Airflow within Astronomer, see [Orchestrate Great Expectations with Airflow](https://www.astronomer.io/guides/airflow-great-expectations).

## Prerequisites

<Prerequisites>

- [Set up a working deployment of Great Expectations](/docs/guides/setup/setup_overview)
- [Created an Expectation Suite](/docs/guides/expectations/create_expectations_overview)
- [Created a checkpoint for that Expectation Suite and a data asset](../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md)
- An Airflow DAG

</Prerequisites>

## Install the GreatExpectationsOperator

Run the following command to install the Great Expectations provider in your Airflow environment:

```
pip install airflow-provider-great-expectations==0.1.1
```

GX recommends specifying a version when installing the package. To make use of the latest Great Expectations provider for Airflow, specify version 0.1.0 or later.

The current Great Expectations release requires Airflow 2.1 or later. If you're still running Airflow 1.x, you need to upgrade to 2.1 or later before using the `GreatExpectationsOperator`.


## Use the GreatExpectationsOperator

Before you can use the `GreatExpectationsOperator`, you need to import it into your DAG. Depending on how you're using the operator, you might need to import the `DataContextConfig`, `CheckpointConfig`, or `BatchRequest` classes. To import the Great Expectations provider and config and batch classes in a given DAG, add the following line to the top of the DAG file in your `dags` directory:

```python
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)
```

To use the operator in the DAG, define an instance of the `GreatExpectationsOperator` class and assign it to a variable. In the following example, two different instances of the operator are defined to complete two different steps in a data quality check workflow:

```python
ge_data_context_root_dir_with_checkpoint_name_pass = GreatExpectationsOperator(
    task_id="ge_data_context_root_dir_with_checkpoint_name_pass",
    data_context_root_dir=ge_root_dir,
    checkpoint_name="taxi.pass.chk",
)

ge_data_context_config_with_checkpoint_config_pass = GreatExpectationsOperator(
    task_id="ge_data_context_config_with_checkpoint_config_pass",
    data_context_config=example_data_context_config,
    checkpoint_config=example_checkpoint_config,
)
```

After you define your work with operators, you define a [relationship](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships) to specify the order that your DAG completes the work. For example, adding the following code to your DAG ensures that your `name pass` task has to complete before your `config pass` task can start:

```python
ge_data_context_root_dir_with_checkpoint_name_pass >> ge_data_context_config_with_checkpoint_config_pass
```

### Operator parameters

The operator has several optional parameters, but it always requires a `data_context_root_dir` or a `data_context_config` and a `checkpoint_name` or `checkpoint_config`.

The `data_context_root_dir` should point to the `great_expectations` project directory that was generated when you created the project. If you're using an in-memory `data_context_config`, a `DataContextConfig` must be defined. See [this example](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/include/great_expectations/object_configs/example_data_context_config.py).

A `checkpoint_name` references a checkpoint in the project CheckpointStore defined in the DataContext (which is often the `great_expectations/checkpoints/` path), so that a `checkpoint_name = "taxi.pass.chk"` would reference the file `great_expectations/checkpoints/taxi/pass/chk.yml`. With a `checkpoint_name`, `checkpoint_kwargs` can be passed to the operator to specify additional, overwriting configurations. A `checkpoint_config` can be passed to the operator in place of a name, and is defined like [this example](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/include/great_expectations/object_configs/example_checkpoint_config.py).

For a full list of parameters, see [GreatExpectationsOperator](https://registry.astronomer.io/providers/airflow-provider-great-expectations/versions/0.2.6/modules/GreatExpectationsOperator).

### Connections and backends

The `GreatExpectationsOperator` can run a checkpoint on a dataset stored in any backend that is compatible with Great Expectations. All that’s needed to get the Operator to point to an external dataset is to set up an [Airflow Connection](https://www.astronomer.io/guides/connections) to the Datasource, and adding the connection to your Great Expectations project. If you're using a `DataContextConfig` or `CheckpointConfig`, ensure that the `"datasources"` field references your backend connection name.
