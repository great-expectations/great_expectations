---
title: How to Use Great Expectations with Airflow
---
import Prerequisites from '../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you run a Great Expectations checkpoint in Apache Airflow, which allows you to trigger validation of a data asset using an Expectation Suite directly within an Airflow DAG.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../tutorials/getting_started/intro.md)
- [Created an Expectation Suite](../tutorials/getting_started/create_your_first_expectations.md)
- [Created a checkpoint for that Expectation Suite and a data asset](../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md)
- Created an Airflow DAG file

</Prerequisites>

Airflow is a data orchestration tool for creating and maintaining data pipelines through DAGs (directed acyclic graphs) written in Python. DAGs complete work through operators, which are templates that each encapsulate a specific type of work. This document explains how to use the `GreatExpectationsOperator` to perform data quality work in an Airflow DAG.

> *This guide focuses on using Great Expectations with Airflow in a self-hosted environment. See [here](https://www.astronomer.io/guides/airflow-great-expectations) for the guide on using Great Expectations with Airflow from within Astronomer.*

Before you start writing your DAG, you will want to make sure you have a Data Context and Checkpoint configured.

A [Data Context](https://docs.greatexpectations.io/docs/reference/data_context) represents a Great Expectations project. It organizes storage and access for Expectation Suites, Datasources, notification settings, and data fixtures.

[Checkpoints](https://docs.greatexpectations.io/docs/reference/checkpoints_and_actions) provide a convenient abstraction for bundling the validation of a Batch (or Batches) of data against an Expectation Suite (or several), as well as the actions that should be taken after the validation.

## Install the `GreatExpectationsOperator`

To import the GreatExpectationsOperator in your Airflow project, run the following command to install the Great Expectations provider in your Airflow environment:

```
pip install airflow-provider-great-expectations==0.1.1
```

It’s recommended to specify a version when installing the package. To make use of the latest Great Expectations V3 API, you need to specify a version >= `0.1.0`.

> *The Great Expectations V3 API requires Airflow 2.1+. If you're still running Airflow 1.x, you need to upgrade to at least 2.1 before using v0.1.0+ of the GreatExpectationsOperator.*


## Using the `GreatExpectationsOperator`

Before you can use the `GreatExpectationsOperator`, you need to import it in your DAG. You may also need to import the `DataContextConfig`, `CheckpointConfig`, or `BatchRequest` classes as well, depending on how you're using the operator. To import the Great Expectations provider and config and batch classes in a given DAG, add the following line to the top of the DAG file in your `dags` directory:

```python
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)
```

To use the operator in the DAG, define an instance of the `GreatExpectationsOperator` class and assign it to a variable. In the following example, we define two different instances of the operator to complete two different steps in a data quality check workflow:

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

Once you define your work through operators, you need to define the order in which your DAG completes the work. To do this, you can define a [relationship](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships). For example, adding the following line to your DAG ensures that your name pass task has to complete before your config pass task can start:

```python
ge_data_context_root_dir_with_checkpoint_name_pass >> ge_data_context_config_with_checkpoint_config_pass
```

### Operator Parameters

The operator has several optional parameters, but it always requires either a `data_context_root_dir` or a `data_context_config` and either a `checkpoint_name` or `checkpoint_config`.

The `data_context_root_dir` should point to the `great_expectations` project directory generated when you created the project with the CLI. If using an in-memory `data_context_config`, a `DataContextConfig` must be defined, as in [this example](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/include/great_expectations/object_configs/example_data_context_config.py).

A `checkpoint_name` references a checkpoint in the project CheckpointStore defined in the DataContext (which is often the `great_expectations/checkpoints/` path), so that a `checkpoint_name = "taxi.pass.chk"` would reference the file `great_expectations/checkpoints/taxi/pass/chk.yml`. With a `checkpoint_name`, `checkpoint_kwargs` may be passed to the operator to specify additional, overwriting configurations. A `checkpoint_config` may be passed to the operator in place of a name, and can be defined like [this example](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/include/great_expectations/object_configs/example_checkpoint_config.py).

For a full list of parameters, see the `GreatExpectationsOperator` [documentation](https://registry.astronomer.io/providers/great-expectations/modules/greatexpectationsoperator).

### Connections and Backends

The `GreatExpectationsOperator` can run a checkpoint on a dataset stored in any backend compatible with Great Expectations. All that’s needed to get the Operator to point at an external dataset is to set up an [Airflow Connection](https://www.astronomer.io/guides/connections) to the datasource, and add the connection to your Great Expectations project, e.g. [using the CLI to add a Postgres backend](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/postgres). Then, if using a `DataContextConfig` or `CheckpointConfig`, ensure that the `"datasources"` field refers to your backend connection name.
