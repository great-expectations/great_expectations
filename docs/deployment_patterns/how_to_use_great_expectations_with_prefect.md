---
title: How to Use Great Expectations with Prefect
---

import Prerequisites from './components/deployment_pattern_prerequisites.jsx'

This guide will help you run a Great Expectations with [Prefect](https://prefect.io/)

<Prerequisites>

- [Set up a working deployment of Great Expectations](../tutorials/getting_started/intro.md)
- [Created an Expectation Suite](../tutorials/getting_started/create_your_first_expectations.md)
- [Connecting to Data](../tutorials/getting_started/connect_to_data.md)
- [Prefect Quick Start guide](https://docs.prefect.io/core/getting_started/quick-start.html)

</Prerequisites>

[Prefect](https://prefect.io/) is a workflow management system that enables data engineers to build robust data applications. [The Prefect open source library](https://www.prefect.io/opensource/) allows users to create workflows using Python and makes it easy to take your data pipelines and add semantics like retries, logging, dynamic mapping, caching, and failure notifications. [Prefect Cloud](https://www.prefect.io/cloud/) is the easy, powerful, scalable way to automate and monitor dataflows built in Prefect 1.0 — without having to worry about orchestration infrastructure.

Great Expectations validations can be used to validate data passed between tasks in your Prefect flow. By validating your data before operating on it, you can quickly find issues with your data with less debugging. Prefect makes it easy to combine Great Expectations with other services in your data stack and orchestrate them all in a predictable manner.

## The `RunGreatExpectationsValidation` task

With Prefect, you define your workflows with [tasks](https://docs.prefect.io/core/concepts/tasks.html) and [flows](https://docs.prefect.io/core/concepts/flows.html). A `Task` represents a discrete action in a Prefect workflow. A `Flow` is a container for `Tasks`. It represents an entire workflow or application by describing the dependencies between tasks. Prefect offers a suite of over 180 pre-built tasks in the [Prefect Task Library](https://docs.prefect.io/core/task_library/overview.html). The [`RunGreatExpectationsValidation`](https://docs.prefect.io/api/latest/tasks/great_expectations.html) task is one of these pre-built tasks. With the `RunGreatExpectationsValidation` task you can run validations for an existing Great Expectations project.

To use the `RunGreatExpectationsValidation`, you need to install Prefect with the `ge` extra:

```bash
pip install "prefect[ge]"
```

Here is an example of a flow that runs a Great Expectations validation:

```python
from prefect import Flow, Parameter
from prefect.tasks.great_expectations import RunGreatExpectationsValidation

validation_task = RunGreatExpectationsValidation()

with Flow("ge_test") as flow:
   checkpoint_name = Parameter("checkpoint_name")
   prev_run_row_count = 100
   validation_task(
      checkpoint_name=checkpoint_name,
      evaluation_parameters=dict(prev_run_row_count=prev_run_row_count),
   )

flow.run(parameters={"checkpoint_name": "my_checkpoint"})
```

Using the `RunGreatExpectationsValidation` task is as easy as importing the task, instantiating the task, and calling it in your flow. In the flow above, we parameterize our flow with the checkpoint name. This way, we're able to reuse our flow to run different Great Expectations validations based on the input.

## Configuring the root context directory

By default, the `RunGreatExpectationsValidation` task will look in the current directory for a Great Expectations project in a folder named `great_expectations`. If your `great_expectations.yml` is located in another directory, you can configure the `RunGreatExpectationsValidation` tasks with the `context_root_dir` argument:

```python
from prefect import Flow, Parameter
from prefect.tasks.great_expectations import RunGreatExpectationsValidation

validation_task = RunGreatExpectationsValidation()

with Flow("ge_test") as flow:
   checkpoint_name = Parameter("checkpoint_name")
   prev_run_row_count = 100
   validation_task(
      checkpoint_name=checkpoint_name,
      evaluation_parameters=dict(prev_run_row_count=prev_run_row_count),
      context_root_dir="../great_expectations"
   )

flow.run(parameters={"checkpoint_name": "my_checkpoint"})
```

## Using dynamic runtime configuration

The `RunGreatExpectationsValidation` task also enables runtime configuration of your validation run. You can pass in an in memory `DataContext` via the `context` argument or pass an in memory `Checkpoint` via the `ge_checkpoint` argument.

Here is an example with an in memory `DataContext`:

```python
import os
from pathlib import Path

import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
)
from prefect import Flow, Parameter, task
from prefect.tasks.great_expectations import RunGreatExpectationsValidation

@task
def create_in_memory_data_context(project_path: Path, data_path: Path):
    data_context = BaseDataContext(
        project_config=DataContextConfig(
            **{
                "config_version": 3.0,
                "datasources": {
                    "data__dir": {
                        "module_name": "great_expectations.datasource",
                        "data_connectors": {
                            "data__dir_example_data_connector": {
                                "default_regex": {
                                    "group_names": ["data_asset_name"],
                                    "pattern": "(.*)",
                                },
                                "base_directory": str(data_path),
                                "module_name": "great_expectations.datasource.data_connector",
                                "class_name": "InferredAssetFilesystemDataConnector",
                            },
                            "default_runtime_data_connector_name": {
                                "batch_identifiers": ["default_identifier_name"],
                                "module_name": "great_expectations.datasource.data_connector",
                                "class_name": "RuntimeDataConnector",
                            },
                        },
                        "execution_engine": {
                            "module_name": "great_expectations.execution_engine",
                            "class_name": "PandasExecutionEngine",
                        },
                        "class_name": "Datasource",
                    }
                },
                "config_variables_file_path": str(
                    project_path / "uncommitted" / "config_variables.yml"
                ),
                "stores": {
                    "expectations_store": {
                        "class_name": "ExpectationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(
                                project_path / "expectations"
                            ),
                        },
                    },
                    "validations_store": {
                        "class_name": "ValidationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(
                                project_path / "uncommitted" / "validations"
                            ),
                        },
                    },
                    "evaluation_parameter_store": {
                        "class_name": "EvaluationParameterStore"
                    },
                    "checkpoint_store": {
                        "class_name": "CheckpointStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "suppress_store_backend_id": True,
                            "base_directory": str(
                                project_path / "checkpoints"
                            ),
                        },
                    },
                },
                "expectations_store_name": "expectations_store",
                "validations_store_name": "validations_store",
                "evaluation_parameter_store_name": "evaluation_parameter_store",
                "checkpoint_store_name": "checkpoint_store",
                "data_docs_sites": {
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "show_how_to_buttons": True,
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(
                                project_path / "uncommitted" / "data_docs" / "local_site"
                            ),
                        },
                        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                    }
                },
                "anonymous_usage_statistics": {
                    "data_context_id": "abcdabcd-1111-2222-3333-abcdabcdabcd",
                    "enabled": False,
                },
                "notebooks": None,
                "concurrency": {"enabled": False},
            }
        )
    )

    return data_context

validation_task = RunGreatExpectationsValidation()

with Flow("ge_test") as flow:
   checkpoint_name = Parameter("checkpoint_name")
   prev_run_row_count = 100
   data_context = create_in_memory_data_context(project_path=Path.cwd(), data_path=Path.cwd().parent)
   validation_task(
      checkpoint_name=checkpoint_name,
      evaluation_parameters=dict(prev_run_row_count=prev_run_row_count),
      context=data_context
   )

flow.run(parameters={"checkpoint_name": "my_checkpoint"})
```

## Validating in memory data

Because Prefect allows first class passing of data between tasks, you can even use the `RunGreatExpectationsValidation` task on in memory dataframes! This means you won't need to write to and read data from remote storage between steps of your pipeline.

Here is an example of how to run a validation on an in memory dataframe by passing in a `RuntimeBatchRequest` via the `checkpoint_kwargs` argument:

```python
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
from prefect import Flow, Parameter, task
from prefect.tasks.great_expectations import RunGreatExpectationsValidation

validation_task = RunGreatExpectationsValidation()

@task
def create_runtime_batch_request(df: pd.DataFrame):
   return RuntimeBatchRequest(
        datasource_name="data__dir",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2019-02_df",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "default_identifier_name": "ingestion step 1",
        },
    )

with Flow("ge_test") as flow:
   checkpoint_name = Parameter("checkpoint_name")
   prev_run_row_count = 100

   df = dataframe_creation_task()

   in_memory_runtime_batch_request = create_runtime_batch_request(df)

   validation_task(
      checkpoint_name=checkpoint_name,
      evaluation_parameters=dict(prev_run_row_count=prev_run_row_count),
      checkpoint_kwargs={
         "validations": [
            {
               "batch_request": in_memory_runtime_batch_request,
               "expectation_suite_name": "taxi.demo_pass",
            }
         ]
      },
   )

flow.run(parameters={"checkpoint_name": "my_checkpoint"})
```

## Where to go for more information

The flexibility that Prefect and the `RunGreatExpectationsValidation` task offer makes it easy to incorporate data validation into your dataflows with Great Expectations. 

For more info about the `RunGreatExpectationsValidation` task, refer to the (Prefect documentation)[https://docs.prefect.io/api/latest/tasks/great_expectations.html#rungreatexpectationsvalidation].
