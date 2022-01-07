---
title: How to run a Checkpoint in Airflow
---
import Prerequisites from '../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you run a Great Expectations checkpoint in Apache Airflow, which allows you to trigger validation of a data asset using an Expectation Suite directly within an Airflow DAG.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../tutorials/getting_started/intro.md)
- [Created an Expectation Suite](../tutorials/getting_started/create_your_first_expectations.md)
- [Created a checkpoint for that Expectation Suite and a data asset](../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md)
- Created an Airflow DAG file

</Prerequisites>

Airflow is a data orchestration tool focused on the easy creation and maintenance of data pipelines through DAGs (directed acyclic graphs) written in Python. The base unit of work done in a DAG is the Operator, and this document shows how to use the `GreatExpectationsOperator` to perform data quality checks in the context of an ELT pipeline.

While Airflow alone is a powerful tool, [Astronomer](https://www.astronomer.io/) provides a CLI and cloud platform to bring your DAGs to the next level by providing simple and efficient ways to create Airflow projects, author and publish DAGs, and easily maintain the Kubernetes engine powering DAG runs. To get started quickly with Astronomer’s CLI for use in the steps below, check out the [Astronomer CLI quick start guide](https://docs.astronomer.io/enterprise/cli-quickstart/).

## Background

Importing providers into Airflow is an easy two-step process. The first step is either `pip install`-ing the Great Expectations provider in your Airflow environment or adding the provider `airflow-provider-great-expectations` to your `requirements.txt` file in your Airflow project. This file is generated for you if you are using the Astronomer CLI and run `astro dev init`, and the provider is installed on to your Docker image when spinning up Airflow.

It’s recommended to specify a version in the `requirements.txt` file. To make use of the latest Great Expectations V3 API, you’ll need to specify a version `>=0.1.0`, and if you’re using Astronomer, you’ll need version `>=0.1.1`. In either case, the Great Expectations V3 API provider requires Airflow 2, so if you’re still running Airflow 1.X, upgrade that first.

## Using the Operator

The second step to use the operator is to import it into the DAG. The DAG file is a python file in the `dags/` folder of your Airflow project. To import the Great Expectations provider, add the following line at the top of your DAG file:

`from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator`

Using the operator in a DAG is as simple as defining an instance of the `GreatExpectationsOperator` class and assigning it to a variable, like so:

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

The above code creates two Great Expectations operators assigned to two distinct variables. Once defined, the operators must be put into the DAG, like so:

```python
ge_data_context_root_dir_with_checkpoint_name_pass >> ge_data_context_config_with_checkpoint_config_pass
```

The above code defines the DAG as two operations, the first being `ge_data_context_root_dir_with_checkpoint_name_pass` and the second `ge_data_context_config_with_checkpoint_config_pass`, which will run sequentially in that order.

The operator has several optional arguments, but it always requires either a `data_context_root_dir` or a `data_context_config` and either a `checkpoint_name` or `checkpoint_config`.

The `data_context_root_dir` should point to the `great_expectations` project directory previously generated. When running an Astronomer deployment, it is recommended that this directory is under the `include/` directory, which is automatically generated when creating an Astronomer project. When using a `data_context_config`, the `DataContextConfig` object must also be imported into the project file using:
`from great_expectations.data_context.types.base import DataContextConfig`

An example `DataContextConfig` object, with a `great_expectations` project folder in the `include/` directory, looks like:

```python
base_path = Path(__file__).parents[2]
data_dir = os.path.join(base_path, "include", "data")
ge_root_dir = os.path.join(base_path, "include", "great_expectations")

data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_datasource": {
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "default_inferred_data_connector_name": {
                        "default_regex": {
                            "group_names": ["data_asset_name"],
                            "pattern": "(.*)",
                        },
                        "base_directory": data_dir,
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
        "config_variables_file_path": os.path.join(
            ge_root_dir, "uncommitted", "config_variables.yml"
        ),
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "expectations"),
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(
                        ge_root_dir, "uncommitted", "validations"
                    ),
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory": os.path.join(ge_root_dir, "checkpoints"),
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
                    "base_directory": os.path.join(
                        ge_root_dir, "uncommitted", "data_docs", "local_site"
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
```

A `checkpoint_name` references a checkpoint in a series of subdirectories relative to the module (which is often the `great_expectations/checkpoints/` path), so that a `checkpoint_name = "taxi.pass.chk"`would reference the file `great_expectations/checkpoints/taxi/pass/chk.yml`. With a `checkpoint_name`, `checkpoint_kwargs` may be passed to the operator to specify additional, overwriting configurations. A `checkpoint_config` may be passed to the operator in place of a name. An example `checkpoint_config` is provided here:

```python
checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "taxi.demo",
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": "yellow_tripdata_sample_2019-01.csv",
                    "data_connector_query": {"index": -1},
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)
```

The `DataContextConfig` and  `CheckpointConfig` objects may be defined in the DAG file, but to avoid excess top-level code, it is recommended these objects be defined in a config file in the `include/` directory and imported into the DAG file.

For a full list of parameters, see the `GreatExpectationsOperator` [documentation](https://registry.astronomer.io/providers/great-expectations/modules/greatexpectationsoperator). Additional [Great Expectations example DAGs](https://registry.astronomer.io/dags?providers=Great+Expectations&page=1) may be found on the [Astronomer Registry](https://registry.astronomer.io/).

## Connections

The `GreatExpectationsOperator` can run a checkpoint on a dataset stored in any backend compatible with Great Expectations. All that’s needed to get the Operator to point at an external dataset is to set up an [Airflow Connection](https://www.astronomer.io/guides/connections) to the datasource, and add the connection to your Great Expectations project, e.g. [using the CLI to add an Athena backend](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/athena). Then, make sure your data context being used is the external data source. For example, the `DataContextConfig` above would be changed from having `"my_datasource"`:

```python
data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_datasource": {
				...
		...
)
```

to having `"my_backend_datasource"`:

```python
data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_backend_datasource": {
				...
		...
)
```

where the “backend” is one of the supported external data backends.

A similar change would be made if using a `CheckpointConfig`, where `datasource_name` under `batch_request`:

```python
checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
				...
				"validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
				...
		...
)
```

would become:

```python
checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
				...
				"validations": [
            {
                "batch_request": {
                    "datasource_name": "my_backend_datasource",
				...
		...
)
```
