import logging
import os
import shutil
from typing import List

import nbformat
import pytest
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor
from nbformat import NotebookNode

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions.exceptions import GreatExpectationsError

logger = logging.getLogger(__name__)

# constants used by the sql example
CONNECTION_STRING: str = "postgresql+psycopg2://postgres:@localhost/test_ci"
table_name: str = "yellow_tripdata_sample_2020"


def clean_up_test_files(base_dir: str, paths: List[str]) -> None:
    """
    Helper method to clean-up the files created by tests
    Args:
        base_dir str: tmp path created by the test
        paths List(str): paths or directories to delete
    """
    for path in paths:
        full_path: str = os.path.join(base_dir, path)
        if not os.path.exists(full_path):
            continue
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)
        else:
            os.remove(full_path)


def test_run_rbp_notebook(tmp_path):
    """
    What does this test and why?

    One of the resources we have for RuleBaseProfiler is a Jupyter notebook that explains/shows the components in code.

    This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.

    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "BasicExample_RBP_Instantiation_and_running.ipynb"
    )

    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "BasicExample_RBP_Instantiation_and_running_executed.ipynb"
    )

    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=30, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_data_assistants_notebook(tmp_path):
    """
    What does this test and why?

    One of the resources we have for DataAssistants is a Jupyter notebook that explains/shows the components in code.

    This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "DataAssistants_Instantiation_And_Running.ipynb"
    )
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "DataAssistants_Instantiation_and_running_executed.ipynb"
    )

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=30, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/taxi_data_suite.json",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_self_initializing_expectations_notebook(tmp_path):
    """
    What does this test and why?
    One of the resources we have for self-initializing Expectations is a Jupyter notebook that explains/shows the
    components in code.  This test ensures the codepaths and examples described in the Notebook actually run and pass,
    nbconvert's `preprocess` function.
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(base_dir, "SelfInitializingExpectations.ipynb")
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "SelfInitializingExpectations_executed.ipynb"
    )

    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=30, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/new_expectation_suite.json",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_multibatch_inferred_asset_example_spark(tmp_path, spark_session):
    """
    What does this test and why?
    One of the resources we have for multi-batch Expectations is a Jupyter notebook that explains/shows the components
    in code. This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.


    **Note**: The spark_session parameter, although not accessed, is what enables this test to be run or skipped using
    the correct spark environment
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "MultiBatchExample_InferredAssetFileSystemExample_Spark.ipynb"
    )
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path,
        "MultiBatchExample_InferredAssetFileSystemExample_Spark_executed.ipynb",
    )

    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=60, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/example_inferred_suite.json",
            "great_expectations/checkpoints/my_checkpoint.yml",
            "great_expectations/uncommitted/data_docs",
            "great_expectations/uncommitted/validations/example_inferred_suite",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_multibatch_configured_asset_example_spark(tmp_path, spark_session):
    """
    What does this test and why?
    One of the resources we have for multi-batch Expectations is a Jupyter notebook that explains/shows the components
    in code. This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.

    **Note**: The spark_session parameter, although not accessed, is what enables this test to be run or skipped using
    the correct spark environment
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "MultiBatchExample_ConfiguredAssetFileSystemExample_Spark.ipynb"
    )
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path,
        "MultiBatchExample_ConfiguredAssetFileSystemExample_Spark_executed.ipynb",
    )

    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=30, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/example_configured_suite.json",
            "great_expectations/checkpoints/my_checkpoint.yml",
            "great_expectations/uncommitted/data_docs",
            "great_expectations/uncommitted/validations/example_configured_suite",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_multibatch_inferred_asset_example_pandas(tmp_path):
    """
    What does this test and why?
    One of the resources we have for multi-batch Expectations is a Jupyter notebook that explains/shows the components
    in code. This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "MultiBatchExample_InferredAssetFileSystemExample_Pandas.ipynb"
    )
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path,
        "MultiBatchExample_InferredAssetFileSystemExample_Pandas_executed.ipynb",
    )

    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=60, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/example_inferred_suite.json",
            "great_expectations/checkpoints/my_checkpoint.yml",
            "great_expectations/uncommitted/data_docs",
            "great_expectations/uncommitted/validations/example_inferred_suite",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_multibatch_configured_asset_example_pandas(tmp_path):
    """
    What does this test and why?
    One of the resources we have for multi-batch Expectations is a Jupyter notebook that explains/shows the components
    in code. This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "MultiBatchExample_ConfiguredAssetFileSystemExample_Pandas.ipynb"
    )
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path,
        "MultiBatchExample_ConfiguredAssetFileSystemExample_Pandas_executed.ipynb",
    )

    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=30, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/example_configured_suite.json",
            "great_expectations/checkpoints/my_checkpoint.yml",
            "great_expectations/uncommitted/data_docs",
            "great_expectations/uncommitted/validations/example_configured_suite",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def test_run_multibatch_sql_asset_example(tmp_path, sa, test_backends):
    """
    What does this test and why?
    One of the resources we have for multi-batch Expectations is a Jupyter notebook that explains/shows the components
    in code. This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.
    """
    if "postgresql" not in test_backends:
        pytest.skip("testing multibatch in sql requires postgres backend")
    else:
        load_data_into_postgres_database(sa)

    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(base_dir, "MultiBatchExample_SqlExample.ipynb")
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "MultiBatchExample_SqlExample_executed.ipynb"
    )
    with open(notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=30, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg: str = f'Error executing the notebook "{notebook_path}".\n\n'
        msg += f'See notebook "{output_notebook_path}" for the traceback.'
        raise GreatExpectationsError(msg)
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            "great_expectations/expectations/tmp",
            "great_expectations/expectations/.ge_store_backend_id",
            "great_expectations/expectations/example_sql_suite.json",
            "great_expectations/checkpoints/my_checkpoint.yml",
            "great_expectations/uncommitted/data_docs",
            "great_expectations/uncommitted/validations/example_sql_suite",
        ]
        clean_up_test_files(base_dir=base_dir, paths=paths_to_clean_up)


def load_data_into_postgres_database(sa):
    """
    Method to load our 2020 taxi data into a postgres database.  This is a helper method
    called by test_run_multibatch_sql_asset_example().
    """

    from tests.test_utils import load_data_into_test_database

    data_paths: List[str] = [
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-01.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-02.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-03.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-04.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-05.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-06.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-07.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-08.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-09.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-10.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-11.csv",
        ),
        file_relative_path(
            __file__,
            "../../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-12.csv",
        ),
    ]

    engine: sa.engine.Engine = sa.create_engine(CONNECTION_STRING)
    connection: sa.engine.Connection = engine.connect()

    # ensure we aren't appending to an existing table
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    for data_path in data_paths:
        load_data_into_test_database(
            table_name=table_name,
            csv_path=data_path,
            connection_string=CONNECTION_STRING,
            load_full_dataset=True,
            drop_existing_table=False,
            convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        )


# this works for some reason
def test_with_batch_spec_passthrough_and_schema_data_assistant_pandas(
    empty_data_context,
):
    data_context = empty_data_context
    base_directory = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
        ),
    )
    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            # "class_name": "SparkDFExecutionEngine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": base_directory,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                    },
                },
            },
        },
    }
    # add_datasource only if it doesn't already exist in our configuration
    try:
        data_context.get_datasource(datasource_config["name"])
    except ValueError:
        data_context.add_datasource(**datasource_config)

    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
        data_connector_query={"limit": 2},
        # batch_spec_passthrough={
        #     "reader_method": "csv",
        #     "reader_options": {
        #         "header": True,
        #         "schema": spark_df_taxi_data_schema,
        #     },
        # },
    )

    batch_list = data_context.get_batch_list(batch_request=batch_request)
    multi_batch_batch_request = batch_request
    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )
    # print(result)


# and this does not
def test_spark_with_batch_spec_passthrough_and_schema_data_assistant_spark(
    spark_session, spark_df_taxi_data_schema, empty_data_context
):
    from great_expectations.expectations.metrics.import_manager import F

    data_context = empty_data_context
    base_directory = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
        ),
    )
    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SparkDFExecutionEngine",
            # "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": base_directory,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                    },
                    "yellow_tripdata_2020": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                    },
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "schema": spark_df_taxi_data_schema,
                    },
                },
            },
        },
    }
    # add_datasource only if it doesn't already exist in our configuration
    try:
        data_context.get_datasource(datasource_config["name"])
    except ValueError:
        data_context.add_datasource(**datasource_config)

    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
        data_connector_query={"limit": 1},
        # batch_spec_passthrough={
        #     "reader_method": "csv",
        #     "reader_options": {
        #         "header": True,
        #         "schema": spark_df_taxi_data_schema,
        #     },
        # },
    )

    batch_list = data_context.get_batch_list(batch_request=batch_request)
    # batch_list[0].data.dataframe.show(truncate=False)
    # my_df_0 = batch_list[0].data.dataframe
    # my_df_1 = my_df_0.select(F.col("store_and_fwd_flag"))
    # my_df_2 = my_df_0.filter(F.col("store_and_fwd_flag").isNull())
    # my_df_3 = my_df_0.filter(F.length(F.col("store_and_fwd_flag")) < 1)
    #
    # # check whether a column is null
    # #my_df_1.show()
    # my_df_3.show()

    # do any metrics have an exception?
    multi_batch_batch_request = batch_request
    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )
    #
    # new_suite = result.get_expectation_suite(expectation_suite_name="temp_suite")
    # suite: ExpectationSuite = ExpectationSuite(
    #     expectation_suite_name="taxi_data_2019_suite"
    # )
    # resulting_configurations: List[
    #     ExpectationConfiguration
    # ] = suite.add_expectation_configurations(
    #     expectation_configurations=result.expectation_configurations
    # )
    # data_context.save_expectation_suite(expectation_suite=suite)
    #
    # single_batch_batch_request: BatchRequest = BatchRequest(
    #     datasource_name="taxi_data",
    #     data_connector_name="configured_data_connector_multi_batch_asset",
    #     data_asset_name="yellow_tripdata_2020",
    #     data_connector_query={
    #         "batch_filter_parameters": {"year": "2020", "month": "01"}
    #     },
    #     batch_spec_passthrough={
    #         "reader_options": {
    #             "schema": spark_df_taxi_data_schema,
    #         }
    #     },
    # )
    # checkpoint_config: dict = {
    #     "name": "my_checkpoint",
    #     "config_version": 1,
    #     "class_name": "SimpleCheckpoint",
    #     "validations": [
    #         {
    #             "batch_request": single_batch_batch_request,
    #             "expectation_suite_name": "taxi_data_2019_suite",
    #         }
    #     ],
    #     "action_list": [
    #         {
    #             "name": "update_data_docs",
    #             "action": {
    #                 "class_name": "UpdateDataDocsAction",
    #             },
    #         },
    #     ],
    # }
    # data_context.add_checkpoint(**checkpoint_config)
    # results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    # print(results.success)
