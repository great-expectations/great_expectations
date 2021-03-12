import os

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.render.renderer.suite_scaffold_notebook_renderer import (
    SuiteScaffoldNotebookRenderer,
)
from tests.profile.conftest import get_set_of_columns_and_expectations_from_suite


def test_notebook_execution_with_pandas_backend(
    titanic_data_context_no_data_docs_no_checkpoint_store,
):
    """
    This tests that the notebook is written to disk and executes without error.

    To set this test up we:
    - create a scaffold notebook
    - verify that no validations have happened

    We then:
    - execute that notebook (Note this will raise various errors like
    CellExecutionError if any cell in the notebook fails
    - create a new context from disk
    - verify that a validation has been run with our expectation suite
    """
    # Since we'll run the notebook, we use a context with no data docs to avoid
    # the renderer's default behavior of building and opening docs, which is not
    # part of this test.
    context = titanic_data_context_no_data_docs_no_checkpoint_store
    root_dir = context.root_directory
    uncommitted_dir = os.path.join(root_dir, "uncommitted")
    suite_name = "my_suite"
    suite = context.create_expectation_suite(suite_name)

    csv_path = os.path.join(root_dir, "..", "data", "Titanic.csv")
    batch_kwargs = {"datasource": "mydatasource", "path": csv_path}

    # Sanity check test setup
    assert context.list_expectation_suite_names() == [suite_name]
    assert context.list_datasources() == [
        {
            "module_name": "great_expectations.datasource",
            "class_name": "PandasDatasource",
            "data_asset_type": {
                "module_name": "great_expectations.dataset",
                "class_name": "PandasDataset",
            },
            "batch_kwargs_generators": {
                "mygenerator": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": "../data",
                }
            },
            "name": "mydatasource",
        }
    ]
    assert context.get_validation_result(suite_name) == {}
    notebook_path = os.path.join(uncommitted_dir, f"{suite_name}.ipynb")
    assert not os.path.isfile(notebook_path)

    # Create notebook
    renderer = SuiteScaffoldNotebookRenderer(
        titanic_data_context_no_data_docs_no_checkpoint_store, suite, batch_kwargs
    )
    renderer.render_to_disk(notebook_path)
    assert os.path.isfile(notebook_path)

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    # Run notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    # Useful to inspect executed notebook
    output_notebook = os.path.join(uncommitted_dir, "output.ipynb")
    with open(output_notebook, "w") as f:
        nbformat.write(nb, f)

    # Assertions about output
    context = DataContext(root_dir)
    obs_validation_result = context.get_validation_result(suite_name)
    assert obs_validation_result.statistics == {
        "evaluated_expectations": 2,
        "successful_expectations": 2,
        "unsuccessful_expectations": 0,
        "success_percent": 100,
    }
    suite = context.get_expectation_suite(suite_name)

    assert suite.expectations
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    expected_expectations = {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_row_count_to_be_between",
    }
    assert columns_with_expectations == set()
    assert expectations_from_suite == expected_expectations
