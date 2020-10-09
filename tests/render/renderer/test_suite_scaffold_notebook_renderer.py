import os

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.render.renderer.suite_scaffold_notebook_renderer import (
    SuiteScaffoldNotebookRenderer,
)


def test_render_snapshot_test(titanic_data_context):
    batch_kwargs = titanic_data_context.build_batch_kwargs(
        "mydatasource", "mygenerator", "Titanic"
    )
    csv_path = batch_kwargs["path"]
    suite_name = "my_suite"
    suite = titanic_data_context.create_expectation_suite(suite_name)
    renderer = SuiteScaffoldNotebookRenderer(titanic_data_context, suite, batch_kwargs)
    obs = renderer.render(None)
    assert isinstance(obs, nbformat.NotebookNode)
    ## NOTE!!! - When updating this snapshot be sure to include the dynamic
    # csv_path in the second cell due to pytest fixtures
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": """# Scaffold a new Expectation Suite (Experimental)
This process helps you avoid writing lots of boilerplate when authoring suites by allowing you to select columns you care about and letting a profiler write some candidate expectations for you to adjust.

**Expectation Suite Name**: `my_suite`

We'd love it if you'd **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)!""",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.profile import BasicSuiteBuilderProfiler\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\nexpectation_suite_name = "my_suite"\nsuite = context.create_expectation_suite(\n    expectation_suite_name, overwrite_existing=True\n)\n\nbatch_kwargs = {\n    "path": "'
                + csv_path
                + '",\n    "datasource": "mydatasource",\n    "data_asset_name": "Titanic",\n}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": """## Select the columns on which you would like to scaffold expectations

Great Expectations will choose which expectations might make sense for a column based on the **data type** and **cardinality** of the data in each selected column.

Simply uncomment columns that are important. You can select multiple lines and
use a jupyter keyboard shortcut to toggle each line: **Linux/Windows**:
`Ctrl-/`, **macOS**: `Cmd-/`""",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "included_columns = [\n    # 'Unnamed: 0',\n    # 'Name',\n    # 'PClass',\n    # 'Age',\n    # 'Sex',\n    # 'Survived',\n    # 'SexCode'\n]",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": """## Run the scaffolder

The suites generated here are **not meant to be production suites** - they are **scaffolds to build upon**.

**To get to a production grade suite, you will definitely want to [edit this
suite](https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_edit_an_expectation_suite_using_a_disposable_notebook.html)
after scaffolding gets you close to what you want.**

This is highly configurable depending on your goals. You can include or exclude
columns, and include or exclude expectation types (when applicable). [The
Expectation Glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=scaffold_expectations)
contains a list of possible expectations.""",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": '# Wipe the suite clean to prevent unwanted expectations in the batch\nsuite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)\nbatch = context.get_batch(batch_kwargs, suite)\n\n# In the scaffold_config, included or excluded expectation names should be strings.\nscaffold_config = {\n    "included_columns": included_columns,\n    # "excluded_columns": [],\n    # "included_expectations": [],\n    # "excluded_expectations": [],\n}\nsuite, evr = BasicSuiteBuilderProfiler().profile(batch, profiler_configuration=scaffold_config)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & review the scaffolded Expectation Suite\n\nLet's save the scaffolded expectation suite as a JSON file in the\n`great_expectations/expectations` directory of your project and rebuild the Data\n Docs site to make it easy to review the scaffolded suite.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'context.save_expectation_suite(suite, expectation_suite_name)\n\nresults = context.run_validation_operator("action_list_operator", assets_to_validate=[batch])\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Next steps\nAfter you review this scaffolded Expectation Suite in Data Docs you\nshould edit this suite to make finer grained adjustments to the expectations.\nThis can be done by running `great_expectations suite edit my_suite`.",
                "metadata": {},
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]

    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        assert obs_cell == expected_cell
    assert obs == expected


def test_notebook_execution_with_pandas_backend(titanic_data_context):
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
    context = titanic_data_context
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
    renderer = SuiteScaffoldNotebookRenderer(titanic_data_context, suite, batch_kwargs)
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
        "evaluated_expectations": 3,
        "successful_expectations": 3,
        "unsuccessful_expectations": 0,
        "success_percent": 100,
    }
    suite = context.get_expectation_suite(suite_name)
    assert suite.expectations
