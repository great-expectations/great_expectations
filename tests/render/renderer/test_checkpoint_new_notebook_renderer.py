import nbformat
import pytest

from great_expectations import DataContext
from great_expectations.data_context import BaseDataContext
from great_expectations.render.renderer.checkpoint_new_notebook_renderer import (
    CheckpointNewNotebookRenderer,
)


def test_find_datasource_with_asset_on_context_with_no_datasources(
    empty_data_context,
):
    context = empty_data_context
    assert len(context.list_datasources()) == 0

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs is None


def test_find_datasource_with_asset_on_context_with_a_datasource_with_no_dataconnectors(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    context = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    context.delete_datasource("my_datasource")
    assert len(context.list_datasources()) == 0
    context.add_datasource(
        "aaa_datasource",
        class_name="Datasource",
        module_name="great_expectations.datasource.new_datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
    )
    assert len(context.list_datasources()) == 1

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs is None


def test_find_datasource_with_asset_on_context_with_a_datasource_with_a_dataconnector_that_has_no_assets(
    assetless_dataconnector_context,
):
    context = assetless_dataconnector_context
    assert list(context.get_datasource("my_datasource").data_connectors.keys()) == [
        "my_other_data_connector"
    ]

    # remove data asset name
    config = context.get_config_with_variables_substituted()
    root_directory = context.root_directory

    context = BaseDataContext(project_config=config, context_root_dir=root_directory)

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs is None


def test_find_datasource_with_asset_on_happy_path_context(
    deterministic_asset_dataconnector_context,
):
    context = deterministic_asset_dataconnector_context
    assert len(context.list_datasources()) == 1

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()

    assert obs == {
        "asset_name": "users",
        "data_connector_name": "my_other_data_connector",
        "datasource_name": "my_datasource",
    }


def test_find_datasource_with_asset_on_context_with_a_full_datasource_and_one_with_no_dataconnectors(
    deterministic_asset_dataconnector_context,
):
    context = deterministic_asset_dataconnector_context
    assert len(context.list_datasources()) == 1
    context.add_datasource(
        "aaa_datasource",
        class_name="Datasource",
        module_name="great_expectations.datasource.new_datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
    )
    assert len(context.list_datasources()) == 2

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs == {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_other_data_connector",
        "asset_name": "users",
    }


@pytest.fixture
def checkpoint_new_notebook_assets():
    header = [
        {
            "cell_type": "markdown",
            "source": "# Create Your Checkpoint\nUse this notebook to create your Checkpoint:\n\n**Checkpoint Name**: `my_checkpoint_name`\n\nWe'd love it if you'd **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)!",
            "metadata": {},
        }
    ]
    imports = [
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "import great_expectations as ge\n\ncontext = ge.get_context()",
            "outputs": [],
        },
    ]
    example_configuration = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# Example Configuration\n**If you are new to Great Expectations or the Checkpoint feature**, you should probably start with SimpleCheckpoint because it includes default configurations like a default list of post validation actions.\n\nThe example in the cell below shows a SimpleCheckpoint for validating a single Batch of data against a single Expectation Suite.\n\n**My configuration is not so simple - are there more advanced options?**\n\nGlad you asked! Checkpoints are very versatile. For example, you can validate many Batches in a single Checkpoint, validate Batches against different Expectation Suites or against many Expectation Suites, control the specific post-validation actions based on Expectation Suite / Batch / results of validation among other features. Check out our documentation on Checkpoints for more info:\n\n- https://docs.greatexpectations.io/en/latest/reference/core_concepts/checkpoints_and_actions.html\n- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint.html\n- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html""",
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": (
                'example_config = """'
                """
name: my_checkpoint
config_version: 1
class_name: SimpleCheckpoint
run_name_template: "%Y-%M-my-run-name-template"
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_data_connector
      data_asset_name: MyDataAsset
      partition_request:
        index: -1
    expectation_suite_name: my_suite
"""
                '"""'
            ),
            "outputs": [],
        },
    ]
    optional_list_your_config = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# List Your Configuration (Optional)\nThe following cells show examples for listing your current configuration.\n\nYou may wish to run these cells to view your currently configured Checkpoints and choose a Datasource & Expectation Suite.""",
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "context.list_checkpoints()",
            "outputs": [],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": """list_of_existing_datasources_by_name = [\n    datasource["name"] for datasource in context.list_datasources()\n]\nlist_of_existing_datasources_by_name""",
            "outputs": [],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "context.list_expectation_suite_names()",
            "outputs": [],
        },
    ]

    sample_checkpoint_config_markdown_description = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# Sample Checkpoint Config\n\nIn the cell below we have created a sample Checkpoint configuration using **your configuration** and **SimpleCheckpoint** to run a single validation of a single Expectation Suite against a single Batch of data.\n\nTo keep it simple, we are just choosing the first Datasource, DataConnector, DataAsset, Partition and Expectation Suite you have configured to create the example yaml config.\n\nOf course this is purely an example, you may edit this to your heart's content.\n\nPlease also see the docs linked below for instructions on how to implement other more advanced features including using the **Checkpoint** class:\n- https://docs.greatexpectations.io/en/latest/reference/core_concepts/checkpoints_and_actions.html\n- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint.html\n- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html""",
        },
    ]
    sample_checkpoint_config_code_correct = [
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": (
                'sample_yaml = """\n'
                "name: my_checkpoint_name\n"
                """config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_other_data_connector
      data_asset_name: users
      partition_request:
        index: -1
    expectation_suite_name: Titanic.warning
"""
                '"""'
                "\nprint(sample_yaml)"
            ),
            "outputs": [],
        },
    ]
    sample_checkpoint_config_markdown_error_message = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": "Sorry, we were unable to create a sample configuration. Perhaps you don't have a Datasource or Expectation Suite configured.",
        },
    ]

    test_and_save_your_checkpoint_configuration = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# Test the Checkpoint Configuration\nHere we will test your Checkpoint configuration to make sure it is valid.\n\nNote that if it is valid, it will be automatically saved to your Checkpoint Store.\n\nThis test_yaml_config() function is meant to enable fast dev loops. You can continually edit your Checkpoint config yaml and re-run the cell to check until the new config is valid.\n\nIf you instead wish to use python instead of yaml to configure your Checkpoint, you can always use context.add_checkpoint() and specify all the required parameters.""",
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": 'checkpoint_name = "my_checkpoint_name" # From your CLI command, feel free to change this.',
            "outputs": [],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "my_checkpoint_config = sample_yaml # Change `sample_yaml` to your custom Checkpoint config if you wish",
            "outputs": [],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": """my_checkpoint = context.test_yaml_config(
    name=checkpoint_name, yaml_config=my_checkpoint_config
)""",
            "outputs": [],
        },
    ]
    review_checkpoint = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# Review Checkpoint\n\nYou can run the following cell to print out the full yaml configuration. For example, if you used **SimpleCheckpoint**  this will show you the default action list.""",
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "print(my_checkpoint.get_substituted_config().to_yaml_str())",
            "outputs": [],
        },
    ]
    optional_run_checkpoint = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# Run Checkpoint (Optional)\n\nYou may wish to run the Checkpoint now to see a sample of it's output. If so run the following cell.""",
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "context.run_checkpoint(checkpoint_name=checkpoint_name)",
            "outputs": [],
        },
    ]
    optional_open_data_docs = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": """# Open Data Docs (Optional)\nYou may also wish to open up Data Docs to review the results of the Checkpoint run if you ran the above cell.""",
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "source": "# context.open_data_docs()",
            "outputs": [],
        },
    ]

    return {
        "header": header,
        "imports": imports,
        "example_configuration": example_configuration,
        "optional_list_your_config": optional_list_your_config,
        "sample_checkpoint_config_markdown_description": sample_checkpoint_config_markdown_description,
        "sample_checkpoint_config_code_correct": sample_checkpoint_config_code_correct,
        "sample_checkpoint_config_markdown_error_message": sample_checkpoint_config_markdown_error_message,
        "test_and_save_your_checkpoint_configuration": test_and_save_your_checkpoint_configuration,
        "review_checkpoint": review_checkpoint,
        "optional_run_checkpoint": optional_run_checkpoint,
        "optional_open_data_docs": optional_open_data_docs,
    }


def test_render_checkpoint_new_notebook_with_available_data_asset(
    deterministic_asset_dataconnector_context,
    titanic_expectation_suite,
    checkpoint_new_notebook_assets,
):
    """
    What does this test and why?
    The CheckpointNewNotebookRenderer should generate a notebook with an example SimpleCheckpoint yaml config based on the first available data asset.
    """

    context: DataContext = deterministic_asset_dataconnector_context

    assert context.list_checkpoints() == []
    context.save_expectation_suite(titanic_expectation_suite)
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    checkpoint_new_notebook_renderer = CheckpointNewNotebookRenderer(
        context=context, checkpoint_name="my_checkpoint_name"
    )
    obs: nbformat.NotebookNode = checkpoint_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    expected_cells = (
        checkpoint_new_notebook_assets["header"]
        + checkpoint_new_notebook_assets["imports"]
        + checkpoint_new_notebook_assets["example_configuration"]
        + checkpoint_new_notebook_assets["optional_list_your_config"]
        + checkpoint_new_notebook_assets[
            "sample_checkpoint_config_markdown_description"
        ]
        # Testing to make sure everything in the notebook but especially this checkpoint config code is correct.
        + checkpoint_new_notebook_assets["sample_checkpoint_config_code_correct"]
        + checkpoint_new_notebook_assets["test_and_save_your_checkpoint_configuration"]
        + checkpoint_new_notebook_assets["review_checkpoint"]
        + checkpoint_new_notebook_assets["optional_run_checkpoint"]
        + checkpoint_new_notebook_assets["optional_open_data_docs"]
    )

    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": expected_cells,
    }

    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_checkpoint_new_notebook_with_unavailable_data_asset(
    assetless_dataconnector_context,
    checkpoint_new_notebook_assets,
):
    context: DataContext = assetless_dataconnector_context

    assert context.list_checkpoints() == []

    # This config is bad because of a missing expectation suite

    checkpoint_new_notebook_renderer = CheckpointNewNotebookRenderer(
        context=context, checkpoint_name="my_checkpoint_name"
    )
    obs: nbformat.NotebookNode = checkpoint_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    expected_cells = (
        checkpoint_new_notebook_assets["header"]
        + checkpoint_new_notebook_assets["imports"]
        + checkpoint_new_notebook_assets["example_configuration"]
        + checkpoint_new_notebook_assets["optional_list_your_config"]
        + checkpoint_new_notebook_assets[
            "sample_checkpoint_config_markdown_description"
        ]
        # Testing to make sure the error message here is displayed appropriately
        + checkpoint_new_notebook_assets[
            "sample_checkpoint_config_markdown_error_message"
        ]
        + checkpoint_new_notebook_assets["test_and_save_your_checkpoint_configuration"]
        + checkpoint_new_notebook_assets["review_checkpoint"]
        + checkpoint_new_notebook_assets["optional_run_checkpoint"]
        + checkpoint_new_notebook_assets["optional_open_data_docs"]
    )

    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": expected_cells,
    }

    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected
