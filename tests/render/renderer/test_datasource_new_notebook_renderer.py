import nbformat
import pytest

from great_expectations import DataContext
from great_expectations.datasource.types import DatasourceTypes
from great_expectations.render.renderer.datasource_new_notebook_renderer import (
    DatasourceNewNotebookRenderer,
)


@pytest.fixture
def construct_datasource_new_notebook_assets():
    def _construct_datasource_new_notebook_assets(
        datasource_name: str,
        datasource_yaml: str,
    ):

        pandas_header = [
            {
                "cell_type": "markdown",
                "source": f"# Create a new {DatasourceTypes.PANDAS.value} Datasource\nUse this notebook to configure a new {DatasourceTypes.PANDAS.value} Datasource and add it to your project.\n\n**Datasource Name**: `{datasource_name}`",
                "metadata": {},
            }
        ]
        spark_header = [
            {
                "cell_type": "markdown",
                "source": f"# Create a new {DatasourceTypes.SPARK.value} Datasource\nUse this notebook to configure a new {DatasourceTypes.SPARK.value} Datasource and add it to your project.\n\n**Datasource Name**: `{datasource_name}`",
                "metadata": {},
            }
        ]
        sql_header = [
            {
                "cell_type": "markdown",
                "source": f"# Create a new {DatasourceTypes.SQL.value} Datasource\nUse this notebook to configure a new {DatasourceTypes.SQL.value} Datasource and add it to your project.\n\n**Datasource Name**: `{datasource_name}`",
                "metadata": {},
            }
        ]
        imports = [
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "import yaml\nimport great_expectations as ge\ncontext = ge.get_context()",
                "outputs": [],
            },
        ]

        files_docs_cell = [
            {
                "cell_type": "markdown",
                "source": "## Customize Your Datasource Configuration\n\n**If you are new to Great Expectations Datasources,** you should check out our [how-to documentation](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html)\n\n**My configuration is not so simple - are there more advanced options?**\n\nGlad you asked! Datasources are versatile. Please see our [How To Guides](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html)!\n### For files based Datasources:\nHere we are creating an example configuration using an InferredAssetDataConnector which will add a Data Asset for each file in the base directory you provided. This is just a sample, you may customize this as you wish!\n\nSee our docs for other methods to organize assets, handle multi-file assets, name assets based on parts of a filename, etc.",
                "metadata": {},
            }
        ]
        sql_docs_cell = [
            {
                "cell_type": "markdown",
                "source": "## Customize Your Datasource Configuration\n\n**If you are new to Great Expectations Datasources,** you should check out our [how-to documentation](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html)\n\n**My configuration is not so simple - are there more advanced options?**\n\nGlad you asked! Datasources are versatile. Please see our [How To Guides](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html)!\n### For SQL based Datasources:\n\nHere we are creating an example configuration using SimpleSqlalchemyDatasource based on the credentials you supplied in the CLI.\n\nThe credentials will be saved in `uncommitted/config_variables.yml`",
                "metadata": {},
            }
        ]

        template_cell = [
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": f'''example_yaml = """\n{datasource_yaml}"""\nprint(example_yaml)''',
                "outputs": [],
            },
        ]

        test_yaml_cells = [
            {
                "cell_type": "markdown",
                "source": "# Test Your Datasource Configuration\nHere we will test your Datasource configuration to make sure it is valid.\n\nThis `test_yaml_config()` function is meant to enable fast dev loops. You can continually edit your Datasource config yaml and re-run the cell to check until the new config is valid.\n\nIf you instead wish to use python instead of yaml to configure your Datasource, you can use `context.add_datasource()` and specify all the required parameters.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": f"""context.test_yaml_config(example_yaml, name="{datasource_name}")""",
                "outputs": [],
            },
        ]

        save_datasource_cells = [
            {
                "cell_type": "markdown",
                "source": """## Save Your Datasource Configuration\nHere we will save your Datasource in your Data Context once you are satisfied with the configuration.""",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": f"""context.add_datasource(name="{datasource_name}", **yaml.load(example_yaml))
context.list_datasources()""",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "Now you can close this notebook and delete it!",
                "metadata": {},
            },
        ]

        return {
            "pandas_header": pandas_header,
            "spark_header": spark_header,
            "sql_header": sql_header,
            "imports": imports,
            "files_docs_cell": files_docs_cell,  # pandas and spark
            "sql_docs_cell": sql_docs_cell,
            "template_cell": template_cell,
            "test_yaml_cells": test_yaml_cells,
            "save_datasource_cells": save_datasource_cells,
        }

    return _construct_datasource_new_notebook_assets


def test_render_datasource_new_notebook_with_pandas_Datasource(
    empty_data_context,
    construct_datasource_new_notebook_assets,
):
    """
    What does this test and why?
    The DatasourceNewNotebookRenderer should generate a notebook with text based on the datasource we are trying to implement. Here we are testing pandas Datasource.
    """

    context: DataContext = empty_data_context

    datasource_name = "my_pandas_datasource_name"
    datasource_yaml = "test_yaml:\n  indented_key: value"

    datasource_new_notebook_renderer = DatasourceNewNotebookRenderer(
        context=context,
        datasource_name=datasource_name,
        datasource_type=DatasourceTypes.PANDAS,
        datasource_yaml=datasource_yaml,
    )
    obs: nbformat.NotebookNode = datasource_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    datasource_new_notebook_assets = construct_datasource_new_notebook_assets(
        datasource_name=datasource_name, datasource_yaml=datasource_yaml
    )

    expected_cells = (
        datasource_new_notebook_assets["pandas_header"]
        + datasource_new_notebook_assets["imports"]
        + datasource_new_notebook_assets["files_docs_cell"]
        + datasource_new_notebook_assets["template_cell"]
        + datasource_new_notebook_assets["test_yaml_cells"]
        + datasource_new_notebook_assets["save_datasource_cells"]
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


def test_render_datasource_new_notebook_with_spark_Datasource(
    empty_data_context,
    construct_datasource_new_notebook_assets,
):
    """
    What does this test and why?
    The DatasourceNewNotebookRenderer should generate a notebook with text based on the datasource we are trying to implement. Here we are testing spark Datasource.
    """

    context: DataContext = empty_data_context

    datasource_name = "my_spark_datasource_name"
    datasource_yaml = "test_yaml:\n  indented_key: value"

    datasource_new_notebook_renderer = DatasourceNewNotebookRenderer(
        context=context,
        datasource_name=datasource_name,
        datasource_type=DatasourceTypes.SPARK,
        datasource_yaml=datasource_yaml,
    )
    obs: nbformat.NotebookNode = datasource_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    datasource_new_notebook_assets = construct_datasource_new_notebook_assets(
        datasource_name=datasource_name, datasource_yaml=datasource_yaml
    )

    expected_cells = (
        datasource_new_notebook_assets["spark_header"]
        + datasource_new_notebook_assets["imports"]
        + datasource_new_notebook_assets["files_docs_cell"]
        + datasource_new_notebook_assets["template_cell"]
        + datasource_new_notebook_assets["test_yaml_cells"]
        + datasource_new_notebook_assets["save_datasource_cells"]
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


def test_render_datasource_new_notebook_with_sql_Datasource(
    empty_data_context,
    construct_datasource_new_notebook_assets,
):
    """
    What does this test and why?
    The DatasourceNewNotebookRenderer should generate a notebook with text based on the datasource we are trying to implement. Here we are testing sql Datasource.
    """

    context: DataContext = empty_data_context

    datasource_name = "my_sql_datasource_name"
    datasource_yaml = "test_yaml:\n  indented_key: value"

    datasource_new_notebook_renderer = DatasourceNewNotebookRenderer(
        context=context,
        datasource_name=datasource_name,
        datasource_type=DatasourceTypes.SQL,
        datasource_yaml=datasource_yaml,
    )
    obs: nbformat.NotebookNode = datasource_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    datasource_new_notebook_assets = construct_datasource_new_notebook_assets(
        datasource_name=datasource_name, datasource_yaml=datasource_yaml
    )

    expected_cells = (
        datasource_new_notebook_assets["sql_header"]
        + datasource_new_notebook_assets["imports"]
        + datasource_new_notebook_assets["sql_docs_cell"]
        + datasource_new_notebook_assets["template_cell"]
        + datasource_new_notebook_assets["test_yaml_cells"]
        + datasource_new_notebook_assets["save_datasource_cells"]
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
