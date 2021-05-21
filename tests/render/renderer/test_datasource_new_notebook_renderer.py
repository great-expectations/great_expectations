import nbformat
import pytest

from great_expectations import DataContext
from great_expectations.datasource.types import DatasourceTypes
from great_expectations.render.renderer.datasource_new_notebook_renderer import (
    DatasourceNewNotebookRenderer,
)


@pytest.fixture
def docs_base_url() -> str:
    # TODO taylor update this
    return "https://knoxpod.netlify.app"


@pytest.fixture
def construct_datasource_new_notebook_assets(docs_base_url):
    def _construct_datasource_new_notebook_assets(
        datasource_name: str,
        datasource_yaml: str,
    ):

        pandas_header = [
            {
                "cell_type": "markdown",
                "source": f"""\
# Create a new pandas Datasource

Use this notebook and these guides to configure your new pandas Datasource and add it to your project.

- [How to connect to your data on a filesystem using pandas]({docs_base_url}/docs/guides/connecting_to_your_data/filesystem/pandas/)
- [How to connect to your data on S3 using pandas]({docs_base_url}/docs/guides/connecting_to_your_data/cloud/s3/pandas/)
- [How to connect to your data on GCS using pandas]({docs_base_url}/docs/guides/connecting_to_your_data/cloud/gcs/pandas/)
- [How to connect to your data on Azure using pandas]({docs_base_url}/docs/guides/connecting_to_your_data/cloud/azure/pandas/)

- 🍏 CORE SKILLS ICON [How to configure a DataConnector to introspect and partition a filesystem or blob store](#)
""",
                "metadata": {},
            }
        ]
        spark_header = [
            {
                "cell_type": "markdown",
                "source": f"""\
# Create a new Spark Datasource

Use this notebook and these guides to configure your new Spark Datasource and add it to your project.

- [How to connect to your data on a filesystem using spark]({docs_base_url}/docs/guides/connecting_to_your_data/filesystem/spark/)
- [How to connect to your data on S3 using spark]({docs_base_url}/docs/guides/connecting_to_your_data/cloud/s3/spark/)
- [How to connect to your data on GCS using spark]({docs_base_url}/docs/guides/connecting_to_your_data/cloud/gcs/spark/)
- [How to connect to your data on Azure using spark]({docs_base_url}/docs/guides/connecting_to_your_data/cloud/azure/spark/)

- 🍏 CORE SKILLS ICON [How to configure a DataConnector to introspect and partition a filesystem or blob store](#)
""",
                "metadata": {},
            }
        ]
        sql_header = [
            {
                "cell_type": "markdown",
                "source": f"""\
# Create a new SQL Datasource

Use this notebook and these guides to configure your new SQL Datasource and add it to your project.

- [How to connect to your data in a Athena database]({docs_base_url}/docs/guides/connecting_to_your_data/database/athena)
- [How to connect to your data in a Bigquery database]({docs_base_url}/docs/guides/connecting_to_your_data/database/bigquery)
- [How to connect to your data in a MSSQL database]({docs_base_url}/docs/guides/connecting_to_your_data/database/mssql)
- [How to connect to your data in a MySQL database]({docs_base_url}/docs/guides/connecting_to_your_data/database/mysql)
- [How to connect to your data in a Postgres database]({docs_base_url}/docs/guides/connecting_to_your_data/database/postgres)
- [How to connect to your data in a Redshift database]({docs_base_url}/docs/guides/connecting_to_your_data/database/redshift)
- [How to connect to your data in a Snowflake database]({docs_base_url}/docs/guides/connecting_to_your_data/database/snowflake)
- [How to connect to your data in a Sqlite database]({docs_base_url}/docs/guides/connecting_to_your_data/database/sqlite)

- 🍏 CORE SKILLS ICON [How to configure a DataConnector to introspect and partition tables in SQL](#)
""",
                "metadata": {},
            }
        ]
        imports = [
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "import great_expectations as ge\nfrom great_expectations.cli.datasource import sanitize_yaml_and_save_datasource, check_if_datasource_name_exists\ncontext = ge.get_context()",
                "outputs": [],
            },
        ]

        customize_docs_cell = [
            {
                "cell_type": "markdown",
                "source": """## Customize Your Datasource Configuration

Give your datasource a unique name:""",
                "metadata": {},
            }
        ]

        datasource_name_cell = [
            {
                "cell_type": "code",
                "source": f'datasource_name = "{datasource_name}"',
                "execution_count": None,
                "metadata": {},
                "outputs": [],
            }
        ]

        files_docs_cell = [
            {
                "cell_type": "markdown",
                "source": """### For files based Datasources:
Here we are creating an example configuration using an InferredAssetDataConnector which will add a Data Asset for each file in the base directory you provided. This is just an example and you may customize this as you wish!

See our docs for other methods to organize assets, handle multi-file assets, name assets based on parts of a filename, etc.""",
                "metadata": {},
            }
        ]
        sql_docs_cell = [
            {
                "cell_type": "markdown",
                "source": """### For SQL based Datasources:

Here we are creating an example configuration using `SimpleSqlalchemyDatasource` based on the database backend you specified in the CLI.

Credentials will not be saved until you run the last cell. The credentials will be saved in `uncommitted/config_variables.yml` which should not be added to source control.""",
                "metadata": {},
            }
        ]

        sql_credentials_cell = [
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'host = "localhost"',
                "outputs": [],
            },
        ]
        template_cell = [
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": f"""example_yaml = {datasource_yaml}\nprint(example_yaml)""",
                "outputs": [],
            },
        ]

        test_yaml_cells = [
            {
                "cell_type": "markdown",
                "source": """# Test Your Datasource Configuration
Here we will test your Datasource configuration to make sure it is valid.

This `test_yaml_config()` function is meant to enable fast dev loops. **If your
configuration is correct, this cell will show you some snippets of the data
assets in the data source.** You can continually edit your Datasource config
yaml and re-run the cell to check until the new config is valid.

If you instead wish to use python instead of yaml to configure your Datasource,
you can use `context.add_datasource()` and specify all the required parameters.""",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "context.test_yaml_config(yaml_config=example_yaml)",
                "outputs": [],
            },
        ]

        save_datasource_cells = [
            {
                "cell_type": "markdown",
                "source": """## Save Your Datasource Configuration\nHere we will save your Datasource in your Data Context once you are satisfied with the configuration. Note that `overwrite_existing` defaults to False, but you may change it to True if you wish to overwrite. Please note that if you wish to include comments you must add them directly to your `great_expectations.yml`.""",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)\ncontext.list_datasources()",
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
            "customize_docs_cell": customize_docs_cell,
            "datasource_name_cell": datasource_name_cell,
            "files_docs_cell": files_docs_cell,  # pandas and spark
            "sql_docs_cell": sql_docs_cell,
            "sql_credentials_cell": sql_credentials_cell,
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
    datasource_yaml = '"""test_yaml:\n  indented_key: value"""'

    datasource_new_notebook_renderer = DatasourceNewNotebookRenderer(
        context=context,
        datasource_type=DatasourceTypes.PANDAS,
        datasource_yaml=datasource_yaml,
        datasource_name=datasource_name,
    )
    obs: nbformat.NotebookNode = datasource_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    datasource_new_notebook_assets = construct_datasource_new_notebook_assets(
        datasource_name=datasource_name, datasource_yaml=datasource_yaml
    )

    expected_cells = (
        datasource_new_notebook_assets["pandas_header"]
        + datasource_new_notebook_assets["imports"]
        + datasource_new_notebook_assets["customize_docs_cell"]
        + datasource_new_notebook_assets["datasource_name_cell"]
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
    datasource_yaml = '"""test_yaml:\n  indented_key: value"""'

    datasource_new_notebook_renderer = DatasourceNewNotebookRenderer(
        context=context,
        datasource_type=DatasourceTypes.SPARK,
        datasource_yaml=datasource_yaml,
        datasource_name=datasource_name,
    )
    obs: nbformat.NotebookNode = datasource_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    datasource_new_notebook_assets = construct_datasource_new_notebook_assets(
        datasource_name=datasource_name, datasource_yaml=datasource_yaml
    )

    expected_cells = (
        datasource_new_notebook_assets["spark_header"]
        + datasource_new_notebook_assets["imports"]
        + datasource_new_notebook_assets["customize_docs_cell"]
        + datasource_new_notebook_assets["datasource_name_cell"]
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
    datasource_yaml = '"""test_yaml:\n  indented_key: value"""'

    datasource_new_notebook_renderer = DatasourceNewNotebookRenderer(
        context=context,
        datasource_type=DatasourceTypes.SQL,
        datasource_yaml=datasource_yaml,
        datasource_name=datasource_name,
        sql_credentials_snippet='host = "localhost"',
    )
    obs: nbformat.NotebookNode = datasource_new_notebook_renderer.render()

    assert isinstance(obs, dict)

    datasource_new_notebook_assets = construct_datasource_new_notebook_assets(
        datasource_name=datasource_name, datasource_yaml=datasource_yaml
    )

    expected_cells = (
        datasource_new_notebook_assets["sql_header"]
        + datasource_new_notebook_assets["imports"]
        + datasource_new_notebook_assets["customize_docs_cell"]
        + datasource_new_notebook_assets["datasource_name_cell"]
        + datasource_new_notebook_assets["sql_docs_cell"]
        + datasource_new_notebook_assets["sql_credentials_cell"]
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
