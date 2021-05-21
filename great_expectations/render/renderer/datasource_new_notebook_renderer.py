from typing import Optional

import nbformat

from great_expectations import DataContext
from great_expectations.datasource.types import DatasourceTypes
from great_expectations.render.renderer.notebook_renderer import BaseNotebookRenderer

# TODO taylor update this
DOCS_BASE_URL = "https://knoxpod.netlify.app"


class DatasourceNewNotebookRenderer(BaseNotebookRenderer):
    SQL_DOCS = """\
### For SQL based Datasources:

Here we are creating an example configuration using `SimpleSqlalchemyDatasource` based on the database backend you specified in the CLI.

Credentials will not be saved until you run the last cell. The credentials will be saved in `uncommitted/config_variables.yml` which should not be added to source control."""

    FILES_DOCS = """### For files based Datasources:
Here we are creating an example configuration using an InferredAssetDataConnector which will add a Data Asset for each file in the base directory you provided. This is just an example and you may customize this as you wish!

See our docs for other methods to organize assets, handle multi-file assets, name assets based on parts of a filename, etc."""

    DOCS_INTRO = f"""## Customize Your Datasource Configuration

Give your datasource a unique name:"""

    PANDAS_HEADER = f"""\
# Create a new pandas Datasource

Use this notebook and these guides to configure your new pandas Datasource and add it to your project.

- [How to connect to your data on a filesystem using pandas]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/filesystem/pandas/)
- [How to connect to your data on S3 using pandas]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/cloud/s3/pandas/)
- [How to connect to your data on GCS using pandas]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/cloud/gcs/pandas/)
- [How to connect to your data on Azure using pandas]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/cloud/azure/pandas/)

- 🍏 CORE SKILLS ICON [How to configure a DataConnector to introspect and partition a filesystem or blob store](#)
"""

    SPARK_HEADER = f"""\
# Create a new Spark Datasource

Use this notebook and these guides to configure your new Spark Datasource and add it to your project.

- [How to connect to your data on a filesystem using spark]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/filesystem/spark/)
- [How to connect to your data on S3 using spark]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/cloud/s3/spark/)
- [How to connect to your data on GCS using spark]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/cloud/gcs/spark/)
- [How to connect to your data on Azure using spark]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/cloud/azure/spark/)

- 🍏 CORE SKILLS ICON [How to configure a DataConnector to introspect and partition a filesystem or blob store](#)
"""

    SQL_HEADER = f"""\
# Create a new SQL Datasource

Use this notebook and these guides to configure your new SQL Datasource and add it to your project.

- [How to connect to your data in a Athena database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/athena)
- [How to connect to your data in a Bigquery database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/bigquery)
- [How to connect to your data in a MSSQL database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/mssql)
- [How to connect to your data in a MySQL database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/mysql)
- [How to connect to your data in a Postgres database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/postgres)
- [How to connect to your data in a Redshift database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/redshift)
- [How to connect to your data in a Snowflake database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/snowflake)
- [How to connect to your data in a Sqlite database]({DOCS_BASE_URL}/docs/guides/connecting_to_your_data/database/sqlite)

- 🍏 CORE SKILLS ICON [How to configure a DataConnector to introspect and partition tables in SQL](#)
"""

    def __init__(
        self,
        context: DataContext,
        datasource_type: DatasourceTypes,
        datasource_yaml: str,
        datasource_name: Optional[str] = "my_datasource",
        sql_credentials_snippet: Optional[str] = None,
    ):
        super().__init__(context=context)
        self.datasource_type = datasource_type
        self.datasource_yaml = datasource_yaml
        self.sql_credentials_code_snippet = sql_credentials_snippet
        if datasource_name is None:
            datasource_name = "my_datasource"
        self.datasource_name = datasource_name

    def _add_header(self):
        if self.datasource_type == DatasourceTypes.PANDAS:
            self.add_markdown_cell(self.PANDAS_HEADER)
        elif self.datasource_type == DatasourceTypes.SPARK:
            self.add_markdown_cell(self.SPARK_HEADER)
        elif self.datasource_type == DatasourceTypes.SQL:
            self.add_markdown_cell(self.SQL_HEADER)

        self.add_code_cell(
            """import great_expectations as ge
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource, check_if_datasource_name_exists
context = ge.get_context()""",
        )

    def _add_docs_cell(self):
        self.add_markdown_cell(self.DOCS_INTRO)
        self.add_code_cell(f'datasource_name = "{self.datasource_name}"')

        if self.datasource_type in [DatasourceTypes.PANDAS, DatasourceTypes.SPARK]:
            self.add_markdown_cell(self.FILES_DOCS)
        elif self.datasource_type == DatasourceTypes.SQL:
            self.add_markdown_cell(self.SQL_DOCS)

    def _add_sql_credentials_cell(self):
        self.add_code_cell(self.sql_credentials_code_snippet)

    def _add_template_cell(self):
        self.add_code_cell(
            f"""example_yaml = {self.datasource_yaml}
print(example_yaml)""",
            lint=True,
        )

    def _add_test_yaml_cells(self):
        self.add_markdown_cell(
            """\
# Test Your Datasource Configuration
Here we will test your Datasource configuration to make sure it is valid.

This `test_yaml_config()` function is meant to enable fast dev loops. **If your
configuration is correct, this cell will show you some snippets of the data
assets in the data source.** You can continually edit your Datasource config
yaml and re-run the cell to check until the new config is valid.

If you instead wish to use python instead of yaml to configure your Datasource,
you can use `context.add_datasource()` and specify all the required parameters."""
        )
        self.add_code_cell(
            "context.test_yaml_config(yaml_config=example_yaml)",
            lint=True,
        )

    def _add_save_datasource_cell(self):
        self.add_markdown_cell(
            """## Save Your Datasource Configuration
Here we will save your Datasource in your Data Context once you are satisfied with the configuration. Note that `overwrite_existing` defaults to False, but you may change it to True if you wish to overwrite. Please note that if you wish to include comments you must add them directly to your `great_expectations.yml`."""
        )
        self.add_code_cell(
            """sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)
context.list_datasources()""",
            lint=True,
        )
        self.add_markdown_cell("Now you can close this notebook and delete it!")

    def render(self) -> nbformat.NotebookNode:
        self._notebook: nbformat.NotebookNode = nbformat.v4.new_notebook()
        self._add_header()
        self._add_docs_cell()
        if self.datasource_type == DatasourceTypes.SQL:
            self._add_sql_credentials_cell()
        self._add_template_cell()
        self._add_test_yaml_cells()
        self._add_save_datasource_cell()
        return self._notebook

    def render_to_disk(
        self,
        notebook_file_path: str,
    ) -> None:
        self.render()
        self.write_notebook_to_disk(self._notebook, notebook_file_path)
