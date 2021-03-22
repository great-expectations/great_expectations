import nbformat

from great_expectations import DataContext
from great_expectations.datasource.types import DatasourceTypes
from great_expectations.render.renderer.notebook_renderer import BaseNotebookRenderer


class DatasourceNewNotebookRenderer(BaseNotebookRenderer):
    _DATASOURCE_TYPE_MAPPING = {
        DatasourceTypes.PANDAS.value: "PandasDatasource",
        DatasourceTypes.SPARK.value: "SparkDFDatasource,",
        DatasourceTypes.SQL.value: "SimpleSqlalchemyDatasource",
    }

    # TODO taylor deal with sql credentials
    SQL_DOCS = """
### For SQL based Datasources:

Here we are creating an example configuration using SimpleSqlalchemyDatasource based on the credentials you supplied in the CLI.

The credentials will be saved in `uncommitted/config_variables.yml`"""

    FILES_DOCS = """
### For files based Datasources:
Here we are creating an example configuration using an InferredAssetDataConnector which will add a Data Asset for each file in the base directory you provided. This is just a sample, you may customize this as you wish!

See our docs for other methods to organize assets, handle multi-file assets, name assets based on parts of a filename, etc."""

    DOCS_INTRO = f"""## Customize Your Datasource Configuration

**If you are new to Great Expectations Datasources,** you should check out our [how-to documentation](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html)

**My configuration is not so simple - are there more advanced options?**

Glad you asked! Datasources are versatile. Please see our [How To Guides](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html)!"""

    def __init__(
        self,
        context: DataContext,
        datasource_name: str,
        datasource_type: DatasourceTypes,
        datasource_yaml: str,
    ):
        super().__init__(context=context)
        self.datasource_name = datasource_name
        self.datasource_type = datasource_type
        self.datasource_yaml = datasource_yaml
        self.datasource_class = self._DATASOURCE_TYPE_MAPPING[
            self.datasource_type.value
        ]

    def _add_header(self):
        self.add_markdown_cell(
            f"""# Create a new {self.datasource_type.value} Datasource
Use this notebook to configure a new {self.datasource_type.value} Datasource and add it to your project.

**Datasource Name**: `{self.datasource_name}`"""
        )
        self.add_code_cell(
            """from ruamel.yaml import YAML
import great_expectations as ge
yaml = YAML()
context = ge.get_context()""",
        )

    def _add_docs_cell(self):
        docs = self.DOCS_INTRO
        if self.datasource_type in [DatasourceTypes.PANDAS, DatasourceTypes.SPARK]:
            docs += self.FILES_DOCS
        elif self.datasource_type == DatasourceTypes.SQL:
            docs += self.SQL_DOCS

        self.add_markdown_cell(docs)

    def _add_template_cell(self):
        self.add_code_cell(
            f'''example_yaml = """
{self.datasource_yaml}"""
print(example_yaml)
''',
            lint=True,
        )

    def _add_test_yaml_cells(self):
        self.add_markdown_cell(
            """# Test Your Datasource Configuration
Here we will test your Datasource configuration to make sure it is valid.

This `test_yaml_config()` function is meant to enable fast dev loops. You can continually edit your Datasource config yaml and re-run the cell to check until the new config is valid.

If you instead wish to use python instead of yaml to configure your Datasource, you can use `context.add_datasource()` and specify all the required parameters."""
        )
        self.add_code_cell(
            f"""context.test_yaml_config(example_yaml, name="{self.datasource_name}")""",
            lint=True,
        )

    def _add_save_datasource_cell(self):
        self.add_markdown_cell(
            """## Save Your Datasource Configuration
Here we will save your Datasource in your Data Context once you are satisfied with the configuration. Note that saving comments via `context.add_datasource()` is not yet fully supported, please modify your `great_expectations.yml` config if you wish to add comments."""
        )
        self.add_code_cell(
            f"""context.add_datasource(name="{self.datasource_name}", **yaml.load(example_yaml))
context.list_datasources()""",
            lint=True,
        )
        self.add_markdown_cell("""Now you can close this notebook and delete it!""")

    def render(self) -> nbformat.NotebookNode:
        self._notebook: nbformat.NotebookNode = nbformat.v4.new_notebook()
        self._add_header()
        self._add_docs_cell()
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
