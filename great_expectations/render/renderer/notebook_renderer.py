import nbformat

from great_expectations.render.renderer.renderer import Renderer


class NotebookRenderer(Renderer):
    """
    Render a notebook that can re-create or edit the suite.

    Possible use cases:
    - Make an easy path to edit a suite that a Profiler created.
    - Make it easy to edit a suite where only JSON exists.
    """

    def __init__(self, data_asset_name=None, suite_name=None):
        self.data_asset_name = data_asset_name
        self.suite_name = suite_name

    @classmethod
    def _get_expectations_by_column(cls, suite):
        expectations_by_column = {"table_expectations": []}
        for exp in suite["expectations"]:
            if "_table_" in exp["expectation_type"]:
                expectations_by_column["table_expectations"].append(exp)
            else:
                col = exp["kwargs"]["column"]

                if col not in expectations_by_column.keys():
                    expectations_by_column[col] = []
                expectations_by_column[col].append(exp)
        return expectations_by_column

    @classmethod
    def _build_kwargs_string(cls, expectation):
        kwargs = []
        for k, v in expectation["kwargs"].items():
            if k == "column":
                # make the column a positional argument
                kwargs.append(f"'{v}'")
            elif isinstance(v, str):
                # Put strings in quotes
                kwargs.append(f"{k}='{v}'")
            else:
                # Pass other types as is
                kwargs.append(f"{k}={v}")

        return ", ".join(kwargs)

    def _create_new_notebook(self):
        nb = nbformat.v4.new_notebook()
        # TODO better wording in the intro cells
        title_cell = nbformat.v4.new_markdown_cell(
            f"""# Create & Edit Expectation Suite
### Data Asset: `{self.data_asset_name}`
### Expecation Suite: `{self.suite_name}`

Use this notebook to recreate and modify your expectation suite.
"""
        )
        nb["cells"].append(title_cell)

        # TODO deal with hard coded project root and batch_kwargs situation - they may have to be backend specific.
        # TODO deal with hard coded project root and batch_kwargs situation - they may have to be backend specific.
        # TODO deal with hard coded project root and batch_kwargs situation - they may have to be backend specific.
        # TODO deal with hard coded project root and batch_kwargs situation - they may have to be backend specific.
        # TODO deal with hard coded project root and batch_kwargs situation - they may have to be backend specific.
        imports_cell = nbformat.v4.new_code_cell(
            """\
import great_expectations as ge

project_root = "/Users/taylor/repos/demo_public_data_test/great_expectations"
context = ge.data_context.DataContext(project_root)
# context.get_available_data_asset_names()"""
        )
        nb["cells"].append(imports_cell)

        batch_cell = nbformat.v4.new_code_cell(
            """\
# If you would like to validate an entire table or view in your database's default schema:
batch_kwargs = {'table': "YOUR_TABLE"}

# If you would like to validate an entire table or view from a non-default schema in your database:
batch_kwargs = {'table': "staging_npi", "schema": "pre_prod_staging"}

batch = context.get_batch(\""""
            + self.data_asset_name
            + '", "'
            + self.suite_name
            + '", batch_kwargs)'
        )
        nb["cells"].append(batch_cell)
        return nb

    @classmethod
    def _convert_expectation_suite_to_notebook_cells(cls, suite):
        cells = []
        expectations_by_column = cls._get_expectations_by_column(suite)
        md_cell = nbformat.v4.new_markdown_cell(f"## Table Expectation(s)")
        cells.append(md_cell)
        if expectations_by_column["table_expectations"]:
            for exp in expectations_by_column["table_expectations"]:
                kwargs_string = cls._build_kwargs_string(exp)
                code_cell = nbformat.v4.new_code_cell(
                    f"batch.{exp['expectation_type']}({kwargs_string})"
                )
                cells.append(code_cell)
        else:
            md_cell = nbformat.v4.new_markdown_cell(
                "No table level expectations are in this suite. Feel free to add some."
            )
            cells.append(md_cell)

        # Remove the table expectations since they are dealt with
        expectations_by_column.pop("table_expectations")

        md_cell = nbformat.v4.new_markdown_cell("## Column Expectation(s)")
        cells.append(md_cell)

        for column, expectations in expectations_by_column.items():
            md_cell = nbformat.v4.new_markdown_cell(f"### `{column}`")
            cells.append(md_cell)

            for exp in expectations:
                kwargs_string = cls._build_kwargs_string(exp)
                code_cell = nbformat.v4.new_code_cell(
                    f"batch.{exp['expectation_type']}({kwargs_string})"
                )
                cells.append(code_cell)
        return cells

    @classmethod
    def _write_notebook_to_disk(cls, notebook, notebook_file_path):
        with open(notebook_file_path, "w") as f:
            nbformat.write(notebook, f)

    def render(self, suite):
        """
        Render a notebook dict from an expectation suite in dict form.

        :type suite: dict
        """
        self.suite_name = suite["expectation_suite_name"]
        self.data_asset_name = suite["data_asset_name"]
        notebook = self._create_new_notebook()
        notebook["cells"] = notebook[
            "cells"
        ] + self._convert_expectation_suite_to_notebook_cells(suite)
        return notebook

    def render_to_disk(self, suite, notebook_file_path):
        """
        Create a notebook on disk from an expectation suite in dict form.

        :type suite: dict
        :type notebook_file_path: str
        """
        notebook = self.render(suite)
        self._write_notebook_to_disk(notebook, notebook_file_path)
