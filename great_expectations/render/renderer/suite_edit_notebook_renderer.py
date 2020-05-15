import os
from typing import Union

import nbformat
from great_expectations.core import ExpectationSuite
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.util import lint_code


class SuiteEditNotebookRenderer(Renderer):
    """
    Render a notebook that can re-create or edit a suite.

    Use cases:
    - Make an easy path to edit a suite that a Profiler created.
    - Make it easy to edit a suite where only JSON exists.
    """

    @classmethod
    def _get_expectations_by_column(cls, expectations):
        # TODO probably replace this with Suite logic at some point
        expectations_by_column = {"table_expectations": []}
        for exp in expectations:
            if "column" in exp["kwargs"]:
                col = exp["kwargs"]["column"]

                if col not in expectations_by_column.keys():
                    expectations_by_column[col] = []
                expectations_by_column[col].append(exp)
            else:
                expectations_by_column["table_expectations"].append(exp)

        return expectations_by_column

    @classmethod
    def _build_kwargs_string(cls, expectation):
        kwargs = []
        for k, v in expectation["kwargs"].items():
            if k == "column":
                # make the column a positional argument
                kwargs.append("'{}'".format(v))
            elif isinstance(v, str):
                # Put strings in quotes
                kwargs.append("{}='{}'".format(k, v))
            else:
                # Pass other types as is
                kwargs.append("{}={}".format(k, v))

        return ", ".join(kwargs)

    def add_header(self, suite_name: str, batch_kwargs) -> None:
        self.add_markdown_cell(
            """# Edit Your Expectation Suite
Use this notebook to recreate and modify your expectation suite:

**Expectation Suite Name**: `{}`

We'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)""".format(
                suite_name
            )
        )

        if not batch_kwargs:
            batch_kwargs = dict()
        self.add_code_cell(
            """\
from datetime import datetime
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier

context = ge.data_context.DataContext()

# Feel free to change the name of your suite here. Renaming this will not
# remove the other one.
expectation_suite_name = "{}"
suite = context.get_expectation_suite(expectation_suite_name)
suite.expectations = []

batch_kwargs = {}
batch = context.get_batch(batch_kwargs, suite)
batch.head()""".format(
                suite_name, batch_kwargs
            ),
            lint=True,
        )

    def add_footer(self) -> None:
        self.add_markdown_cell(
            """\
## Save & Review Your Expectations

Let's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.
If you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/module_docs/data_asset_module.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.data_asset.DataAsset.remove_expectation).

Let's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans."""
        )
        # TODO this may become confusing for users depending on what they are trying
        #  to accomplish in their dev loop
        self.add_code_cell(
            """\
batch.save_expectation_suite(discard_failed_expectations=False)

# Let's make a simple sortable timestamp. Note this could come from your pipeline runner.
run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch], run_id=run_id)
expectation_suite_identifier = list(results["details"].keys())[0]
validation_result_identifier = ValidationResultIdentifier(
    expectation_suite_identifier=expectation_suite_identifier,
    batch_identifier=batch.batch_kwargs.to_id(),
    run_id=run_id
)
context.build_data_docs()
context.open_data_docs(validation_result_identifier)"""
        )

    def add_code_cell(self, code: str, lint: bool = False) -> None:
        """
        Add the given code as a new code cell.
        """
        if lint:
            code = lint_code(code).rstrip("\n")

        cell = nbformat.v4.new_code_cell(code)
        self._notebook["cells"].append(cell)

    def add_markdown_cell(self, markdown: str) -> None:
        """
        Add the given markdown as a new markdown cell.
        """
        cell = nbformat.v4.new_markdown_cell(markdown)
        self._notebook["cells"].append(cell)

    def add_expectation_cells_from_suite(self, expectations):
        expectations_by_column = self._get_expectations_by_column(expectations)
        self.add_markdown_cell("### Table Expectation(s)")
        self._add_table_level_expectations(expectations_by_column)
        # Remove the table expectations since they are dealt with
        expectations_by_column.pop("table_expectations")
        self.add_markdown_cell("### Column Expectation(s)")
        self._add_column_level_expectations(expectations_by_column)

    def _add_column_level_expectations(self, expectations_by_column):
        if not expectations_by_column:
            self.add_markdown_cell(
                "No column level expectations are in this suite. Feel free to "
                "add some here. They all begin with `batch.expect_column_...`."
            )
            return

        for column, expectations in expectations_by_column.items():
            self.add_markdown_cell("#### `{}`".format(column))

            for exp in expectations:
                kwargs_string = self._build_kwargs_string(exp)
                meta_args = self._build_meta_arguments(exp.meta)
                code = "batch.{}({}{})".format(
                    exp["expectation_type"], kwargs_string, meta_args
                )
                self.add_code_cell(code, lint=True)

    def _add_table_level_expectations(self, expectations_by_column):
        if not expectations_by_column["table_expectations"]:
            self.add_markdown_cell(
                "No table level expectations are in this suite. Feel free to "
                "add some here. They all begin with `batch.expect_table_...`."
            )
            return

        for exp in expectations_by_column["table_expectations"]:
            kwargs_string = self._build_kwargs_string(exp)
            code = "batch.{}({})".format(exp["expectation_type"], kwargs_string)
            self.add_code_cell(code, lint=True)

    @staticmethod
    def _build_meta_arguments(meta):
        if not meta:
            return ""

        profiler = "BasicSuiteBuilderProfiler"
        if profiler in meta.keys():
            meta.pop(profiler)

        if meta.keys():
            return ", meta={}".format(meta)

        return ""

    @classmethod
    def write_notebook_to_disk(cls, notebook, notebook_file_path):
        with open(notebook_file_path, "w") as f:
            nbformat.write(notebook, f)

    def render(
        self, suite: ExpectationSuite, batch_kwargs=None
    ) -> nbformat.NotebookNode:
        """
        Render a notebook dict from an expectation suite.
        """
        if not isinstance(suite, ExpectationSuite):
            raise RuntimeWarning("render must be given an ExpectationSuite.")

        self._notebook = nbformat.v4.new_notebook()

        suite_name = suite.expectation_suite_name

        batch_kwargs = self.get_batch_kwargs(suite, batch_kwargs)
        self.add_header(suite_name, batch_kwargs)
        self.add_authoring_intro()
        self.add_expectation_cells_from_suite(suite.expectations)
        self.add_footer()

        return self._notebook

    def render_to_disk(
        self, suite: ExpectationSuite, notebook_file_path: str, batch_kwargs=None
    ) -> None:
        """
        Render a notebook to disk from an expectation suite.

        If batch_kwargs are passed they will override any found in suite
        citations.
        """
        self.render(suite, batch_kwargs)
        self.write_notebook_to_disk(self._notebook, notebook_file_path)

    def add_authoring_intro(self):
        self.add_markdown_cell(
            """\
## Create & Edit Expectations

Add expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.

You can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/expectation_glossary.html?utm_source=notebook&utm_medium=create_expectations)**."""
        )

    def get_batch_kwargs(
        self, suite: ExpectationSuite, batch_kwargs: Union[dict, BatchKwargs]
    ):
        if isinstance(batch_kwargs, dict):
            return self._fix_path_in_batch_kwargs(batch_kwargs)

        citations = suite.meta.get("citations")
        if not citations:
            return self._fix_path_in_batch_kwargs(batch_kwargs)

        citations = suite.get_citations(require_batch_kwargs=True)
        if not citations:
            return None

        citation = citations[-1]
        batch_kwargs = citation.get("batch_kwargs")
        return self._fix_path_in_batch_kwargs(batch_kwargs)

    @staticmethod
    def _fix_path_in_batch_kwargs(batch_kwargs):
        if isinstance(batch_kwargs, BatchKwargs):
            batch_kwargs = dict(batch_kwargs)
        if batch_kwargs and "path" in batch_kwargs.keys():
            base_dir = batch_kwargs["path"]
            if not os.path.isabs(base_dir):
                batch_kwargs["path"] = os.path.join("..", "..", base_dir)

        return batch_kwargs
