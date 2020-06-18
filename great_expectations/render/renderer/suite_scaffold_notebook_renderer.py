import nbformat

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import Dataset
from great_expectations.render.renderer.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)


class SuiteScaffoldNotebookRenderer(SuiteEditNotebookRenderer):
    def __init__(self, context: DataContext, suite: ExpectationSuite, batch_kwargs):
        self.context = context
        self.suite = suite
        self.suite_name = suite.expectation_suite_name
        self.batch_kwargs = self.get_batch_kwargs(self.suite, batch_kwargs)
        self.batch = self.load_batch()
        super().__init__()

    def add_header(self):
        self.add_markdown_cell(
            """# Scaffold a new Expectation Suite (Experimental)
This process helps you avoid writing lots of boilerplate when authoring suites by allowing you to select columns you care about and letting a profiler write some candidate expectations for you to adjust.

**Expectation Suite Name**: `{}`

We'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)""".format(
                self.suite_name
            )
        )

        if not self.batch_kwargs:
            self.batch_kwargs = dict()
        self.add_code_cell(
            """\
import datetime
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.profile import BasicSuiteBuilderProfiler
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier

context = ge.data_context.DataContext()

expectation_suite_name = "{}"
suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

batch_kwargs = {}
batch = context.get_batch(batch_kwargs, suite)
batch.head()""".format(
                self.suite_name, self.batch_kwargs
            ),
            lint=True,
        )

    def _add_scaffold_column_list(self):
        columns = [f"    # '{col}'" for col in self.batch.get_table_columns()]
        columns = ",\n".join(columns)
        code = f"""\
included_columns = [
{columns}
]"""
        self.add_code_cell(code, lint=True)

    def add_footer(self):
        self.add_markdown_cell(
            """## Save & review the scaffolded Expectation Suite

Let's save the scaffolded expectation suite as a JSON file in the
`great_expectations/expectations` directory of your project and rebuild the Data
 Docs site to make reviewing the scaffolded suite easy."""
        )
        self.add_code_cell(
            """\
context.save_expectation_suite(suite, expectation_suite_name)

\"""
Let's create a run_id. The run_id must be of type RunIdentifier, with optional run_name and run_time instantiation
arguments (or a dictionary with these keys). The run_name can be any string (this could come from your pipeline
runner, e.g. Airflow run id). The run_time can be either a dateutil parsable string or a datetime object.
Note - any provided datetime will be assumed to be a UTC time. If no instantiation arguments are given, run_name will
be None and run_time will default to the current UTC datetime.
\"""

run_id = {
  "run_name": "some_string_that_uniquely_identifies_this_run",  # insert your own run_name here
  "run_time": datetime.datetime.now(datetime.timezone.utc)
}

results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch], run_id=run_id)
validation_result_identifier = results.list_validation_result_identifiers()[0]
context.build_data_docs()
context.open_data_docs(validation_result_identifier)"""
        )
        self.add_markdown_cell(
            f"""## Next steps
After you are happy with this scaffolded Expectation Suite in Data Docs you
should edit this suite to make finer grained adjustments to the expectations.
This is be done by running `great_expectations suite edit {self.suite_name}`."""
        )

    def load_batch(self):
        batch = self.context.get_batch(self.batch_kwargs, self.suite)
        assert isinstance(
            batch, Dataset
        ), "Batch failed to load. Please check your batch_kwargs"
        return batch

    def render(self, batch_kwargs=None, **kwargs) -> nbformat.NotebookNode:
        self._notebook = nbformat.v4.new_notebook()
        self.add_header()
        self.add_markdown_cell(
            """## Select the columns you want to scaffold expectations on

Great Expectations will choose which expectations might make sense for a column based on the **data type** and **cardinality** of the data in each selected column.

Simply uncomment columns that are important. You can select multiple lines and
use a jupyter keyboard shortcut to toggle each line: **Linux/Windows**:
`Ctrl-/`, **macOS**: `Cmd-/`"""
        )
        self._add_scaffold_column_list()
        # TODO probably more explanation here about the workflow
        self.add_markdown_cell(
            """## Run the scaffolder

The suites generated here are **not meant to be production suites** - they are **scaffolds to build upon**.

**To get to a production grade suite, will definitely want to [edit this
suite](http://docs.greatexpectations.io/en/latest/command_line.html#great-expectations-suite-edit)
after scaffolding gets you close to what you want.**

This is highly configurable depending on your goals. You can include or exclude
columns, and include or exclude expectation types (when applicable). [The
Expectation Glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=scaffold_expectations)
contains a list of possible expectations."""
        )
        self._add_scaffold_cell()
        self.add_footer()
        return self._notebook

    def render_to_disk(self, notebook_file_path: str) -> None:
        """
        Render a notebook to disk from an expectation suite.

        If batch_kwargs are passed they will override any found in suite
        citations.
        """
        self.render(self.batch_kwargs)
        self.write_notebook_to_disk(self._notebook, notebook_file_path)

    def _add_scaffold_cell(self):
        self.add_code_cell(
            """\
# Wipe the suite clean to prevent unwanted expectations on the batch
suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
batch = context.get_batch(batch_kwargs, suite)

scaffold_config = {
    "included_columns": included_columns,
    # "excluded_columns": [],
    # "included_expectations": [],
    # "excluded_expectations": [],
}
suite, evr = BasicSuiteBuilderProfiler().profile(batch, profiler_configuration=scaffold_config)"""
        )
