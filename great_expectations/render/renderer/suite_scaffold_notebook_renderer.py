import nbformat

from great_expectations import DataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset import Dataset
from great_expectations.render.renderer.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)


class SuiteScaffoldNotebookRenderer(SuiteEditNotebookRenderer):
    def __init__(
        self, context: DataContext, suite: ExpectationSuite, batch_kwargs
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(context=context)
        self.suite = suite
        self.suite_name = suite.expectation_suite_name
        self.batch_kwargs = self.get_batch_kwargs(self.suite, batch_kwargs)
        self.batch = self.load_batch()

    def add_header(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.add_markdown_cell(
            f"""# Scaffold a new Expectation Suite (Experimental)
This process helps you avoid writing lots of boilerplate when authoring suites by allowing you to select columns and other factors that you care about and letting a profiler write some candidate expectations for you to adjust.

**Expectation Suite Name**: `{self.suite_name}`

We'd love it if you'd **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)!"""
        )
        if not self.batch_kwargs:
            self.batch_kwargs = {}
        self.add_code_cell(
            f"""import great_expectations as ge
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier

context = ge.data_context.DataContext()

expectation_suite_name = "{self.suite_name}"

# Wipe the suite clean to prevent unwanted expectations in the batch
suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

batch_kwargs = {self.batch_kwargs}
batch = context.get_batch(batch_kwargs, suite)
batch.head()""",
            lint=True,
        )

    def _add_scaffold_column_list(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        columns = [f"    '{col}'" for col in self.batch.get_table_columns()]
        columns = ",\n".join(columns)
        code = f"""ignored_columns = [
{columns}
]"""
        self.add_code_cell(code, lint=True)

    def add_footer(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.add_markdown_cell(
            "## Save & review the scaffolded Expectation Suite\n\nLet's save the scaffolded expectation suite as a JSON file in the\n`great_expectations/expectations` directory of your project and rebuild the Data\n Docs site to make it easy to review the scaffolded suite."
        )
        if self.context and self.context.validation_operators.get(
            "action_list_operator"
        ):
            code_cell = 'context.save_expectation_suite(suite, expectation_suite_name)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ],\n    validation_operator_name="action_list_operator"\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)'
        else:
            code_cell = 'context.save_expectation_suite(suite, expectation_suite_name)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)'
        self.add_code_cell(code_cell)
        self.add_markdown_cell(
            f"""## Next steps
After you review this scaffolded Expectation Suite in Data Docs you
should edit this suite to make finer grained adjustments to the expectations.
This can be done by running `great_expectations suite edit {self.suite_name}`."""
        )

    def load_batch(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch = self.context.get_batch(self.batch_kwargs, self.suite)
        assert isinstance(
            batch, Dataset
        ), "Batch failed to load. Please check your batch_kwargs"
        return batch

    def render(self, batch_kwargs=None, **kwargs) -> nbformat.NotebookNode:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._notebook = nbformat.v4.new_notebook()
        self.add_header()
        self.add_markdown_cell(
            "## Select the columns on which you would like to scaffold expectations and those which you would like to ignore.\n\nGreat Expectations will choose which expectations might make sense for a column based on the **data type** and **cardinality** of the data in each selected column.\n\nSimply comment out columns that are important and should be included. You can select multiple lines and\nuse a jupyter keyboard shortcut to toggle each line: **Linux/Windows**:\n`Ctrl-/`, **macOS**: `Cmd-/`"
        )
        self._add_scaffold_column_list()
        self.add_markdown_cell(
            "## Run the scaffolder\n\nThe suites generated here are **not meant to be production suites** - they are **scaffolds to build upon**.\n\n**To get to a production grade suite, you will definitely want to [edit this\nsuite](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_edit_an_expectation_suite_using_a_disposable_notebook.html?utm_source=notebook&utm_medium=scaffold_expectations)\nafter scaffolding gets you close to what you want.**\n\nThis is highly configurable depending on your goals.\nYou can ignore columns or exclude certain expectations, specify a threshold for creating value set expectations, or even specify semantic types for a given column.\nYou can find more information about [how to configure this profiler, including a list of the expectations that it uses, here.](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_create_an_expectation_suite_with_the_user_configurable_profiler.html)\n\n"
        )
        self._add_scaffold_cell()
        self.add_footer()
        return self._notebook

    def render_to_disk(self, notebook_file_path: str) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Render a notebook to disk from an expectation suite.\n\n        If batch_kwargs are passed they will override any found in suite\n        citations.\n        "
        self.render(self.batch_kwargs)
        self.write_notebook_to_disk(self._notebook, notebook_file_path)

    def _add_scaffold_cell(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.add_code_cell(
            'profiler = UserConfigurableProfiler(profile_dataset=batch,\n    ignored_columns=ignored_columns,\n    excluded_expectations=None,\n    not_null_only=False,\n    primary_or_compound_key=False,\n    semantic_types_dict=None,\n    table_expectations_only=False,\n    value_set_threshold="MANY",\n    )\n\nsuite = profiler.build_suite()'
        )
