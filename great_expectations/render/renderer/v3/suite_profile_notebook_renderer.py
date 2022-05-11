
from typing import Any, Dict, List, Union
import nbformat
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest, standardize_batch_request_display_ordering
from great_expectations.render.renderer.suite_edit_notebook_renderer import SuiteEditNotebookRenderer
from great_expectations.util import deep_filter_properties_iterable

class SuiteProfileNotebookRenderer(SuiteEditNotebookRenderer):

    def __init__(self, context: DataContext, expectation_suite_name: str, profiler_name: str, batch_request: Union[(str, Dict[(str, Union[(str, int, Dict[(str, Any)])])])]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(context=context)
        if (batch_request is None):
            batch_request = {}
        deep_filter_properties_iterable(properties=batch_request, inplace=True)
        batch_request = standardize_batch_request_display_ordering(batch_request=batch_request)
        self._batch_request = batch_request
        self._validator = context.get_validator(batch_request=BatchRequest(**batch_request), expectation_suite_name=expectation_suite_name)
        self._profiler_name = profiler_name
        self._expectation_suite_name = self._validator.expectation_suite_name

    def render(self) -> nbformat.NotebookNode:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._notebook = nbformat.v4.new_notebook()
        self.add_header()
        if self._profiler_name:
            self._add_rule_based_profiler_cells()
        else:
            self._add_user_configurable_profiler_cells()
        self.add_footer()
        return self._notebook

    def render_to_disk(self, notebook_file_path: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Render a notebook to disk from an expectation suite.\n        '
        self.render()
        self.write_notebook_to_disk(notebook=self._notebook, notebook_file_path=notebook_file_path)

    def add_header(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.add_markdown_cell(markdown=f'''# Initialize a new Expectation Suite by profiling a batch of your data.
This process helps you avoid writing lots of boilerplate when authoring suites by allowing you to select columns and other factors that you care about and letting a profiler write some candidate expectations for you to adjust.

**Expectation Suite Name**: `{self._expectation_suite_name}`
''')

    def add_footer(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.add_markdown_cell(markdown="# Save & review your new Expectation Suite\n\nLet's save the draft expectation suite as a JSON file in the\n`great_expectations/expectations` directory of your project and rebuild the Data\n Docs site to make it easy to review your new suite.")
        code_cell: str = 'print(validator.get_expectation_suite(discard_failed_expectations=False))\nvalidator.save_expectation_suite(discard_failed_expectations=False)\n\ncheckpoint_config = {\n    "class_name": "SimpleCheckpoint",\n    "validations": [\n        {\n            "batch_request": batch_request,\n            "expectation_suite_name": expectation_suite_name\n        }\n    ]\n}\ncheckpoint = SimpleCheckpoint(\n    f"_tmp_checkpoint_{expectation_suite_name}",\n    context,\n    **checkpoint_config\n)\ncheckpoint_result = checkpoint.run()\n\ncontext.build_data_docs()\n\nvalidation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]\ncontext.open_data_docs(resource_identifier=validation_result_identifier)\n'
        self.add_code_cell(code=code_cell, lint=True)
        self.add_markdown_cell(markdown=f'''## Next steps
After you review this initial Expectation Suite in Data Docs you
should edit this suite to make finer grained adjustments to the expectations.
This can be done by running `great_expectations suite edit {self._expectation_suite_name}`.''')

    def _add_user_configurable_profiler_cells(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.add_code_cell(code=f'''import datetime

import pandas as pd

import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

context = ge.data_context.DataContext()

batch_request = {self._batch_request}

expectation_suite_name = "{self._expectation_suite_name}"

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)
column_names = [f'"{{column_name}}"' for column_name in validator.columns()]
print(f"Columns: {{', '.join(column_names)}}.")
validator.head(n_rows=5, fetch_all=False)
''', lint=True)
        self.add_markdown_cell(markdown='# Select columns\n\nSelect the columns on which you would like to set expectations and those which you would like to ignore.\n\nGreat Expectations will choose which expectations might make sense for a column based on the **data type** and **cardinality** of the data in each selected column.\n\nSimply comment out columns that are important and should be included. You can select multiple lines and\nuse a jupyter keyboard shortcut to toggle each line: **Linux/Windows**:\n`Ctrl-/`, **macOS**: `Cmd-/`')
        self._add_available_columns_list()
        self._add_profiler_instructions()
        self.add_code_cell(code='profiler = UserConfigurableProfiler(\n    profile_dataset=validator,\n    excluded_expectations=None,\n    ignored_columns=ignored_columns,\n    not_null_only=False,\n    primary_or_compound_key=False,\n    semantic_types_dict=None,\n    table_expectations_only=False,\n    value_set_threshold="MANY",\n)\nsuite = profiler.build_suite()', lint=True)

    def _add_rule_based_profiler_cells(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.add_code_cell(code=f'''import datetime

import pandas as pd

import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

context = ge.data_context.DataContext()

batch_request = {self._batch_request}

expectation_suite_name = "{self._expectation_suite_name}"

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)
validator.head(n_rows=5, fetch_all=False)
''', lint=True)
        self._add_profiler_instructions()
        self.add_code_cell(code=f'''suite = context.run_profiler_with_dynamic_arguments(
    name="{self._profiler_name}",
    expectation_suite=validator.expectation_suite
)
''', lint=True)

    def _add_profiler_instructions(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.add_markdown_cell(markdown='# Run the data profiler\n\nThe suites generated here are **not meant to be production suites** -- they are **a starting point to build upon**.\n\n**To get to a production-grade suite, you will definitely want to [edit this\nsuite](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_edit_an_expectation_suite_using_a_disposable_notebook.html?utm_source=notebook&utm_medium=profile_based_expectations)\nafter this initial step gets you started on the path towards what you want.**\n\nThis is highly configurable depending on your goals.\nYou can ignore columns or exclude certain expectations, specify a threshold for creating value set expectations, or even specify semantic types for a given column.\nYou can find more information about [how to configure this profiler, including a list of the expectations that it uses, here.](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_create_an_expectation_suite_with_the_user_configurable_profiler.html)\n\n')

    def _add_available_columns_list(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        column_names: List[str]
        column_name: str
        column_names = [f'''    "{column_name}",
''' for column_name in self._validator.columns()]
        code: str = f'''ignored_columns = [
{''.join(column_names)}]'''
        self.add_code_cell(code=code, lint=True)
