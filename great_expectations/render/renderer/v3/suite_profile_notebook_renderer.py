from typing import Any, Dict, List, Optional, Union

import nbformat

from great_expectations import DataContext
from great_expectations.core.batch import (
    BatchRequest,
    standardize_batch_request_display_ordering,
)
from great_expectations.render.renderer.v3.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)
from great_expectations.util import deep_filter_properties_iterable


class SuiteProfileNotebookRenderer(SuiteEditNotebookRenderer):
    def __init__(
        self,
        context: DataContext,
        expectation_suite_name: str,
        profiler_name: str,
        batch_request: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> None:
        super().__init__(context=context)

        if batch_request is None:
            batch_request = {}

        deep_filter_properties_iterable(
            properties=batch_request,
            inplace=True,
        )
        batch_request = standardize_batch_request_display_ordering(
            batch_request=batch_request
        )

        self._batch_request = batch_request
        self._validator = context.get_validator(
            batch_request=BatchRequest(**batch_request),
            expectation_suite_name=expectation_suite_name,
        )

        self._profiler_name = profiler_name
        self._expectation_suite_name = self._validator.expectation_suite_name

    def render(self, **kwargs) -> nbformat.NotebookNode:
        self._notebook = nbformat.v4.new_notebook()

        self.add_header()

        if self._profiler_name:
            self._add_rule_based_profiler_cells()
        else:
            self._add_user_configurable_profiler_cells()

        self.add_footer()

        return self._notebook

    def render_to_disk(self, notebook_file_path: str, **kwargs) -> None:
        """
        Render a notebook to disk from an expectation suite.
        """
        self.render()
        self.write_notebook_to_disk(
            notebook=self._notebook, notebook_file_path=notebook_file_path
        )

    # noinspection PyMethodOverriding
    def add_header(self) -> None:
        self.add_markdown_cell(
            markdown=f"""# Initialize a new Expectation Suite by profiling a batch of your data.
This process helps you avoid writing lots of boilerplate when authoring suites by allowing you to select columns and other factors that you care about and letting a profiler write some candidate expectations for you to adjust.

**Expectation Suite Name**: `{self._expectation_suite_name}`
"""
        )

    def add_footer(
        self,
        batch_request: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> None:
        self.add_markdown_cell(
            markdown="""# Save & review your new Expectation Suite

Let's save the draft expectation suite as a JSON file in the
`great_expectations/expectations` directory of your project and rebuild the Data
 Docs site to make it easy to review your new suite."""
        )
        code_cell: str = """\
print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name
        }
    ]
}
checkpoint = SimpleCheckpoint(
    f"_tmp_checkpoint_{expectation_suite_name}",
    context,
    **checkpoint_config
)
checkpoint_result = checkpoint.run()

context.build_data_docs()

validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)
"""
        self.add_code_cell(code=code_cell, lint=True)
        self.add_markdown_cell(
            markdown=f"""## Next steps
After you review this initial Expectation Suite in Data Docs you
should edit this suite to make finer grained adjustments to the expectations.
This can be done by running `great_expectations suite edit {self._expectation_suite_name}`."""
        )

    def _add_user_configurable_profiler_cells(self) -> None:
        self.add_code_cell(
            code=f"""\
import datetime

import pandas as pd

import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
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
""",
            lint=True,
        )
        self.add_markdown_cell(
            markdown="""\
# Select columns

Select the columns on which you would like to set expectations and those which you would like to ignore.

Great Expectations will choose which expectations might make sense for a column based on the **data type** and **cardinality** of the data in each selected column.

Simply comment out columns that are important and should be included. You can select multiple lines and use a Jupyter
keyboard shortcut to toggle each line: **Linux/Windows**:
`Ctrl-/`, **macOS**: `Cmd-/`

Other directives are shown (commented out) as examples of the depth of control possible (see documentation for details).
"""
        )
        self._add_available_columns_list()
        self._add_profiler_instructions()
        self.add_code_cell(
            code="""\
data_assistant_result: DataAssistantResult = context.assistants.onboarding.run(
    batch_request=batch_request,
    # include_column_names=include_column_names,
    exclude_column_names=exclude_column_names,
    # include_column_name_suffixes=include_column_name_suffixes,
    # exclude_column_name_suffixes=exclude_column_name_suffixes,
    # semantic_type_filter_module_name=semantic_type_filter_module_name,
    # semantic_type_filter_class_name=semantic_type_filter_class_name,
    # include_semantic_types=include_semantic_types,
    # exclude_semantic_types=exclude_semantic_types,
    # allowed_semantic_types_passthrough=allowed_semantic_types_passthrough,
    cardinality_limit_mode="rel_100",  # case-insenstive (see documentaiton for other options)
    # max_unique_values=max_unique_values,
    # max_proportion_unique=max_proportion_unique,
    # column_value_uniqueness_rule={
    #     "success_ratio": 0.8,
    # },
    # column_value_nullity_rule={
    # },
    # column_value_nonnullity_rule={
    # },
    # numeric_columns_rule={
    #     "false_positive_rate": 0.1,
    #     "random_seed": 43792,
    # },
    # datetime_columns_rule={
    #     "truncate_values": {
    #         "lower_bound": 0,
    #         "upper_bound": 4481049600,  # Friday, January 1, 2112 0:00:00
    #     },
    #     "round_decimals": 0,
    # },
    # text_columns_rule={
    #     "strict_min": True,
    #     "strict_max": True,
    #     "success_ratio": 0.8,
    # },
    # categorical_columns_rule={
    #     "false_positive_rate": 0.1,
    #     "round_decimals": 3,
    # },
)
validator.expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
""",
            lint=True,
        )

    def _add_rule_based_profiler_cells(self) -> None:
        self.add_code_cell(
            code=f"""\
import datetime

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
""",
            lint=True,
        )
        self._add_profiler_instructions()
        self.add_code_cell(
            code=f"""\
result = context.run_profiler_with_dynamic_arguments(
    name="{self._profiler_name}",
    batch_request=batch_request,
)
validator.expectation_suite = result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
""",
            lint=True,
        )

    def _add_profiler_instructions(self) -> None:
        self.add_markdown_cell(
            markdown="""# Run the OnboardingDataAssistant

The suites generated here are **not meant to be production suites** -- they are **a starting point to build upon**.

**To get to a production-grade suite, you will definitely want to [edit this
suite](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_edit_an_expectation_suite_using_a_disposable_notebook.html?utm_source=notebook&utm_medium=profile_based_expectations)
after this initial step gets you started on the path towards what you want.**

This is highly configurable depending on your goals.
You can ignore columns, specify cardinality of categorical columns, configure semantic types for columns, even adjust thresholds and/or different estimator parameters, etc.
You can find more information about OnboardingDataAssistant and other DataAssistant components (please see documentation for the complete set of DataAssistant controls) [how to choose and control the behavior of the DataAssistant tailored to your goals](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.html).

"""
        )

    def _add_available_columns_list(self) -> None:
        column_names: List[str]
        column_name: str
        column_names = [
            f'    "{column_name}",\n' for column_name in self._validator.columns()
        ]
        code: str = f'exclude_column_names = [\n{"".join(column_names)}]'
        self.add_code_cell(code=code, lint=True)
