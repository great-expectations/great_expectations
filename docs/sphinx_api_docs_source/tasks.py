import logging
import os
import pathlib
from typing import Union

import invoke

from build_sphinx_api_docs import SphinxInvokeDocsBuilder
from scripts import public_api_report

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


@invoke.task
def docs(ctx):
    """Build documentation. Note: Currently only builds the sphinx based api docs, please build docusaurus docs separately."""
    sphinx_api_docs_source_dir = pathlib.Path(__file__).parent

    _exit_with_error_if_not_run_from_correct_dir(
        task_name="docs", correct_dir=sphinx_api_docs_source_dir
    )

    doc_builder = SphinxInvokeDocsBuilder(ctx=ctx, base_path=sphinx_api_docs_source_dir)

    doc_builder.build_docs()


@invoke.task
def public_api(ctx):
    """Generate a report to determine the state of our Public API. Lists classes, methods and functions that are used in examples in our documentation, and any manual includes or excludes (see public_api_report.py). Items listed when generating this report need the @public_api decorator (and a good docstring) or to be excluded from consideration if they are not applicable to our Public API."""

    sphinx_api_docs_source_dir = pathlib.Path(__file__).parent

    _exit_with_error_if_not_run_from_correct_dir(
        task_name="public_api", correct_dir=sphinx_api_docs_source_dir
    )

    public_api_report.main()


def _exit_with_error_if_not_run_from_correct_dir(
    task_name: str, correct_dir: Union[pathlib.Path, None] = None
) -> None:
    """Exit if the command was not run from the correct directory."""
    if not correct_dir:
        correct_dir = pathlib.Path(
            os.path.realpath(os.path.dirname(os.path.realpath(__file__)))
        )
    curdir = pathlib.Path(os.path.realpath(os.getcwd()))
    exit_message = f"The {task_name} task must be invoked from the same directory as the tasks.py file."
    if correct_dir != curdir:
        raise invoke.Exit(
            exit_message,
            code=1,
        )
