import logging
import os
import pathlib
from typing import Union

import invoke

from build_sphinx_api_docs import SphinxInvokeDocsBuilder

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


@invoke.task()
def docs(
    ctx,
):
    """Build documentation. Note: Currently only builds the sphinx based api docs, please build docusaurus docs separately."""
    sphinx_api_docs_source_dir = pathlib.Path(
        os.path.realpath(os.path.dirname(os.path.realpath(__file__)))
    )

    _exit_with_error_if_not_run_from_correct_dir(
        task_name="docs", correct_dir=sphinx_api_docs_source_dir
    )

    doc_builder = SphinxInvokeDocsBuilder(ctx=ctx, base_path=sphinx_api_docs_source_dir)

    doc_builder.build_docs()


def _exit_with_error_if_not_run_from_correct_dir(
    task_name: str, correct_dir: Union[pathlib.Path, None] = None
) -> None:
    """Exit if the command was not run from the correct directory."""
    if not correct_dir:
        correct_dir = pathlib.Path(
            os.path.realpath(os.path.dirname(os.path.realpath(__file__)))
        )
    curdir = pathlib.Path(os.path.realpath(os.getcwd()))
    exit_message = f"The {task_name} task must be invoked from the same directory as the task.py file at the top of the repo."
    if correct_dir != curdir:
        raise invoke.Exit(
            exit_message,
            code=1,
        )
