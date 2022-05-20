import logging
import os
import shutil
from typing import List, Optional

import nbformat
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor

from great_expectations.data_context.util import file_relative_path

logger = logging.getLogger(__name__)


def clean_up_test_files(paths: List[str]) -> None:
    """
    Helper method to clean-up the files created by tests
    Args:
        paths List(str): paths or directories to delete
    """
    for path in paths:
        if not os.path.exists(path):
            continue
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def test_run_rbp_notebook(tmp_path):
    """
    What does this test and why?

    One of the resources we have for RuleBaseProfiler is a Jupyter notebook that explains/shows the components in code.

    This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.

    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "BasicExample_RBP_Instantiation_and_running.ipynb"
    )

    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "BasicExample_RBP_Instantiation_and_running_executed.ipynb"
    )

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg = 'Error executing the notebook "%s".\n\n' % notebook_path
        msg += 'See notebook "%s" for the traceback.' % output_notebook_path
        print(msg)
        raise
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            os.path.join(base_dir, "great_expectations/expectations/tmp"),
            os.path.join(
                base_dir, "great_expectations/expectations/.ge_store_backend_id"
            ),
        ]
        clean_up_test_files(paths=paths_to_clean_up)


def test_run_data_assistants_notebook(tmp_path):
    """
    What does this test and why?

    One of the resources we have for DataAssistants is a Jupyter notebook that explains/shows the components in code.

    This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(
        base_dir, "DataAssistants_Instantiation_And_Running.ipynb"
    )
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "DataAssistants_Instantiation_and_running_executed.ipynb"
    )

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg = 'Error executing the notebook "%s".\n\n' % notebook_path
        msg += 'See notebook "%s" for the traceback.' % output_notebook_path
        print(msg)
        raise
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        paths_to_clean_up: List[str] = [
            os.path.join(base_dir, "great_expectations/expectations/tmp"),
            os.path.join(
                base_dir, "great_expectations/expectations/.ge_store_backend_id"
            ),
            os.path.join(
                base_dir, "great_expectations/expectations/taxi_data_suite.json"
            ),
        ]
        clean_up_test_files(paths=paths_to_clean_up)


def test_run_self_initializing_expectations_notebook(tmp_path):
    """
    What does this test and why?
    One of the resources we have for self-initializing Expectations is a Jupyter notebook that explains/shows the components in code.
    This test ensures the codepaths and examples described in the Notebook actually run and pass, nbconvert's
    `preprocess` function.
    """
    base_dir: str = file_relative_path(
        __file__, "../../../test_fixtures/rule_based_profiler/example_notebooks"
    )
    notebook_path: str = os.path.join(base_dir, "SelfInitializingExpectations.ipynb")
    # temporary output notebook for traceback and debugging
    output_notebook_path: str = os.path.join(
        tmp_path, "SelfInitializingExpectations_executed.ipynb"
    )

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": base_dir}})
    except CellExecutionError:
        msg = 'Error executing the notebook "%s".\n\n' % notebook_path
        msg += 'See notebook "%s" for the traceback.' % output_notebook_path
        print(msg)
        raise
    finally:
        with open(output_notebook_path, mode="w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        paths_to_clean_up: List[str] = [
            os.path.join(base_dir, "great_expectations/expectations/tmp"),
            os.path.join(
                base_dir, "great_expectations/expectations/.ge_store_backend_id"
            ),
            os.path.join(
                base_dir, "great_expectations/expectations/new_expectation_suite.json"
            ),
        ]
        clean_up_test_files(paths=paths_to_clean_up)
