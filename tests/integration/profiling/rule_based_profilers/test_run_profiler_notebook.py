import os
import shutil

import nbformat
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor

from great_expectations.data_context.util import file_relative_path


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

    # clean up Expectations directory after running test
    shutil.rmtree(os.path.join(base_dir, "great_expectations/expectations/tmp"))
    os.remove(
        os.path.join(base_dir, "great_expectations/expectations/.ge_store_backend_id")
    )
