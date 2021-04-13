import copy
from typing import Dict, List, Optional

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor
from nbformat.notebooknode import NotebookNode

from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render.util import (
    num_to_str,
    resource_key_passes_run_name_filter,
)


def test_num_to_str():
    f = 0.99999999999999
    # We can round
    assert num_to_str(f, precision=4) == "≈1"
    # Specifying extra precision should not cause a problem
    assert num_to_str(f, precision=20) == "0.99999999999999"

    f = 1234567890.123456  # Our float can only hold 17 significant digits
    assert num_to_str(f, precision=4) == "≈1.235e+9"
    assert num_to_str(f, precision=20) == "1234567890.123456"
    assert num_to_str(f, use_locale=True, precision=40) == "1,234,567,890.123456"

    f = 1.123456789012345  # 17 sig digits mostly after decimal
    assert num_to_str(f, precision=5) == "≈1.1235"
    assert num_to_str(f, precision=20) == "1.123456789012345"

    f = 0.1  # A classic difficulty for floating point arithmetic
    assert num_to_str(f) == "0.1"
    assert num_to_str(f, precision=20) == "0.1"
    assert num_to_str(f, no_scientific=True) == "0.1"

    f = 1.23456789012345e-10  # significant digits can come late
    assert num_to_str(f, precision=20) == "1.23456789012345e-10"
    assert num_to_str(f, precision=5) == "≈1.2346e-10"
    assert (
        num_to_str(f, precision=20, no_scientific=True) == "0.000000000123456789012345"
    )
    assert num_to_str(f, precision=5, no_scientific=True) == "≈0.00000000012346"

    f = 100.0  # floats should have trailing digits and numbers stripped
    assert num_to_str(f, precision=10, no_scientific=True) == "100"
    assert num_to_str(f, precision=10) == "100"
    assert num_to_str(f, precision=10, use_locale=True) == "100"

    f = 100  # integers should never be stripped!
    assert num_to_str(f, precision=10, no_scientific=True) == "100"
    assert num_to_str(f, precision=10) == "100"
    assert num_to_str(f, precision=10, use_locale=True) == "100"

    f = 1000  # If we have a number longer than our precision, we should still be able to correctly format
    assert num_to_str(f, precision=4) == "1000"
    assert num_to_str(f) == "1000"


def test_resource_key_passes_run_name_filter():
    resource_key = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("test_suite"),
        run_id=RunIdentifier(run_name="foofooprofilingfoo"),
        batch_identifier="f14c3d2f6e8028c2db0c25edabdb0d61",
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"equals": "profiling"}
        )
        is False
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"equals": "foofooprofilingfoo"}
        )
        is True
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_equals": "profiling"}
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_equals": "foofooprofilingfoo"}
        )
        is False
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"includes": "profiling"}
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"includes": "foobar"}
        )
        is False
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_includes": "foobar"}
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_includes": "profiling"}
        )
        is False
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key,
            run_name_filter={"matches_regex": "(foo){2}profiling(" "foo)+"},
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key,
            run_name_filter={"matches_regex": "(foo){3}profiling(" "foo)+"},
        )
        is False
    )
    with pytest.warns(DeprecationWarning):
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"eq": "profiling"}
            )
            is False
        )
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"eq": "foofooprofilingfoo"}
            )
            is True
        )
    with pytest.warns(DeprecationWarning):
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"ne": "profiling"}
            )
            is True
        )
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"ne": "foofooprofilingfoo"}
            )
            is False
        )


def run_notebook(
    notebook_path: str,
    notebook_dir: str,
    string_to_be_replaced: Optional[str] = None,
    replacement_string: Optional[str] = None,
):
    if not notebook_path and notebook_dir:
        raise ValueError(
            "A path to and the directory containing the valid Jupyter notebook are required."
        )

    nb: NotebookNode = load_notebook_from_path(notebook_path=notebook_path)

    nb = replace_code_in_notebook(
        nb=nb,
        string_to_be_replaced=string_to_be_replaced,
        replacement_string=replacement_string,
    )

    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": notebook_dir}})


# noinspection PyShadowingNames
def replace_code_in_notebook(
    nb: NotebookNode,
    string_to_be_replaced: Optional[str] = None,
    replacement_string: Optional[str] = None,
) -> Optional[NotebookNode]:
    cond_neither: bool = string_to_be_replaced is None and replacement_string is None
    cond_both: bool = not (string_to_be_replaced is None or replacement_string is None)
    if not (cond_neither or cond_both):
        raise ValueError(
            "Either both or neither of the string replacement arguments (to/from) are required."
        )

    if (
        nb is None
        or not nb
        or "cells" not in nb
        or not nb["cells"]
        or len(nb["cells"]) == 0
    ):
        return None

    idx: int
    cell: dict

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=nb, search_string=string_to_be_replaced
    )

    if cells_of_interest_dict is None:
        return None

    for idx, cell in cells_of_interest_dict.items():
        cell["source"] = cell["source"].replace(
            string_to_be_replaced, replacement_string
        )

    nb["cells"] = list(
        filter(
            lambda cell: not (
                (cell["cell_type"] == "code")
                and (cell["source"].find(string_to_be_replaced) != -1)
            ),
            nb["cells"],
        )
    )

    for idx in cells_of_interest_dict.keys():
        nb["cells"].insert(idx, cells_of_interest_dict[idx])

    return nb


def load_notebook_from_path(
    notebook_path: str,
) -> NotebookNode:
    if not notebook_path:
        raise ValueError("A path to the valid Jupyter notebook is required.")

    nb: NotebookNode
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    return nb


# noinspection PyShadowingNames
def find_code_in_notebook(
    nb: NotebookNode,
    search_string: str,
) -> Optional[Dict[int, dict]]:
    if (
        nb is None
        or not nb
        or "cells" not in nb
        or not nb["cells"]
        or len(nb["cells"]) == 0
    ):
        return None

    idx: int
    cell: dict

    indices: List[int] = [
        idx
        for idx, cell in enumerate(nb["cells"])
        if (
            (cell["cell_type"] == "code") and (cell["source"].find(search_string) != -1)
        )
    ]

    if len(indices) == 0:
        return None

    cells_of_interest_dict: Dict[int, dict] = {
        idx: copy.deepcopy(nb["cells"][idx]) for idx in indices
    }

    return cells_of_interest_dict
