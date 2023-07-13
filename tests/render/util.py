import copy
from typing import Dict, List, Optional

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbformat.notebooknode import NotebookNode


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
