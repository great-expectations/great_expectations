from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import nbformat

from great_expectations.render.renderer.renderer import Renderer
from great_expectations.util import (
    convert_json_string_to_be_python_compliant,
    lint_code,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class BaseNotebookRenderer(Renderer):
    """
    Abstract base class for methods that help with rendering a jupyter notebook.
    """

    def __init__(self, context: Optional[AbstractDataContext] = None) -> None:
        super().__init__()
        self.context = context
        # Add cells to this notebook, then render by implementing a
        # self.render() and/or self.render_to_disk() method(s):
        self._notebook: Optional[nbformat.NotebookNode] = None

    def add_code_cell(
        self, code: str, lint: bool = False, enforce_py_syntax: bool = True
    ) -> None:
        """
        Add the given code as a new code cell.
        Args:
            code: Code to render into the notebook cell
            lint: Whether to lint the code before adding it
            enforce_py_syntax: Directive to convert code to Python-compliant format

        Returns:
            Nothing, adds a cell to the class instance notebook
        """
        if enforce_py_syntax:
            code = convert_json_string_to_be_python_compliant(code)

        if lint:
            code = lint_code(code).rstrip("\n")

        cell = nbformat.v4.new_code_cell(code)
        self._notebook["cells"].append(cell)

    def add_markdown_cell(self, markdown: str) -> None:
        """
        Add the given markdown as a new markdown cell.
        Args:
            markdown: Code to render into the notebook cell

        Returns:
            Nothing, adds a cell to the class instance notebook
        """
        cell = nbformat.v4.new_markdown_cell(markdown)
        self._notebook["cells"].append(cell)

    @classmethod
    def write_notebook_to_disk(
        cls, notebook: nbformat.NotebookNode, notebook_file_path: str
    ) -> None:
        """
        Write a given Jupyter notebook to disk.
        Args:
            notebook: Jupyter notebook
            notebook_file_path: Location to write notebook
        """
        with open(notebook_file_path, "w") as f:
            nbformat.write(notebook, f)

    def render(self, **kwargs: dict) -> nbformat.NotebookNode:
        """
        Render a notebook from parameters.
        """
        raise NotImplementedError

    def render_to_disk(self, notebook_file_path: str, **kwargs: dict) -> None:
        """
        Render a notebook to disk from arguments.
        """
        raise NotImplementedError
