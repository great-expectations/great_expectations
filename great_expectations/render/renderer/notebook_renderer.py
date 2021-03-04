from typing import Optional

import nbformat

from great_expectations import DataContext
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.util import lint_code


class BaseNotebookRenderer(Renderer):
    """
    Abstract base class for methods that help with rendering a jupyter notebook.
    """

    def __init__(self, context: Optional[DataContext] = None):
        super().__init__()
        self.context = context
        # Add cells to this notebook, then render by implementing a
        # self.render() and/or self.render_to_disk() method(s):
        self._notebook: Optional[nbformat.NotebookNode] = None

    def add_code_cell(self, code: str, lint: bool = False) -> None:
        """
        Add the given code as a new code cell.
        Args:
            code: Code to render into the notebook cell
            lint: Whether to lint the code before adding it

        Returns:
            Nothing, adds a cell to the class instance notebook
        """
        if lint:
            code: str = lint_code(code).rstrip("\n")

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

    def render(self):
        """
        Render a notebook from parameters.
        """
        # Implementation example from `suite edit`:

        # def render(
        #         self, suite: ExpectationSuite, batch_kwargs=None
        # ) -> nbformat.NotebookNode:
        #     """
        #     Render a notebook dict from an expectation suite.
        #     """
        # Check for errors in parameters, get values to be used
        #     if not isinstance(suite, ExpectationSuite):
        #         raise RuntimeWarning("render must be given an ExpectationSuite.")
        #
        #     self._notebook = nbformat.v4.new_notebook()
        #
        #     suite_name = suite.expectation_suite_name
        #
        #     batch_kwargs = self.get_batch_kwargs(suite, batch_kwargs)
        # Add cells
        #     self.add_header(suite_name, batch_kwargs)
        #     self.add_authoring_intro()
        #     self.add_expectation_cells_from_suite(suite.expectations)
        #     self.add_footer()
        #
        # Return notebook
        #     return self._notebook
        raise NotImplementedError

    def render_to_disk(
        self,
        notebook_file_path: str,
    ) -> None:
        """
        Render a notebook to disk from arguments
        """
        # Implementation example from `suite edit`:

        # def render_to_disk(
        #         self, suite: ExpectationSuite, notebook_file_path: str, batch_kwargs=None
        # ) -> None:
        #     """
        #     Render a notebook to disk from an expectation suite.
        #
        #     If batch_kwargs are passed they will override any found in suite
        #     citations.
        #     """
        #     self.render(suite, batch_kwargs)
        #     self.write_notebook_to_disk(self._notebook, notebook_file_path)
        raise NotImplementedError
