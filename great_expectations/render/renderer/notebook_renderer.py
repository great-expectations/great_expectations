
from typing import Optional
import nbformat
from great_expectations import DataContext
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.util import convert_json_string_to_be_python_compliant, lint_code

class BaseNotebookRenderer(Renderer):
    '\n    Abstract base class for methods that help with rendering a jupyter notebook.\n    '

    def __init__(self, context: Optional[DataContext]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__()
        self.context = context
        self._notebook: Optional[nbformat.NotebookNode] = None

    def add_code_cell(self, code: str, lint: bool=False, enforce_py_syntax: bool=True) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Add the given code as a new code cell.\n        Args:\n            code: Code to render into the notebook cell\n            lint: Whether to lint the code before adding it\n\n        Returns:\n            Nothing, adds a cell to the class instance notebook\n        '
        if enforce_py_syntax:
            code = convert_json_string_to_be_python_compliant(code)
        if lint:
            code = lint_code(code).rstrip('\n')
        cell = nbformat.v4.new_code_cell(code)
        self._notebook['cells'].append(cell)

    def add_markdown_cell(self, markdown: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Add the given markdown as a new markdown cell.\n        Args:\n            markdown: Code to render into the notebook cell\n\n        Returns:\n            Nothing, adds a cell to the class instance notebook\n        '
        cell = nbformat.v4.new_markdown_cell(markdown)
        self._notebook['cells'].append(cell)

    @classmethod
    def write_notebook_to_disk(cls, notebook: nbformat.NotebookNode, notebook_file_path: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Write a given Jupyter notebook to disk.\n        Args:\n            notebook: Jupyter notebook\n            notebook_file_path: Location to write notebook\n        '
        with open(notebook_file_path, 'w') as f:
            nbformat.write(notebook, f)

    def render(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Render a notebook from parameters.\n        '
        raise NotImplementedError

    def render_to_disk(self, notebook_file_path: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Render a notebook to disk from arguments\n        '
        raise NotImplementedError
