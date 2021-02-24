import nbformat

from great_expectations import DataContext
from great_expectations.render.renderer.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)


class CheckpointNewNotebookRenderer(SuiteEditNotebookRenderer):
    def __init__(self, context: DataContext, checkpoint_name: str):
        super().__init__(context=context)
        self.checkpoint_name = checkpoint_name

    def add_header(self):
        self.add_markdown_cell(
            f"""# Create Your Checkpoint
Use this notebook to create your checkpoint:
**Checkpoint Name**: `{self.checkpoint_name}`
We'd love it if you'd **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)!"""
        )

    def render(self) -> nbformat.NotebookNode:
        self._notebook = nbformat.v4.new_notebook()
        self.add_header()

        return self._notebook

    def render_to_disk(
        self,
        notebook_file_path: str,
    ) -> None:
        self.render()
        self.write_notebook_to_disk(self._notebook, notebook_file_path)
