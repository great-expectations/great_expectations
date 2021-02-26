import nbformat

from great_expectations import DataContext
from great_expectations.render.renderer.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)


class CheckpointNewNotebookRenderer(SuiteEditNotebookRenderer):
    def __init__(self, context: DataContext, checkpoint_name: str):
        super().__init__(context=context)
        self.context = context
        self.checkpoint_name = checkpoint_name
        self._notebook = None

    def _find_datasource_with_asset(self):
        datasource_candidate = None
        for datasource in self.context.list_datasources():
            for data_connector_name in datasource.get("data_connectors"):
                data_connector = datasource["data_connectors"][data_connector_name]
                if "assets" in data_connector:
                    datasource_candidate = {
                        "datasource_name": datasource["name"],
                        "data_connector_name": data_connector_name,
                        "asset_name": list(data_connector["assets"].keys())[0],
                    }
                    break
        return datasource_candidate

    def _add_header(self):
        self.add_markdown_cell(
            f"""# Create Your Checkpoint
Use this notebook to create your checkpoint:

**Checkpoint Name**: `{self.checkpoint_name}`

We'd love it if you'd **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)!"""
        )

    def _add_imports(self):
        self.add_code_cell(
            """import great_expectations as ge
context = ge.get_context()
""",
            lint=True,
        )

    def _add_example_configuration(self):
        self.add_markdown_cell(
            """# Example Configuration
**If you are new to Great Expectations or the Checkpoint feature**, you should probably start with SimpleCheckpoint because it includes default configurations like a default list of post validation actions.

The example in the cell below shows a SimpleCheckpoint for validating a single Batch of data against a single Expectation Suite.

**My configuration is not so simple - are there more advanced options?**

Glad you asked! Checkpoints are very versatile. For example, you can validate many batches in a single checkpoint, validate batches against different suites or against many suites, control the specific post-validation actions based on suite / batch / results of validation among other features. Check out our documentation on Checkpoints for more info:

- https://docs.greatexpectations.io/en/latest/reference/core_concepts/checkpoints_and_actions.html
- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint.html
- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html
        """
        )
        self.add_code_cell(
            (
                'example_config = """'
                """
name: my_checkpoint
config_version: 1
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_data_connector
      data_asset_name: MyDataAsset
      partition_request:
        index: -1
    expectation_suite_name: my_suite
"""
                '"""'
            ),
            lint=True,
        )

    def _add_optional_list_your_config(self):
        self.add_markdown_cell(
            """# List Your Configuration (Optional)
The following cells show examples for listing your current configuration.

You may wish to run these cells to view your currently configured checkpoints and choose a datasource & expectation suite."""
        )
        self.add_code_cell("context.list_checkpoints()")
        self.add_code_cell(
            """list_of_existing_datasources_by_name = [datasource["name"] for datasource in context.list_datasources()]
list_of_existing_datasources_by_name""",
            lint=True,
        )
        self.add_code_cell("context.list_expectation_suite_names()")

    def _add_sample_checkpoint_config(self):
        self.add_markdown_cell(
            """# Sample Checkpoint Config

In the cell below we have created a sample Checkpoint configuration using **your configuration** and **SimpleCheckpoint** to run a single validation of a single expectation suite against a single batch of data.

To keep it simple, we are just choosing the first Datasource, DataConnector, DataAsset, Partition and Expectation Suite you have configured to create the example yaml config.

Of course this is purely an example, you may edit this to your heart's content.

Please also see the docs linked below for instructions on how to implement other more advanced features including using the **Checkpoint** class:
- https://docs.greatexpectations.io/en/latest/reference/core_concepts/checkpoints_and_actions.html
- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint.html
- https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html"""
        )
        try:
            first_datasource_with_asset = self._find_datasource_with_asset()
            first_datasource_name = first_datasource_with_asset["datasource_name"]
            first_data_connector_name = first_datasource_with_asset[
                "data_connector_name"
            ]
            first_asset_name = first_datasource_with_asset["asset_name"]

            first_expectation_suite = self.context.list_expectation_suites()[0]
            first_expectation_suite_name = (
                first_expectation_suite.expectation_suite_name
            )
            sample_yaml_str = 'sample_yaml = """'
            sample_yaml_str += f"""
name: {self.checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: {first_datasource_name}
      data_connector_name: {first_data_connector_name}
      data_asset_name: {first_asset_name}
      partition_request:
        index: 0
    expectation_suite_name: {first_expectation_suite_name}
"""
            sample_yaml_str += '"""'
            sample_yaml_str += "\nprint(sample_yaml)"

            self.add_code_cell(
                sample_yaml_str,
                lint=True,
            )
        except:
            # For any error
            self.add_markdown_cell(
                "Sorry, we were unable to create a sample configuration. Perhaps you don't have a Datasource or Expectation Suite configured."
            )

    def _add_test_and_save_your_checkpoint_configuration(self):
        self.add_markdown_cell(
            """# Test the Checkpoint Configuration
Here we will test your Checkpoint configuration to make sure it is valid.

Note that if it is valid, it will be automatically saved to your Checkpoint Store.

This test_yaml_config() function is meant to enable fast dev loops. You can continually edit your Checkpoint config yaml and re-run the cell to check until the new config is valid.

If you instead wish to use python instead of yaml to configure your Checkpoint, you can always use context.add_checkpoint() and specify all the required parameters."""
        )
        self.add_code_cell(
            f'checkpoint_name = "{self.checkpoint_name}" # From your CLI command, feel free to change this.'
        )
        self.add_code_cell(
            "my_checkpoint_config = sample_yaml # Change `sample_yaml` to your custom Checkpoint config if you wish"
        )
        self.add_code_cell(
            """my_checkpoint = context.test_yaml_config(
    name=checkpoint_name,
    yaml_config=my_checkpoint_config
)""",
            lint=True,
        )

    def _add_review_checkpoint(self):
        self.add_markdown_cell(
            """# Review Checkpoint

You can run the following cell to print out the full yaml configuration. For example, if you used **SimpleCheckpoint**  this will show you the default action list."""
        )
        self.add_code_cell(
            "print(my_checkpoint.get_substituted_config().to_yaml_str())", lint=True
        )

    def _add_optional_run_checkpoint(self):
        self.add_markdown_cell(
            """# Run Checkpoint (Optional)

You may wish to run the checkpoint now to see a sample of it's output. If so run the following cell."""
        )
        self.add_code_cell(
            "context.run_checkpoint(checkpoint_name=checkpoint_name)", lint=True
        )

    def _add_optional_open_data_docs(self):
        self.add_markdown_cell(
            """# Open Data Docs (Optional)
You may also wish to open up Data Docs to review the results of the Checkpoint run if you ran the above cell."""
        )
        self.add_code_cell("context.open_data_docs()", lint=True)

    def render(self) -> nbformat.NotebookNode:
        self._notebook: nbformat.NotebookNode = nbformat.v4.new_notebook()
        self._add_header()
        self._add_imports()
        self._add_example_configuration()
        self._add_optional_list_your_config()
        self._add_sample_checkpoint_config()
        self._add_test_and_save_your_checkpoint_configuration()
        self._add_review_checkpoint()
        self._add_optional_run_checkpoint()
        self._add_optional_open_data_docs()

        return self._notebook

    def render_to_disk(
        self,
        notebook_file_path: str,
    ) -> None:
        self.render()
        self.write_notebook_to_disk(self._notebook, notebook_file_path)
