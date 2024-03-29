Use the information provided here to install GX on an EMR Spark cluster and instantiate a Data Context without a full configuration directory.

## Additional prerequisites

- An EMR Spark cluster.
- Access to the EMR Spark notebook.

## Installation and setup

1. To install Great Expectations on your EMR Spark cluster copy this code snippet into a cell in your EMR Spark notebook and then run it:

  ```python title="Python"
  sc.install_pypi_package("great_expectations")
  ```
2. Create an in-code Data Context. See [Instantiate an Ephemeral Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=ephemeral#initialize-a-new-data-context).

3. Copy the Python code at the end of **How to instantiate an Ephemeral Data Context** into a cell in your EMR Spark notebook, or use the other examples to customize your configuration. The code instantiates and configures a Data Context for an EMR Spark cluster.

4. Execute the cell with your Data Context initialization and configuration.

5. Run the following command to verify that GX was installed and your in-memory Data Context was instantiated successfully:

  ```python title="Python"
      context.list_datasources()
   ```