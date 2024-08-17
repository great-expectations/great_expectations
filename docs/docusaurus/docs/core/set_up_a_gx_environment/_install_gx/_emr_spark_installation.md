Use the information provided here to install GX on an EMR Spark cluster and instantiate a Data Context without a full configuration directory.

### Additional prerequisites

- An EMR Spark cluster.
- Access to the EMR Spark notebook.

### Installation and setup

1. To install GX on your EMR Spark cluster copy this code snippet into a cell in your EMR Spark notebook and then run it:

   ```python title="Python"
   sc.install_pypi_package("great_expectations")
   ```

2. Create an in-code Data Context. See [Instantiate an Ephemeral Data Context](/core/set_up_a_gx_environment/create_a_data_context.md?context_type=ephemeral).

3. Copy the Python code at the end of **How to instantiate an Ephemeral Data Context** into a cell in your EMR Spark notebook, or use the other examples to customize your configuration. The code instantiates and configures a Data Context for an EMR Spark cluster.

4. Execute the cell with your Data Context initialization and configuration.

5. Run the following command to verify that GX was installed and your in-memory Data Context was instantiated successfully:

   ```python title="Python"
      context.list_datasources()
   ```