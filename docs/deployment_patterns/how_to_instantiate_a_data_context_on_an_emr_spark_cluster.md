---
title: How to instantiate a Data Context on an EMR Spark cluster
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'

This guide will help you instantiate a Data Context on an EMR Spark cluster.


The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations [command line interface (CLI)](../guides/miscellaneous/how_to_use_the_great_expectations_cli.md).


<Prerequisites>

</Prerequisites>

Steps
-----

1. **Install Great Expectations on your EMR Spark cluster.**

   Copy this code snippet into a cell in your EMR Spark notebook and run it:

    ```python
  sc.install_pypi_package("great_expectations")
    ```


2. **Configure a Data Context in code.**

    Follow the steps for creating an in-code Data Context in [How to instantiate a Data Context without a yml file](../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md).

    The snippet at the end of the guide shows Python code that instantiates and configures a Data Context in code for an EMR Spark cluster. Copy this snippet into a cell in your EMR Spark notebook or use the other examples to customize your configuration.


3. **Test your configuration.**

       Execute the cell with the snippet above.

       Then copy this code snippet into a cell in your EMR Spark notebook, run it and verify that no error is displayed:

      ```python
      context.list_datasources()
      ```

