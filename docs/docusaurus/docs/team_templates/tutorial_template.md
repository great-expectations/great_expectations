---
title: <!-- Add the topic title here -->
---

<!-- Provide a meaningful overview here. Tell the user what they will accomplish and what value it provides. Use second person rather than first person — you instead of we. Address the reader as you, and assume that the reader is the person who's doing the tasks that you're documenting. Limit your introductory statement to two or three sentences. -->

<!-- To link to a specific section within this template, use the format provided in the following example. -->

[First task](#first-task)

## Assumed knowledge

<!-- List the existing knowledge a user should have before they start the tutorial. Link to relevant information if it's available. What follows is an example provided for your reference. If existing knowledge isn't required, remove this section.-->

- [An understanding of Great Expectations functionality](../guides/setup/setup_overview.md)
- [How to connect to source data](../guides/connecting_to_your_data/connect_to_data_lp.md)
- [How to create an Expectation Suite](../guides/expectations/create_expectations_overview.md)

## First task 

<!-- Update the task heading. Use sentence case for headings and titles and avoid using -ing verb forms (gerunds) in headings or titles. Section headings should describe the type of content that's in the section. For example, Create an instance. For more information about the correct heading format, see [Headings and titles](https://developers.google.com/style/headings).-->

<!-- In a numbered list, describe what the user must do to accomplish the task successfully. Avoid long narrative descriptions of functionality and behaviour. If the behaviour is obvious, it doesn't need to be described. Provide users with only the information they need to know. When necessary, provide or link to code samples. What follows is an example provided for your reference. -->

1. Run the following command in an empty base directory inside a Python virtual environment:

    ```bash title="Terminal input"
    pip install great_expectations
    ```

    It can take several minutes for the installation to complete. Jupyter Notebook is included with Great Expectations, and it lets you edit code and view the results of code runs.

2. Open Jupyter Notebook, a command line, or a terminal and then run the following command to import the `great_expectations` module:

    ```python name="tutorials/quickstart/quickstart.py import_gx"
    ```

3. Run the following command to import the `DataContext` object:

    ```python name="tutorials/quickstart/quickstart.py get_context"
    ```
4. Add a comment when the prompt appears:

    ![Response dialog](../team_templates/images/comments_box_with_comment.png)

    <!-- To include images in procedure steps, or elsewhere in this template, add them to the images sub-folder in the `team_templates` folder and use underscores as delimiters in the filename. For example, `comments_box_with_comment`. Use the .png format for images. Use images sparingly.  -->

## Second task

<!-- If necessary, add a secondary task here. Use the same format that you used in the first task. What follows is an example provided for your reference. -->

1. Run the following command to create two Expectations. The first Expectation uses domain knowledge (the `pickup_datetime` shouldn't be null), and the second Expectation uses [`auto=True`](../guides/expectations/how_to_use_auto_initializing_expectations.md#using-autotrue) to detect a range of values in the `passenger_count` column. 

    ```python name="tutorials/quickstart/quickstart.py create_expectation"
    ```
    The Expectation assumes the `pickup_datetime` column always contains data.  None of the column's values are null.

    To analyze Validator data, you can create multiple Expectations that call multiple methods with the `validator.expect_*` syntax.

## Third task

<!-- If necessary, add a tertiary task here. Use the same format that you used in the first task. What follows is an example provided for your reference. -->

1. Run the following command to define a Checkpoint and examine the data to determine if it matches the defined Expectations: 

    ```python name="tutorials/quickstart/quickstart.py create_checkpoint"
    ```

2. Run the following command to return the Validation results:

    ```python name="tutorials/quickstart/quickstart.py run_checkpoint"
    ```

## Additional tasks

<!-- If necessary, continue adding tasks following the format you used in the first, second, and third tasks. If there aren't any additional tasks, remove this section. -->

## Related documentation

<!-- List the secondary resources that can help a user get a better understanding of the subject matter discussed in this tutorial. Don't add an introductory statement for the list. If a user needs to complete additional tasks to complete this process, use a Next steps section instead. What follows is an example provided for your reference. If there aren't any related documents, remove this section.-->

- [How to install Great Expectations locally](../guides/setup/installation/install_gx.md)
- [How to set up GX to work with data on AWS S3](../guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3.md)
- [How to set up GX to work with data in Azure Blob Storage](../guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs.md)
- [How to set up GX to work with data on GCS](../guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs.md)
- [How to set up GX to work with SQL databases](../guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases.md) 
- [How to instantiate a Data Context on an EMR Spark Cluster](../deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster.md)
- [How to use Great Expectations in Databricks](../tutorials/getting_started/how_to_use_great_expectations_in_databricks.md)

## Next steps

<!-- If this tutorial is the first step in a process and there are other tasks the user must complete to finish the process, list the topics here with links to the relevant information. If you want to link to related information, use a Related topics section instead. Don't add an introductory statement for the list. What follows is an example provided for your reference. If there aren't next steps, remove this section.-->

- [Install Great Expectations locally](../guides/setup/installation/install_gx.md)
- [Set up GX to work with data on AWS S3](../guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3.md)
- [Set up GX to work with data in Azure Blob Storage](../guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs.md)
- [Set up GX to work with data on GCS](../guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs.md)