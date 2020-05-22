.. _how_to_guides__validate_data_in_a_pipeline_with_a_checkpoint

.. warning:: This doc is spare parts: leftover pieces of old documentation.
  It's potentially helpful, but may be incomplete, incorrect, or confusing.

How to validate data in a pipeline with a Checkpoint
=================================================================

.. note:: This is an ideal way to deploy automated data testing if you want to take automated interventions based on the results of data validation.
  For example, you may want your pipeline to quarantine data that does not meet your expectations.


A data engineer can add a :ref:`Validation Operator<validation_operators_and_actions>` to your pipeline and configure it.
These :ref:`Validation Operators<validation_operators_and_actions>` evaluate the new batches of data that flow through your pipeline against the expectations your team defined in the previous sections.

While data pipelines can be implemented with various technologies, at their core they are all DAGs (directed acyclic graphs) of computations and transformations over data.

This drawing shows an example of a node in a pipeline that loads data from a CSV file into a database table.

- Two expectation suites are deployed to monitor data quality in this pipeline.
- The first suite validates the pipeline's input - the CSV file - before the pipeline executes.
- The second suite validates the pipeline's output - the data loaded into the table.

.. image:: ../images/pipeline_diagram_two_nodes.png

To implement this validation logic, a data engineer inserts a Python code snippet into the pipeline - before and after the node. The code snippet prepares the data for the GE Validation Operator and calls the operator to perform the validation.

The exact mechanism of deploying this code snippet depends on the technology used for the pipeline.

If Airflow drives the pipeline, the engineer adds a new node in the Airflow DAG. This node will run a PythonOperator that executes this snippet. If the data is invalid, the Airflow PythonOperator will raise an error which will stop the rest of the execution.

If the pipeline uses something other than Airflow for orchestration, as long as it is possible to add a Python code snippet before and/or after a node, this will work.

Below is an example of this code snippet, with comments that explain what each line does.

.. code-block:: python

    # Data Context is a GE object that represents your project.
    # Your project's great_expectations.yml contains all the config
    # options for the project's GE Data Context.
    context = ge.data_context.DataContext()

    datasource_name = "my_production_postgres" # a datasource configured in your great_expectations.yml

    # Tell GE how to fetch the batch of data that should be validated...

    # ... from the result set of a SQL query:
    batch_kwargs = {"query": "your SQL query", "datasource": datasource_name}

    # ... or from a database table:
    # batch_kwargs = {"table": "name of your db table", "datasource": datasource_name}

    # ... or from a file:
    # batch_kwargs = {"path": "path to your data file", "datasource": datasource_name}

    # ... or from a Pandas or PySpark DataFrame
    # batch_kwargs = {"dataset": "your Pandas or PySpark DataFrame", "datasource": datasource_name}

    # Get the batch of data you want to validate.
    # Specify the name of the expectation suite that holds the expectations.
    expectation_suite_name = "movieratings.ratings" # this is an example of
                                                        # a suite that you created
    batch = context.get_batch(batch_kwargs, expectation_suite_name)

    # Call a validation operator to validate the batch.
    # The operator will evaluate the data against the expectations
    # and perform a list of actions, such as saving the validation
    # result, updating Data Docs, and firing a notification (e.g., Slack).
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id=run_id) # e.g., Airflow run id or some run identifier that your pipeline uses.

    if not results["success"]:
        # Decide what your pipeline should do in case the data does not
        # meet your expectations.

