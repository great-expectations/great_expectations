.. _tutorials__getting_started__set_up_your_first_checkpoint:

Validate your data
============================

As we said earlier, validation is the core operation of Great Expectations: “Validate X data against Y Expectations.”

In normal usage, the best way to validate data is with a :ref:`Checkpoint`. Checkpoints bring :ref:`Batches` of data together with corresponding :ref:`Expectation Suites` for validation. Configuring Checkpoints simplifies deployment, by pre-specifying the "X"s and "Y"s that you want to validate at any given point in your data infrastructure.

Let’s set up our first Checkpoint by running another CLI command:

.. code-block:: bash

  great_expectations checkpoint new my_checkpoint npidata_pfile_20200511-20200517.warning.json

``my_checkpoint`` will be the name of your new Checkpoint. It will use ``npidata_pfile_20200511-20200517.warning`` as its primary :ref:`Expectation Suite`. (You can add other Expectation Suites later.)

From there, you can configure the Checkpoint using the CLI:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    
    Would you like to:
        1. choose from a list of data assets in this datasource
        2. enter the path of a data file
    : 1
    
    Which data would you like to use?
        1. npidata_pfile_20200511-20200517 (file)
        Don't see the name of the data asset in the list above? Just type it
    : 1
    A checkpoint named `my_checkpoint` was added to your project!
      - To edit this checkpoint edit the checkpoint file: /home/ubuntu/example_project/great_expectations/checkpoints/my_checkpoint.yml
      - To run this checkpoint run `great_expectations checkpoint run my_checkpoint`
    
Let’s pause there before continuing.

How Checkpoints work
--------------------

Your new checkpoint file is in ``my_checkpoint.yml``. With comments removed, it looks like this:

.. code-block:: yaml

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          path: /home/ubuntu/example_project/great_expectations/../my_data/npidata_pfile_20200511-20200517.csv
          datasource: my_data__dir
          data_asset_name: npidata_pfile_20200511-20200517
        expectation_suite_names: # one or more suites may validate against a single batch
          - npidata_pfile_20200511-20200517.warning


Our newly configured Checkpoint knows how to load ``npidata_pfile_20200511-20200517.csv`` as a Batch, pair it with the ``npidata_pfile_20200511-20200517.warning`` Expectation Suite, and execute them both using a pre-configured :ref:`Validation Operator <Validation Operators>` called ``action_list_operator``.

You don't need to worry much about the details of Validation Operators for now. They orchestrate the actual work of validating data and processing the results. After executing validation, the Validation Operator can kick off additional workflows through :ref:`Validation Actions`.

For more examples of post-validation actions, please see the :ref:`How-to section for Validation <how_to_guides__validation>`.

How to run Checkpoints
----------------------

Checkpoints can be run like applications from the command line or cron:

.. code-block:: bash

    great_expectations checkpoint run my_checkpoint

You can also generate Checkpoint scripts that you can edit and run using python, or within data orchestration tools like Airflow. For example, see the How to Run a Checkpoint in Airflow how-to guide.

.. code-block:: bash

    great_expectations checkpoint script my_checkpoint

Once the Checkpoint is run, you can head back to your Data Docs to see the results of the latest Validation run with your Checkpoint.


Congratulations! Where to go from here?
----------------------

At this point, you have your first, working local deployment of Great Expectations. This is the end of the Getting Started tutorial!

You've also been introduced to the foundational concepts in the library: :ref:`Data Contexts`, :ref:`Datasources`, :ref:`Expectations`, :ref:`Profilers`, :ref:`Data Docs`, :ref:`Validation`, and :ref:`Checkpoints`.

Data Contexts make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps---so you can upgrade from a basic demo deployment to a full production deployment at your own pace, and be confident that your Data Context will continue to work at every step along the way. The next step is to :ref:`tutorials__getting_started__customize_your_deployment`.

