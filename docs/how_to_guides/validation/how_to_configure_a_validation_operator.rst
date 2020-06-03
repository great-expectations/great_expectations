.. _how_to_guides__validation__how_to_configure_a_validation_operator:

How to configure a Validation Operator
======================================

This guide will help you configure a :ref:`Validation Operator <validation_operators_and_actions>`. This will allow you to validate tables and queries within this project.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`
  - Created at least one Expectation Suite

Steps
-----


1. Open your project's great_expectations.yml configuration file and navigate to the `validation_operators` section.

2. Find the class name of the Validation Operator you want to add to your project. This can be one of the Validation Operators that are included in
Great Expectations or a class that you implemented.

3. For this example let's assume the class name is `AmazingValidationOperator`.

.. code-block:: yaml

    validation_operators:
      action_list_operator:
        class_name: AmazingValidationOperator


2. Choose "Big Query" from the list of database engines, when prompted.
3. Identify the connection string you would like Great Expectations to use to connect to BigQuery, using the examples below and the `PyBigQuery <https://github.com/mxmzdlv/pybigquery>`_ documentation.

6. Should you need to modify your connection string, you can manually edit the
   ``great_expectations/uncommitted/config_variables.yml`` file.


Additional notes
----------------


Additional resources
--------------------

.. discourse::
    :topic_identifier: 217
