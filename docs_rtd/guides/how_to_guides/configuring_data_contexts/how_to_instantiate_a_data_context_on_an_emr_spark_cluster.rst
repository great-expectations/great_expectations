.. _how_to_instantiate_a_data_context_on_an_emr_spark_cluster:

How to instantiate a Data Context on an EMR Spark cluster
=========================================================

This guide will help you instantiate a Data Context on an EMR Spark cluster.


The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations :ref:`command line interface (CLI) <command_line>`.


.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration<tutorials__getting_started>`.

Steps
-----

#. **Install Great Expectations on your EMR Spark cluster.**

   Copy this code snippet into a cell in your EMR Spark notebook and run it:

   .. code-block:: python

      sc.install_pypi_package("great_expectations")


#. **Configure a Data Context in code.**

Follow the steps for creating an in-code Data Context in :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>`

The snippet at the end of the guide shows Python code that instantiates and configures a Data Context in code for an EMR Spark cluster. Copy this snippet into a cell in your EMR Spark notebook or use the other examples to customize your configuration.


#. **Test your configuration.**

   Execute the cell with the snippet above.

   Then copy this code snippet into a cell in your EMR Spark notebook, run it and verify that no error is displayed:

   .. code-block:: python

      context.list_datasources()


.. discourse::
    :topic_identifier: 291
