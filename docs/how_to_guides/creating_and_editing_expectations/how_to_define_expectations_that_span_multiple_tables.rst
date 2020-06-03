.. _how_to_guides__creating_and_editing_expectations__how_to_create_expectations_that_span_multiple_tables_using_evaluation_parameters:

How to create Expectations that span multiple Batches using Evaluation Parameters
=================================================================================

This guide will help you create Expectations that span multiple :ref:`Batches` of data using :ref:`Evaluation Parameters`. This pattern is useful for things like verifying that row counts between tables stay consistent.


Data Assets within a Validation operation. This is done

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`
  - Configured a :ref:`Datasource <Datasources>` (or Datasources) with at least two Data Assets.
  - Also created :ref:`Expectation Suites` for those Data Assets.
  - Have a working :ref:`Evaluation Parameter Store`. (The default in-memory store from ``great_expectations init`` can work for this.)
  - Have a working :ref:`Validation Operator`. (The default Validation Operator from ``great_expectations init`` can work for this.)

Steps
-----

In a notebook, 

1. **Import great_expectations and instantiate your Data Context**

    .. code-block:: python

        import great_expectations as ge
        context = ge.DataContext()

2. **Instantiate two Batches**

    We'll call one of these Batches the *upstream* Batch and the other the *downstream* Batch. It's common but not required for both Batches to come from the same :ref:`Datasource <Datasources>` and :ref:`BatchKwargsGenerator`.

    .. code-block:: python

        upstream_batch = context.get_batch(
            context.build_batch_kwargs("my_datasource", "my_generator_name", "my_data_asset_name_1"),
            expectation_suite_name='my_expectation_suite_1'
        )

        downstream_batch = context.get_batch(
            context.build_batch_kwargs("my_datasource", "my_generator_name", "my_data_asset_name_2"),
            expectation_suite_name='my_expectation_suite_2'
        )

3. **Define an Expectation using an Evaluation Parameter on the downstream Batch.**

    .. code-block:: python

        eval_param_urn = 'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'
        downstream_batch.expect_table_row_count_to_equal(
            value={
                '$PARAMETER': eval_param_urn, # this is the actual parameter we're going to use in the validation
                '$PARAMETER.' + eval_param_urn: 10  # this is a *temporary* value so we can execute the notebook
            }
        )
    
    The core of this is a ``$PARAMETER : URN`` pair. When Great Expectations encounters a ``$PARAMETER`` flag during validation, it will attempt to replace the ``URN`` with a value retrieved from an :ref:`Evaluation Parameter Store` or :ref:`Metrics Store`. If a matching value exists, the 

    .. warning::

        At present, the development loop for testing and debugging URNs is not very user-friendly. We plan to make changes in the future to simplify this workflow.

    This will generate an :ref:`Expectation Validation Result`:

    .. code-block:: python

        {
            "result": {
                "observed_value": 506
            },
            "meta": {},
            "exception_info": null,
            "success": false
        }

Here's a pinch of connecting text.

.. code-block:: python

    batch.expect_table_row_count_to_be_between(min_value=2000, max_value=5000)


Additional notes
----------------

How-to guides are not about teaching or explanation. They are about providing clear, bite-sized replication steps. If you **must** include a longer explanation, it should go in this section.

Additional resources
--------------------

- `Links in RST <https://docutils.sourceforge.io/docs/user/rst/quickref.html#hyperlink-targets>`_ are a pain.

Comments
--------

.. discourse::
    :topic_identifier: 206
