.. _how_to_guides__validation__how_to_implement_custom_notifications:

How to implement custom notifications
=====================================

If you would like to implement custom notifications that include a link to Data Docs, you can access the Data Docs URL from your Data Context after a validation run following the steps below.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

First, this is the standard boilerplate to load a Data Context and run validation on a Batch:

    .. code-block:: python

        context.expect_table_row_count_to_be_between(min_value=1000, max_value=4000)

.. discourse::
    :topic_identifier: 222
