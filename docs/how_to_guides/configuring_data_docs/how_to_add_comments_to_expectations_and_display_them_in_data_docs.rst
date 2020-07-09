.. _how_to_guides__configuring_data_docs__how_to_add_comments_to_expectations_and_display_them_in_data_docs:

How to add comments to Expectations and display them in Data Docs
=================================================================

This guide will help you add descriptive comments (or notes, here used interchangeably) to Expectations and display those comments in Data Docs. In these comments you can add some clarification or motivation to the expectation definition, to help you communicate more clearly with your team about specific expectations.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <getting_started>`
    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.

Steps
-----

#. First, edit your Expectation Suite.

    .. code-block:: bash

        great_expectations suite edit <your_suite_name>

#. Next, add comments to specific Expectations.

    For each Expectation you wish to add notes to, add a dictionary to the ``meta`` field with the key ``notes`` and your comment as the value. Here is an example.

    .. code-block:: python

        batch.expect_table_row_count_to_be_between(
            max_value=1000000, min_value=1,
            meta={"notes": "Example notes about this expectation."}
        )


#. Review your comments in the Expectation Suite overview of your Data Docs.


Comments
--------

.. discourse::
   :topic_identifier: {{topic_id}}
