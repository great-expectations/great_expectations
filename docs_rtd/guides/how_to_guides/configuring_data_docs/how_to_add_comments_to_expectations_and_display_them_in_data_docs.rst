.. _how_to_guides__configuring_data_docs__how_to_add_comments_to_expectations_and_display_them_in_data_docs:

How to add comments to Expectations and display them in Data Docs
=================================================================

This guide will help you add descriptive comments (or notes, here used interchangeably) to Expectations and display those comments in Data Docs. In these comments you can add some clarification or motivation to the expectation definition to help you communicate more clearly with your team about specific expectations. Markdown is supported in these comments.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.

Steps
-----
.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

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

            Leads to the following representation in the Data Docs (click on the speech bubble to view the comment).

            .. image:: table_level_no_format.png
                :width: 800
                :alt: Expectation with simple comment, no formatting.

        #. Add styling to your comments (optional).

            To add styling to your comments, you can add a format tag. Here are a few examples.

            A single line of markdown is rendered in red, with any Markdown formatting applied.

            .. code-block:: python

                batch.expect_column_values_to_not_be_null(
                    column="column_name",
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Example notes about this expectation. **Markdown** `Supported`."
                        }
                    }
                )

            .. image:: single_line_markdown_red.png
                :width: 800
                :alt: Expectation with a single line of markdown comment is rendered in red with markdown formatting.


            Multiple lines can be rendered by using a list for ``content``; these lines are rendered in black text with any Markdown formatting applied.

            .. code-block:: python

                batch.expect_column_values_to_not_be_null(
                    column="column_name",
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": [
                                "Example notes about this expectation. **Markdown** `Supported`.",
                                "Second example note **with** *Markdown*",
                            ]
                        }
                    }
                )

            .. image:: multiple_line_markdown.png
                :width: 800
                :alt: Multiple lines of markdown rendered with formatting.


            You can also change the ``format`` to ``string`` and single or multiple lines will be formatted similar to the above, but the Markdown formatting will not be applied.

            .. code-block:: python

                batch.expect_column_values_to_not_be_null(
                    column="column_name",
                    meta={
                        "notes": {
                            "format": "string",
                            "content": [
                                "Example notes about this expectation. **Markdown** `Not Supported`.",
                                "Second example note **without** *Markdown*",
                            ]
                        }
                    }
                )

            .. image:: multiple_line_string.png
                :width: 800
                :alt: Multiple lines of string rendered without formatting.



        #. Review your comments in the Expectation Suite overview of your Data Docs.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        #. First, edit your Expectation Suite.

            .. code-block:: bash

                great_expectations --v3-api suite edit <your_suite_name>

        #. Next, add comments to specific Expectations.

            For each Expectation you wish to add notes to, add a dictionary to the ``meta`` field with the key ``notes`` and your comment as the value. Here is an example.

            .. code-block:: python

                validator.expect_table_row_count_to_be_between(
                    max_value=1000000, min_value=1,
                    meta={"notes": "Example notes about this expectation."}
                )

            Leads to the following representation in the Data Docs (click on the speech bubble to view the comment).

            .. image:: table_level_no_format.png
                :width: 800
                :alt: Expectation with simple comment, no formatting.

        #. Add styling to your comments (optional).

            To add styling to your comments, you can add a format tag. Here are a few examples.

            A single line of markdown is rendered in red, with any Markdown formatting applied.

            .. code-block:: python

                validator.expect_column_values_to_not_be_null(
                    column="column_name",
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Example notes about this expectation. **Markdown** `Supported`."
                        }
                    }
                )

            .. image:: single_line_markdown_red.png
                :width: 800
                :alt: Expectation with a single line of markdown comment is rendered in red with markdown formatting.


            Multiple lines can be rendered by using a list for ``content``; these lines are rendered in black text with any Markdown formatting applied.

            .. code-block:: python

                validator.expect_column_values_to_not_be_null(
                    column="column_name",
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": [
                                "Example notes about this expectation. **Markdown** `Supported`.",
                                "Second example note **with** *Markdown*",
                            ]
                        }
                    }
                )

            .. image:: multiple_line_markdown.png
                :width: 800
                :alt: Multiple lines of markdown rendered with formatting.


            You can also change the ``format`` to ``string`` and single or multiple lines will be formatted similar to the above, but the Markdown formatting will not be applied.

            .. code-block:: python

                validator.expect_column_values_to_not_be_null(
                    column="column_name",
                    meta={
                        "notes": {
                            "format": "string",
                            "content": [
                                "Example notes about this expectation. **Markdown** `Not Supported`.",
                                "Second example note **without** *Markdown*",
                            ]
                        }
                    }
                )

            .. image:: multiple_line_string.png
                :width: 800
                :alt: Multiple lines of string rendered without formatting.



        #. Review your comments in the Expectation Suite overview of your Data Docs.



Comments
--------

.. discourse::
   :topic_identifier: 281
