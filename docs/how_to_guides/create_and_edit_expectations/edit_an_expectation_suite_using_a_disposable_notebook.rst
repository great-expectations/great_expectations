


How to edit an Expectation Suite using a disposable notebook
************************************************************

Editing an Expectation Suite means adding, removing, and modifying the arguments of existing expectations.


Similar to writing SQL queries, Expectations are best edited interactively against your data.
The best interface for this is in a Jupyter notebook where you can get instant feedback as you iterate.

For every expectation type there is a Python method that sets its arguments, evaluates this expectation against a sample batch of data and adds it to the Expectation Suite.

The screenshot below shows the Python method and the Data Docs view for the same expectation (``expect_column_distinct_values_to_be_in_set``):

.. image:: ../images/exp_html_python_side_by_side.png

The Great Expectations :ref:`CLI <command_line>` command ``suite edit`` generates a Jupyter notebook to edit a suite.
This command saves you time by generating boilerplate that loads a batch of data and builds a cell for every expectation in the suite.
This makes editing suites a breeze.

For example, to edit a suite called ``movieratings.ratings`` you would run:

.. code-block:: bash

    great_expectations suite edit movieratings.ratings

These generated Jupyter notebooks can be discarded and should not be kept in source control since they are auto-generated at will, and may contain snippets of actual data.

To make this easier still, the Data Docs page for each Expectation Suite has the :ref:`CLI <command_line>` command syntax for you.
Simply press the "How to Edit This Suite" button, and copy/paste the :ref:`CLI <command_line>` command into your terminal.

.. image:: ../images/edit_e_s_popup.png
