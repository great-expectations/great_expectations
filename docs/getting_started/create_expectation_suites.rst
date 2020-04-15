.. _getting_started__creat_expectation_suites:

Authoring Expectation Suites
----------------------------------------------------------

Earlier in this article we said that capturing and documenting the team's shared understanding of its data as expectations is the core part of this typical workflow.

Expectation Suites combine multiple expectations into an overall description of a dataset. For example, a team can group all the expectations about its ``rating`` table in the movie ratings database from our previous example into an Expectation Suite and call it ``movieratings.ratings``. Note these names are completely flexible and the only constraint on the name of a suite is that it must be unique to a given project.

Each Expectation Suite is saved as a JSON file in the ``great_expectations/expectations`` subdirectory of the Data Context. Users check these files into the version control each time they are updated, same way they treat their source files. This discipline allows data quality to be an integral part of versioned pipeline releases.

The lifecycle of an Expectation Suite starts with creating it. Then it goes through an iterative loop of Review and Edit as the team's understanding of the data described by the suite evolves.

Create
********************************************


While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let :ref:`CLI <command_line>` save you time and typos.
Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:


.. code-block:: bash

    great_expectations suite new


This command prompts you to name your new Expectation Suite and to select a sample batch of data the suite will describe.
Then it uses a sample of the selected data to add some initial expectations to the suite.
The purpose of these is expectations is to provide examples of data assertions, and not to be meaningful.
They are intended only a starting point for you to build upon.

The command concludes by saving the newly generated Expectation Suite as a JSON file and rendering the expectation suite into an HTML page in Data Docs.


Review
********************************************

Reviewing expectations is best done visually in Data Docs. Here's an example of what that might look like:

.. image:: ../images/sample_e_s_view.png

Note that many of these expectations might have meaningless ranges.
Also note that all expectations will have passed, since this is an example suite only.
When you interactively edit your suite you will likely see failures as you iterate.


Edit
********************************************

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
