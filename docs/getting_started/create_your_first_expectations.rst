.. _getting_started__create_your_first_expectations:

Create your first Expectations
------------------------------

Expectation Suites combine multiple expectations into an overall description of a dataset. For example, a team can group all the expectations about its ``rating`` table in the movie ratings database from our previous example into an Expectation Suite and call it ``movieratings.ratings``. Note these names are completely flexible and the only constraint on the name of a suite is that it must be unique to a given project.

Each Expectation Suite is saved as a JSON file in the ``great_expectations/expectations`` subdirectory of the Data Context. Users check these files into the version control each time they are updated, same way they treat their source files. This discipline allows data quality to be an integral part of versioned pipeline releases.

The lifecycle of an Expectation Suite starts with creating it. Then it goes through an iterative loop of Review and Edit as the team's understanding of the data described by the suite evolves.

Review
********************************************

Reviewing expectations is best done visually in Data Docs. Here's an example of what that might look like:

.. image:: ../images/sample_e_s_view.png

Note that many of these expectations might have meaningless ranges.
Also note that all expectations will have passed, since this is an example suite only.
When you interactively edit your suite you will likely see failures as you iterate.
