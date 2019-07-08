.. _tutorial_create_expectations:

Step 2: Create Expectations
==============================

.. toctree::
   :maxdepth: 2

Creating expectations is an opportunity to blend contextual knowledge from subject-matter experts and insights
from profiling and performing exploratory analysis on your dataset.

Video
------

This brief video covers the basics of creating expectations


Get DataContext Object
-----------------------

The DataContext object manages


Data Assets
-------------



Get Batch
----------



Reader Options
---------------



Create Expectations
--------------------------------


Now that you have one of the data batches loaded, you can call expect* methods on the dataframe in order to check
if you can make an assumption about the data.

For example, to check if you can expect values in column "order_date" to never be empty, call: `df.expect_column_values_to_not_be_null('order_date')`

### How do I know which types of expectations I can add?
* *Tab-complete* this statement, and add an expectation of your own; copy the cell to add more
* In jupyter, you can also use *shift-tab* to see the docstring for each expectation, to see what parameters it takes and get more information about the expectation.
* Here is a glossary of expectations you can add:
https://great-expectations.readthedocs.io/en/latest/glossary.html



Expectations include:

- ``expect_table_row_count_to_equal``
- ``expect_column_values_to_be_unique``
- ``expect_column_values_to_be_in_set``
- ``expect_column_mean_to_be_between``
- ...and many more

Visit the `glossary of
expectations <https://docs.greatexpectations.io/en/latest/glossary.html>`__
for a complete list of expectations that are currently part of the great
expectations vocabulary.


How do I learn more?
--------------------

For full documentation, visit `Great Expectations on
readthedocs.io <https://docs.greatexpectations.io/en/latest/>`__.

`Down with Pipeline
Debt! <https://medium.com/@expectgreatdata/down-with-pipeline-debt-introducing-great-expectations-862ddc46782a>`__
explains the core philosophy behind Great Expectations. Please give it a
read, and clap, follow, and share while you're at it.

For quick, hands-on introductions to Great Expectations' key features,
check out our walkthrough videos:

-  `Introduction to Great
   Expectations <https://www.youtube.com/watch?v=-_0tG7ACNU4>`__
-  `Using Distributional
   Expectations <https://www.youtube.com/watch?v=l3DYPVZAUmw&t=20s>`__


Great Expectations doesn't do X. Is it right for my use case?
-------------------------------------------------------------

It depends. If you have needs that the library doesn't meet yet, please
`upvote an existing
issue(s) <https://github.com/great-expectations/great_expectations/issues>`__
or `open a new
issue <https://github.com/great-expectations/great_expectations/issues/new>`__
and we'll see what we can do. Great Expectations is under active
development, so your use case might be supported soon.
