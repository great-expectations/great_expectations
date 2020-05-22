.. _how_to__use_check_assets:

##########################################
How to use check assets to validate data
##########################################

While Great Expectations has nearly :ref:`50 built in expectations<expectation_glossary>`, the need for complex data assertions is common.

**Check assets** are a **design pattern** that enables even more complex and fine-grained data tests, such as:

assertions on **slices of the data**
  For example: How many visits occurred last week?

assertions **across logical tables**
  For example: Does my visits table join with my patients table?

A check asset is a slice of data that is only created for validation purposes and may not feed into pipeline or analytical output. It helps express your expectations against a data object that more naturally reflects the meaning of the expectation. For example, a Check Asset for event data might be built as an aggregate rollup binning events by the hour in which they occurred. Then, you can easily express expectations about how many events should happen in the day versus the night.

These check assets should be built in your pipeline\'s native transformation language.
For example, if your pipeline is primarily SQL, create an additional table or view that slices the data so that you can use the built in expectations found here: :ref:`expectation_glossary`.

Using a check asset introduces a new node into your data pipeline. You should clearly name the expectations about a check asset in a way that makes it easy to understand how it is used in the pipeline, for example by creating an :ref:`Expectation Suite`_ with the name ``event_data.time_rollup_check.warning``.

-----------------------
Postgres SQL example.
-----------------------

Let's suppose we have a ``visits`` table and we want to make an assertion about the typical number of visits in the last 30 days.

1. Create a new table with an obvious name like ``visits.last_30_days`` that is populated with the following query:

.. code-block:: SQL

    SELECT *
    FROM visits
    WHERE visit_date > current_date - interval '30' day;

2. Create a new Expectation Suite for this new table.
Again, we recommend using an obvious name such as ``visits.last_30_days.warning``.

3. Add an Expectation as follows:

.. code-block:: python

    batch.expect_table_row_count_to_be_between(min_value=2000, max_value=5000)
