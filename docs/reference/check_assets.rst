.. _check_assets_reference:

############################
Check Assets Pattern
############################

While Great Expectations has nearly :ref:`50 built in expectations<expectation_glossary>`, the need for complex data assertions is common.

**Check assets** are a **simple design pattern** that enables even more complex and fine-grained data tests, such as:

assertions on on **slices of the data**
  For example: How many visits occurred last week?

assertions **across logical tables**
  For example: Does my visits table join with my patients table?

A check asset is a slice of data that is only created for validation purposes and may not feed into pipeline or analytical output.

These check assets should be built in your pipeline\'s native transformation language.
For example, if your pipeline is primarily SQL, create an additional table or view that slices the data so that you can use the built in expectations found here: :ref:`expectation_glossary`.

-----------------------
Postgres SQL example.
-----------------------

Let's suppose we have a ``visits`` table and we want to make an assertion about the typical number of visits in the last 30 days.

1. Create a new table with an obvious name like ``ge_asset_visits_last_30_days`` that is populated with the following query:

.. code-block:: SQL

    SELECT *
    FROM visits
    WHERE visit_date > current_date - interval '30' day;

2. Create a new expectation suite against this new table.
Again, we recommend using an obvious name such as ``visits_last_30_days``.

3. Add an expectation as follows:

.. code-block:: python

    batch.expect_table_row_count_to_be_between(min_value=2000, max_value=5000)
