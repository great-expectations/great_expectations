.. _profiling:

##############
Profiling
##############

Profiling evaluates a data asset and summarizes its observed characteristics. By computing the observed properties of
data, Profiling helps to reason about the data's expected properties when creating expectation suites.

Profiling results are usually rendered into HTML - see :ref:`data_docs`.
GE ships with the default BasicDatasetProfiler, which will produce an expectation_suite and so validation_results
that compile to a page for each table or DataFrame including an overview section:

.. image:: ../images/movie_db_profiling_screenshot_2.jpg

And then detailed statistics for each column:

.. image:: ../images/movie_db_profiling_screenshot_1.jpg


Profiling is still a beta feature in Great Expectations. Over time, we plan to extend and improve the
``BasicDatasetProfiler`` and also add additional profilers.

Profiling relies on automated inspection of data batches to generate and encode expectations. Together,
encoding expectations, testing data, and presenting expectation validation results are the three core services
offered by GE.

Warning: ``BasicDatasetProfiler`` will evaluate the entire batch
without limits or sampling, which may be very time consuming. As a rule of thumb, we recommend starting with batches
smaller than 100MB.

****************************
Expectations and Profiling
****************************

In order to characterize a data asset, profiling creates an expectation suite. Unlike the expectations that are
typically used for data validation, these expectations do not necessarily apply any constraints; they can simply
identify statistics or other data characteristics that should be evaluated and made available in GE. For example, when
the ``BasicDatasetProfiler`` encounters a numeric column, it will add an ``expect_column_mean_to_be_between``
expectation but choose the min_value and max_value to both be None: essentially only saying that it expects a mean
to exist.

.. code-block:: json

    {
      "expectation_type": "expect_column_mean_to_be_between",
      "kwargs": {
        "column": "rating",
        "min_value": null,
        "max_value": null
      }
    }

To "profile" a datasource, therefore, the :class:`~great_expectations.profile.basic_dataset_profiler.\
BasicDatasetProfiler` included in GE will generate a large number of very loosely-specified expectations. Effectively
it is asserting that the given statistic is relevant for evaluating batches of that data asset, but it is not yet sure
what the statistic's value should be.

In addition to creating an expectation suite, profiling data tests the suite against data.
The validation_result contains the output of that expectation suite when validated against the same batch of data.
For a loosely specified expectation like in our example above, getting the observed value was the sole purpose of
the expectation.

.. code-block:: json

    {
      "success": true,
      "result": {
        "observed_value": 4.05,
        "element_count": 10000,
        "missing_count": 0,
        "missing_percent": 0
      }
    }

Running a profiler on a data asset can also be useful to produce a large number of expectations to review
and potentially transfer to a new expectation suite used for validation in a pipeline.

See the :ref:`profiling_reference` for more information.
