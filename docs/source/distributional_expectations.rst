.. _distributional_expectations:

================================================================================
Distributional Expectations
================================================================================

Distributional expectations help identify when new datasets or samples may be different than expected, and can help ensure that assumptions developed during exploratory analysis still hold as new data becomes available. You should use distributional expectations in the same way as other expectations: to provide a rough analogue to unit testing for software development, where the expectations can help to accelerate identification of risks as diverse as changes in a system being modeled or disruptions to a complex upstream data feed.

Great Expectations' Philosophy of Distributional Expectations
--------------------------------------------------------------------------------

Great Expectations attempts to provide a simple, expressive framework for describing distributional expectations. The framework generally adopts a nonparametric approach, although it is possible to build expectations from parameterized distributions.

The design is motivated by the following assumptions:

* Encoding expectations into a simple object that allows for portable data pipeline testing is the top priority. In many circumstances the loss of precision associated with "compressing" data into an expectation may be beneficial because of its intentional simplicity as well as because it adds a very light layer of obfuscation over the data which may align with privacy preservation goals.
* While it should be possible to easily extend the framework with more rigorous statistical tests, great expectations should provide simple, reasonable defaults. Care should be taken in cases where robust statistical guarantees are expected.


Distributional Expectations Core' Datatypes
--------------------------------------------------------------------------------

Distributional expectations currently focus exclusively on unidimensional (columnar) data. The core constructs of a great expectations distributional expectation are the partition and associated weights.

For continuous data:

* A partition is defined by an ordered list of points that define intervals on the real number line. Note that partition intervals do not need to be uniform. Currently, -inf and +inf are not explicitly supported as endpoints, so the term partition is used somewhat loosely.
* Partition weights define the probability of the associated interval. Note that this effectively applies a "piecewise uniform" distribution to the data for the purpose of statistical tests.

Example continuous partition object:
.. code-block:: python
  {
    "partition": [ 0, 1, 2, 10],
    "weights": [0.3, 0.3, 0.4]
  }

For discrete/categorical data:

* A partition defines the categorical values present in the data.
* Partition weights define the probability of the associated categorical value.

Example discrete partition object:
.. code-block:: python
  {
    "partition": [ "cat", "dog", "fish"],
    "weights": [0.3, 0.3, 0.4]
  }


Constructing Partition Objects
--------------------------------------------------------------------------------
Three convenience functions are available to easily construct partition objects from existing data:

* :func:`categorical_partition_data <great_expectations.dataset.util.categorical_partition_data>`
* :func:`kde_smooth_data <great_expectations.dataset.util.kde_smooth_data>`
* :func:`partition_data <great_expectations.dataset.util.partition_data>`

A convenience function s provided to validate that an object is a valid partition density object:

* :func:`is_valid_partition_object <great_expectations.dataset.util.is_valid_partition_object>`

Distributional Expectations Core Tests
--------------------------------------------------------------------------------
Distributional expectations rely on three tests for their work.

Kullback-Leibler (KL) divergence is available as an expectation for both categorical and continuous data (continuous data will be discretized according to the provided partition prior to computing divergence). Unlike KS and Chi-Squared tests which can use a pvalue, you must provide a threshold for the relative entropy to use KL divergence. Further, KL divergence is not symmetric, which can make it difficult to differentiate certain distributions.

* :func:`expect_column_kl_divergence_to_be <great_expectations.dataset.base.DataSet.expect_column_kl_divergence_to_be>`


For continuous data, the expect_column_bootstrapped_ks_test_p_value_greater_than expectation uses the Kolmogorov-Smirnov (KS) test, which compares the actual and expected cumulative densities of the data. Because of the partition_object's piecewise uniform approximation of the expected distribution, the test would be overly sensitive to differences when used with a sample of data of much larger than the size of the partition. The expectation consequently uses a bootstrapping method to sample the provided data into samples of the same size as the partition and uses the mean of the resulting pvalues as the final test's pvalue.

* :func:`expect_column_bootstrapped_ks_test_p_value_greater_than <great_expectations.dataset.base.DataSet.expect_column_bootstrapped_ks_test_p_value_greater_than>`

For categorical data, the expect_column_chisquare_test_p_value_greater_than expectation uses the Chi-Squared test. The provided weights are scaled to the size of the data in the tested column at the time of the test.

* :func:`expect_column_chisquare_test_p_value_greater_than <great_expectations.dataset.base.DataSet.expect_column_chisquare_test_p_value_greater_than>`



Distributional Expectations Alternatives
--------------------------------------------------------------------------------
The core partition density object used in current expectations focuses on a particular (partition-based) method of "compressing" the data into a testable form, however it may be desireable to use alternative nonparametric approaches (e.g. Fourier transform/wavelets) to describe expected data.
