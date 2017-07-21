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

Example partition density object.
.. code-block:: bash
  {
    "partition": [ 0, 1, 2, 10],
    "weights": [0.3, 0.3, 0.4]
  }

For discrete/categorical data:
* A partition defines the categorical values present in the data.
* Partition weights define the probability of the associated categorical value.

.. code-block:: bash
  {
    "partition": [ "cat", "dog", "fish"],
    "weights": [0.3, 0.3, 0.4]
  }

A convenience function :func:`is_valid_partition_object <great_expectations.dataset.util.is_valid_partition_object(partition_object)>` is provided to validate that an object is a valid partition density object.


Constructing Partition Objects
--------------------------------------------------------------------------------
Three convenience functions are available to easily construct partition objects from existing data.
* :func:`categorical_partition_data <great_expectations.dataset.util.categorical_partition_data(data)>`
* :func:`kde_smooth_data <great_expectations.dataset.util.kde_smooth_data(data)>`
* :func:`partition_data <great_expectations.dataset.util.partition_data(data, bins='auto', n_bins)>`


Distributional Expectations Core Tests
--------------------------------------------------------------------------------
Distributional expectations rely on three tests for their work.

For continuous data, the expect_column_numerical_distribution_to_be expectation uses the Kolmogorov-Smirnov (KS) test, which compares the actual and expected cumulative densities of the data. Because of the partition_object's piecewise uniform approximation of the expected distribution, the test will be overly sensitive to differences when used with a sample of data of much larger than the size of the partition. The expectation consequently has a default rule of thumb to test using a subsample from the tested column of the same size as the partition. Because the test is generally run using a subsample of the data in the tested column, it may also be advisable to use a bootstrapping method around the expectation to improve the detection probability of divergence from the expected distribution.

For categorical data, the expect_column_frequency_distribution_to_be expectation uses the Chi-Squared test. The provided weights are scaled to the size of the data in the tested column at the time of the test.

Finally, Kullback-Leibler (KL) divergence is available as an expectation for both categorical and continuous data (because the data will be discretizes into the partition interval prior to computing divergence). Unlike KS and Chi-Squared tests which can use a pvalue, you must provide a threshold for the relative entropy to use KL divergence; further KL divergence is not symmetric, which can make it difficult to differentiate certain distributions.

Distributional Expectations Alternatives
--------------------------------------------------------------------------------
The core partition density object used in current expectations focuses on a particular (partition-based) method of "compressing" the data into a testable form, however it may be desireable to use alternative nonparametric approaches (e.g. Fourier transform/wavelets) to describe expected data.
