.. _distributional_expectations:

================================================================================
Distributional Expectations
================================================================================

Distributional expectations help identify when new datasets or samples may be different than expected, and can help ensure that assumptions developed during exploratory analysis still hold as new data becomes available. You should use distributional expectations in the same way as other assumptions: to provide a rough analogue to unit testing for software development, where the expectations can help to accelerate identification of risks as diverse as changes in a system being modeled or simply disruptions in a complex upstream data feed.

Great Expectations' Philosophy of Distributional Expectations --------------------------------------------------------------------------------

Great Expectations attempts to provide a simple, expressive framework for describing distributional expectations. The framework generally adopts a nonparametric approach, although it is possible to build expectations from parameterized distributions.

The design is motivated by several constraints or assumptions:
* Encoding expectations into a simple object that allows for portable data pipeline testing is the top priority. In many circumstances the loss of precision associated with "compressing" data into an expectation may be beneficial because of its intentional simplicity as well as because it adds a very light layer of obfuscation over the data which may align with privacy preservation goals.
* While it should be possible to easily extend the framework with more rigorous statistical tests, great expectations should provide simple, reasonable defaults

Distributional Expectations Core' Datatypes
--------------------------------------------------------------------------------

Distributional expectations currently focus exclusively on unidimensional (columnar) data. The core constructs of a great expectations distributional expectation are the partition and the associated probability densities.

* A partition is defined by an ordered list of points that define intervals on the real number line. Currently, -inf and +inf are not explicitly supported as endpoints, so the term partition is used somewhat loosely.
* Partition densities define the density of the associated interval. Note that densities are not weights, but are weighted by the length of the associated interval. A convenience function,

A convenience function * :func:`is_valid_partition_object <great_expectations.dataset.util.is_valid_partition_object(partition_object)>` is provided to validate that an object is a valid partition density object.

Example partition density object.
.. code-block:: bash
  {
    "partition": [ 0, 1, 2, 10],
    "weights": [0.3, 0.3, 0.4]
  }

Categorical elements all have equal "size" so their densities should always sum to one.
.. code-block:: bash
  {
    "partition": [ "cat", "dog", "fish"],
    "weights": [0.3, 0.3, 0.4]
  }


Distributional Expectations Core Tests
--------------------------------------------------------------------------------
Distributional expectations rely on three tests for their work. The expect_column_numerical_distribution_to_be expectation uses the Kolmogorov-Smirnov (KS) test, which compares the actual and expected cumulative densities of the data, and applies a simple rule of thumb based on the size of the partition to calibrate the pvalue.

Alternately, it may be preferable to use a measure such as Kullback-Leibler (KL) divergence. However, you must provide a threshold for the relative entropy to use that test, and the test is not symmetric, which can make it difficult to differentiate certain distributions. The KL divergence can be used through the expect_column_kl_divergence_to_be expectation.

Finally, for categorical data, the expect_column_frequency_distribution_to_be expectation uses the Chi-Squared test.

Distributional Expectations Alternatives
--------------------------------------------------------------------------------
The core partition density object used in current expectations focuses on a particular (partition-based) method of "compressing" the data into a testable form, however it may be desireable to use alternative nonparametric approaches (e.g. Fourier transform/wavelets) to describe expected data.
