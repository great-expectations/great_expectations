.. _distributional_expectations:

===========================
Distributional Expectations
===========================

Distributional expectations help identify when new datasets or samples may be different than expected, and can help ensure that assumptions developed during exploratory analysis still hold as new data becomes available. You should use distributional expectations in the same way as other expectations: to help accelerate identification of risks and changes to a modeled system or disruptions to a complex upstream data feed.

Great Expectations provides a direct, expressive framework for describing distributional expectations. Most distributional expectations apply nonparametric tests, although it is possible to build expectations from parameterized distributions.

.. _partition_object:

Partition Objects
--------------------------------------------------------------------------------

Distributional expectations rely on expected distributions or "partition objects", which are built from intervals for continuous data or categorical classes and their associated weights.

For continuous data:

* A partition is defined by an ordered list of points that define intervals on the real number line. Note that partition intervals do not need to be uniform.
* Each bin in a partition is partially open: a data element x is in bin i if lower_bound_i <= x < upper_bound_i.
* However, following the behavior of numpy.histogram, a data element x is in the largest bin k if x == upper_bound_k.
* A partition object can also include ``tail_weights`` which extend from -Infinity to the lowest bound, and from the highest bound to +Infinity.

* Partition weights define the probability of the associated bin or interval. Note that this applies a "piecewise uniform" distribution to the data for the purpose of statistical tests. The weights must define a valid probability distribution, i.e. they must be non-negative numbers that sum to 1.

Example continuous partition object:

.. code-block:: python

    partition = {
        "bins": [0, 1, 2, 10],
        "weights": [0.2, 0.3, 0.3],
        "tail_weights": [0.1, 0.1]
    }

>>> json.dumps(partition, indent=2)
{
  "bins": [
    0,
    1,
    2,
    10
  ],
  "weights": [
    0.3,
    0.3,
    0.4
  ],
  "tail_weights": [
    0.1,
    0.1
  ]
}

For discrete/categorical data:

* A partition defines the categorical values present in the data.
* Partition weights define the probability of the associated categorical value.

Example discrete partition object:

.. code-block:: python

  {
    "values": [ "cat", "dog", "fish"],
    "weights": [0.3, 0.3, 0.4]
  }


Constructing Partition Objects
------------------------------
Convenience functions are available to easily construct partition objects from existing data:

* :func:`build_continuous_partition_object <great_expectations.dataset.util.build_continuous_partition_object>`
* :func:`build_categorical_partition_object <great_expectations.dataset.util.build_categorical_partition_object>`

Convenience functions are also provided to validate that an object is valid:

* :func:`is_valid_continuous_partition_object <great_expectations.dataset.util.is_valid_continuous_partition_object>`
* :func:`is_valid_categorical_partition_object <great_expectations.dataset.util.is_valid_categorical_partition_object>`

Tests interpret partition objects literally, so care should be taken when a partition includes a segment with zero weight. The convenience methods consequently allow you to include small amounts of residual weight on the "tails" of a dataset used to construct a partition.


Available Distributional Expectations
--------------------------------------

`Kullback-Leibler (KL) divergence <https://www.youtube.com/watch?v=ErfnhcEV1O8)/>`_ (also known as relative entropy) is available as an expectation for both categorical and continuous data (continuous data will be discretized according to the provided partition prior to computing divergence). Unlike KS and Chi-Squared tests which can use a p-value, you must provide a threshold for the relative entropy to use KL divergence. Further, KL divergence is not symmetric.

* :func:`expect_column_kl_divergence_to_be_less_than <great_expectations.dataset.dataset.Dataset.expect_column_kl_divergence_to_be_less_than>`

For continuous data, the expect_column_bootstrapped_ks_test_p_value_to_be_greater_than expectation uses the `Kolmogorov-Smirnov (KS) test <https://www.youtube.com/watch?v=ZO2RmSkXK3c)/>`_, which compares the actual and expected cumulative densities of the data. Because of the partition_object's piecewise uniform approximation of the expected distribution, the test would be overly sensitive to differences when used with a sample of data of much larger than the size of the partition interval. The expectation consequently uses a bootstrapping method to sample the provided data with tunable specificity.

* :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than <great_expectations.dataset.dataset.Dataset.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than>`

For categorical data, the expect_column_chisquare_test_p_value_to_be_greater_than expectation uses the `Chi-Squared test <https://www.youtube.com/watch?v=7_cs1YlZoug&t=435s>`_. The Chi-Squared test works with expected and observed counts, but that is handled internally in this function -- both the input and output to this function are valid partition objects (ie with weights that are probabilities and sum to 1).

* :func:`expect_column_chisquare_test_p_value_to_be_greater_than <great_expectations.dataset.dataset.Dataset.expect_column_chisquare_test_p_value_to_be_greater_than>`
