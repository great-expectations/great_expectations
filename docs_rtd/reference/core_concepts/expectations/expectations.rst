.. _expectations:

############
Expectations
############

An Expectation is a statement describing a verifiable property of data. Like assertions in traditional python unit tests,
Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests,
Great Expectations applies Expectations to data instead of code.

Great Expectations' built-in library includes more than 50 common Expectations, such as:

* ``expect_column_values_to_not_be_null``
* ``expect_column_values_to_match_regex``
* ``expect_column_values_to_be_unique``
* ``expect_column_values_to_match_strftime_format``
* ``expect_table_row_count_to_be_between``
* ``expect_column_median_to_be_between``

For a full list of available Expectations, please check out the :ref:`expectation_glossary`. Please note that not all Expectations are implemented on all Execution Engines yet. You can see the grid of supported Expectations :ref:`here <implemented_expectations>`. We welcome :ref:`contributions <contributing>` to fill in the gaps.

You can also extend Great Expectations by :ref:`creating your own custom Expectations <how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations>`.

Expectations *enhance communication* about your data and *improve quality* for data applications. Using expectations
helps reduce trips to domain experts and avoids leaving insights about data on the "cutting room floor."


.. _reference__core_concepts__expectations__domain_and_success_keys:

Expectation Concepts: Domain and Success Keys
**********************************************

A **domain** for an Expectation or Metric is all of the data that the Expectation or Metric evaluates. In Great Expectations, Expectation Configurations and Metric Configurations include ``domain_kwargs``, which are sets of key-value pairs, to define the data to which they should be applied.

- **Domain keys** are the keys in a set of ``domain_kwargs``. Different domains may use different domain keys, such as ``batch_id`` to specify data from a particular batch, ``table`` to define data from a specific table, ``column`` to define data from a particular column, or ``row_condition`` to filter data meeting a specified condition.

- In most Expectation Configurations, the ``batch_id`` and/or ``table`` is omitted, in which case the domain is evaluated *relative to the currently active batch*. For example, most Column Map Expectations use a single domain key, ``column`` to describe the domain to which they apply.


** ADVANCED NOTE: SAFELY IGNORE THIS UNLESS YOU ARE EXTENDING GREAT EXPECTATIONS **

When Great Expectations computes Metrics, it will attempt to bundle compute requests for efficiency. To do that, Execution Engines provide a ``get_compute_domain`` method which returns a tuple containing three elements. The first is an Engine-specific pointer to the data object that should be used to compute the Metric (e.g. a DataFrame for pandas/Spark Execution Engine or SQLAlchemy ``Selectable``). The second and third items are ``compute_domain_kwargs`` and ``accessor_domain_kwargs`` respectively, which are built by splitting the original ``domain_kwargs`` into two parts based on whether the ``domain_key`` has already been processed or still needs to be applied to the Engine-specific data object.

An Expectation also defines **success keys** that determine the values of its metrics and when the Expectation will succeed.

For example, the ``expect_column_values_to_be_in_set`` Expectation relies on the ``batch_id``, ``table``, ``column``, and ``row_condition`` **domain keys** to determine what data are described by a particular configuration, and the ``value_set`` and ``mostly`` **success keys** to evaluate whether the Expectation is actually met for that data.

- **Note**: The *batch_id* and *table* domain keys are often omitted when running a validation, because the Expectation is being applied to a single batch and table. However, they must be provided in cases where they could be ambiguous.

**Metrics** use a similar concept: they also use the same kind of **domain keys** as Expectations, but instead of success keys, we call the keys that determine a Metric's value its **value keys**.



.. _reference__core_concepts__expectations__expectation_suites:

Expectation Suites
******************

Expectation Suites combine multiple expectations into an overall description of a dataset. For example, a team can group all the expectations about its ``rating`` table in the movie ratings database from our previous example into an Expectation Suite and call it ``movieratings.ratings``. Note these names are completely flexible and the only constraint on the name of a suite is that it must be unique to a given project.

Each Expectation Suite is saved as a JSON file in the ``great_expectations/expectations`` subdirectory of the Data Context. Users check these files into the version control each time they are updated, same way they treat their source files. This discipline allows data quality to be an integral part of versioned pipeline releases.

The lifecycle of an Expectation Suite starts with creating it. Then it goes through an iterative loop of Review and Edit as the team's understanding of the data described by the suite evolves.



Methods for creating and editing Expectations
*********************************************

Generating expectations is one of the most important parts of using Great Expectations effectively, and there are
a variety of methods for generating and encoding expectations. When expectations are encoded in the GE format, they
become shareable and persistent sources of truth about how data was expected to behave-and how it actually did.

There are several paths to generating expectations:

1. Automated inspection of datasets. Currently, the profiler mechanism in GE produces expectation suites that can be
   used for validation. In some cases, the goal is :ref:`profiling` your data, and in other cases automated inspection
   can produce expectations that will be used in validating future batches of data.

2. Expertise. Rich experience from Subject Matter Experts, Analysts, and data owners is often a critical source of
   expectations. Interviewing experts and encoding their tacit knowledge of common distributions, values, or failure
   conditions can be can excellent way to generate expectations.

3. Exploratory Analysis. Using GE in an exploratory analysis workflow (e.g. within Jupyter notebooks) is an important \
   way to develop experience with both raw and derived datasets and generate useful and
   testable expectations about characteristics that may be important for the data's eventual purpose, whether
   reporting or feeding another downstream model or data system.


Custom expectations
*******************

Expectations are especially useful when they capture critical aspects of data understanding that analysts and
practitioners know based on its *semantic* meaning. It's common to want to extend Great Expectations with application-or domain-specific Expectations. For example:

.. code-block:: bash

    expect_column_text_to_be_in_english
    expect_column_value_to_be_valid_icd_code

These Expectations aren't included in the default set, but could be very useful for specific applications.

Fear not! Great Expectations is designed for customization and extensibility.

Building custom expectations is easy and allows your custom logic to become part of the validation, documentation, and even profiling workflows that make Great Expectations stand out. See the guide on :ref:`how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations`
for more information on building expectations and updating DataContext configurations to automatically load batches
of data with custom Data Assets.
