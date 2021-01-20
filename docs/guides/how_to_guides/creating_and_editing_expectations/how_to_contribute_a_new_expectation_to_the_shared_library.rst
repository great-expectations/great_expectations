.. _how_to_guides__creating_and_editing_expectations__how_to_template:

How to contribute a new Expectation to the shared library
=========================================================

This guide will help you add a new Expectation to Great Expectations’ shared library. Your Expectation will be featured in the Expectations Gallery, along with many others developed by data practitioners from around the world as part of this collaborative community effort.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up your dev environment to contribute <contributing_setting_up_your_dev_environment>`
  - :ref:`Signed the Contributor License Agreement (CLA) <contributing_cla>`

Steps
-----

#. Choose the type of Expectation you want to create.

    There are four Expectation classes that make the development of particular types of Expectations significantly easier by hiding the "crud" code and letting you focus on the business logic of your Expectation. Decide which one suites your Expectation:

        - ``ColumnMapExpectation`` - Expectations of this type validate a single column of tabular data. First they ask a yes/no question from every row in that column. Then they ask what percentage of rows gave a positive answer to the first question. If the answer to the second question is above a specified threshold, the Expectation considers the data valid.
        - ``ColumnExpectation`` s are also evaluated for a single column, but produce an aggregate metric, such as a mean, standard deviation, number of unique values, type, etc.
        - ``ColumnPairMapExpectation`` s are similar to ``ColumnMapExpectations``, except that they are based on two columns, instead of one.
        - ``TableExpectation`` are a generic catchall for other types of Expectations applied to tabular data.


    Find the appropriate template file in ``great_expectations/examples/expectations/``. Starting your development with these templates is significantly easier than developing from scratch.

#. Copy the template file into the appropriate `contrib/` directory.

    Recently we introduced a fast-track release process for Expectations that a contributor places in one of the subdirectories of ``contrib``.
    They are released as PyPI packages separate from ``great-expectations``. When you create a new Expectation in ``contrib/experimental/great_expectations_experimental/expectations/``,
    once your PR is approved and merged, a new version of PyPI package ``great-expectations-experimental``is automatically published.

#. Pick a name for your Expectation, rename the file and the class within it.

    Great Expectations follows a naming convention. Classes that implement Expectations have CamelCase names (e.g., "ExpectColumnValuesToBeThree"). The framework will
    automatically translate this class name into a method with the snake_case name of "expect_column_values_to_be_three".
    The Python file that contains the class should be given the snake_case name of the Expectation (e.g., "expect_column_values_to_be_three.py").

    Give your new Expectation a name that will be clear to its future users. Based on the class that your new Expectation will be extending, use the following conventions:

    * Column map Expectations: ``expect_column_values_...`` (e.g., "expect_column_values_to_match_regex")
    * Column aggregate Expectations: ``expect_column_...`` (e.g., "expect_column_mean_to_be_between")
    * Column pair map Expectations: ``expect_column_pair_values_...`` (e.g., "expect_column_pair_values_to_be_in_set")
    * Tabe Expectatons: ``expect_table_...`` (e.g., "expect_table_row_count_to_be_equal")

    For example, if you call your Expectation ``ExpectColumnValuesToEqualThree``, you will copy it to ``contrib/experimental/great_expectations_experimental/expectations/expect_column_values_to_equal_three.py``

    For more style conventions that your code should follow consult our :ref:`Style Guide <contributing__style_guide>`

#. Within the file, update the name of your Expectation.

    You'll to do this in two places:

    * Class declaration (search for ``class ExpectColumnValuesToEqualThree``)
    * A call to ``run_diagnostic`` in the very end of the template (search for ``diagnostics_report = ExpectColumnValuesToEqualThree().run_diagnostics()``). Next section explains the role this code plays.

#. Execute the template file.

    The simplest way to do this is as a standalone script. Note: if you prefer, you can also execute within a notebook or IDE.

    .. code-block:: yaml

        python expect_column_values_to_equal_three.py

    Running the script will execute the ``run_diagnostics`` method for your new class. Initially, it will just return:

    .. code-block:: json

        {
          "description": {
            "camel_name": "ExpectColumnValuesToEqualThree",
            "snake_name": "expect_column_values_to_equal_three",
            "short_description": "",
            "docstring": ""
          },
          "library_metadata": {
            "maturity": "experimental",
            "package": "experimental_expectations",
            "tags": [],
            "contributors": []
          },
          "renderers": {},
          "examples": [],
          "metrics": [],
          "execution_engines": {}
        }

    This output is a report on the completeness of your Expectation.

    You will repeat this step many times during developing your Expectation. ``run_diagnostics`` creates an easy and fast "dev loop" for you -
    make a small change in the code, run ``run_diagnostics``, examine its output for failures and next steps.

    From this point on, we'll start filling in the pieces of your Expectation. You can stop this at any point.

#. Add an example test.

    Search for ``examples = [`` in your file.

    These examples serve a dual purpose:

        * help the users of the Expectation understand its logic by providing examples of input data that the Expectation will evaluate as valid and as invalid. When your Expectation is released, its entry in the Expectations Gallery site will render these examples.
        * provide test cases that the Great Expectations testing framework can execute automatically

    We will explain the structure of these tests using the example provided in one of the templates that implements ``expect_column_values_to_equal_three``.

    .. code-block:: python

        examples = [{
            "data": {
                "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                }
            ],
        }]



    The value of ``examples`` is a list of examples.

    Each example is a dictionary with two keys:
    * data: defines the input data of the example as a table/data frame. In this example the table has one column named "mostly_threes" with 10 rows.
    * tests: a list of test cases that use the data defined above as input to validate
        * ‘in’ contains exactly the parameters that you want to pass in to the Expectation. ``"in": {"column": "mostly_threes", "mostly": 0.6}`` in the example above is equivalent to ``expect_column_values_to_equal_three(column="mostly_threes, mostly=0.6)``
        * ‘out’ is based on the Validation Result returned when executing the Expectation. If you set exact_match=False, then you don’t need to include all the elements of the result object---only the ones that are important to test.

    Uncomment that code snippet and replace with your examples.

    Run ``run_diagnostics`` again. The newly added examples will appear in the output. They are not executed as tests yet, because most of the code in the Expectation is still commented out.

    .. admonition:: Note:

        - When you define data in your examples, we will mostly guess the type of the columns. Sometimes you need to specify the precise type of the columns for each backend. Then you use ``schema`` atribute in an example to achieve this:

        .. code-block:: json

            "schemas": {
              "spark": {
                "mostly_threes": "IntegerType",
              },
              "sqlite": {
                "mostly_threes": "INTEGER",
              },



    Next implementation steps differ based on the type of Expectations you are implementing. Click on the appropriate tab below.

.. content-tabs::

    .. tab-container:: tab0
        :title: ColumnMapExpectations

        :ref:`Core Concepts: Expectations and Metrics <reference__core_concepts__expectations>`

        #. Implement the ``_pandas`` method within your Metric class.

            Rename the metric in three places:
                1. The class name in your Metric class
                2. condition_metric_name in your Metric class
                3. map_metric in your Expectation class

            Uncomment the ``_pandas`` method with its decorator. Lines AAA through BBB.

            Add logic.

            About adding arguments:

                Can I add a positional argument to the method signature, or must it be a keyword argument?

                Aside from the method sig itself, where else do you need to make changes to add an argument?

                    Metric.condition_value_keys
                    Expectation.success_keys

                Add validation, if necessary.

                    If I'm adding validation, what error do I throw?

                What is the ``column`` argument?

                What about ``column_A`` and ``column_B``?

                How do I add additional arguments?
                    ``column``


            {{Execute again}}

            If tests pass, great!

        #. Fill in the ``library_metadata`` dictionary.
        #. Add Renderers.
        #. Implement the ``_sql`` method within the Metric class.
        #. Implement the ``_spark`` method within the Metric class.

    .. tab-container:: tab1
        :title: ColumnExpectation

        TODO

    .. tab-container:: tab2
        :title: ColumnPairMapExpectation

        TODO

    .. tab-container:: tab3
        :title: TableExpectation

        TODO



Additional notes
----------------

How-to guides are not about teaching or explanation. They are about providing clear, bite-sized replication steps. If you **must** include a longer explanation, it should go in this section.

Additional resources
--------------------

- `Links in RST <https://docutils.sourceforge.io/docs/user/rst/quickref.html#hyperlink-targets>`_ are a pain.

Comments
--------

.. discourse::
   :topic_identifier: {{topic_id}}
