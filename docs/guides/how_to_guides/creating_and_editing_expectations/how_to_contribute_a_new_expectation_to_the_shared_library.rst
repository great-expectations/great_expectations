.. _how_to_guides__creating_and_editing_expectations__how_to_template:

How to contribute a new Expectation to the shared library
=========================================================

This guide will help you add a new Expectation to Great Expectations’ shared library. {{Talk about why this is cool - will appear in the gallery - can be bite-sized - collaborative community effort}}

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up your dev environment to contribute <_contributing_setting_up_your_dev_environment>`
  - Set up a github account and signed the Contributor License Agreement (CLA)

Steps
-----

#. Choose the type of Expectation you want to create.

    There are four main Expectation classes, listed here from most specific to most general:

        - ``ColumnMapExpectations`` are evaluated for a single column, producing True or False on a row by row basis.
        - ``ColumnAggregateExpectation`` are also evaluated for a single column, but produce an aggregate metric, such as a mean, standard deviation, number of unique values, type, etc.
        - ``ColumnPairMapExpectation`` are similar to ``ColumnMapExpectations``, except that they are based on two columns, instead of one.
        - ``TableExpectation`` are a generic catchall for other types of Expectations applied to tabular data.

    For more details on classes of Expectations, please check out {{link to doc in Core Concepts.}}

    Find the appropriate template file in ``great_expectations/examples/expectations/``.

#. Pick a name for your Expectation and copy the template file into the appropriate `contrib/` directory.

    {{Explain naming conventions for Expectations.}}

    {{Explain the roles of `contrib/` and `experimental_expectations/`}}

    If your Expectation was called ``ExpectColumnValuesToEqualThree``, you would copy it to ``contrib/experimental_expectations/expectations/expect_column_values_to_equal_three.py``

#. Within the file, update the name of your Expectation.

    You'll to do this in two places: {{...}}

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
        "library_metadata": {},
        "renderers": {},
        "examples": [],
        "metrics": [],
        "execution_engines": {}
      }

    From this point on, we'll start filling in the pieces of your Expectation. You can stop this at any point.

    Recommended order:

        #. Create an example
        #. Implement a single method in the Metric. Probably the ``_pandas`` method.
        #. Fill in the ``library_metadata`` dictionary.
        #. Add Renderers.
        #. Implement the other Metric methods.


#. Add an example test in the ``examples`` dictionary staring on line 46.

    Most of the other functionality in ``run_diagnostics`` depends on having an example to work from.

    The ``examples`` dictionary contains 
    
    ...

    Add a corresponding test in the ``examples`` dictionary.

    Within ``in``, you will need to add parameters.


    {{Execute again}}

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
