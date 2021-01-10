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

    There are four main Expectation classes, listed here from most common and specific to most general:

        - ``ColumnMapExpectations`` are {{explain}}
        - ``ColumnAggregateExpectation`` are {{explain}}
        - ``ColumnPairMapExpectation`` are {{explain}}
        - ``TableExpectation`` are {{explain}}

    For more details on classes of Expectations, please check out {{link to doc in Core Concepts.}}

    Find the appropriate template file in ``great_expectations/examples/expectations/``.

#. Pick a name for your Expectation and copy the template file into the appropriate `contrib/` directory.

    If your Expectation was called ``ExpectColumnValuesToEqualThree``, you would copy it to ``contrib/experimental_expectations/expectations/expect_column_values_to_equal_three.py``

    {{Explain the roles of `contrib/` and `experimental_expectations/`}}

#. Execute the template file as a script.

    .. code-block:: yaml

        python expect_column_values_to_equal_three.py

    Note: if you prefer, you can also execute within a notebook or IDE.

    Running the script will execute the ``self_check`` method for your new class. Initially, it will just return:

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

#. Add an example in the dictionary.

    Most of the other functionality in ``self_check`` depends on having an example to work from.
    
    ...

#. Fill in the other components of the Expectation.

    Recommended order:

    #. Create an example
    #. Implement a single method in the Metric. Probably the ``_pandas`` method.
    #. Fill in the ``library_metadata`` dictionary.
    #. Add Renderers.
    #. Implement the other Metric methods.

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
