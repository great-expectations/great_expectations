.. _how_to_guides__how_to_create_a_suite_from_a_json_schema_file:

How to create a new Expectation Suite from a jsonschema file
============================================================


The ``JsonSchemaProfiler`` helps you quickly create :ref:`Expectation Suites <reference__core_concepts__expectations__expectation_suites>` from jsonschema files.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Have a valid jsonschema file that has top level object of type `object`.

.. warning:: This implementation does not traverse any levels of nesting.

Steps
-----

1. **Set a filename and a suite name**

    .. code-block:: python

        jsonschema_file = "YOUR_JSON_SCHEMA_FILE.json"
        suite_name = "YOUR_SUITE_NAME"

2. **Load a DataContext**

    .. code-block:: python

        context = ge.data_context.DataContext()

3. **Load the jsonschema file**

    .. code-block:: python

        with open(jsonschema_file, "r") as f:
            schema = json.load(f)

4. **Instantiate the profiler**

    .. code-block:: python

        profiler = JsonSchemaProfiler()

5. **Create the suite**

    .. code-block:: python

        suite = profiler.profile(schema, suite_name)

6. **Save the suite**

    .. code-block:: python

        context.save_expectation_suite(suite)

7. **Optionally, generate Data Docs and review the results there.**

    Data Docs provides a concise and useful way to review the Expectation Suite that has been created.

    .. code-block:: bash

        context.build_data_docs()

     You can also review and update the Expectations created by the profiler to get to the Expectation Suite you want using ``great_expectations suite edit``.

Additional notes
----------------

.. important::

    Note that JsonSchemaProfiler generates Expectation Suites using column map expectations, which assumes a tabular data structure, because Great Expectations does not currently support nested data structures.

The full example script is here:

.. code-block:: python

    import json
    import great_expectations as ge
    from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler

    jsonschema_file = "YOUR_JSON_SCHEMA_FILE.json"
    suite_name = "YOUR_SUITE_NAME"

    context = ge.data_context.DataContext()

    with open(jsonschema_file, "r") as f:
        raw_json = f.read()
        schema = json.loads(raw_json)

    print("Generating suite...")
    profiler = JsonSchemaProfiler()
    suite = profiler.profile(schema, suite_name)
    context.save_expectation_suite(suite)

Comments
--------

    .. discourse::
        :topic_identifier: 268
