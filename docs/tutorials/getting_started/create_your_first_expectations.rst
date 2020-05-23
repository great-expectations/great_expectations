.. _getting_started__create_your_first_expectations:

Create your first Expectations
==============================

:ref:`Expectations` are the workhorse abstraction in Great Expectations.

Each Expectation is a declarative, machine-verifiable assertion about the expected format, content, or behavior of your data. Great Expectations comes with :ref:`dozens of built-in Expectations <Glossary of Expectations>`, and it's easy to :ref:`develop your own custom Expectations <how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations>`, too.

.. admonition:: Admonition from Mr. Dickens.

    "Take nothing on its looks; take everything on evidence. There's no better rule."

The CLI will help you create your first Expectations. You can accept the defaults by typing [Enter] twice:

.. code-block:: bash

    Would you like to profile new Expectations for a single data asset within your new Datasource? [Y/n]: 

    Would you like to: 
        1. choose from a list of data assets in this datasource
        2. enter the path of a data file
    : 1

    Which data would you like to use?
        1. 7210_1 (file)
        2. Datafiniti_Womens_Shoes (file)
        3. Datafiniti_Womens_Shoes_Jun19 (file)
        Don't see the name of the data asset in the list above? Just type it
    : 1

    Name the new Expectation Suite [7210_1.warning]: 

    Great Expectations will choose a couple of columns and generate Expectations about them
    to demonstrate some examples of assertions you can make about your data.
        
    Great Expectations will store these expectations in a new Expectation Suite '7210_1.warning' here:

      file:///Users/eugenemandel/projects/fellows/great_expectations/expectations/7210_1/warning.json

    OK to proceed? [Y/n]: 

    Generating example Expectation Suite...

    Done generating example Expectation Suite

What just happened?
-------------------

You can create and edit Expectations using several different workflows. The CLI just used one of the quickest and simplest: scaffolding Expectations using an automated :ref:`Profiler <Profilers>`.

This Profiler connected to your data (using the Datasource you configured in the previous step), took a quick look at the contents, and produced an initial set of Expectations. These Expectations are not intended to be very smart. Instead, the goal is to quickly provide some good examples, so that you're not starting from a blank slate.

Later, you should also take a look at other workflows for :ref:`Creating and editing Expectations`, such as:

    * #FIXME: Finalize this list after we flesh out the how-tos for creating and editing expectations
    * :ref:`How to edit Expectations in a disposable notebook`
    * :ref:`How to adjust Expectations in a disposable notebook after Validation`
    * :ref:`How to profile many tables at once`
    * :ref:`How to calibrate Expectation Suite parameters using multibatch profiling`

Creating and editing Expectations is a very active area of work in the Great Expectations community. Stay tuned for improvements over time.

Note: the Profiler also validated the source data using the new Expectations, producing a set of :ref:`Validation Results`. We'll explain why in the next step of the tutorial.

A first look at real Expectations
---------------------------------

The newly profiled Expectations are stored in an :ref:`Expectation Suite`.

For now, they're stored in a JSON file in a subdirectory subdirectory of your ``great_expectations/`` folder. You can also configure Great Expectations to store Expectations to other locations, like S3, postgresql, etc. We'll come back to these options in the last step of the tutorial.

If you open up the suite in ``great_expectations/expectations/something-something.json`` in a text editor, you'll see:

.. code-block:: JSON

    #FIXME


There's a lot of information here. (This is good.)

Every Expectation in the file expresses a test that can be validated against data. (This is very good.)

We were able to generate all of this information very quickly. (Also good.)

However, most human beings find that dense JSON objects are very hard to read. (This is bad.)

In the next step of the tutorial, we'll show how to convert Expectations into more human-friendly formats: :ref:`Set up Data Docs`.