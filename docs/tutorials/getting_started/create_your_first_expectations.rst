.. _getting_started__create_your_first_expectations:

Create your first Expectations
==============================

:ref:`Expectations` are the workhorse abstraction in Great Expectations.

The CLI will help you create your first Expectations. You can accept the defaults by typing [Enter] twice:

.. code-block:: bash

    Name the new expectation suite [religion-survey-results.warning]: 

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data. 
        
    Press Enter to continue
    :

    Generating example Expectation Suite...

.. admonition:: Admonition from Mr. Dickens.

    "Take nothing on its looks; take everything on evidence. There's no better rule."

What just happened?
-------------------

You can create and edit Expectations using several different workflows. The CLI just used one of the quickest and simplest: scaffolding Expectations using an automated :ref:`Profiler`_.

This Profiler connected to your data (using the Datasource you configured in the previous step), took a quick look at the contents, and produced an initial set of Expectations. These Expectations are not intended to be very smart. Instead, the goal is to quickly provide some good examples, so that you're not starting from a blank slate when you begin.

Later, you should also take a look at other workflows for :ref:`creating and editing Expectations`_, such as:

    * :ref:`How to edit Expectations in a disposable notebook`
    * :ref:`How to adjust Expectations in a disposable notebook after Validation`
    * :ref:`How to profile many tables at once`
    * :ref:`How to calibrate Expectation Suite parameters using multibatch profiling`

Note: the Profiler also validated the source data using the new Expectations, producing a set of :ref:`Validation Results`. We'll explain why in the next step of the tutorial.

A first look at real Expectations
---------------------------------

The newly profiled Expectations are stored in an :ref:`Expectation Suite`_.

For now, they're stored in a JSON file in a subdirectory subdirectory of your ``great_expectations/`` folder. You can also configure Great Expectations to store Expectations to other locations, like S3, postgresql, etc. We'll come back to these options in the last step of the tutorial.

If you open up the suite in ``great_expectations/expectations/something-something.json`` in a text editor, you'll see:

.. code-block:: JSON

    #FIXME


There's a lot of information here. (This is good.)

Every Expectation in the file expresses a test that can be validated against data. (This is very good.)

We were able to generate all of this information very quickly. (Also good.)

However, as a human, dense JSON objects are very hard to read (This is bad.)

In the next step of the tutorial, we'll show how to convert Expectations into more human-friendly formats: :ref:`Set up Auto Docs`.