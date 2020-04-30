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

What just happened?
-------------------

You can create and edit Expectations using several different workflows. The CLI just used one of the quickest and simplest: scaffolding Expectations using an automated :ref:`Profiler`_.

This Profiler connected to your data (using the Datasource you configured in the previous step), took a quick look at the contents, and produced an initial set of Expectations. These Expectations are *not* intended to be very smart. Instead, the goal is to provide some good examples, so that you're not starting from a blank slate when you begin.

Later, you should also take a look at other workflows for :ref:`creating and editing Expectations`_, such as:

    * :ref:`how_to_X`
    * :ref:`how_to_Y`
    * :ref:`how_to_Z`

The newly profiled Expectations are stored in an :ref:`Expectation Suite`. For now, they're stored in a subdirectory of your ``great_expectations/`` folder.

You can also configure Great Expectations to store Expectations to other locations, like S3, postgresql, etc. We'll come back to these options in the last step of the tutorial.

For now, let's continue to :ref:`Setting up data docs`.