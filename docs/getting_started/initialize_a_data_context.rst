.. _getting_started__initialize_a_data_context:

Initialize a Data Context
===============================================

In Great Expectations, your ``DataContext`` manages boilerplate configuration. Using a DataContext is almost always the fastest way to get up and running, even though some teams don't need every component of a DataContext.

If you really want to learn the components of Great Expectations without a DataContext, check out `A magic-free introduction to Great Expectations.`_


Install Great Expectations
-----------------------------------------------

If you haven't already, install Great Expectations.

We recommend deploying within a virtual environment. If you're not familiar with pip, virtual environments, notebooks,
or git, you may want to check out the :ref:`supporting_resources` section before continuing.

.. raw:: html

   The command to install is especially great <a href="https://great-expectations-web-assets.s3.us-east-2.amazonaws.com/pip_install_great_expectations.png" target="_blank">if you're a Dickens fan</a>:
   <br/>
   <br/>

.. code-block:: bash

    $ pip install great_expectations

To install from a git fork or branch:

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install -e great_expectations/


Run ``great_expectations init``
-----------------------------------------------

The :ref:`command line interface (CLI) <command_line>` provides the easiest way to start using Great Expectations.

The ``init`` command will walk you through setting up a new project and connecting to your data.

Make sure that the machine that you installed GE on has access to a filesystem with data files (e.g., CSV) or a database.

If you prefer to use some sample data first, we suggest this example data from the United States Centers for Medicare and Medicaid Services `National Provider
Identifier Standard <https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination.html>`_
(NPI). Some later Great Expectations tutorials use this dataset, so this will make it easy to follow along.

To download this sample dataset:

.. code-block:: bash

    git clone https://github.com/superconductive/ge_example_project.git
    cd ge_example_project


Once you have decided which data you will use, you are ready to start. Run this command in the terminal:

.. code-block:: bash

    great_expectations init


After you complete the ``init`` command, read this article to get a more complete picture of how data teams use Great Expectations:  :ref:`typical workflow <typical_workflow>`.
