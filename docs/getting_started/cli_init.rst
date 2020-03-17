.. _tutorial_init:

Run ``great_expectations init``
===============================================

The :ref:`command line interface (CLI) <command_line>` provides the easiest way to start using Great Expectations.

The `init` command will walk you through setting up a new project and connecting to your data.

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


After you complete the `init` command, read this article to get a more complete picture of how data teams use Great Expectations:  :ref:`typical_workflow`.
