.. _getting_started:

Getting Started
==================

Requirements
---------------------

Great expectations requires a python compute environment and access to data, either locally or \
through a database or distributed cluster.

Installing with pip
---------------------

It's easy! Just use pip install:

::

    $ pip install great_expectations


Installing within a project
-----------------------------

As of v0.7.0, Great Expectations includes a mildly opinionated framework for deploying pipeline tests within projects.
This is now the recommended path for using Great Expectations.

To install within a project, go to the root directory of the project and run:
::

    great_expectations init

.. TODO: Optional dependencies

Tutorials
----------

The tutorials below walk you through getting started \
with an example project.

.. toctree::
   :maxdepth: 2

   /getting_started/cli_init
   /getting_started/create_expectations
   /getting_started/pipeline_integration
