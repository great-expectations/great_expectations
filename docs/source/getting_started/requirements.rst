.. _requirements:

Requirements
==============================

Great expectations requires a python compute environment and access to data, either locally or \
through a database or distributed cluster. The tutorials below walk you through getting started \
with an example project.

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
