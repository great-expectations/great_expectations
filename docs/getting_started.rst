.. _getting_started:

Getting started
==================

It's easy! Just use pip install:

::

    $ pip install great_expectations

(You may want to deploy within a virtual environment. If you're not familiar with pip, virtual environments, notebooks, or git, please see Supporting Resources below for links to tutorials.)

From there, follow these steps to deploy Great Expectations.

.. toctree::
   :maxdepth: 1

   /getting_started/cli_init
   /getting_started/create_expectations
   /getting_started/pipeline_integration

Supporting resources
-------------------------------------

Great expectations requires a python compute environment and access to data, either locally or \
through a database or distributed cluster. In addition, developing with great expectations relies \
heavily on tools in the Python engineering ecosystem: pip, virtual environments, jupyter notebooks. \
We also assume some level of familiarity with git and version control.

See the links below for good, practical tutorials for these tools.

**pip**

**virtual_environments**

**jupyter notebooks**

**git**



Installing from github
--------------------------------------

If you plan to make changes to great expectations, you may want to clone from GitHub and pip install using the `--editable <https://stackoverflow.com/questions/35064426/when-would-the-e-editable-option-be-useful-with-pip-install>`__ flag. 

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install -e great_expectations/

