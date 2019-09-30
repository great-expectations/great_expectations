.. _reference:

#############
Reference
#############

Reference materials provide clear descriptions of GE features. See the :ref:`module_docs` for more detailed descriptions
of classes and methods.

********************
Basic Functionality
********************

.. toctree::
   :maxdepth: 2

   /reference/creating_expectations
   /reference/expectation_glossary
   /reference/distributional_expectations
   /reference/custom_expectations
   /reference/implemented_expectations

**************
Core GE Types
**************

.. toctree::
   :maxdepth: 2

   /reference/standard_arguments
   /reference/result_format
   /reference/validation_result
   /reference/batch_kwargs

************************
Configuration Guides
************************

.. toctree::
   :maxdepth: 2

   /reference/data_context_reference
   /reference/data_documentation_reference
   /reference/profiling_reference
   /reference/migrating_versions

******************
Advanced Features
******************

.. toctree::
   :maxdepth: 2

   /reference/evaluation_parameters
   /reference/data_asset_features
   /reference/stores_reference
   /reference/batch_identification
   /reference/validation_operators

*****************************
Extending Great Expectations
*****************************

.. toctree::
   :maxdepth: 2

   /reference/extending_great_expectations
   /reference/contributing
   /reference/improving_library_documentation


.. _supporting_resources:

**********************************
Supporting Resources
**********************************

Great Expectations requires a python compute environment and access to data, either locally or
through a database or distributed cluster. In addition, developing with great expectations relies
heavily on tools in the Python engineering ecosystem: pip, virtual environments, and jupyter notebooks.
We also assume some level of familiarity with git and version control.

See the links below for good, practical tutorials for these tools.

Tools Reference
==================

pip
-------------------------------------------

    * https://pip.pypa.io/en/stable/
    * https://www.datacamp.com/community/tutorials/pip-python-package-manager

virtual environments
-------------------------------------------

    * https://virtualenv.pypa.io/en/latest/
    * https://python-guide-cn.readthedocs.io/en/latest/dev/virtualenvs.html
    * https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/

jupyter notebooks and jupyter lab
-------------------------------------------

    * https://jupyter.org/
    * https://jupyterlab.readthedocs.io/en/stable/
    * https://towardsdatascience.com/jupyter-lab-evolution-of-the-jupyter-notebook-5297cacde6b

git
-------------------------------------------

    * https://git-scm.com/
    * https://reference.github.com/
    * https://www.atlassian.com/git/tutorials


Installing from github
===========================

If you plan to make changes to great expectations or live on the bleeding edge, you may want to clone from GitHub and \
pip install using the `--editable <https://stackoverflow.com/questions/35064426/when-would-the-e-editable-option-be-\
useful-with-pip-install>`__ flag.

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install -e great_expectations/

