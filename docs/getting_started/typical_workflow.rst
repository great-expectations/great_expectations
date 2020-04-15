
Typical Workflow
===============================================

This article describes how data teams typically use Great Expectations.

The objective of this workflow is to gain control and confidence in your data pipeline and to address the challenges of validating and monitoring the quality and accuracy of your data.

Once the setup is complete, the workflow looks like a loop over the following steps:

1. Data team members capture and document their shared understanding of their data as expectations.
2. As new data arrives in the pipeline, Great Expectations evaluates it against these expectations.
3. If the observed properties of the data are found to be different from the expected ones, the team responds by rejecting (or fixing) the data, updating the expectations, or both.

The article focuses on the "What" and the "Why" of each step in this workflow, and touches on the "How" only briefly. The exact details of configuring and executing these steps are intentionally left out - they can be found in the tutorials and reference linked from each section.

If you have not installed Great Expectations and executed the :ref:`command line interface (CLI) <command_line>` init command, as described in this :ref:`tutorial <tutorial_init>`, we recommend you do so before reading the rest of the article. This will make a lot of concepts mentioned below more familiar to you.


Setting Up a Project
----------------------------------------

To use Great Expectations in a new data project, a :ref:`Data Context<data_context>` needs to be initialized.
You will see references to the Data Context throughout the documentation.
A Data Context provides the core services used in a Great Expectations project.

The :ref:`CLI <command_line>` command ``init`` does the initialization. Run this command in the terminal in the root of your project's repo:

.. code-block:: bash

    great_expectations init

This command has to be run only once per project.

The command creates ``great_expectations`` subdirectory in the current directory. The team member who runs it, commits the generated directory into the version control. The contents of ``great_expectations`` look like this:

.. code-block:: bash

    great_expectations
    ...
    ├── expectations
    ...
    ├── great_expectations.yml
    ├── notebooks
    ...
    ├── .gitignore
    └── uncommitted
        ├── config_variables.yml
        ├── documentation
        │   └── local_site
        └── validations

* The ``great_expectations/great_expectations.yml`` configuration file defines how to access the project's data, expectations, validation results, etc.
* The ``expectations`` directory is where the expectations are stored as JSON files.
* The ``uncommitted`` directory is the home for files that should not make it into the version control - it is configured to be excluded in the ``.gitignore`` file. Each team member will have their own content of this directory. In Great Expectations, files should not go into the version control for two main reasons:

  * They contain sensitive information. For example, to allow the ``great_expectations/great_expectations.yml`` configuration file, it must not contain any database credentials and other secrets. These secrets are stored in the ``uncommitted/config_variables.yml`` that is not checked in.

  * They are not a "primary source of truth" and can be regenerated. For example, ``uncommitted/documentation`` contains generated data documentation (this article will cover data documentation in a later section).


