.. _getting_started__pick_your_deployment_pattern:

Pick your deployment pattern
===============================================

At this point, you have your first, working deployment of Great Expectations. It's pretty basic: most of the storage and execution are handled locally.

The next step is to pick your deployment pattern. DataContexts make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps---so you can upgrade from a basic demo deployment to a full production deployment at your own pace and be confident that your DataContext will continue to work at every step along the way.

This last section of the :ref:`getting_started` tutorial is designed to present you with clear options. For specific implementation steps, please check out the linked :ref:`how_to_guides`.

Components
--------------------------------------------------

Here's an overview of the components of a typical Great Expectations deployment:

* Great Expectations configs and metadata 

    * Options for storing Great Expectations configuration
    * Options for storing Expectations
    * Options for storing ValidationResults

* Integrations to related systems

    * Additional DataSources and Generators
    * Other options for hosting data documentation
    * Other options for triggering validation
    * Additional ValidationActions

Caveats on work in development
--------------------------------------------------

We wish everything worked seamlessly today, but the Great Expectations ecosystem is expanding rapidly and `good things take time`_ #FIXME: Find an actual Dickens quote.

You will 

.. raw:: html

   <embed>
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css">
   </embed>

   <embed>
      <p>First, some components are still at the <span class="fas fa-circle" style="color:red"></span> experimental or <span class="fas fa-circle" style="color:yellow"></span> beta stage. In that case, they are marked with one of these icons: <span class="fas fa-circle" style="color:yellow"></span> <span class="fas fa-circle" style="color:red"></span>.
      
      Please see `Feature maturity grid`_ and `Levels of maturity`_ for more details.
      </p>
   </embed>


Second, in some cases, tutorials are stubbed out. In that case, please vote with your

Great Expectation is an open source community. If you really want it built, please join the community and help us build it!

In the meantime, we're committed to `making levels of maturity transparent`_, and `streamlining the process for contribution and partnership`_.


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
        │   └── local_site
        └── validations

This ``great_expectations/`` directory contains all of the important components of a Great Expectations deployment, in miniature:

* The ``great_expectations.yml`` configuration file defines how to access the project's Data Sources, Expectations, Validation Results, etc.
* The ``expectations/`` directory stores all your Expectations as JSON files.
* The ``uncommitted/`` directory contains files that shouldn't live in version control. It has a ``.gitignore`` configured to exclude all its contents from version control. The main contents of the default ``uncommitted/`` directory are:

  * ``uncommitted/config_variables.yml``, which should hold sensitive information, such as database credentials and other secrets.
  * ``uncommitted/validations``, which will hold Validation Results.
  * ``uncommitted/documentation``, which will hold contains data documentation generated from Expectations and Validation Results.

A note on git: many teams find it convenient to store Expectations and their ``great_expectations.yml`` in git . Essentially, this approach treats Expectations like test fixtures: they live adjacent to code and are stored within version control. git acts as a collaboration tool and source of record. Other alternatives, such as storing Expectations in a file store, or database are also possible. We'll discuss these more at the end of this tutorial.

