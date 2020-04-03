.. _contributing_types_of_contributions:


Types of contributions
==========================================

* Bug fixes
* Improvements to documentation
* Tutorials and examples
* Plugins
* Core code changes

Bug reports and bug fixes
----------------------------

Improvements to documentation
--------------------------------

Tutorials and examples
--------------------------------

Plugins
--------------------------------

Plugins are ...

We are actively seeking contributions for five types of plugin:

* Expectations
* DataSources
* StoreBackends
* ValidationActions
* Renderers

As the ecosystem grows and we extend the project's QA capabilities, the number of plugin categories will grow. (For example: automated profilers.) If you have ideas or questions for these kinds of extensions, please reach out via ``#contributors`` in the `public Slack channel <greatexpectations.io/slack>`__.


Execution environments
************************************

[Feed forward from Eugene's doc]

#FIXME: For each of these:
* What is it?
* Link to examples?
* What's the typical dev loop?
* Do I need special infrastructure to build this?
* How do I test it?



Stores and store backends
*************************************

...are used to persist Great Expectations metadata. See [James' explainer videos number 2 (?) for more detail.]

.. raw:: html

   <embed>
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css">
   </embed>

   <embed>
      <p><i class="fas fa-exclamation-triangle" style="color:orange"></i> Stores and store backends are in beta. Develop with caution.</p>
   </embed>

The base classes are ``Store`` and ``StoreBackend.`` For good reference examples, see ``S3Store`` and ``LocalFileStore``, with tests in ``some_file`` and ``someother_file``, respectively.

Our test requirements for StoreBackends are:

1. First requirement

    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.

2. Another requirement for tests

    Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.

3. A third requirement

    Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.

To promote a new StoreBackend to {{maturity level X}}, we will need to add Y and Z to the public test grid.

1. Do something

    Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.

2. Do something else

    Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.

3. Then do a third thing

    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.

DataDocs deployment targets
*************************************


Expectations
*************************************


Validation operators
*************************************


Validation actions
*************************************


Renderers
*************************************

.. raw:: html

   <embed>
      <p><i class="fas fa-exclamation-triangle" style="color:red"></i> Renderers are in <a href="">alpha</a>. Develop at your own risk.</p>
   </embed>


Profilers
*************************************


Core code changes
----------------------------



