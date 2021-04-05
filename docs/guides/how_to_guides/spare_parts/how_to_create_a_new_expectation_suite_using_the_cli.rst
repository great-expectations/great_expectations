

How to create a new Expectation Suite using the CLI
***************************************************

While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let :ref:`CLI <command_line>` save you time and typos.
Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:


.. code-block:: bash

    great_expectations suite new


This command prompts you to name your new Expectation Suite and to select a sample batch of data the suite will describe.
Then it uses a sample of the selected data to add some initial expectations to the suite.
The purpose of these is expectations is to provide examples of data assertions, and not to be meaningful.
They are intended only a starting point for you to build upon.

The command concludes by saving the newly generated Expectation Suite as a JSON file and rendering the expectation suite into an HTML page in Data Docs.
