.. _how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli:

How to create a new Expectation Suite using the CLI
***************************************************

While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let :ref:`CLI <command_line>` save you time and typos.
Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:


.. code-block:: bash

    great_expectations suite new


This command prompts you to name your new Expectation Suite and to select a sample batch of data the suite will eventually describe.
Then an empty suite is created and added to your project.
Then it creates a jupyter notebook for you to start creating your new suite.
The command concludes by opening the newly generated jupyter notebook.

If you wish to skip the automated opening of jupyter notebook, add the `--no-jupyter` flag:


.. code-block:: bash

    great_expectations suite new --no-jupyter


.. discourse::
    :topic_identifier: 240
