.. _how_to_guides__validation__how_to_run_a_checkpoint_in_terminal:

How to run a Checkpoint in terminal
===================================

This guide will help you run a Checkpoint in a terminal.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - :ref:`Created a Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`

Steps
-----

1. Checkpoints can be run like applications from the command line by running:

.. code-block:: bash

    great_expectations checkpoint run my_checkpoint
    Validation Failed!

2. Next, observe the output which will tell you if all validations passed or failed.

Additional notes
----------------

This command will return posix status codes and print messages as follows:

+-------------------------------+-----------------+-----------------------+
| **Situation**                 | **Return code** | **Message**           |
+-------------------------------+-----------------+-----------------------+
| all validations passed        | 0               | Validation Succeeded! |
+-------------------------------+-----------------+-----------------------+
| one or more validation failed | 1               | Validation Failed!    |
+-------------------------------+-----------------+-----------------------+


.. discourse::
    :topic_identifier: 226
