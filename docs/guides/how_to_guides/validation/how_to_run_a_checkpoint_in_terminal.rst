.. _how_to_guides__validation__how_to_run_a_checkpoint_in_terminal:

How to run a Checkpoint in terminal
===================================

This guide will help you run a Checkpoint in a terminal.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - :ref:`Created a Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Docs for Legacy Checkpoints (<=0.13.7)

        1. Checkpoints can be run like applications from the command line by running:

        .. code-block:: bash

            great_expectations checkpoint run my_checkpoint
            Validation failed!

        2. Next, observe the output which will tell you if all validations passed or failed.

        Additional notes
        ----------------

        This command will return posix status codes and print messages as follows:

        +-------------------------------+-----------------+-----------------------+
        | **Situation**                 | **Return code** | **Message**           |
        +-------------------------------+-----------------+-----------------------+
        | all validations passed        | 0               | Validation succeeded! |
        +-------------------------------+-----------------+-----------------------+
        | one or more validation failed | 1               | Validation failed!    |
        +-------------------------------+-----------------+-----------------------+

    .. tab-container:: tab1
        :title: Docs for Class-Based Checkpoints (>=0.13.8)

        The CLI does not yet support the new-style Checkpoints. Please refer to :ref:`how_to_guides__validation__how_to_run_a_checkpoint_in_python` for a how-to guide on running a Checkpoint.

.. discourse::
    :topic_identifier: 226
