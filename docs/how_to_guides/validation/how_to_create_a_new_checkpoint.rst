.. _how_to_guides__validation__how_to_create_a_new_checkpoint:

How to create a new Checkpoint
==============================

This guide will help you create a new Checkpoint.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`
  - :ref:`Created an Expectation Suite <how_to_create_a_new_expectation_suite_using_the_cli>`

Steps
-----

1. First, run the CLI command below.

.. code-block:: bash

    great_expectations checkpoint new my_checkpoint my_suite

2. Next, you will be prompted to select some data.
3. You will then see a message that indicates the checkpoint has been added to your project.

.. code-block:: bash

    A checkpoint named `my_checkpoint` was added to your project!
    - To edit this checkpoint edit the checkpoint file: /home/ubuntu/my_project/great_expectations/checkpoints/my_checkpoint.yml
    - To run this checkpoint run `great_expectations checkpoint run my_checkpoint`
