.. _how_to_guides__validation__how_to_deploy_a_scheduled_checkpoint_with_cron:

How to deploy a scheduled Checkpoint with cron
==============================================

This guide will help you deploy a scheduled Checkpoint with cron.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - You have :ref:`created a Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. First, verify that your Checkpoint is runnable via shell:

        .. code-block:: bash

            great_expectations checkpoint run my_checkpoint

        2. Next, to prepare for editing the cron file, you'll need the full path of the project's ``great_expectations`` directory.
        3. Next, get full path to the ``great_expectations`` executable by running:

        .. code-block:: bash

            which great_expectations
            /full/path/to/your/environment/bin/great_expectations

        4. Next, open the cron schedule in a text editor. On most operating systems, ``crontab -e`` will open your cron file in an editor.

        5. To run the Checkpoint ``my_checkpoint`` every morning at 0300, add the following line in the text editor that opens:

        .. code-block:: bash

            0  3  *  *  *    /full/path/to/your/environment/bin/great_expectations checkpoint run ratings --directory /full/path/to/my_project/great_expectations/

        6. Finally save the text file and exit the text editor.

        Additional notes
        ----------------

        The five fields correspond to the minute, hour, day of the month, month and day of the week.

        It is critical that you have full paths to both the ``great_expectations`` executable in your project's environment and the full path to the project's ``great_expectations/`` directory.

        If you have not used cron before, we suggest searching for one of the many excellent cron references on the web.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. First, verify that your Checkpoint is runnable via shell:

        .. code-block:: bash

            great_expectations --v3-api checkpoint run my_checkpoint

        2. Next, to prepare for editing the cron file, you'll need the full path of the project's ``great_expectations`` directory.
        3. Next, get full path to the ``great_expectations`` executable by running:

        .. code-block:: bash

            which great_expectations
            /full/path/to/your/environment/bin/great_expectations

        4. Next, open the cron schedule in a text editor. On most operating systems, ``crontab -e`` will open your cron file in an editor.

        5. To run the Checkpoint ``my_checkpoint`` every morning at 0300, add the following line in the text editor that opens:

        .. code-block:: bash

            0  3  *  *  *    /full/path/to/your/environment/bin/great_expectations --v3-api checkpoint run ratings --directory /full/path/to/my_project/great_expectations/

        6. Finally save the text file and exit the text editor.

        Additional notes
        ----------------

        The five fields correspond to the minute, hour, day of the month, month and day of the week.

        It is critical that you have full paths to both the ``great_expectations`` executable in your project's environment and the full path to the project's ``great_expectations/`` directory.

        If you have not used cron before, we suggest searching for one of the many excellent cron references on the web.
