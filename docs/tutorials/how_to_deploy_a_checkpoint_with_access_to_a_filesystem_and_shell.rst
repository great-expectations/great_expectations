####################################################
How to deploy a checkpoint via cron
####################################################

In this guide you'll learn how to deploy a `checkpoint <link here>_` using cron.

.. Important::

    This article assumes that in your deployment environment you have access to:
    
    - the filesystem
    - the shell
    - cron

If your project has a checkpoint named ``staging_loads`` and you wish to run it via shell simply run:

.. code-block:: bash

    $ great_expectations checkpoint run staging_loads
    Validation Succeeded!

This command will return posix status codes and print messages as follows:

+-------------------------------+-----------------+-----------------------+
| **Situation**                 | **Return code** | **Message**           |
+-------------------------------+-----------------+-----------------------+
| all validations passed        | 0               | Validation Succeeded! |
+-------------------------------+-----------------+-----------------------+
| one or more validation failed | 1               | Validation Failed!    |
+-------------------------------+-----------------+-----------------------+

To deploy this via cron 
========================

To prepare for editing the cron file you'll need the full path of the project's ``great_expectations`` directory and the full path to the ``great_expectations`` executable. Find this by running

.. code-block:: bash

    $ which great_expectations
    /full/path/to/your/environment/bin/great_expectations

Edit your cron schedule by running which will open your cron file in an editor.

.. code-block:: bash

    $ crontab -e

To run the checkpoint ``staging_loads`` every morning at 0300, add the following line in the text editor that opens:

.. code-block:: bash

    0  3  *  *  *    /full/path/to/your/environment/bin/great_expectations checkpoint run ratings --directory /full/path/to/my_project/great_expectations/

The five fields correspond to the minute, hour, day of the month, month and day of the week.

It is critical that you have full paths to both the ``great_expectations`` executable in your project's environment and the full path to the project's ``great_expectations/`` directory.

If you have not used cron before, we suggest searching for one of the many excellent cron references on the web.

